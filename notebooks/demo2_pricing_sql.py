"""The Demo #2 DDL and UC functions, as one place so both the live test and the
notebook use identical SQL."""

def ordm_product(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.ordm_product
CLUSTER BY (product_id)
COMMENT 'ORDM Product canonical core, projected from billing_plans. lifecycle_status is synthetic — derived from a stable hash so a deprecated plan exists for the drift demo.'
AS
SELECT
  Plan_id                                     AS product_id,
  Plan_key                                    AS product_key,
  Plan_name                                   AS product_name,
  CASE WHEN UPPER(Plan_name) LIKE 'UNLIMITED%' THEN 'Unlimited' ELSE 'Metered' END
                                              AS product_category,
  contract_in_months                          AS contract_term_months,
  CAST(monthly_charges_dollars AS DOUBLE)     AS list_price_current,
  CASE WHEN PMOD(HASH(Plan_id, 'lifecycle'), 5) = 0 THEN 'Deprecated' ELSE 'Active' END
                                              AS lifecycle_status
FROM {c}.{s}.billing_plans
"""

def ordm_product_pricing(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.ordm_product_pricing
CLUSTER BY (product_id, effective_from)
COMMENT 'ORDM effective-dated price book. Two versions per product: a legacy price and the current one. Synthetic history — replace with the real price book feed.'
AS
WITH base AS (
  SELECT product_id, list_price_current,
         6 + PMOD(HASH(product_id, 'pricechange'), 7) AS change_months_ago
  FROM {c}.{s}.ordm_product
)
SELECT
  CONCAT(product_id, '-V1')                                   AS pricing_rule_id,
  product_id,
  ROUND(list_price_current * 0.85, 2)                         AS list_price,
  'USD'                                                       AS currency,
  ADD_MONTHS(CURRENT_DATE(), -24)                             AS effective_from,
  DATE_SUB(ADD_MONTHS(CURRENT_DATE(), -change_months_ago), 1) AS effective_to,
  'PB-LEGACY'                                                 AS price_book_version,
  FALSE                                                       AS is_current
FROM base
UNION ALL
SELECT
  CONCAT(product_id, '-V2'),
  product_id,
  list_price_current,
  'USD',
  ADD_MONTHS(CURRENT_DATE(), -change_months_ago),
  CAST(NULL AS DATE),
  'PB-CURRENT',
  TRUE
FROM base
"""

def ordm_customer_contract(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.ordm_customer_contract
CLUSTER BY (customer_id)
COMMENT 'ORDM customer contracts with negotiated rates. Synthetic: roughly one customer in three has a negotiated price below the price book, which is what produces ContractMismatch drift.'
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
AS
SELECT
  CONCAT('CTR-', LPAD(CAST(c.customer_id AS STRING), 8, '0'))          AS contract_id,
  c.customer_id,
  p.product_id,
  c.contract_start_dt,
  ADD_MONTHS(c.contract_start_dt, p.contract_term_months)              AS contract_end_dt,
  CASE WHEN PMOD(HASH(c.customer_id, 'contract'), 3) = 0
       THEN ROUND(p.list_price_current * 0.80, 2)
       ELSE CAST(NULL AS DOUBLE) END                                   AS negotiated_monthly_price,
  CASE WHEN ADD_MONTHS(c.contract_start_dt, p.contract_term_months) >= CURRENT_DATE()
       THEN 'Active' ELSE 'Expired' END                                AS contract_status
FROM {c}.{s}.customers c
JOIN {c}.{s}.ordm_product p ON c.plan = p.product_key
"""

def fn_detect_pricing_drift(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.detect_pricing_drift(
  input_customer STRING COMMENT 'Customer ID whose invoice should be checked',
  input_event_month STRING COMMENT 'Billing month to check, formatted yyyy-MM'
)
RETURNS TABLE (
  customer_id           BIGINT,
  event_month           STRING,
  plan_name             STRING,
  product_id            STRING,
  lifecycle_status      STRING,
  billed_monthly_charge DOUBLE,
  contract_price        DOUBLE,
  price_book_price      DOUBLE,
  expected_charge       DOUBLE,
  variance              DOUBLE,
  drift_detected        BOOLEAN,
  drift_type            STRING,
  explanation           STRING
)
COMMENT 'Compares what a customer was actually billed for a month against the price effective for their plan that month and any negotiated contract rate. Returns at most one row. Use this first for any question about a charge being wrong, a price change, or a contract rate. No PII.'
RETURN
WITH inv AS (
  SELECT
    i.customer_id, i.event_month, i.plan_name,
    CAST(i.monthly_charges AS DOUBLE)          AS billed_monthly_charge,
    TO_DATE(CONCAT(i.event_month, '-01'))      AS month_start
  FROM {c}.{s}.invoice i
  WHERE i.customer_id = TRY_CAST(input_customer AS DECIMAL)
    AND i.event_month = input_event_month
  LIMIT 1
),
prod AS (
  SELECT inv.*, p.product_id, p.lifecycle_status
  FROM inv
  LEFT JOIN {c}.{s}.ordm_product p ON p.product_name = inv.plan_name
),
priced AS (
  SELECT prod.*, pr.list_price AS price_book_price
  FROM prod
  LEFT JOIN {c}.{s}.ordm_product_pricing pr
         ON pr.product_id = prod.product_id
        AND pr.effective_from <= LAST_DAY(prod.month_start)
        AND (pr.effective_to IS NULL OR pr.effective_to >= prod.month_start)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY prod.customer_id, prod.event_month ORDER BY pr.effective_from DESC
  ) = 1
),
joined AS (
  SELECT priced.*, ct.negotiated_monthly_price AS contract_price
  FROM priced
  LEFT JOIN {c}.{s}.ordm_customer_contract ct
         ON ct.customer_id = priced.customer_id
        AND ct.product_id  = priced.product_id
),
scored AS (
  SELECT
    joined.*,
    COALESCE(contract_price, price_book_price) AS expected_charge,
    ROUND(billed_monthly_charge - COALESCE(contract_price, price_book_price), 2) AS variance
  FROM joined
)
SELECT
  customer_id, event_month, plan_name, product_id, lifecycle_status,
  billed_monthly_charge, contract_price, price_book_price, expected_charge, variance,
  CASE WHEN product_id IS NULL OR expected_charge IS NULL THEN TRUE
       WHEN ABS(variance) > 0.01 THEN TRUE
       ELSE FALSE END AS drift_detected,
  CASE
    WHEN product_id IS NULL                        THEN 'PlanNotInCatalog'
    WHEN expected_charge IS NULL                   THEN 'RuleMissing'
    WHEN ABS(variance) > 0.01
     AND contract_price IS NOT NULL                THEN 'ContractMismatch'
    WHEN ABS(variance) > 0.01                      THEN 'PriceBookMismatch'
    WHEN lifecycle_status = 'Deprecated'           THEN 'PlanDeprecated'
    ELSE 'NoDrift'
  END AS drift_type,
  CASE
    WHEN product_id IS NULL THEN
      CONCAT('Plan "', plan_name, '" billed on ', event_month,
             ' is not in the product catalogue, so no price can be verified.')
    WHEN expected_charge IS NULL THEN
      CONCAT('No price was effective for ', plan_name, ' in ', event_month,
             '. The price book has a gap for this period.')
    WHEN ABS(variance) > 0.01 AND contract_price IS NOT NULL THEN
      CONCAT('Billed $', CAST(billed_monthly_charge AS STRING), ' for ', event_month,
             ' but the negotiated contract rate is $', CAST(contract_price AS STRING),
             ' (price book: $', CAST(price_book_price AS STRING), '). Overcharged by $',
             CAST(variance AS STRING), '.')
    WHEN ABS(variance) > 0.01 THEN
      CONCAT('Billed $', CAST(billed_monthly_charge AS STRING), ' for ', event_month,
             ' but the price effective that month was $', CAST(price_book_price AS STRING),
             '. Variance $', CAST(variance AS STRING), '.')
    WHEN lifecycle_status = 'Deprecated' THEN
      CONCAT('Charge matches the price book, but plan "', plan_name,
             '" is deprecated and should be migrated.')
    ELSE
      CONCAT('Charge of $', CAST(billed_monthly_charge AS STRING), ' for ', event_month,
             ' matches the effective price. No drift.')
  END AS explanation
FROM scored
"""

def fn_lookup_pricing_history(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_pricing_history(
  input_plan STRING COMMENT 'Plan name or product id, e.g. "100GB SIM12" or "PLAN003"'
)
RETURNS TABLE (
  product_id STRING, product_name STRING, price_book_version STRING,
  list_price DOUBLE, currency STRING, effective_from DATE, effective_to DATE,
  is_current BOOLEAN
)
COMMENT 'Full effective-dated price history for a plan, newest first. Use to answer when a price changed and what it was before. Capped at 50 rows.'
RETURN
SELECT
  pr.product_id, p.product_name, pr.price_book_version,
  pr.list_price, pr.currency, pr.effective_from, pr.effective_to, pr.is_current
FROM {c}.{s}.ordm_product_pricing pr
JOIN {c}.{s}.ordm_product p ON p.product_id = pr.product_id
WHERE p.product_name = input_plan OR pr.product_id = input_plan
ORDER BY pr.effective_from DESC
LIMIT 50
"""

def fn_lookup_product_pricing(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_product_pricing(
  input_plan STRING COMMENT 'Plan name or product id',
  input_as_of STRING COMMENT 'Date to price as of, yyyy-MM-dd. Empty string means today.'
)
RETURNS TABLE (
  product_id STRING, product_name STRING, product_category STRING,
  lifecycle_status STRING, list_price DOUBLE, currency STRING,
  price_book_version STRING, effective_from DATE, effective_to DATE, as_of DATE
)
COMMENT 'The price in effect for a plan on a given date. Use when asked what a plan costs or cost at some point in time. Returns at most one row.'
RETURN
WITH asof AS (
  SELECT COALESCE(TRY_CAST(NULLIF(input_as_of, '') AS DATE), CURRENT_DATE()) AS d
)
SELECT
  p.product_id, p.product_name, p.product_category, p.lifecycle_status,
  pr.list_price, pr.currency, pr.price_book_version,
  pr.effective_from, pr.effective_to, asof.d AS as_of
FROM {c}.{s}.ordm_product p
JOIN {c}.{s}.ordm_product_pricing pr ON pr.product_id = p.product_id
CROSS JOIN asof
WHERE (p.product_name = input_plan OR p.product_id = input_plan)
  AND pr.effective_from <= asof.d
  AND (pr.effective_to IS NULL OR pr.effective_to >= asof.d)
ORDER BY pr.effective_from DESC
LIMIT 1
"""

def fn_lookup_customer_contract(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_customer_contract(
  input_customer STRING COMMENT 'Customer ID'
)
RETURNS TABLE (
  customer_id BIGINT, contract_id STRING, product_id STRING, product_name STRING,
  contract_start_dt DATE, contract_end_dt DATE, contract_status STRING,
  negotiated_monthly_price DOUBLE, price_book_price DOUBLE, discount_vs_price_book DOUBLE
)
COMMENT 'A customer contract: term dates, status, and any negotiated rate against the current price book. Use when a customer refers to what their contract says. Returns at most one row. No PII.'
RETURN
SELECT
  ct.customer_id, ct.contract_id, ct.product_id, p.product_name,
  ct.contract_start_dt, ct.contract_end_dt, ct.contract_status,
  ct.negotiated_monthly_price,
  p.list_price_current AS price_book_price,
  ROUND(p.list_price_current - COALESCE(ct.negotiated_monthly_price, p.list_price_current), 2)
    AS discount_vs_price_book
FROM {c}.{s}.ordm_customer_contract ct
JOIN {c}.{s}.ordm_product p ON p.product_id = ct.product_id
WHERE ct.customer_id = TRY_CAST(input_customer AS DECIMAL)
LIMIT 1
"""

ALL_TABLES = [ordm_product, ordm_product_pricing, ordm_customer_contract]
ALL_FUNCTIONS = [fn_detect_pricing_drift, fn_lookup_pricing_history,
                 fn_lookup_product_pricing, fn_lookup_customer_contract]
