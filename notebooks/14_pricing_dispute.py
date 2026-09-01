# Databricks notebook source
# MAGIC %md
# MAGIC # 💵 Pricing Dispute Resolution — ORDM Product Core
# MAGIC
# MAGIC Demo #2. Answers the question a billing agent gets more than any other:
# MAGIC *"I was charged $299 for Pro but my contract says $199."*
# MAGIC
# MAGIC Projects the accelerator's `billing_plans` onto the
# MAGIC [Open Retail Data Model](https://github.com/databricks-industry-solutions/open-retail-data-model)
# MAGIC **Product** canonical core, adds the two things a price dispute actually turns on —
# MAGIC **effective-dated pricing** and **negotiated contract rates** — and reconciles both
# MAGIC against what was really billed.
# MAGIC
# MAGIC ### What this notebook creates
# MAGIC
# MAGIC | Object | Layer | Purpose |
# MAGIC |--------|-------|---------|
# MAGIC | `ordm_product` | Canonical core | ORDM Product attributes, projected from `billing_plans`. |
# MAGIC | `ordm_product_pricing` | Canonical core | Effective-dated price book. Two versions per product. |
# MAGIC | `ordm_customer_contract` | Canonical core | Contract terms and negotiated rates. |
# MAGIC | `detect_pricing_drift` | UC function | **The flagship.** Billed vs price book vs contract, in one call. |
# MAGIC | `lookup_pricing_history` | UC function | When did this price change, and what was it before? |
# MAGIC | `lookup_product_pricing` | UC function | What did this plan cost on a given date? |
# MAGIC | `lookup_customer_contract` | UC function | What does this customer's contract say? |
# MAGIC
# MAGIC ### Drift types `detect_pricing_drift` distinguishes
# MAGIC
# MAGIC | Type | Meaning |
# MAGIC |------|---------|
# MAGIC | `ContractMismatch` | Billed above the negotiated contract rate. The headline case. |
# MAGIC | `PriceBookMismatch` | Billed at a price that was not effective for that month. |
# MAGIC | `PlanDeprecated` | Charge is correct, but the plan is retired and should be migrated. |
# MAGIC | `PlanNotInCatalog` | The billed plan is not in the product catalogue at all. |
# MAGIC | `RuleMissing` | The price book has a gap for that period. |
# MAGIC | `NoDrift` | Charge matches. |
# MAGIC
# MAGIC ### Writes
# MAGIC
# MAGIC Two write actions live in `write_actions.py`, not here — `submit_pricing_dispute`
# MAGIC and `apply_pricing_correction`. The correction updates `billing_disputes` **and**
# MAGIC `invoice` inside one `BEGIN ATOMIC` block: a dispute cannot be marked corrected
# MAGIC unless the invoice is corrected in the same transaction. Run `09_writeback_setup`
# MAGIC first so catalog commits are enabled on `invoice`.
# MAGIC
# MAGIC ### Synthetic attributes
# MAGIC
# MAGIC `lifecycle_status`, the price history, and the negotiated rates are derived
# MAGIC deterministically from stable hashes so the demo is reproducible. Each is labelled
# MAGIC below. Replace them with your real product and contract feeds.
# MAGIC
# MAGIC **Prerequisites:** `000-config` → `00_data_preparation` → `09_writeback_setup`.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Set Working Catalog and Schema
CATALOG = config['catalog']
SCHEMA = config['database']

FQ = lambda t: f"{CATALOG}.{SCHEMA}.{t}"
print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Helper — Enable Predictive Optimization
def enable_predictive_optimization(table: str) -> None:
    """Hand OPTIMIZE/VACUUM/ANALYZE to the platform. Not fatal if the account
    does not permit the override — the table is still correct, just not
    auto-maintained."""
    try:
        spark.sql(f"ALTER TABLE {table} ENABLE PREDICTIVE OPTIMIZATION")
        print(f"predictive optimization enabled: {table}")
    except Exception as e:
        print(f"predictive optimization NOT enabled for {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ORDM Canonical Core — `ordm_product`
# MAGIC
# MAGIC Projects `billing_plans` onto the ORDM Product domain. `lifecycle_status` is
# MAGIC **synthetic**, derived from a stable hash so the catalogue contains a retired plan
# MAGIC and the `PlanDeprecated` case is reachable.

# COMMAND ----------

# DBTITLE 1,Create ordm_product
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_product
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
FROM {CATALOG}.{SCHEMA}.billing_plans
""")

enable_predictive_optimization(FQ("ordm_product"))
display(spark.sql(f"SELECT product_id, product_name, product_category, contract_term_months, list_price_current, lifecycle_status FROM {FQ(chr(34) + table + chr(34))} ORDER BY product_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ORDM Canonical Core — `ordm_product_pricing`
# MAGIC
# MAGIC The effective-dated price book — the piece the accelerator was missing and the
# MAGIC reason a drift question can be answered at all. Two versions per product: a legacy
# MAGIC price and the current one, with the change landing 6–12 months ago depending on the
# MAGIC plan so not everything moves at once.
# MAGIC
# MAGIC Entirely **synthetic** — replace with the real price book feed.

# COMMAND ----------

# DBTITLE 1,Create ordm_product_pricing
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_product_pricing
CLUSTER BY (product_id, effective_from)
COMMENT 'ORDM effective-dated price book. Two versions per product: a legacy price and the current one. Synthetic history — replace with the real price book feed.'
AS
WITH base AS (
  SELECT product_id, list_price_current,
         6 + PMOD(HASH(product_id, 'pricechange'), 7) AS change_months_ago
  FROM {CATALOG}.{SCHEMA}.ordm_product
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
""")

enable_predictive_optimization(FQ("ordm_product_pricing"))
display(spark.sql(f"SELECT product_id, price_book_version, list_price, effective_from, effective_to, is_current FROM {FQ(chr(34) + table + chr(34))} ORDER BY product_id, effective_from"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ORDM Canonical Core — `ordm_customer_contract`
# MAGIC
# MAGIC Contract terms and, for roughly one customer in three, a negotiated rate below the
# MAGIC price book. That negotiated rate is what produces `ContractMismatch` — the charge is
# MAGIC right against the list price and wrong against the contract.
# MAGIC
# MAGIC **Synthetic** — replace with the real contract feed.

# COMMAND ----------

# DBTITLE 1,Create ordm_customer_contract
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_customer_contract
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
FROM {CATALOG}.{SCHEMA}.customers c
JOIN {CATALOG}.{SCHEMA}.ordm_product p ON c.plan = p.product_key
""")

enable_predictive_optimization(FQ("ordm_customer_contract"))
display(spark.sql(f"SELECT contract_id, customer_id, product_id, contract_status, negotiated_monthly_price FROM {FQ(chr(34) + table + chr(34))} WHERE negotiated_monthly_price IS NOT NULL ORDER BY customer_id LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. UC Function — `detect_pricing_drift`
# MAGIC
# MAGIC The flagship. One bounded row that reconciles what was billed against the price
# MAGIC effective that month and any negotiated contract rate, and says in plain language
# MAGIC which of those the charge disagrees with.

# COMMAND ----------

# DBTITLE 1,Create detect_pricing_drift
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('detect_pricing_drift')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.detect_pricing_drift(
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
  FROM {CATALOG}.{SCHEMA}.invoice i
  WHERE i.customer_id = TRY_CAST(input_customer AS DECIMAL)
    AND i.event_month = input_event_month
  LIMIT 1
),
prod AS (
  SELECT inv.*, p.product_id, p.lifecycle_status
  FROM inv
  LEFT JOIN {CATALOG}.{SCHEMA}.ordm_product p ON p.product_name = inv.plan_name
),
priced AS (
  SELECT prod.*, pr.list_price AS price_book_price
  FROM prod
  LEFT JOIN {CATALOG}.{SCHEMA}.ordm_product_pricing pr
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
  LEFT JOIN {CATALOG}.{SCHEMA}.ordm_customer_contract ct
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
""")
print(f"created {FQ('detect_pricing_drift')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. UC Function — `lookup_pricing_history`
# MAGIC
# MAGIC Answers *"when did this price change?"* straight from the effective-dated price book.

# COMMAND ----------

# DBTITLE 1,Create lookup_pricing_history
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_pricing_history')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_pricing_history(
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
FROM {CATALOG}.{SCHEMA}.ordm_product_pricing pr
JOIN {CATALOG}.{SCHEMA}.ordm_product p ON p.product_id = pr.product_id
WHERE p.product_name = input_plan OR pr.product_id = input_plan
ORDER BY pr.effective_from DESC
LIMIT 50
""")
print(f"created {FQ('lookup_pricing_history')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. UC Function — `lookup_product_pricing`
# MAGIC
# MAGIC Answers *"what did this plan cost on this date?"* — the as-of lookup the drift
# MAGIC function uses internally, exposed so the agent can answer it directly.

# COMMAND ----------

# DBTITLE 1,Create lookup_product_pricing
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_product_pricing')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_product_pricing(
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
FROM {CATALOG}.{SCHEMA}.ordm_product p
JOIN {CATALOG}.{SCHEMA}.ordm_product_pricing pr ON pr.product_id = p.product_id
CROSS JOIN asof
WHERE (p.product_name = input_plan OR p.product_id = input_plan)
  AND pr.effective_from <= asof.d
  AND (pr.effective_to IS NULL OR pr.effective_to >= asof.d)
ORDER BY pr.effective_from DESC
LIMIT 1
""")
print(f"created {FQ('lookup_product_pricing')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. UC Function — `lookup_customer_contract`
# MAGIC
# MAGIC Answers *"what does my contract say?"*, including the discount against the current
# MAGIC price book. No PII.

# COMMAND ----------

# DBTITLE 1,Create lookup_customer_contract
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_customer_contract')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_customer_contract(
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
FROM {CATALOG}.{SCHEMA}.ordm_customer_contract ct
JOIN {CATALOG}.{SCHEMA}.ordm_product p ON p.product_id = ct.product_id
WHERE ct.customer_id = TRY_CAST(input_customer AS DECIMAL)
LIMIT 1
""")
print(f"created {FQ('lookup_customer_contract')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validation
# MAGIC
# MAGIC Asserts the invariants that matter for an agent tool: the price book has no
# MAGIC overlapping effective periods, results are bounded, an unknown customer returns
# MAGIC empty rather than erroring, and no PII can reach the agent through any signature.

# COMMAND ----------

# DBTITLE 1,Validate the price book
overlaps = spark.sql(f"""
  SELECT COUNT(*) AS n
  FROM {FQ('ordm_product_pricing')} a
  JOIN {FQ('ordm_product_pricing')} b
    ON a.product_id = b.product_id AND a.pricing_rule_id <> b.pricing_rule_id
  WHERE a.effective_to >= b.effective_from AND a.effective_from <= b.effective_from
""").first()["n"]
assert overlaps == 0, f"{overlaps} overlapping effective periods — as-of pricing would be ambiguous"

current = spark.sql(f"""
  SELECT COUNT(*) AS versions, COUNT(DISTINCT product_id) AS products
  FROM {FQ('ordm_product_pricing')} WHERE effective_to IS NULL
""").first()
assert current["versions"] == current["products"], "each product needs exactly one current price"
print(f"price book clean: no overlaps, {current['products']} products each with one current version")

# COMMAND ----------

# DBTITLE 1,Validate detect_pricing_drift
sample = spark.sql(f"""
    SELECT i.customer_id, i.event_month
    FROM {FQ('invoice')} i
    ORDER BY i.event_month DESC, i.customer_id
    LIMIT 40
""").collect()
assert sample, "No invoices found — run 00_data_preparation first."

seen = {}
for row in sample:
    d = spark.sql(
        f"SELECT * FROM {FQ('detect_pricing_drift')}"
        f"('{row['customer_id']}', '{row['event_month']}')"
    ).collect()
    assert len(d) <= 1, "detect_pricing_drift must return at most one row"
    if d:
        seen.setdefault(d[0]["drift_type"], d[0])

print(f"drift types reachable across {len(sample)} sampled invoices:\n")
for drift_type, row in sorted(seen.items()):
    print(f"  {drift_type:<20} customer {row['customer_id']} {row['event_month']}  variance {row['variance']}")
    print(f"    {row['explanation']}\n")

assert "NoDrift" in seen, "expected at least one correctly-billed invoice"
assert any(k != "NoDrift" for k in seen), (
    "no drift of any kind was detected — check that ordm_product_pricing and "
    "ordm_customer_contract were built from the same plans the invoices reference"
)

# COMMAND ----------

# DBTITLE 1,Validate the supporting lookups
unknown = spark.sql(f"SELECT * FROM {FQ('detect_pricing_drift')}('not-a-customer', '1999-01')")
assert unknown.count() == 0, "unknown customer must return zero rows, not error"

a_plan = spark.sql(f"SELECT product_name FROM {FQ('ordm_product')} LIMIT 1").first()["product_name"]

history = spark.sql(f"SELECT * FROM {FQ('lookup_pricing_history')}('{a_plan}')")
assert history.count() >= 2, "expected a price change in the history"
assert history.count() <= 50, "pricing history must stay bounded"

today = spark.sql(f"SELECT * FROM {FQ('lookup_product_pricing')}('{a_plan}', '')")
assert today.count() == 1, "as-of pricing must return exactly one row"

legacy_from = spark.sql(f"""
    SELECT DATE_FORMAT(DATE_ADD(effective_from, 30), 'yyyy-MM-dd') AS d
    FROM {FQ('ordm_product_pricing')} pr
    JOIN {FQ('ordm_product')} p USING (product_id)
    WHERE p.product_name = '{a_plan}' AND pr.is_current = FALSE
""").first()["d"]
was = spark.sql(f"SELECT list_price FROM {FQ('lookup_product_pricing')}('{a_plan}', '{legacy_from}')").first()
now = today.first()
assert was["list_price"] != now["list_price"], "as-of pricing is not honouring effective dates"
print(f"{a_plan}: was ${was['list_price']} on {legacy_from}, now ${now['list_price']}")

# COMMAND ----------

# DBTITLE 1,No PII may reach the agent
PII_COLUMNS = {"customer_name", "email", "phone_number"}
a_customer = spark.sql(f"SELECT customer_id FROM {FQ('ordm_customer_contract')} LIMIT 1").first()["customer_id"]

for call in [
    f"detect_pricing_drift('{a_customer}', '1999-01')",
    f"lookup_customer_contract('{a_customer}')",
    f"lookup_pricing_history('{a_plan}')",
    f"lookup_product_pricing('{a_plan}', '')",
]:
    cols = {c.lower() for c in spark.sql(f"SELECT * FROM {FQ(call.split('(')[0])}"
                                        f"({call.split('(', 1)[1]}").columns}
    leaked = PII_COLUMNS & cols
    assert not leaked, f"{call} leaks PII columns: {leaked}"

print("no PII in any function signature")
print("\nAll checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Grants and next steps
# MAGIC
# MAGIC The agent service principal needs `EXECUTE` on the four functions and must **not**
# MAGIC hold `SELECT` on the ORDM tables — definer rights are what keep them closed.
# MAGIC
# MAGIC ```sql
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.detect_pricing_drift     TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_pricing_history   TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_product_pricing   TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_customer_contract TO `<agent-sp>`;
# MAGIC ```
# MAGIC
# MAGIC Then re-run `03_agent_deployment_and_evaluation` to pick up the four tools and redeploy.
# MAGIC
# MAGIC ### Demo script
# MAGIC
# MAGIC | Ask | What the agent should do |
# MAGIC |-----|--------------------------|
# MAGIC | *"I was charged more than my contract says."* | `detect_pricing_drift` → reads back the explanation verbatim |
# MAGIC | *"When did the price for my plan change?"* | `lookup_pricing_history` |
# MAGIC | *"What did this plan cost last year?"* | `lookup_product_pricing` with an as-of date |
# MAGIC | *"What rate did I negotiate?"* | `lookup_customer_contract` |
# MAGIC | *"Open a dispute for it."* | `request_write_confirmation` with action `submit_pricing_dispute`, passing `event_month` and `disputed_amount` in `extra` |
# MAGIC | *"Approve it and fix my bill."* | `apply_pricing_correction` — updates the dispute **and** the invoice in one transaction |
# MAGIC
# MAGIC ### Staging a correction
# MAGIC
# MAGIC ```python
# MAGIC request_write_confirmation(
# MAGIC     action="apply_pricing_correction",
# MAGIC     target_id="DSP-1a2b3c4d",       # the dispute
# MAGIC     customer_id="4401",
# MAGIC     reason="Contract rate applied",
# MAGIC     extra='{"event_month": "2026-07", "corrected_amount": 199.0}',
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Keys the action does not declare are dropped, and numeric parameters are rejected
# MAGIC before binding if they are not numeric.
