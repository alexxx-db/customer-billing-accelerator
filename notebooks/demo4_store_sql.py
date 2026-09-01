"""Demo #4 — store intelligence.

The weakest fit of the series and worth saying so: this accelerator is telco, so
there are no stores. Every store attribute here is synthetic, derived
deterministically from customer_id so the demo is reproducible.

What is *not* synthetic is the reconciliation itself. A POS sale is generated for
each activation order, then `detect_pos_reconciliation_gap` compares what the
store recorded against what was actually ordered and billed — the same class of
check a retailer runs nightly, on data the accelerator genuinely has.

Deliberately does NOT define a regional DSO function: Demo #3 already has
`lookup_dso_by_region`, and the tool-selection evaluation showed duplicate
capabilities make the model pick between near-identical tools.

Depends on Demo #1 (`ordm_customer`) for region and Demo #3 (`ordm_order_header`).
"""

N_STORES = 12


def ordm_store(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.ordm_store
CLUSTER BY (store_id)
COMMENT 'ORDM Store canonical core. Entirely synthetic — this accelerator is telco and has no store master. Stores are derived from the regions in ordm_customer so store and customer geography agree. Replace with the real store feed.'
AS
WITH regions AS (
  SELECT region,
         CAST(ROW_NUMBER() OVER (ORDER BY region) - 1 AS INT) AS region_idx,
         CAST(COUNT(*) OVER () AS INT)                        AS region_count
  FROM (SELECT DISTINCT region FROM {c}.{s}.ordm_customer WHERE region IS NOT NULL)
),
seq AS (SELECT CAST(id AS INT) AS n FROM RANGE({N_STORES}))
SELECT
  CONCAT('STR-', LPAD(CAST(seq.n + 1 AS STRING), 4, '0'))            AS store_id,
  CONCAT(r.region, ' Store ', CAST(seq.n + 1 AS STRING))             AS store_name,
  r.region,
  CASE PMOD(seq.n, 3) WHEN 0 THEN 'Flagship' WHEN 1 THEN 'Standard' ELSE 'Franchise' END
                                                                     AS store_type,
  CASE PMOD(seq.n, 4) WHEN 0 THEN 'Mall' WHEN 1 THEN 'HighStreet'
                      WHEN 2 THEN 'RetailPark' ELSE 'Airport' END    AS location_type,
  800 + PMOD(seq.n * 137, 2200)                                      AS floor_area_sqft,
  DATE_SUB(CURRENT_DATE(), 400 + PMOD(seq.n * 211, 2000))            AS opened_dt,
  'Active'                                                           AS store_status
FROM seq
JOIN regions r ON PMOD(seq.n, r.region_count) = r.region_idx
"""


def ordm_store_hierarchy(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.ordm_store_hierarchy
CLUSTER BY (store_id)
COMMENT 'ORDM Store hierarchy: store to district to region. The flagship store in each region is its district lead. Synthetic, like ordm_store.'
AS
WITH lead AS (
  SELECT region, MIN(store_id) AS district_lead_store_id
  FROM {c}.{s}.ordm_store GROUP BY region
)
SELECT
  st.store_id, st.store_name, st.region,
  CONCAT('DIST-', UPPER(SUBSTRING(st.region, 1, 3)))            AS district_id,
  l.district_lead_store_id,
  CASE WHEN st.store_id = l.district_lead_store_id THEN 'DistrictLead' ELSE 'Member' END
                                                                AS hierarchy_role,
  CASE WHEN st.store_id = l.district_lead_store_id THEN 0 ELSE 1 END AS hierarchy_level
FROM {c}.{s}.ordm_store st
JOIN lead l ON l.region = st.region
"""


def pos_transaction(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.pos_transaction
CLUSTER BY (store_id, transaction_date)
COMMENT 'Point-of-sale record of each activation sale, attributed to a store. The sale amount is the first month charge plus any bundled device, so it can be reconciled against the order. Roughly one sale in twelve is deliberately mis-keyed to a plan the order does not name — that is what detect_pos_reconciliation_gap exists to find.'
AS
WITH assigned AS (
  SELECT
    o.order_id, o.customer_id, o.order_date, o.product_id, o.order_total,
    o.term_months, o.order_channel,
    CONCAT('STR-', LPAD(CAST(PMOD(HASH(o.customer_id, 'store'), {N_STORES}) + 1 AS STRING), 4, '0'))
      AS store_id
  FROM {c}.{s}.ordm_order_header o
  WHERE o.order_channel = 'Retail'
)
SELECT
  CONCAT('POS-', LPAD(CAST(a.customer_id AS STRING), 10, '0'))  AS transaction_id,
  a.store_id,
  a.order_id,
  a.customer_id,
  a.order_date                                                  AS transaction_date,
  -- one sale in twelve is keyed to the wrong plan at the till
  CASE WHEN PMOD(HASH(a.customer_id, 'miskey'), 12) = 0
       THEN CONCAT(a.product_id, 'X') ELSE a.product_id END      AS pos_product_id,
  ROUND(a.order_total / a.term_months, 2)
    + CASE WHEN a.term_months >= 24 THEN 149.0 ELSE 0.0 END      AS pos_amount,
  CASE PMOD(HASH(a.customer_id, 'tender'), 3)
    WHEN 0 THEN 'Card' WHEN 1 THEN 'Cash' ELSE 'Finance'
  END                                                            AS tender_type,
  'Completed'                                                    AS transaction_status
FROM assigned a
"""


def store_billing_intelligence(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.store_billing_intelligence
CLUSTER BY (store_id)
COMMENT 'Gold: per-store rollup joining POS sales to what those customers were subsequently billed and disputed. This is the sheet a regional manager actually reads.'
AS
WITH sales AS (
  SELECT store_id,
         COUNT(*)                       AS activations,
         SUM(pos_amount)                AS pos_sales_total,
         MAX(transaction_date)          AS last_activation_dt
  FROM {c}.{s}.pos_transaction GROUP BY store_id
),
billed AS (
  SELECT t.store_id,
         SUM(CAST(i.total_charges AS DOUBLE)) AS billed_total,
         COUNT(DISTINCT i.customer_id)        AS billed_customers
  FROM {c}.{s}.pos_transaction t
  JOIN {c}.{s}.invoice i ON i.customer_id = t.customer_id
  GROUP BY t.store_id
),
disputed AS (
  SELECT t.store_id, COUNT(*) AS dispute_count
  FROM {c}.{s}.pos_transaction t
  JOIN {c}.{s}.billing_disputes d ON d.customer_id = t.customer_id
  GROUP BY t.store_id
)
SELECT
  st.store_id, st.store_name, st.region, st.store_type, st.location_type,
  st.floor_area_sqft,
  h.district_id, h.hierarchy_role,
  COALESCE(sa.activations, 0)                                    AS activations,
  ROUND(COALESCE(sa.pos_sales_total, 0.0), 2)                    AS pos_sales_total,
  sa.last_activation_dt,
  ROUND(COALESCE(b.billed_total, 0.0), 2)                        AS billed_total,
  COALESCE(b.billed_customers, 0)                                AS billed_customers,
  COALESCE(d.dispute_count, 0)                                   AS dispute_count,
  ROUND(COALESCE(d.dispute_count, 0) * 100.0
        / NULLIF(COALESCE(sa.activations, 0), 0), 2)             AS dispute_rate_pct,
  ROUND(COALESCE(b.billed_total, 0.0)
        / NULLIF(st.floor_area_sqft, 0), 2)                      AS billed_per_sqft,
  ROUND(COALESCE(b.billed_total, 0.0)
        / NULLIF(COALESCE(sa.activations, 0), 0), 2)             AS billed_per_activation
FROM {c}.{s}.ordm_store st
LEFT JOIN {c}.{s}.ordm_store_hierarchy h ON h.store_id = st.store_id
LEFT JOIN sales sa ON sa.store_id = st.store_id
LEFT JOIN billed b ON b.store_id = st.store_id
LEFT JOIN disputed d ON d.store_id = st.store_id
"""


def fn_compare_stores_by_region(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.compare_stores_by_region(
  input_region STRING COMMENT 'Region name, or an empty string for every region'
)
RETURNS TABLE (
  region STRING, store_id STRING, store_name STRING, store_type STRING,
  activations BIGINT, pos_sales_total DOUBLE, billed_total DOUBLE,
  billed_per_activation DOUBLE, billed_per_sqft DOUBLE,
  dispute_count BIGINT, dispute_rate_pct DOUBLE, rank_in_region INT
)
COMMENT 'Stores in a region ranked by revenue billed, with activation volume, revenue per square foot and dispute rate. Use to compare store performance or find the weakest store in a region. Capped at 50 rows.'
RETURN
SELECT
  region, store_id, store_name, store_type,
  activations, pos_sales_total, billed_total,
  billed_per_activation, billed_per_sqft, dispute_count, dispute_rate_pct,
  CAST(RANK() OVER (PARTITION BY region ORDER BY billed_total DESC) AS INT) AS rank_in_region
FROM {c}.{s}.store_billing_intelligence
WHERE input_region = '' OR region = input_region
ORDER BY region, billed_total DESC
LIMIT 50
"""


def fn_lookup_store_profile(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_store_profile(
  input_store STRING COMMENT 'Store ID, e.g. STR-0003'
)
RETURNS TABLE (
  store_id STRING, store_name STRING, region STRING, district_id STRING,
  store_type STRING, location_type STRING, floor_area_sqft INT,
  activations BIGINT, pos_sales_total DOUBLE, billed_total DOUBLE,
  billed_per_sqft DOUBLE, dispute_count BIGINT, dispute_rate_pct DOUBLE,
  last_activation_dt DATE
)
COMMENT 'One store: its attributes plus activation volume, revenue and dispute rate. Use when asked about a specific store. Returns at most one row.'
RETURN
SELECT
  store_id, store_name, region, district_id, store_type, location_type,
  floor_area_sqft, activations, pos_sales_total, billed_total,
  billed_per_sqft, dispute_count, dispute_rate_pct, last_activation_dt
FROM {c}.{s}.store_billing_intelligence
WHERE store_id = input_store OR store_name = input_store
LIMIT 1
"""


def fn_lookup_store_hierarchy(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_store_hierarchy(
  input_store STRING COMMENT 'Store ID whose district should be returned'
)
RETURNS TABLE (
  store_id STRING, store_name STRING, region STRING, district_id STRING,
  hierarchy_role STRING, billed_total DOUBLE, dispute_rate_pct DOUBLE,
  is_requested_store BOOLEAN
)
COMMENT 'Every store in the requested store district, ranked by revenue, with the requested store flagged. Use for questions about a district, a store group, or which store in a district is underperforming. Capped at 50 rows.'
RETURN
SELECT
  b.store_id, b.store_name, b.region, b.district_id, h.hierarchy_role,
  b.billed_total, b.dispute_rate_pct,
  b.store_id = input_store AS is_requested_store
FROM {c}.{s}.store_billing_intelligence b
JOIN {c}.{s}.ordm_store_hierarchy h ON h.store_id = b.store_id
WHERE b.district_id = (
  SELECT district_id FROM {c}.{s}.store_billing_intelligence
  WHERE store_id = input_store LIMIT 1
)
ORDER BY h.hierarchy_level, b.billed_total DESC
LIMIT 50
"""


def fn_detect_pos_reconciliation_gap(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.detect_pos_reconciliation_gap(
  input_store STRING COMMENT 'Store ID, or an empty string to scan every store'
)
RETURNS TABLE (
  store_id STRING, store_name STRING, transaction_id STRING, order_id STRING,
  customer_id BIGINT, transaction_date DATE, pos_product_id STRING,
  order_product_id STRING, pos_amount DOUBLE, expected_amount DOUBLE,
  variance DOUBLE, gap_type STRING, explanation STRING
)
COMMENT 'Point-of-sale records that disagree with the order they claim to represent: wrong plan keyed at the till, wrong amount taken, or a sale with no matching order. Use for daily store reconciliation or when a store total does not tie out. Capped at 100 rows. No PII.'
RETURN
SELECT
  t.store_id, st.store_name, t.transaction_id, t.order_id, t.customer_id,
  t.transaction_date, t.pos_product_id,
  o.product_id                                                     AS order_product_id,
  t.pos_amount,
  ROUND(o.order_total / o.term_months
        + CASE WHEN o.term_months >= 24 THEN 149.0 ELSE 0.0 END, 2) AS expected_amount,
  ROUND(t.pos_amount - (o.order_total / o.term_months
        + CASE WHEN o.term_months >= 24 THEN 149.0 ELSE 0.0 END), 2) AS variance,
  CASE
    WHEN o.order_id IS NULL                     THEN 'OrphanSale'
    WHEN t.pos_product_id <> o.product_id       THEN 'PlanMiskeyed'
    ELSE 'AmountVariance'
  END AS gap_type,
  CASE
    WHEN o.order_id IS NULL THEN
      CONCAT('POS sale ', t.transaction_id, ' at ', st.store_name,
             ' has no matching order. $', CAST(t.pos_amount AS STRING), ' taken and unattributed.')
    WHEN t.pos_product_id <> o.product_id THEN
      CONCAT('Till keyed plan "', t.pos_product_id, '" but order ', t.order_id,
             ' is for "', o.product_id, '". The customer will be billed on the order plan, ',
             'so the store sale and the invoice will not tie out.')
    ELSE
      CONCAT('POS took $', CAST(t.pos_amount AS STRING), ' against an expected $',
             CAST(ROUND(o.order_total / o.term_months
                  + CASE WHEN o.term_months >= 24 THEN 149.0 ELSE 0.0 END, 2) AS STRING), '.')
  END AS explanation
FROM {c}.{s}.pos_transaction t
LEFT JOIN {c}.{s}.ordm_order_header o ON o.order_id = t.order_id
LEFT JOIN {c}.{s}.ordm_store st ON st.store_id = t.store_id
WHERE (input_store = '' OR t.store_id = input_store)
  AND (o.order_id IS NULL
       OR t.pos_product_id <> o.product_id
       OR ABS(t.pos_amount - (o.order_total / o.term_months
              + CASE WHEN o.term_months >= 24 THEN 149.0 ELSE 0.0 END)) > 0.01)
ORDER BY ABS(COALESCE(t.pos_amount, 0)) DESC
LIMIT 100
"""


ALL_TABLES = [ordm_store, ordm_store_hierarchy, pos_transaction, store_billing_intelligence]
ALL_FUNCTIONS = [fn_compare_stores_by_region, fn_lookup_store_profile,
                 fn_lookup_store_hierarchy, fn_detect_pos_reconciliation_gap]
