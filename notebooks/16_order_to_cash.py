# Databricks notebook source
# MAGIC %md
# MAGIC # 🧾 Order-to-Cash Reconciliation — ORDM Order Core
# MAGIC
# MAGIC Demo #3. Traces an order from placed, through fulfilled and billed, to collected —
# MAGIC and finds where it stalls.
# MAGIC
# MAGIC The accelerator has no order management system, but it has everything an order
# MAGIC *is* in telco. `customers.contract_start_dt` is the activation order. The first
# MAGIC `billing_items` event for a device is the fulfilment — a SIM that has never carried
# MAGIC traffic was never fulfilled, and that is real, not synthetic. `invoice` is the
# MAGIC billing. Only settlement is synthesised, because the accelerator records no payments.
# MAGIC
# MAGIC | Object | Purpose |
# MAGIC |--------|---------|
# MAGIC | `ordm_order_header` | One activation order per customer contract |
# MAGIC | `ordm_order_line_item` | Subscription line, plus a device line on 24-month terms |
# MAGIC | `ordm_fulfillment` | Fulfilment = first observed billing event for the SIM |
# MAGIC | `ordm_payment` | One row per invoice; settlement timing is synthetic |
# MAGIC | `reconcile_order_to_cash` | **The flagship.** Where an order has got to, and why cash has not arrived |
# MAGIC | `detect_revenue_leakage` | Fulfilled but never billed, or billed short |
# MAGIC | `lookup_dso_by_region` | Days sales outstanding and collection rate by region |
# MAGIC | `lookup_order_line_items` | What an order actually contains |
# MAGIC
# MAGIC ### Writes
# MAGIC
# MAGIC `submit_revenue_adjustment` lives in `write_actions.py`. It restates the order and
# MAGIC reissues the affected invoice in one `BEGIN ATOMIC` block — either half alone is
# MAGIC precisely the leakage `detect_revenue_leakage` reports.
# MAGIC
# MAGIC **Prerequisites:** `000-config` → `00_data_preparation` → `09_writeback_setup` →
# MAGIC `13_customer_360` (supplies `ordm_customer.region`).

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
    does not permit the override."""
    try:
        spark.sql(f"ALTER TABLE {table} ENABLE PREDICTIVE OPTIMIZATION")
        print(f"predictive optimization enabled: {table}")
    except Exception as e:
        print(f"predictive optimization NOT enabled for {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `ordm_order_header`\n# MAGIC\n# MAGIC One activation order per customer. `order_total` is the full contract value.\n# MAGIC Region comes from Demo #1 so order and customer geography agree.

# COMMAND ----------

# DBTITLE 1,Create ordm_order_header
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_order_header
CLUSTER BY (order_id, customer_id)
COMMENT 'ORDM Order canonical core. One activation order per customer contract, derived from customers.contract_start_dt and the plan they took. order_total is the full contract value: monthly charge x term.'
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
AS
SELECT
  CONCAT('ORD-', LPAD(CAST(cu.customer_id AS STRING), 10, '0'))   AS order_id,
  cu.customer_id,
  cu.contract_start_dt                                            AS order_date,
  p.Plan_id                                                       AS product_id,
  p.Plan_name                                                     AS product_name,
  p.contract_in_months                                            AS term_months,
  CAST(p.monthly_charges_dollars * p.contract_in_months AS DOUBLE) AS order_total,
  'USD'                                                           AS currency,
  oc.region,
  CASE PMOD(HASH(cu.customer_id, 'channel'), 3)
    WHEN 0 THEN 'Retail' WHEN 1 THEN 'Online' ELSE 'Telesales'
  END                                                             AS order_channel,
  CASE
    WHEN ADD_MONTHS(cu.contract_start_dt, p.contract_in_months) < CURRENT_DATE()
      THEN 'Completed'
    ELSE 'Active'
  END                                                             AS order_status,
  CAST(NULL AS DOUBLE)                                            AS adjusted_total,
  CAST(NULL AS STRING)                                            AS adjustment_reason
FROM {CATALOG}.{SCHEMA}.customers cu
JOIN {CATALOG}.{SCHEMA}.billing_plans p ON p.Plan_key = cu.plan
LEFT JOIN {CATALOG}.{SCHEMA}.ordm_customer oc ON oc.customer_id = cu.customer_id
""")

enable_predictive_optimization(FQ("ordm_order_header"))
display(spark.sql(f"SELECT order_status, order_channel, COUNT(*) AS orders, ROUND(SUM(order_total),2) AS value FROM {FQ(chr(34) + tbl + chr(34))} GROUP BY 1,2 ORDER BY value DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `ordm_order_line_item`

# COMMAND ----------

# DBTITLE 1,Create ordm_order_line_item
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_order_line_item
CLUSTER BY (order_id)
COMMENT 'ORDM Order line items. The plan subscription line plus, for orders on longer terms, a device line. Quantities and prices come from billing_plans.'
AS
SELECT
  CONCAT(o.order_id, '-1')                       AS order_line_id,
  o.order_id, 1                                  AS line_number,
  o.product_id, o.product_name                   AS description,
  'Subscription'                                 AS line_type,
  CAST(o.term_months AS DOUBLE)                  AS quantity,
  ROUND(o.order_total / o.term_months, 2)        AS unit_price,
  o.order_total                                  AS line_total
FROM {CATALOG}.{SCHEMA}.ordm_order_header o
UNION ALL
SELECT
  CONCAT(o.order_id, '-2'), o.order_id, 2,
  CONCAT(o.product_id, '-DEVICE'), 'Handset bundled with a 24-month term',
  'Device', 1.0, 149.0, 149.0
FROM {CATALOG}.{SCHEMA}.ordm_order_header o
WHERE o.term_months >= 24
""")

enable_predictive_optimization(FQ("ordm_order_line_item"))
display(spark.sql(f"SELECT line_type, COUNT(*) AS lines, ROUND(SUM(line_total),2) AS value FROM {FQ(chr(34) + tbl + chr(34))} GROUP BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `ordm_fulfillment`\n# MAGIC\n# MAGIC An order with no billing events was never fulfilled. That is observed, not invented.

# COMMAND ----------

# DBTITLE 1,Create ordm_fulfillment
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_fulfillment
CLUSTER BY (order_id)
COMMENT 'ORDM Fulfilment. A telco order is fulfilled when the SIM starts carrying traffic, so fulfilled_dt is the first observed billing event for the device. An order with no events was never fulfilled — which is real, not synthetic.'
AS
WITH first_event AS (
  SELECT device_id, MIN(event_ts) AS first_event_ts
  FROM {CATALOG}.{SCHEMA}.billing_items GROUP BY device_id
)
SELECT
  CONCAT('FUL-', LPAD(CAST(o.customer_id AS STRING), 10, '0')) AS fulfillment_id,
  o.order_id,
  o.customer_id,
  cu.device_id                                                 AS asset_id,
  CAST(fe.first_event_ts AS DATE)                              AS fulfilled_dt,
  'SIM activation'                                             AS fulfillment_method,
  CASE WHEN fe.first_event_ts IS NULL THEN 'NotFulfilled' ELSE 'Fulfilled' END
                                                               AS fulfillment_status,
  DATEDIFF(CAST(fe.first_event_ts AS DATE), o.order_date)       AS days_to_fulfil
FROM {CATALOG}.{SCHEMA}.ordm_order_header o
JOIN {CATALOG}.{SCHEMA}.customers cu ON cu.customer_id = o.customer_id
LEFT JOIN first_event fe ON fe.device_id = cu.device_id
""")

enable_predictive_optimization(FQ("ordm_fulfillment"))
display(spark.sql(f"SELECT fulfillment_status, COUNT(*) AS orders, ROUND(AVG(days_to_fulfil),1) AS avg_days FROM {FQ(chr(34) + tbl + chr(34))} GROUP BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `ordm_payment`\n# MAGIC\n# MAGIC The one synthetic layer: the accelerator records no payments, so settlement timing\n# MAGIC and status are derived deterministically from customer_id. Replace with the real\n# MAGIC remittance feed.

# COMMAND ----------

# DBTITLE 1,Create ordm_payment
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_payment
CLUSTER BY (customer_id, event_month)
COMMENT 'ORDM Payment, one row per invoice. The accelerator records no payments, so settlement timing and status are synthetic: deterministic from customer_id so roughly one invoice in six is unpaid and one in five settles late. Replace with the real remittance feed.'
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
AS
SELECT
  CONCAT('PAY-', LPAD(CAST(i.customer_id AS STRING), 10, '0'), '-', i.event_month) AS payment_id,
  i.customer_id,
  CONCAT('ORD-', LPAD(CAST(i.customer_id AS STRING), 10, '0'))                     AS order_id,
  i.event_month,
  CAST(i.total_charges AS DOUBLE)                                                  AS invoice_amount,
  LAST_DAY(TO_DATE(CONCAT(i.event_month, '-01')))                                  AS invoice_date,
  -- ~1 in 6 invoices unpaid; the rest settle 5-40 days after invoice date
  CASE WHEN PMOD(HASH(i.customer_id, i.event_month, 'paid'), 6) = 0
       THEN CAST(NULL AS DATE)
       ELSE DATE_ADD(LAST_DAY(TO_DATE(CONCAT(i.event_month, '-01'))),
                     CAST(5 + PMOD(HASH(i.customer_id, i.event_month, 'lag'), 36) AS INT))
  END                                                                              AS payment_date,
  CASE WHEN PMOD(HASH(i.customer_id, i.event_month, 'paid'), 6) = 0
       THEN 0.0 ELSE CAST(i.total_charges AS DOUBLE) END                           AS amount_paid,
  CASE WHEN PMOD(HASH(i.customer_id, i.event_month, 'paid'), 6) = 0
       THEN 'Unpaid' ELSE 'Settled' END                                            AS payment_status
FROM {CATALOG}.{SCHEMA}.invoice i
""")

enable_predictive_optimization(FQ("ordm_payment"))
display(spark.sql(f"SELECT payment_status, COUNT(*) AS invoices, ROUND(SUM(invoice_amount - amount_paid),2) AS unpaid FROM {FQ(chr(34) + tbl + chr(34))} GROUP BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `reconcile_order_to_cash`\n# MAGIC\n# MAGIC The flagship. One bounded row naming the stage an order has reached —\n# MAGIC `AwaitingFulfilment`, `FulfilledNotBilled`, `BilledNotCollected` or `Collected` —\n# MAGIC with the money at each step and a plain-language explanation.

# COMMAND ----------

# DBTITLE 1,Create reconcile_order_to_cash
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('reconcile_order_to_cash')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.reconcile_order_to_cash(
  input_order STRING COMMENT 'Order ID, e.g. ORD-0000004401. A customer ID is also accepted.'
)
RETURNS TABLE (
  order_id STRING, customer_id BIGINT, region STRING, order_channel STRING,
  order_date DATE, order_status STRING, order_total DOUBLE,
  fulfillment_status STRING, fulfilled_dt DATE, days_to_fulfil INT,
  months_billed BIGINT, total_billed DOUBLE,
  total_invoiced DOUBLE, total_paid DOUBLE, unpaid_amount DOUBLE,
  unbilled_amount DOUBLE, dso_days DOUBLE, stage STRING, explanation STRING
)
COMMENT 'Traces one order end to end: ordered, fulfilled, billed, invoiced, paid. Use for any question about where an order has got to, whether it has been billed, or why cash has not arrived. Returns at most one row. No PII.'
RETURN
WITH ord AS (
  SELECT * FROM {CATALOG}.{SCHEMA}.ordm_order_header
  WHERE order_id = input_order
     OR customer_id = TRY_CAST(input_order AS DECIMAL)
  LIMIT 1
),
ful AS (
  SELECT f.order_id, f.fulfillment_status, f.fulfilled_dt, f.days_to_fulfil
  FROM {CATALOG}.{SCHEMA}.ordm_fulfillment f JOIN ord ON ord.order_id = f.order_id
),
bill AS (
  SELECT i.customer_id, COUNT(*) AS months_billed,
         SUM(CAST(i.total_charges AS DOUBLE)) AS total_billed
  FROM {CATALOG}.{SCHEMA}.invoice i JOIN ord ON ord.customer_id = i.customer_id
  GROUP BY i.customer_id
),
pay AS (
  SELECT p.customer_id,
         SUM(p.invoice_amount)                                   AS total_invoiced,
         SUM(p.amount_paid)                                      AS total_paid,
         SUM(p.invoice_amount - p.amount_paid)                   AS unpaid_amount,
         AVG(CASE WHEN p.payment_date IS NOT NULL
                  THEN DATEDIFF(p.payment_date, p.invoice_date) END) AS dso_days
  FROM {CATALOG}.{SCHEMA}.ordm_payment p JOIN ord ON ord.customer_id = p.customer_id
  GROUP BY p.customer_id
)
SELECT
  ord.order_id, ord.customer_id, ord.region, ord.order_channel,
  ord.order_date, ord.order_status,
  COALESCE(ord.adjusted_total, ord.order_total)                   AS order_total,
  COALESCE(ful.fulfillment_status, 'NotFulfilled')                AS fulfillment_status,
  ful.fulfilled_dt, ful.days_to_fulfil,
  COALESCE(bill.months_billed, 0)                                 AS months_billed,
  ROUND(COALESCE(bill.total_billed, 0.0), 2)                      AS total_billed,
  ROUND(COALESCE(pay.total_invoiced, 0.0), 2)                     AS total_invoiced,
  ROUND(COALESCE(pay.total_paid, 0.0), 2)                         AS total_paid,
  ROUND(COALESCE(pay.unpaid_amount, 0.0), 2)                      AS unpaid_amount,
  ROUND(COALESCE(ord.adjusted_total, ord.order_total) - COALESCE(bill.total_billed, 0.0), 2)
                                                                  AS unbilled_amount,
  ROUND(pay.dso_days, 1)                                          AS dso_days,
  CASE
    WHEN COALESCE(ful.fulfillment_status, 'NotFulfilled') = 'NotFulfilled' THEN 'AwaitingFulfilment'
    WHEN COALESCE(bill.months_billed, 0) = 0                               THEN 'FulfilledNotBilled'
    WHEN COALESCE(pay.unpaid_amount, 0.0) > 0.01                           THEN 'BilledNotCollected'
    ELSE 'Collected'
  END AS stage,
  CASE
    WHEN COALESCE(ful.fulfillment_status, 'NotFulfilled') = 'NotFulfilled' THEN
      CONCAT('Order ', ord.order_id, ' placed ', CAST(ord.order_date AS STRING),
             ' has never been fulfilled — the SIM has carried no traffic. Nothing to bill yet.')
    WHEN COALESCE(bill.months_billed, 0) = 0 THEN
      CONCAT('Fulfilled ', CAST(ful.fulfilled_dt AS STRING),
             ' but no invoice has been raised. $',
             CAST(ROUND(COALESCE(ord.adjusted_total, ord.order_total), 2) AS STRING),
             ' of contract value is unbilled.')
    WHEN COALESCE(pay.unpaid_amount, 0.0) > 0.01 THEN
      CONCAT('Billed ', CAST(COALESCE(bill.months_billed, 0) AS STRING), ' months totalling $',
             CAST(ROUND(COALESCE(bill.total_billed, 0.0), 2) AS STRING), '. $',
             CAST(ROUND(COALESCE(pay.unpaid_amount, 0.0), 2) AS STRING),
             ' remains uncollected, averaging ', CAST(ROUND(pay.dso_days, 1) AS STRING),
             ' days to settle.')
    ELSE
      CONCAT('Order complete: fulfilled ', CAST(ful.fulfilled_dt AS STRING), ', billed $',
             CAST(ROUND(COALESCE(bill.total_billed, 0.0), 2) AS STRING),
             ', fully collected in ', CAST(ROUND(pay.dso_days, 1) AS STRING), ' days on average.')
  END AS explanation
FROM ord
LEFT JOIN ful ON ful.order_id = ord.order_id
LEFT JOIN bill ON bill.customer_id = ord.customer_id
LEFT JOIN pay ON pay.customer_id = ord.customer_id
""")
print(f"created {FQ('reconcile_order_to_cash')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `detect_revenue_leakage`\n# MAGIC\n# MAGIC Orders that were delivered but never invoiced, ranked by exposure.

# COMMAND ----------

# DBTITLE 1,Create detect_revenue_leakage
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('detect_revenue_leakage')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.detect_revenue_leakage(
  min_days_unbilled INT DEFAULT 30 COMMENT 'Only report orders fulfilled at least this many days ago. Default 30.'
)
RETURNS TABLE (
  order_id STRING, customer_id BIGINT, region STRING, order_channel STRING,
  fulfilled_dt DATE, days_since_fulfilment INT, order_total DOUBLE,
  total_billed DOUBLE, leakage_amount DOUBLE, leakage_type STRING
)
COMMENT 'Orders that were fulfilled but never billed, or billed for less than the contract value. Use to find revenue that has been delivered but not invoiced. Ranked by exposure, capped at 100 rows. No PII.'
RETURN
WITH billed AS (
  SELECT customer_id, SUM(CAST(total_charges AS DOUBLE)) AS total_billed
  FROM {CATALOG}.{SCHEMA}.invoice GROUP BY customer_id
)
SELECT
  o.order_id, o.customer_id, o.region, o.order_channel,
  f.fulfilled_dt,
  DATEDIFF(CURRENT_DATE(), f.fulfilled_dt)                       AS days_since_fulfilment,
  COALESCE(o.adjusted_total, o.order_total)                      AS order_total,
  ROUND(COALESCE(b.total_billed, 0.0), 2)                        AS total_billed,
  ROUND(COALESCE(o.adjusted_total, o.order_total) - COALESCE(b.total_billed, 0.0), 2)
                                                                 AS leakage_amount,
  CASE WHEN b.total_billed IS NULL THEN 'NeverBilled' ELSE 'PartiallyBilled' END
                                                                 AS leakage_type
FROM {CATALOG}.{SCHEMA}.ordm_order_header o
JOIN {CATALOG}.{SCHEMA}.ordm_fulfillment f
  ON f.order_id = o.order_id AND f.fulfillment_status = 'Fulfilled'
LEFT JOIN billed b ON b.customer_id = o.customer_id
WHERE DATEDIFF(CURRENT_DATE(), f.fulfilled_dt) >= min_days_unbilled
  AND COALESCE(o.adjusted_total, o.order_total) - COALESCE(b.total_billed, 0.0) > 0.01
ORDER BY leakage_amount DESC
LIMIT 100
""")
print(f"created {FQ('detect_revenue_leakage')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `lookup_dso_by_region`

# COMMAND ----------

# DBTITLE 1,Create lookup_dso_by_region
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_dso_by_region')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_dso_by_region()
RETURNS TABLE (
  region STRING, orders BIGINT, invoices BIGINT, total_invoiced DOUBLE,
  total_paid DOUBLE, unpaid_amount DOUBLE, collection_rate_pct DOUBLE,
  dso_days DOUBLE, unpaid_invoice_count BIGINT
)
COMMENT 'Days sales outstanding and collection rate by region. Use for questions about which regions are slow to pay or carry the most uncollected cash. At most 20 rows.'
RETURN
SELECT
  o.region,
  COUNT(DISTINCT o.order_id)                                   AS orders,
  COUNT(p.payment_id)                                          AS invoices,
  ROUND(SUM(p.invoice_amount), 2)                              AS total_invoiced,
  ROUND(SUM(p.amount_paid), 2)                                 AS total_paid,
  ROUND(SUM(p.invoice_amount - p.amount_paid), 2)              AS unpaid_amount,
  ROUND(SUM(p.amount_paid) * 100.0 / NULLIF(SUM(p.invoice_amount), 0), 1)
                                                               AS collection_rate_pct,
  ROUND(AVG(CASE WHEN p.payment_date IS NOT NULL
                 THEN DATEDIFF(p.payment_date, p.invoice_date) END), 1) AS dso_days,
  SUM(CASE WHEN p.payment_status = 'Unpaid' THEN 1 ELSE 0 END)  AS unpaid_invoice_count
FROM {CATALOG}.{SCHEMA}.ordm_order_header o
JOIN {CATALOG}.{SCHEMA}.ordm_payment p ON p.customer_id = o.customer_id
WHERE o.region IS NOT NULL
GROUP BY o.region
ORDER BY unpaid_amount DESC
LIMIT 20
""")
print(f"created {FQ('lookup_dso_by_region')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. `lookup_order_line_items`

# COMMAND ----------

# DBTITLE 1,Create lookup_order_line_items
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_order_line_items')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_order_line_items(
  input_order STRING COMMENT 'Order ID, e.g. ORD-0000004401'
)
RETURNS TABLE (
  order_id STRING, order_line_id STRING, line_number INT, line_type STRING,
  product_id STRING, description STRING, quantity DOUBLE, unit_price DOUBLE,
  line_total DOUBLE
)
COMMENT 'The individual lines on an order: subscription and any bundled device. Use when asked what an order actually contains. Capped at 50 rows.'
RETURN
SELECT order_id, order_line_id, line_number, line_type, product_id,
       description, quantity, unit_price, line_total
FROM {CATALOG}.{SCHEMA}.ordm_order_line_item
WHERE order_id = input_order
ORDER BY line_number
LIMIT 50
""")
print(f"created {FQ('lookup_order_line_items')}")
