# Databricks notebook source
# MAGIC %md
# MAGIC # 📶 Usage-Based Billing with Inventory — ORDM Inventory Core
# MAGIC
# MAGIC Demo #5. Answers the other question every billing agent gets:
# MAGIC *"Why did my bill jump this month?"*
# MAGIC
# MAGIC This accelerator is **already** usage-based billing — `billing_items` carries data,
# MAGIC roaming and international events, and `billing_plans` carries the allowances and
# MAGIC overage rates. What was missing was the layer that reconciles the two. This notebook
# MAGIC adds it, projecting onto the
# MAGIC [Open Retail Data Model](https://github.com/databricks-industry-solutions/open-retail-data-model)
# MAGIC **Inventory** canonical core.
# MAGIC
# MAGIC ### What this notebook creates
# MAGIC
# MAGIC | Object | Layer | Purpose |
# MAGIC |--------|-------|---------|
# MAGIC | `ordm_inventory_asset` | Canonical core | SIMs and devices with observed activity status. |
# MAGIC | `ordm_plan_entitlement` | Canonical core | Allowances and overage rates in long form, one row per metered dimension. |
# MAGIC | `usage_by_asset_month` | Gold | Events rolled up per customer, asset, month and dimension. |
# MAGIC | `usage_forecast` | Gold | Next-month data usage from `AI_FORECAST`. |
# MAGIC | `detect_overage` | UC function | **The flagship.** Used vs included vs charged, per dimension. |
# MAGIC | `recommend_plan_upgrade` | UC function | Prices real usage against every plan and ranks them. |
# MAGIC | `lookup_usage_history` | UC function | Monthly usage trend. |
# MAGIC | `lookup_plan_entitlement` | UC function | What a plan includes. |
# MAGIC | `lookup_inventory_assets` | UC function | Lines on the account and whether they are live. |
# MAGIC | `lookup_usage_forecast` | UC function | Will they exceed next month? |
# MAGIC
# MAGIC ### One normalisation carries the whole demo
# MAGIC
# MAGIC Everything is expressed in the **entitlement unit** — data in MB, calls in minutes,
# MAGIC texts as a count. That is why `detect_overage` is one calculation across all eight
# MAGIC metered dimensions instead of a branch per charge type, and why
# MAGIC `recommend_plan_upgrade` can reprice a month against any plan.
# MAGIC
# MAGIC An allowance of `NULL` means unlimited; an allowance of `0` means charged per use.
# MAGIC
# MAGIC ### Forecasting
# MAGIC
# MAGIC `usage_forecast` is a **materialised** table built with `AI_FORECAST`, refreshed on a
# MAGIC schedule. `lookup_usage_forecast` reads it. The forecast is not computed inside a UC
# MAGIC function — a function body is a query expression, and this keeps the agent path a
# MAGIC plain bounded read.
# MAGIC
# MAGIC ### Writes
# MAGIC
# MAGIC `submit_auto_upgrade` lives in `write_actions.py`. It updates `customers` **and**
# MAGIC `ordm_customer_contract` in one `BEGIN ATOMIC` block, because applying only one of
# MAGIC them produces exactly the state `detect_pricing_drift` reports as `ContractMismatch`.
# MAGIC It therefore needs `14_pricing_dispute` to have run.
# MAGIC
# MAGIC **Prerequisites:** `000-config` → `00_data_preparation` → `09_writeback_setup` → `14_pricing_dispute`.

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
# MAGIC ## 1. ORDM Canonical Core — `ordm_inventory_asset`\n# MAGIC\n# MAGIC For telco the metered asset is the SIM. `asset_status` is derived from real\n# MAGIC observed activity in `billing_items`, not synthesised — a line with no events in\n# MAGIC six months is genuinely inactive.

# COMMAND ----------

# DBTITLE 1,Create ordm_inventory_asset
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_inventory_asset
CLUSTER BY (customer_id, asset_id)
COMMENT 'ORDM Inventory canonical core. For telco the metered asset is the SIM/device, so this projects customers.device_id and joins its observed activity. asset_type and asset_status are derived from real activity, not synthesised.'
AS
WITH activity AS (
  SELECT device_id,
         MIN(event_ts) AS first_event_ts,
         MAX(event_ts) AS last_event_ts,
         COUNT(*)      AS lifetime_event_count
  FROM {CATALOG}.{SCHEMA}.billing_items
  GROUP BY device_id
)
SELECT
  cu.device_id                                            AS asset_id,
  CONCAT('SIM-', LPAD(CAST(cu.device_id AS STRING), 12, '0')) AS asset_key,
  cu.customer_id,
  p.Plan_id                                               AS product_id,
  p.Plan_name                                             AS product_name,
  'SIM'                                                   AS asset_type,
  cu.contract_start_dt                                    AS activated_dt,
  a.first_event_ts,
  a.last_event_ts,
  COALESCE(a.lifetime_event_count, 0)                     AS lifetime_event_count,
  CASE
    WHEN a.last_event_ts IS NULL                                     THEN 'NeverActivated'
    WHEN a.last_event_ts >= ADD_MONTHS(CURRENT_TIMESTAMP(), -2)      THEN 'Active'
    WHEN a.last_event_ts >= ADD_MONTHS(CURRENT_TIMESTAMP(), -6)      THEN 'Dormant'
    ELSE 'Inactive'
  END                                                     AS asset_status
FROM {CATALOG}.{SCHEMA}.customers cu
LEFT JOIN activity a ON a.device_id = cu.device_id
LEFT JOIN {CATALOG}.{SCHEMA}.billing_plans p ON p.Plan_key = cu.plan
""")

enable_predictive_optimization(FQ("ordm_inventory_asset"))
display(spark.sql(f"SELECT asset_status, COUNT(*) AS assets FROM {FQ(chr(34) + table + chr(34))} GROUP BY asset_status ORDER BY assets DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ORDM Canonical Core — `ordm_plan_entitlement`\n# MAGIC\n# MAGIC The wide `billing_plans` row becomes one row per metered dimension, with quantities\n# MAGIC normalised to the overage unit. This is what lets one overage calculation serve all\n# MAGIC eight dimensions. Derived entirely from `billing_plans` — nothing synthetic.

# COMMAND ----------

# DBTITLE 1,Create ordm_plan_entitlement
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.ordm_plan_entitlement
CLUSTER BY (product_id, entitlement_type)
COMMENT 'ORDM plan entitlements in long form, one row per metered dimension. Quantities are normalised to the overage unit (MB, minutes, texts) so overage is one calculation everywhere. included_quantity NULL means unlimited. Derived entirely from billing_plans — no synthetic values.'
AS
SELECT CONCAT(Plan_id, ':data_local') AS entitlement_id, Plan_id AS product_id, Plan_name AS product_name,
       'data_local' AS entitlement_type,
       TRY_CAST(Data_Limit_GB AS DOUBLE) * 1024.0 AS included_quantity, 'MB' AS usage_unit,
       Data_Outside_Allowance_Per_MB AS overage_rate
FROM {CATALOG}.{SCHEMA}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':data_roaming'), Plan_id, Plan_name, 'data_roaming',
       0.0, 'MB', Roam_Data_charges_per_MB FROM {CATALOG}.{SCHEMA}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':call_mins_roaming'), Plan_id, Plan_name, 'call_mins_roaming',
       0.0, 'min', Roam_Call_charges_per_min FROM {CATALOG}.{SCHEMA}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':texts_roaming'), Plan_id, Plan_name, 'texts_roaming',
       0.0, 'text', Roam_text_charges FROM {CATALOG}.{SCHEMA}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':call_mins_international'), Plan_id, Plan_name, 'call_mins_international',
       0.0, 'min', International_call_charge_per_min FROM {CATALOG}.{SCHEMA}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':texts_international'), Plan_id, Plan_name, 'texts_international',
       0.0, 'text', International_text_charge FROM {CATALOG}.{SCHEMA}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':call_mins_local'), Plan_id, Plan_name, 'call_mins_local',
       CAST(NULL AS DOUBLE), 'min', 0.0 FROM {CATALOG}.{SCHEMA}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':texts_local'), Plan_id, Plan_name, 'texts_local',
       CAST(NULL AS DOUBLE), 'text', 0.0 FROM {CATALOG}.{SCHEMA}.billing_plans
""")

enable_predictive_optimization(FQ("ordm_plan_entitlement"))
display(spark.sql(f"SELECT product_name, entitlement_type, included_quantity, usage_unit, overage_rate FROM {FQ(chr(34) + table + chr(34))} ORDER BY product_id, entitlement_type LIMIT 16"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold — `usage_by_asset_month`\n# MAGIC\n# MAGIC Event-level `billing_items` rolled up per customer, asset, month and dimension. The\n# MAGIC agent never scans raw events, which is what keeps every usage question bounded.

# COMMAND ----------

# DBTITLE 1,Create usage_by_asset_month
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.usage_by_asset_month
CLUSTER BY (customer_id, event_month)
COMMENT 'Gold: event-level billing_items rolled up per customer, asset, month and metered dimension, normalised to the entitlement unit. The agent never scans raw events.'
AS
SELECT
  cu.customer_id,
  bi.device_id                                    AS asset_id,
  DATE_FORMAT(bi.event_ts, 'yyyy-MM')             AS event_month,
  bi.event_type                                   AS entitlement_type,
  CASE WHEN bi.event_type LIKE 'data%'      THEN 'MB'
       WHEN bi.event_type LIKE 'call_mins%' THEN 'min'
       ELSE 'text' END                            AS usage_unit,
  CASE WHEN bi.event_type LIKE 'data%'
         THEN ROUND(SUM(CAST(bi.bytes_transferred AS DOUBLE)) / 1048576.0, 3)
       WHEN bi.event_type LIKE 'call_mins%'
         THEN ROUND(SUM(CAST(bi.minutes AS DOUBLE)), 2)
       ELSE CAST(COUNT(*) AS DOUBLE) END          AS usage_quantity,
  COUNT(*)                                        AS event_count,
  MAX(bi.event_ts)                                AS last_event_ts
FROM {CATALOG}.{SCHEMA}.billing_items bi
JOIN {CATALOG}.{SCHEMA}.customers cu ON cu.device_id = bi.device_id
GROUP BY cu.customer_id, bi.device_id, DATE_FORMAT(bi.event_ts, 'yyyy-MM'), bi.event_type
""")

enable_predictive_optimization(FQ("usage_by_asset_month"))
display(spark.sql(f"SELECT event_month, entitlement_type, usage_unit, ROUND(SUM(usage_quantity),1) AS qty, SUM(event_count) AS events FROM {FQ(chr(34) + table + chr(34))} GROUP BY 1,2,3 ORDER BY 1 DESC, 2 LIMIT 16"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold — `usage_forecast`\n# MAGIC\n# MAGIC Next-month data usage per customer via `AI_FORECAST`, grouped by customer. This is a\n# MAGIC materialised forecast: refresh it on a schedule alongside the usage rollup.

# COMMAND ----------

# DBTITLE 1,Create usage_forecast
spark.sql(f"""CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.usage_forecast
CLUSTER BY (customer_id)
COMMENT 'Gold: next-month data usage per customer from AI_FORECAST over the monthly history. Refresh on a schedule — this is a materialised forecast, not a live model call.'
AS
SELECT
  CAST(customer_id AS BIGINT)                      AS customer_id,
  DATE_FORMAT(ds, 'yyyy-MM')                       AS forecast_month,
  'data_local'                                     AS entitlement_type,
  'MB'                                             AS usage_unit,
  ROUND(y_forecast, 2)                             AS forecast_quantity,
  ROUND(y_lower, 2)                                AS forecast_lower,
  ROUND(y_upper, 2)                                AS forecast_upper,
  CURRENT_TIMESTAMP()                              AS generated_at
FROM AI_FORECAST(
  TABLE(
    SELECT
      CAST(customer_id AS STRING)                    AS customer_id,
      TO_DATE(CONCAT(event_month, '-01'))            AS ds,
      SUM(usage_quantity)                            AS y
    FROM {CATALOG}.{SCHEMA}.usage_by_asset_month
    WHERE entitlement_type = 'data_local'
    GROUP BY customer_id, TO_DATE(CONCAT(event_month, '-01'))
  ),
  horizon    => ADD_MONTHS(CURRENT_DATE(), 1),
  time_col   => 'ds',
  value_col  => 'y',
  group_col  => 'customer_id'
)
""")

enable_predictive_optimization(FQ("usage_forecast"))
display(spark.sql(f"SELECT customer_id, forecast_month, forecast_quantity, forecast_lower, forecast_upper FROM {FQ(chr(34) + table + chr(34))} ORDER BY forecast_quantity DESC LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. UC Function — `detect_overage`\n# MAGIC\n# MAGIC The flagship. One row per metered dimension with usage: what was used, what the plan\n# MAGIC includes, what the excess cost, and a plain-language explanation. This is the first\n# MAGIC call for any *\"why did my bill go up?\"* question.

# COMMAND ----------

# DBTITLE 1,Create detect_overage
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('detect_overage')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.detect_overage(
  input_customer STRING COMMENT 'Customer ID',
  input_event_month STRING COMMENT 'Billing month, formatted yyyy-MM'
)
RETURNS TABLE (
  customer_id       BIGINT,
  event_month       STRING,
  plan_name         STRING,
  entitlement_type  STRING,
  usage_quantity    DOUBLE,
  usage_unit        STRING,
  included_quantity DOUBLE,
  overage_quantity  DOUBLE,
  overage_rate      DOUBLE,
  overage_charge    DOUBLE,
  utilization_pct   DOUBLE,
  status            STRING,
  explanation       STRING
)
COMMENT 'Breaks a month down by metered dimension: how much was used, how much the plan includes, and what the overage cost. Use this first whenever a customer asks why a bill went up or whether they are near a limit. One row per dimension with usage, at most 8. No PII.'
RETURN
WITH usage AS (
  SELECT u.customer_id, u.event_month, u.entitlement_type, u.usage_unit,
         SUM(u.usage_quantity) AS usage_quantity
  FROM {CATALOG}.{SCHEMA}.usage_by_asset_month u
  WHERE u.customer_id = TRY_CAST(input_customer AS DECIMAL)
    AND u.event_month = input_event_month
  GROUP BY u.customer_id, u.event_month, u.entitlement_type, u.usage_unit
),
scoped AS (
  SELECT usage.*, e.product_name AS plan_name, e.included_quantity, e.overage_rate
  FROM usage
  JOIN {CATALOG}.{SCHEMA}.customers cu ON cu.customer_id = usage.customer_id
  JOIN {CATALOG}.{SCHEMA}.billing_plans p ON p.Plan_key = cu.plan
  JOIN {CATALOG}.{SCHEMA}.ordm_plan_entitlement e
    ON e.product_id = p.Plan_id AND e.entitlement_type = usage.entitlement_type
),
scored AS (
  SELECT scoped.*,
         CASE WHEN included_quantity IS NULL THEN 0.0
              ELSE GREATEST(usage_quantity - included_quantity, 0.0) END AS overage_quantity
  FROM scoped
)
SELECT
  customer_id, event_month, plan_name, entitlement_type,
  ROUND(usage_quantity, 2)                         AS usage_quantity,
  usage_unit,
  included_quantity,
  ROUND(overage_quantity, 2)                       AS overage_quantity,
  overage_rate,
  ROUND(overage_quantity * overage_rate, 2)        AS overage_charge,
  CASE WHEN included_quantity IS NULL OR included_quantity = 0 THEN CAST(NULL AS DOUBLE)
       ELSE ROUND(usage_quantity * 100.0 / included_quantity, 1) END AS utilization_pct,
  CASE
    WHEN included_quantity IS NULL                       THEN 'Unlimited'
    WHEN included_quantity = 0 AND usage_quantity > 0    THEN 'ChargedPerUse'
    WHEN usage_quantity > included_quantity              THEN 'OverLimit'
    WHEN usage_quantity >= included_quantity * 0.8       THEN 'ApproachingLimit'
    ELSE 'WithinAllowance'
  END AS status,
  CASE
    WHEN included_quantity IS NULL THEN
      CONCAT(entitlement_type, ': ', CAST(ROUND(usage_quantity, 2) AS STRING), ' ',
             usage_unit, ' used, unlimited on this plan. No charge.')
    WHEN included_quantity = 0 AND usage_quantity > 0 THEN
      CONCAT(entitlement_type, ': ', CAST(ROUND(usage_quantity, 2) AS STRING), ' ', usage_unit,
             ' billed per use at $', CAST(overage_rate AS STRING), ' per ', usage_unit,
             ' = $', CAST(ROUND(overage_quantity * overage_rate, 2) AS STRING),
             '. Not included in the plan.')
    WHEN usage_quantity > included_quantity THEN
      CONCAT(entitlement_type, ': used ', CAST(ROUND(usage_quantity, 2) AS STRING), ' ', usage_unit,
             ' against an allowance of ', CAST(included_quantity AS STRING), ' ', usage_unit,
             '. The ', CAST(ROUND(overage_quantity, 2) AS STRING), ' ', usage_unit,
             ' over cost $', CAST(ROUND(overage_quantity * overage_rate, 2) AS STRING), '.')
    WHEN usage_quantity >= included_quantity * 0.8 THEN
      CONCAT(entitlement_type, ': ', CAST(ROUND(usage_quantity * 100.0 / included_quantity, 1) AS STRING),
             '% of the allowance used. Approaching the limit but no overage yet.')
    ELSE
      CONCAT(entitlement_type, ': ', CAST(ROUND(usage_quantity * 100.0 / included_quantity, 1) AS STRING),
             '% of the allowance used. Within plan.')
  END AS explanation
FROM scored
ORDER BY overage_charge DESC, entitlement_type
LIMIT 20
""")
print(f"created {FQ('detect_overage')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. UC Function — `recommend_plan_upgrade`\n# MAGIC\n# MAGIC Reprices a real month of usage against every plan and ranks by total cost. Because\n# MAGIC entitlements are normalised, this is a single aggregate rather than per-plan logic.

# COMMAND ----------

# DBTITLE 1,Create recommend_plan_upgrade
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('recommend_plan_upgrade')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.recommend_plan_upgrade(
  input_customer STRING COMMENT 'Customer ID',
  input_event_month STRING COMMENT 'Month whose usage to price against, yyyy-MM'
)
RETURNS TABLE (
  customer_id BIGINT, event_month STRING, candidate_plan_key BIGINT,
  candidate_plan_name STRING, is_current_plan BOOLEAN, base_charge DOUBLE,
  projected_overage DOUBLE, projected_total DOUBLE, saving_vs_current DOUBLE,
  recommendation STRING
)
COMMENT 'Prices one month of the customer actual usage against every plan and ranks them by total cost. Use when asked whether a different plan would be cheaper, or after detect_overage shows an overage. Returns the current plan plus the best alternatives, at most 5 rows.'
RETURN
WITH usage AS (
  SELECT entitlement_type, SUM(usage_quantity) AS usage_quantity
  FROM {CATALOG}.{SCHEMA}.usage_by_asset_month
  WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
    AND event_month = input_event_month
  GROUP BY entitlement_type
),
current_plan AS (
  SELECT MAX(p.Plan_key) AS Plan_key
  FROM {CATALOG}.{SCHEMA}.customers cu
  JOIN {CATALOG}.{SCHEMA}.billing_plans p ON p.Plan_key = cu.plan
  WHERE cu.customer_id = TRY_CAST(input_customer AS DECIMAL)
),
priced AS (
  SELECT
    p.Plan_key, p.Plan_name, p.monthly_charges_dollars AS base_charge,
    SUM(CASE WHEN e.included_quantity IS NULL THEN 0.0
             ELSE GREATEST(u.usage_quantity - e.included_quantity, 0.0) * e.overage_rate
        END) AS projected_overage
  FROM {CATALOG}.{SCHEMA}.billing_plans p
  JOIN {CATALOG}.{SCHEMA}.ordm_plan_entitlement e ON e.product_id = p.Plan_id
  JOIN usage u ON u.entitlement_type = e.entitlement_type
  GROUP BY p.Plan_key, p.Plan_name, p.monthly_charges_dollars
),
current_total AS (
  SELECT MAX(pr.base_charge + pr.projected_overage) AS current_total
  FROM priced pr
  JOIN current_plan cp ON pr.Plan_key = cp.Plan_key
),
ranked AS (
  SELECT
    priced.*,
    priced.base_charge + priced.projected_overage AS projected_total,
    priced.Plan_key = cp.Plan_key                  AS is_current_plan,
    ct.current_total
  FROM priced
  CROSS JOIN current_plan cp
  CROSS JOIN current_total ct
)
SELECT
  TRY_CAST(input_customer AS DECIMAL(20,0)) AS customer_id,
  input_event_month                         AS event_month,
  Plan_key                                  AS candidate_plan_key,
  Plan_name                                 AS candidate_plan_name,
  is_current_plan,
  base_charge,
  ROUND(projected_overage, 2)               AS projected_overage,
  ROUND(projected_total, 2)                 AS projected_total,
  ROUND(current_total - projected_total, 2) AS saving_vs_current,
  CASE
    WHEN is_current_plan                            THEN 'Current plan'
    WHEN current_total - projected_total > 0.01     THEN
      CONCAT('Would save $', CAST(ROUND(current_total - projected_total, 2) AS STRING),
             ' for this month of usage')
    ELSE
      CONCAT('No saving — $', CAST(ROUND(projected_total - current_total, 2) AS STRING),
             ' more than the current plan')
  END AS recommendation
FROM ranked
ORDER BY is_current_plan DESC, projected_total ASC
LIMIT 5
""")
print(f"created {FQ('recommend_plan_upgrade')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. UC Function — `lookup_usage_history`

# COMMAND ----------

# DBTITLE 1,Create lookup_usage_history
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_usage_history')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_usage_history(
  input_customer STRING COMMENT 'Customer ID',
  lookback_months INT DEFAULT 6 COMMENT 'Months of history. Default 6, capped at 24.'
)
RETURNS TABLE (
  customer_id BIGINT, event_month STRING, entitlement_type STRING,
  usage_quantity DOUBLE, usage_unit STRING, event_count BIGINT
)
COMMENT 'Monthly usage per metered dimension for a customer, newest first. Use to show a usage trend or explain a change between months. Capped at 200 rows. No PII.'
RETURN
SELECT
  customer_id, event_month, entitlement_type,
  ROUND(SUM(usage_quantity), 2) AS usage_quantity,
  MAX(usage_unit)               AS usage_unit,
  SUM(event_count)              AS event_count
FROM {CATALOG}.{SCHEMA}.usage_by_asset_month
WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
  AND event_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -LEAST(lookback_months, 24)), 'yyyy-MM')
GROUP BY customer_id, event_month, entitlement_type
ORDER BY event_month DESC, entitlement_type
LIMIT 200
""")
print(f"created {FQ('lookup_usage_history')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. UC Function — `lookup_plan_entitlement`

# COMMAND ----------

# DBTITLE 1,Create lookup_plan_entitlement
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_plan_entitlement')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_plan_entitlement(
  input_plan STRING COMMENT 'Plan name or product id'
)
RETURNS TABLE (
  product_id STRING, product_name STRING, entitlement_type STRING,
  included_quantity DOUBLE, usage_unit STRING, overage_rate DOUBLE, is_unlimited BOOLEAN
)
COMMENT 'What a plan includes and what it charges beyond the allowance, one row per metered dimension. Use when asked what is included in a plan.'
RETURN
SELECT
  product_id, product_name, entitlement_type,
  included_quantity, usage_unit, overage_rate,
  included_quantity IS NULL AS is_unlimited
FROM {CATALOG}.{SCHEMA}.ordm_plan_entitlement
WHERE product_name = input_plan OR product_id = input_plan
ORDER BY entitlement_type
LIMIT 20
""")
print(f"created {FQ('lookup_plan_entitlement')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. UC Function — `lookup_inventory_assets`

# COMMAND ----------

# DBTITLE 1,Create lookup_inventory_assets
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_inventory_assets')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_inventory_assets(
  input_customer STRING COMMENT 'Customer ID'
)
RETURNS TABLE (
  customer_id BIGINT, asset_id BIGINT, asset_key STRING, asset_type STRING,
  asset_status STRING, product_name STRING, activated_dt DATE,
  last_event_ts TIMESTAMP, lifetime_event_count BIGINT
)
COMMENT 'The SIMs and devices on a customer account, with activity status. Use when asked what is on the account or whether a line is still in use. Capped at 50 rows. No PII.'
RETURN
SELECT
  customer_id, asset_id, asset_key, asset_type, asset_status,
  product_name, activated_dt, last_event_ts, lifetime_event_count
FROM {CATALOG}.{SCHEMA}.ordm_inventory_asset
WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
ORDER BY asset_status, last_event_ts DESC
LIMIT 50
""")
print(f"created {FQ('lookup_inventory_assets')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. UC Function — `lookup_usage_forecast`

# COMMAND ----------

# DBTITLE 1,Create lookup_usage_forecast
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_usage_forecast')}")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_usage_forecast(
  input_customer STRING COMMENT 'Customer ID'
)
RETURNS TABLE (
  customer_id BIGINT, forecast_month STRING, entitlement_type STRING,
  forecast_quantity DOUBLE, forecast_lower DOUBLE, forecast_upper DOUBLE,
  included_quantity DOUBLE, usage_unit STRING, projected_status STRING, generated_at TIMESTAMP
)
COMMENT 'Forecast data usage for next month against the current plan allowance, from the materialised AI_FORECAST table. Use when asked whether they will exceed their limit. Returns at most one row.'
RETURN
SELECT
  f.customer_id, f.forecast_month, f.entitlement_type,
  f.forecast_quantity, f.forecast_lower, f.forecast_upper,
  e.included_quantity, f.usage_unit,
  CASE
    WHEN e.included_quantity IS NULL                     THEN 'Unlimited'
    WHEN f.forecast_lower  > e.included_quantity         THEN 'WillExceed'
    WHEN f.forecast_quantity > e.included_quantity       THEN 'LikelyToExceed'
    WHEN f.forecast_upper  > e.included_quantity         THEN 'AtRisk'
    ELSE 'WithinAllowance'
  END AS projected_status,
  f.generated_at
FROM {CATALOG}.{SCHEMA}.usage_forecast f
JOIN {CATALOG}.{SCHEMA}.customers cu ON cu.customer_id = f.customer_id
JOIN {CATALOG}.{SCHEMA}.billing_plans p ON p.Plan_key = cu.plan
JOIN {CATALOG}.{SCHEMA}.ordm_plan_entitlement e
  ON e.product_id = p.Plan_id AND e.entitlement_type = f.entitlement_type
WHERE f.customer_id = TRY_CAST(input_customer AS DECIMAL)
LIMIT 1
""")
print(f"created {FQ('lookup_usage_forecast')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validation
# MAGIC
# MAGIC Asserts what matters for an agent tool: usage is normalised consistently, every
# MAGIC overage status is reachable, the recommender includes and flags the current plan,
# MAGIC results are bounded, and no PII reaches any signature.

# COMMAND ----------

# DBTITLE 1,Validate the entitlement and usage layers
units = {r["usage_unit"] for r in spark.sql(
    f"SELECT DISTINCT usage_unit FROM {FQ('usage_by_asset_month')}").collect()}
assert units <= {"MB", "min", "text"}, f"unexpected usage units: {units}"

dims = spark.sql(
    f"SELECT COUNT(DISTINCT entitlement_type) AS n FROM {FQ('ordm_plan_entitlement')}").first()["n"]
assert dims == 8, f"expected 8 metered dimensions per plan, got {dims}"

unlimited = spark.sql(
    f"SELECT COUNT(*) AS n FROM {FQ('ordm_plan_entitlement')} WHERE included_quantity IS NULL"
).first()["n"]
assert unlimited > 0, "unlimited dimensions should be modelled as a NULL allowance"
print(f"usage units {sorted(units)}, {dims} dimensions per plan, {unlimited} unlimited entitlements")

# COMMAND ----------

# DBTITLE 1,Validate detect_overage
month = spark.sql(f"""
    SELECT event_month FROM {FQ('usage_by_asset_month')}
    GROUP BY event_month ORDER BY event_month DESC LIMIT 1 OFFSET 1
""").first()
assert month, "Not enough usage history — run 00_data_preparation first."
EVENT_MONTH = month["event_month"]

customers = [r["customer_id"] for r in spark.sql(f"""
    SELECT DISTINCT customer_id FROM {FQ('usage_by_asset_month')}
    WHERE event_month = '{EVENT_MONTH}' ORDER BY customer_id LIMIT 30
""").collect()]

seen, over_limit_customer = {}, None
for cid in customers:
    for row in spark.sql(f"SELECT * FROM {FQ('detect_overage')}('{cid}', '{EVENT_MONTH}')").collect():
        seen.setdefault(row["status"], (cid, row))
        if row["status"] == "OverLimit" and over_limit_customer is None:
            over_limit_customer = cid

print(f"overage statuses reachable for {EVENT_MONTH}:\n")
for status, (cid, row) in sorted(seen.items()):
    print(f"  {status:<18} customer {cid}  {row['entitlement_type']}")
    print(f"    {row['explanation']}\n")

assert "WithinAllowance" in seen, "expected at least one customer inside their allowance"
assert "Unlimited" in seen, "expected at least one unlimited dimension"
assert over_limit_customer, (
    "no customer exceeded an allowance, so the demo has nothing to show. Generate more "
    "usage in 00_data_preparation, or pick a month with heavier traffic."
)

# COMMAND ----------

# DBTITLE 1,Validate recommend_plan_upgrade
plans = spark.sql(
    f"SELECT * FROM {FQ('recommend_plan_upgrade')}('{over_limit_customer}', '{EVENT_MONTH}')"
).collect()
assert 0 < len(plans) <= 5, f"recommendation must be bounded, got {len(plans)}"

current = [p for p in plans if p["is_current_plan"]]
assert len(current) == 1, "the current plan must appear exactly once"
assert abs(current[0]["saving_vs_current"]) < 0.01, "the current plan cannot save against itself"

print(f"customer {over_limit_customer}, {EVENT_MONTH}:\n")
for p in plans:
    print(f"  {p['candidate_plan_name']:<22} base ${p['base_charge']:<7} "
          f"overage ${p['projected_overage']:<10} total ${p['projected_total']:<10} {p['recommendation']}")

best = min(plans, key=lambda p: p["projected_total"])
assert best["projected_total"] <= current[0]["projected_total"] + 0.01, (
    "the ranking is wrong — a plan costing more than the current one was ranked first"
)

# COMMAND ----------

# DBTITLE 1,Validate the remaining lookups and boundedness
assert spark.sql(
    f"SELECT * FROM {FQ('detect_overage')}('not-a-customer', '{EVENT_MONTH}')").count() == 0, \
    "unknown customer must return zero rows, not error"

history = spark.sql(f"SELECT * FROM {FQ('lookup_usage_history')}('{over_limit_customer}', 6)")
assert 0 < history.count() <= 200, "usage history must stay bounded"

assets = spark.sql(f"SELECT * FROM {FQ('lookup_inventory_assets')}('{over_limit_customer}')")
assert 0 < assets.count() <= 50, "inventory assets must stay bounded"

a_plan = spark.sql(f"SELECT product_name FROM {FQ('ordm_plan_entitlement')} LIMIT 1").first()["product_name"]
assert spark.sql(f"SELECT * FROM {FQ('lookup_plan_entitlement')}('{a_plan}')").count() == 8, \
    "a plan should expose all eight metered dimensions"

forecast = spark.sql(f"SELECT * FROM {FQ('lookup_usage_forecast')}('{over_limit_customer}')").collect()
assert len(forecast) <= 1, "forecast must return at most one row"
if forecast:
    f = forecast[0]
    print(f"\nforecast: {f['forecast_quantity']} {f['usage_unit']} vs allowance "
          f"{f['included_quantity']} -> {f['projected_status']}")

# COMMAND ----------

# DBTITLE 1,No PII may reach the agent
PII_COLUMNS = {"customer_name", "email", "phone_number"}
for call in [
    f"detect_overage('{over_limit_customer}', '{EVENT_MONTH}')",
    f"recommend_plan_upgrade('{over_limit_customer}', '{EVENT_MONTH}')",
    f"lookup_usage_history('{over_limit_customer}', 6)",
    f"lookup_inventory_assets('{over_limit_customer}')",
    f"lookup_usage_forecast('{over_limit_customer}')",
    f"lookup_plan_entitlement('{a_plan}')",
]:
    fn, args = call.split("(", 1)
    cols = {c.lower() for c in spark.sql(f"SELECT * FROM {FQ(fn)}({args}").columns}
    leaked = PII_COLUMNS & cols
    assert not leaked, f"{fn} leaks PII columns: {leaked}"

print("no PII in any function signature")
print("\nAll checks passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Grants and next steps
# MAGIC
# MAGIC ```sql
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.detect_overage           TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.recommend_plan_upgrade   TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_usage_history     TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_plan_entitlement  TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_inventory_assets  TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_usage_forecast    TO `<agent-sp>`;
# MAGIC ```
# MAGIC
# MAGIC Re-run `09_writeback_setup` so catalog commits are enabled on `customers` and
# MAGIC `ordm_customer_contract` — `submit_auto_upgrade` writes to both, and the table list
# MAGIC comes from the write action registry, so it picks them up on its own.
# MAGIC
# MAGIC Then re-run `03_agent_deployment_and_evaluation` to redeploy.
# MAGIC
# MAGIC ### Refreshing the forecast
# MAGIC
# MAGIC `usage_by_asset_month` and `usage_forecast` are materialised. Schedule this notebook
# MAGIC (or just those two cells) monthly, after billing closes.
# MAGIC
# MAGIC ### Demo script
# MAGIC
# MAGIC | Ask | What the agent should do |
# MAGIC |-----|--------------------------|
# MAGIC | *"Why did my bill jump this month?"* | `detect_overage` → reads back the per-dimension explanation |
# MAGIC | *"Am I going to go over again?"* | `lookup_usage_forecast` |
# MAGIC | *"Would a different plan be cheaper?"* | `recommend_plan_upgrade` — prices real usage against every plan |
# MAGIC | *"What's included in my plan?"* | `lookup_plan_entitlement` |
# MAGIC | *"How has my usage been trending?"* | `lookup_usage_history` |
# MAGIC | *"What lines are on my account?"* | `lookup_inventory_assets` |
# MAGIC | *"Move me to that plan."* | `request_write_confirmation` with `submit_auto_upgrade` |
# MAGIC
# MAGIC ### Staging an upgrade
# MAGIC
# MAGIC ```python
# MAGIC request_write_confirmation(
# MAGIC     action="submit_auto_upgrade",
# MAGIC     target_id="4401",
# MAGIC     customer_id="4401",
# MAGIC     reason="Overage three months running; moving to UNLIMITED SIM24",
# MAGIC     extra='{"new_plan_key": 8, "new_product_id": "PLAN008"}',
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC The customer record and the contract move together. Applying only one leaves the
# MAGIC customer billed on a plan their contract does not name — which is precisely what
# MAGIC `detect_pricing_drift` reports as `ContractMismatch`.
