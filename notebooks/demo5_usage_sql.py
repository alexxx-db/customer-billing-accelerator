"""Demo #5 — usage-based billing with inventory.

One module so the notebook and its integration test run byte-identical SQL.

Everything here is normalised to the *entitlement unit*: data in MB, calls in
minutes, texts as a count. That is what makes a single `detect_overage` work
across every metered dimension instead of one branch per charge type.
"""

def ordm_inventory_asset(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.ordm_inventory_asset
CLUSTER BY (customer_id, asset_id)
COMMENT 'ORDM Inventory canonical core. For telco the metered asset is the SIM/device, so this projects customers.device_id and joins its observed activity. asset_type and asset_status are derived from real activity, not synthesised.'
AS
WITH activity AS (
  SELECT device_id,
         MIN(event_ts) AS first_event_ts,
         MAX(event_ts) AS last_event_ts,
         COUNT(*)      AS lifetime_event_count
  FROM {c}.{s}.billing_items
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
FROM {c}.{s}.customers cu
LEFT JOIN activity a ON a.device_id = cu.device_id
LEFT JOIN {c}.{s}.billing_plans p ON p.Plan_key = cu.plan
"""


def ordm_plan_entitlement(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.ordm_plan_entitlement
CLUSTER BY (product_id, entitlement_type)
COMMENT 'ORDM plan entitlements in long form, one row per metered dimension. Quantities are normalised to the overage unit (MB, minutes, texts) so overage is one calculation everywhere. included_quantity NULL means unlimited. Derived entirely from billing_plans — no synthetic values.'
AS
SELECT CONCAT(Plan_id, ':data_local') AS entitlement_id, Plan_id AS product_id, Plan_name AS product_name,
       'data_local' AS entitlement_type,
       TRY_CAST(Data_Limit_GB AS DOUBLE) * 1024.0 AS included_quantity, 'MB' AS usage_unit,
       Data_Outside_Allowance_Per_MB AS overage_rate
FROM {c}.{s}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':data_roaming'), Plan_id, Plan_name, 'data_roaming',
       0.0, 'MB', Roam_Data_charges_per_MB FROM {c}.{s}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':call_mins_roaming'), Plan_id, Plan_name, 'call_mins_roaming',
       0.0, 'min', Roam_Call_charges_per_min FROM {c}.{s}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':texts_roaming'), Plan_id, Plan_name, 'texts_roaming',
       0.0, 'text', Roam_text_charges FROM {c}.{s}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':call_mins_international'), Plan_id, Plan_name, 'call_mins_international',
       0.0, 'min', International_call_charge_per_min FROM {c}.{s}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':texts_international'), Plan_id, Plan_name, 'texts_international',
       0.0, 'text', International_text_charge FROM {c}.{s}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':call_mins_local'), Plan_id, Plan_name, 'call_mins_local',
       CAST(NULL AS DOUBLE), 'min', 0.0 FROM {c}.{s}.billing_plans
UNION ALL
SELECT CONCAT(Plan_id, ':texts_local'), Plan_id, Plan_name, 'texts_local',
       CAST(NULL AS DOUBLE), 'text', 0.0 FROM {c}.{s}.billing_plans
"""


def usage_by_asset_month(c, s):
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.usage_by_asset_month
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
FROM {c}.{s}.billing_items bi
JOIN {c}.{s}.customers cu ON cu.device_id = bi.device_id
GROUP BY cu.customer_id, bi.device_id, DATE_FORMAT(bi.event_ts, 'yyyy-MM'), bi.event_type
"""


def usage_forecast(c, s):
    """Real forecasting via ai_forecast, not a model invented inside a SQL function."""
    return f"""
CREATE OR REPLACE TABLE {c}.{s}.usage_forecast
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
    FROM {c}.{s}.usage_by_asset_month
    WHERE entitlement_type = 'data_local'
    GROUP BY customer_id, TO_DATE(CONCAT(event_month, '-01'))
  ),
  horizon    => ADD_MONTHS(CURRENT_DATE(), 1),
  time_col   => 'ds',
  value_col  => 'y',
  group_col  => 'customer_id'
)
"""


def fn_detect_overage(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.detect_overage(
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
  FROM {c}.{s}.usage_by_asset_month u
  WHERE u.customer_id = TRY_CAST(input_customer AS DECIMAL)
    AND u.event_month = input_event_month
  GROUP BY u.customer_id, u.event_month, u.entitlement_type, u.usage_unit
),
scoped AS (
  SELECT usage.*, e.product_name AS plan_name, e.included_quantity, e.overage_rate
  FROM usage
  JOIN {c}.{s}.customers cu ON cu.customer_id = usage.customer_id
  JOIN {c}.{s}.billing_plans p ON p.Plan_key = cu.plan
  JOIN {c}.{s}.ordm_plan_entitlement e
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
"""


def fn_lookup_usage_history(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_usage_history(
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
FROM {c}.{s}.usage_by_asset_month
WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
  AND event_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -LEAST(lookback_months, 24)), 'yyyy-MM')
GROUP BY customer_id, event_month, entitlement_type
ORDER BY event_month DESC, entitlement_type
LIMIT 200
"""


def fn_lookup_plan_entitlement(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_plan_entitlement(
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
FROM {c}.{s}.ordm_plan_entitlement
WHERE product_name = input_plan OR product_id = input_plan
ORDER BY entitlement_type
LIMIT 20
"""


def fn_lookup_inventory_assets(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_inventory_assets(
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
FROM {c}.{s}.ordm_inventory_asset
WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
ORDER BY asset_status, last_event_ts DESC
LIMIT 50
"""


def fn_lookup_usage_forecast(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.lookup_usage_forecast(
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
FROM {c}.{s}.usage_forecast f
JOIN {c}.{s}.customers cu ON cu.customer_id = f.customer_id
JOIN {c}.{s}.billing_plans p ON p.Plan_key = cu.plan
JOIN {c}.{s}.ordm_plan_entitlement e
  ON e.product_id = p.Plan_id AND e.entitlement_type = f.entitlement_type
WHERE f.customer_id = TRY_CAST(input_customer AS DECIMAL)
LIMIT 1
"""


def fn_recommend_plan_upgrade(c, s):
    return f"""
CREATE OR REPLACE FUNCTION {c}.{s}.recommend_plan_upgrade(
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
  FROM {c}.{s}.usage_by_asset_month
  WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
    AND event_month = input_event_month
  GROUP BY entitlement_type
),
current_plan AS (
  SELECT MAX(p.Plan_key) AS Plan_key
  FROM {c}.{s}.customers cu
  JOIN {c}.{s}.billing_plans p ON p.Plan_key = cu.plan
  WHERE cu.customer_id = TRY_CAST(input_customer AS DECIMAL)
),
priced AS (
  SELECT
    p.Plan_key, p.Plan_name, p.monthly_charges_dollars AS base_charge,
    SUM(CASE WHEN e.included_quantity IS NULL THEN 0.0
             ELSE GREATEST(u.usage_quantity - e.included_quantity, 0.0) * e.overage_rate
        END) AS projected_overage
  FROM {c}.{s}.billing_plans p
  JOIN {c}.{s}.ordm_plan_entitlement e ON e.product_id = p.Plan_id
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
"""


ALL_TABLES = [ordm_inventory_asset, ordm_plan_entitlement, usage_by_asset_month, usage_forecast]
ALL_FUNCTIONS = [fn_detect_overage, fn_lookup_usage_history, fn_lookup_plan_entitlement,
                 fn_lookup_inventory_assets, fn_lookup_usage_forecast, fn_recommend_plan_upgrade]
