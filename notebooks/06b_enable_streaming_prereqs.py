# Databricks notebook source
# MAGIC %md
# MAGIC ## Prerequisite: Enable streaming infrastructure
# MAGIC Run this ONCE before creating the DLT pipeline.
# MAGIC Safe to re-run — all operations are idempotent.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

catalog = config['catalog']
schema  = config['database']

# ── Step 1: Enable CDF on billing_items (required for DLT streaming read) ──
spark.sql(f"""
ALTER TABLE {catalog}.{schema}.billing_items
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("CDF enabled on billing_items")

# ── Step 2: Enable CDF on billing_anomalies (required for monitoring queries) ──
spark.sql(f"""
ALTER TABLE {catalog}.{schema}.billing_anomalies
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("CDF confirmed on billing_anomalies")

# ── Step 3: Create billing_monitoring_state ──────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.billing_monitoring_state (
  alert_id          STRING     NOT NULL,
  anomaly_id        STRING     NOT NULL,
  customer_id       BIGINT,
  event_month       STRING,
  anomaly_type      STRING,
  severity          STRING,
  alert_channel     STRING,
  alert_sent_ts     TIMESTAMP,
  alert_content     STRING,
  recipient         STRING,
  was_delivered     BOOLEAN,
  delivery_error    STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Audit log of all proactive monitoring alerts dispatched by the system'
""")
print("billing_monitoring_state created")

# ── Step 4: Create billing_monitoring_summary view ───────────────────────────
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{schema}.billing_monitoring_summary AS
SELECT
  b.event_month,
  b.anomaly_type,
  COUNT(DISTINCT b.anomaly_id)                                                    AS total_anomalies,
  COUNT(DISTINCT m.anomaly_id)                                                    AS alerted_count,
  COUNT(DISTINCT b.anomaly_id)
    - COUNT(DISTINCT m.anomaly_id)                                                AS pending_alert_count,
  MAX(b.pipeline_run_at)                                                          AS last_detection_ts,
  MAX(m.alert_sent_ts)                                                            AS last_alert_ts
FROM {catalog}.{schema}.billing_anomalies b
LEFT JOIN {catalog}.{schema}.billing_monitoring_state m
  ON b.anomaly_id = m.anomaly_id
  AND m.was_delivered = true
GROUP BY b.event_month, b.anomaly_type
""")
print("billing_monitoring_summary view created")

# ── Step 5: Validate ─────────────────────────────────────────────────────────
for table in ["billing_items", "billing_anomalies", "billing_monitoring_state"]:
    cdf_row = (spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.{table}")
               .filter("col_name = 'delta.enableChangeDataFeed'")
               .collect())
    status = cdf_row[0]["data_type"] if cdf_row else "NOT SET"
    print(f"  {table}: CDF = {status}")

print("\nPrerequisite setup complete. Ready to create DLT pipeline (notebook 06a).")
