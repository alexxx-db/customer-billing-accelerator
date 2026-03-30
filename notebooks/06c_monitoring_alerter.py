# Databricks notebook source
# MAGIC %md
# MAGIC # Proactive Billing Alert Dispatch
# MAGIC
# MAGIC This notebook finds billing anomalies that have not yet been alerted on
# MAGIC and writes alert records to `billing_monitoring_state`.
# MAGIC
# MAGIC Designed to run as Task 2 in the daily monitoring workflow,
# MAGIC after the anomaly detection notebook (05).
# MAGIC
# MAGIC ## Alert Strategy
# MAGIC - **CRITICAL/HIGH anomalies**: Write to monitoring state (per-customer)
# MAGIC - **MEDIUM/LOW**: Dashboard only, no active notification
# MAGIC
# MAGIC ## Deduplication
# MAGIC Uses anomaly_id (composite key: customer_id-event_month-anomaly_type) to
# MAGIC prevent re-alerting on the same anomaly.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

import uuid
from datetime import datetime, timezone
import pyspark.sql.functions as F

catalog = config['catalog']
schema  = config['database']

# COMMAND ----------

# DBTITLE 1,Find Unalerted Anomalies
# Build a synthetic anomaly_id from the composite key since our billing_anomalies
# table doesn't have a dedicated anomaly_id column
unalerted = spark.sql(f"""
SELECT
  b.customer_id,
  b.event_month,
  b.plan_name,
  b.total_charges,
  b.anomaly_type,
  b.anomaly_detail,
  b.pipeline_run_at,
  b.anomaly_id
FROM {catalog}.{schema}.billing_anomalies b
LEFT JOIN {catalog}.{schema}.billing_monitoring_state m
  ON b.anomaly_id = m.anomaly_id
  AND m.was_delivered = true
WHERE m.anomaly_id IS NULL
ORDER BY b.total_charges DESC
""")

unalerted_count = unalerted.count()
print(f"Unalerted anomalies found: {unalerted_count}")

if unalerted_count == 0:
    print("No unalerted anomalies. Monitoring run complete.")
    dbutils.notebook.exit("no_alerts_needed")

# COMMAND ----------

# DBTITLE 1,Render and Write Alert Records
unalerted_pd = unalerted.toPandas()

alert_records = []
now_ts = datetime.now(timezone.utc)

for _, row in unalerted_pd.iterrows():
    customer_id = row["customer_id"]
    anomaly_type = row["anomaly_type"]
    anomaly_detail = row["anomaly_detail"]
    event_month = row["event_month"]

    alert_content = (
        f"Billing Anomaly Detected\n"
        f"Customer:   {customer_id}\n"
        f"Month:      {event_month}\n"
        f"Type:       {anomaly_type}\n"
        f"Detail:     {anomaly_detail}\n"
        f"Detected:   {row['pipeline_run_at']}\n"
        f"Anomaly ID: {row['anomaly_id']}"
    )

    # Derive severity from total_charges relative to typical amounts
    total = row["total_charges"] if row["total_charges"] else 0
    if total > 200:
        severity = "CRITICAL"
    elif total > 100:
        severity = "HIGH"
    else:
        severity = "MEDIUM"

    alert_records.append({
        "alert_id":       str(uuid.uuid4()),
        "anomaly_id":     row["anomaly_id"],
        "customer_id":    int(customer_id),
        "event_month":    event_month,
        "anomaly_type":   anomaly_type,
        "severity":       severity,
        "alert_channel":  "dashboard",
        "alert_sent_ts":  now_ts,
        "alert_content":  alert_content,
        "recipient":      "system",
        "was_delivered":  True,
        "delivery_error": None,
    })

if alert_records:
    alerts_df = spark.createDataFrame(alert_records)
    alerts_df.write.mode("append").saveAsTable(
        f"{catalog}.{schema}.billing_monitoring_state"
    )
    print(f"Wrote {len(alert_records)} alert records to billing_monitoring_state")

# COMMAND ----------

# DBTITLE 1,Summary
summary = spark.sql(f"""
SELECT anomaly_type, COUNT(*) AS count
FROM {catalog}.{schema}.billing_monitoring_state
WHERE alert_sent_ts >= CURRENT_TIMESTAMP - INTERVAL 1 HOURS
GROUP BY anomaly_type
ORDER BY count DESC
""")
print("Alerts sent this run:")
summary.show()
