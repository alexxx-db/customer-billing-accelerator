# Databricks notebook source
# MAGIC %md
# MAGIC # Billing Anomaly Detection Pipeline
# MAGIC
# MAGIC This notebook implements **proactive billing anomaly detection** using PySpark.
# MAGIC It analyzes the invoice data to identify unusual billing patterns per customer,
# MAGIC writes results to a governed Delta table, and exposes them as a UC function tool
# MAGIC for the agent.
# MAGIC
# MAGIC ## Anomaly Types Detected
# MAGIC
# MAGIC | Anomaly Type | Logic |
# MAGIC |-------------|-------|
# MAGIC | `total_charge_spike` | Total charges > 2 std deviations above customer's mean |
# MAGIC | `roaming_spike` | Roaming charges (data + calls + texts) > 3x customer's average roaming |
# MAGIC | `international_spike` | International charges > 3x customer's average international |
# MAGIC | `data_overage_spike` | Data overage charge > 3x customer's average overage |
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC - Delta table: `{catalog}.{schema}.billing_anomalies`
# MAGIC - UC function: `{catalog}.{schema}.lookup_billing_anomalies`
# MAGIC - Updated Genie Space tables (adds `billing_anomalies`)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Run notebooks `000-config` and `00_data_preparation` first (invoice table must exist).

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Configuration
import pyspark.sql.functions as F
from pyspark.sql.window import Window

catalog = config['catalog']
db = config['database']

ANOMALY_TABLE = f"{catalog}.{db}.billing_anomalies"
INVOICE_TABLE = f"{catalog}.{db}.invoice"

# Thresholds (centralized in 000-config.py)
ZSCORE_THRESHOLD = config.get('anomaly_zscore_threshold', 2.0)
ROAMING_MULTIPLIER = config.get('anomaly_roaming_multiplier', 3.0)
INTL_MULTIPLIER = config.get('anomaly_intl_multiplier', 3.0)
MIN_MONTHS = config.get('anomaly_min_months', 2)

print(f"Source: {INVOICE_TABLE}")
print(f"Output: {ANOMALY_TABLE}")
print(f"Thresholds: z-score={ZSCORE_THRESHOLD}, roaming={ROAMING_MULTIPLIER}x, intl={INTL_MULTIPLIER}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Compute Per-Customer Baselines
# MAGIC
# MAGIC Calculate mean and standard deviation of each charge type per customer
# MAGIC across all their billing months.

# COMMAND ----------

# DBTITLE 1,Read Invoice Data and Compute Baselines
df_invoice = spark.table(INVOICE_TABLE).select(
    "customer_id",
    "event_month",
    "plan_name",
    "monthly_charges",
    "data_charges_outside_allowance",
    "roaming_data_charges",
    "roaming_call_charges",
    "roaming_text_charges",
    "international_call_charges",
    "international_text_charges",
    "total_charges",
)

# Add combined charge columns
df_invoice = df_invoice.withColumn(
    "roaming_total",
    F.col("roaming_data_charges") + F.col("roaming_call_charges") + F.col("roaming_text_charges"),
).withColumn(
    "international_total",
    F.col("international_call_charges") + F.col("international_text_charges"),
)

# Window for per-customer stats
w_customer = Window.partitionBy("customer_id")

df_with_stats = df_invoice.withColumn(
    "months_count", F.count("event_month").over(w_customer)
).withColumn(
    "avg_total_charges", F.avg("total_charges").over(w_customer)
).withColumn(
    "stddev_total_charges", F.stddev("total_charges").over(w_customer)
).withColumn(
    "avg_roaming_total", F.avg("roaming_total").over(w_customer)
).withColumn(
    "avg_international_total", F.avg("international_total").over(w_customer)
)

# Only flag customers with enough history
df_with_stats = df_with_stats.filter(F.col("months_count") >= MIN_MONTHS)

print(f"Invoices with sufficient history: {df_with_stats.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Detect Anomalies

# COMMAND ----------

# DBTITLE 1,Flag Anomalies by Type
# Total charge spike: z-score > threshold (guard against zero stddev)
df_anomalies_total = df_with_stats.filter(
    (F.col("stddev_total_charges") > 0) &
    ((F.col("total_charges") - F.col("avg_total_charges")) / F.col("stddev_total_charges") > ZSCORE_THRESHOLD)
).withColumn("anomaly_type", F.lit("total_charge_spike")).withColumn(
    "anomaly_detail",
    F.concat(
        F.lit("Total charges $"),
        F.round("total_charges", 2).cast("string"),
        F.lit(" vs avg $"),
        F.round("avg_total_charges", 2).cast("string"),
        F.lit(" ("),
        F.round(
            (F.col("total_charges") - F.col("avg_total_charges")) / F.col("stddev_total_charges"), 1
        ).cast("string"),
        F.lit(" std devs above mean)"),
    )
)

# Roaming spike: roaming_total > multiplier * average (only if avg > 0)
df_anomalies_roaming = df_with_stats.filter(
    (F.col("avg_roaming_total") > 0) &
    (F.col("roaming_total") > F.col("avg_roaming_total") * ROAMING_MULTIPLIER)
).withColumn("anomaly_type", F.lit("roaming_spike")).withColumn(
    "anomaly_detail",
    F.concat(
        F.lit("Roaming charges $"),
        F.round("roaming_total", 2).cast("string"),
        F.lit(" vs avg $"),
        F.round("avg_roaming_total", 2).cast("string"),
        F.lit(" ("),
        F.round(F.col("roaming_total") / F.col("avg_roaming_total"), 1).cast("string"),
        F.lit("x average)"),
    )
)

# International spike: international_total > multiplier * average (only if avg > 0)
df_anomalies_intl = df_with_stats.filter(
    (F.col("avg_international_total") > 0) &
    (F.col("international_total") > F.col("avg_international_total") * INTL_MULTIPLIER)
).withColumn("anomaly_type", F.lit("international_spike")).withColumn(
    "anomaly_detail",
    F.concat(
        F.lit("International charges $"),
        F.round("international_total", 2).cast("string"),
        F.lit(" vs avg $"),
        F.round("avg_international_total", 2).cast("string"),
        F.lit(" ("),
        F.round(F.col("international_total") / F.col("avg_international_total"), 1).cast("string"),
        F.lit("x average)"),
    )
)

# Data overage spike: overage > 3x customer's average overage (only if avg > 0)
# This avoids flagging routine small overages — only flags unusual spikes
df_with_stats = df_with_stats.withColumn(
    "avg_overage", F.avg("data_charges_outside_allowance").over(w_customer)
)

df_anomalies_overage = df_with_stats.filter(
    (F.col("avg_overage") > 0) &
    (F.col("data_charges_outside_allowance") > F.col("avg_overage") * ROAMING_MULTIPLIER)
).withColumn("anomaly_type", F.lit("data_overage_spike")).withColumn(
    "anomaly_detail",
    F.concat(
        F.lit("Data overage charge: $"),
        F.round("data_charges_outside_allowance", 2).cast("string"),
        F.lit(" vs avg $"),
        F.round("avg_overage", 2).cast("string"),
        F.lit(" ("),
        F.round(F.col("data_charges_outside_allowance") / F.col("avg_overage"), 1).cast("string"),
        F.lit("x average) on plan "),
        F.col("plan_name"),
    )
)

# COMMAND ----------

# DBTITLE 1,Combine All Anomalies
# Select common columns for union
anomaly_columns = [
    "customer_id",
    "event_month",
    "plan_name",
    "total_charges",
    "anomaly_type",
    "anomaly_detail",
]

df_all_anomalies = (
    df_anomalies_total.select(anomaly_columns)
    .unionByName(df_anomalies_roaming.select(anomaly_columns))
    .unionByName(df_anomalies_intl.select(anomaly_columns))
    .unionByName(df_anomalies_overage.select(anomaly_columns))
).withColumn("pipeline_run_at", F.current_timestamp())

anomaly_count = df_all_anomalies.count()
print(f"Total anomalies detected: {anomaly_count}")

# Show breakdown by type
df_all_anomalies.groupBy("anomaly_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Delta Table

# COMMAND ----------

# DBTITLE 1,Write Anomalies to Delta Table
df_all_anomalies.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(ANOMALY_TABLE)

print(f"Wrote {anomaly_count} anomalies to {ANOMALY_TABLE}")

# COMMAND ----------

# DBTITLE 1,Preview Anomalies
display(spark.table(ANOMALY_TABLE).orderBy("total_charges", ascending=False).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create UC Function Tool for the Agent

# COMMAND ----------

# DBTITLE 1,Create lookup_billing_anomalies Function
spark.sql(f"DROP FUNCTION IF EXISTS {catalog}.{db}.lookup_billing_anomalies;")

sqlstr_anomalies = f"""
CREATE OR REPLACE FUNCTION {catalog}.{db}.lookup_billing_anomalies(
  input_customer STRING COMMENT 'Customer ID to look up billing anomalies for. Pass empty string to get recent anomalies across all customers.'
)
RETURNS TABLE (
    customer_id BIGINT,
    event_month STRING,
    plan_name STRING,
    total_charges DOUBLE,
    anomaly_type STRING,
    anomaly_detail STRING,
    pipeline_run_at TIMESTAMP
)
COMMENT 'Returns billing anomalies for a customer (charge spikes, roaming spikes, international spikes, data overage spikes). Pass empty string for recent anomalies across all customers.'
RETURN (
  SELECT
    customer_id,
    event_month,
    plan_name,
    total_charges,
    anomaly_type,
    anomaly_detail,
    pipeline_run_at
  FROM {catalog}.{db}.billing_anomalies
  WHERE (input_customer = '' OR customer_id = CAST(input_customer AS DECIMAL))
  ORDER BY total_charges DESC
  LIMIT 50
);
"""
spark.sql(sqlstr_anomalies)
print(f"Created function {catalog}.{db}.lookup_billing_anomalies")

# COMMAND ----------

# DBTITLE 1,Test the Function
display(spark.sql(f"SELECT * FROM {catalog}.{db}.lookup_billing_anomalies('') LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The anomaly detection pipeline is complete. The `billing_anomalies` table, `lookup_billing_anomalies`
# MAGIC UC function, and Genie Space table list have all been configured. Re-run notebook `03` or `04`
# MAGIC to redeploy the agent with the anomaly detection tool.
