# Databricks notebook source
# MAGIC %md
# MAGIC # Write-Back Infrastructure Setup
# MAGIC
# MAGIC Creates the `billing_disputes` and `billing_write_audit` Delta tables,
# MAGIC adds acknowledgement columns to `billing_anomalies`, and optionally
# MAGIC creates the Lakebase `agent_kpi_context` table.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

catalog = config['catalog']
schema = config['database']

# Guard: billing_anomalies must exist
try:
    spark.table(f"{catalog}.{schema}.billing_anomalies").limit(1)
    print("billing_anomalies exists")
except Exception as e:
    raise RuntimeError(f"billing_anomalies not found. Run notebook 05 first.\nError: {e}") from e

# COMMAND ----------

# DBTITLE 1,Add acknowledgement columns to billing_anomalies
for col_name, col_type, comment in [
    ("acknowledged_by",       "STRING",    "Who acknowledged"),
    ("acknowledged_at",       "TIMESTAMP", "When acknowledged"),
    ("acknowledgement_reason","STRING",    "Why acknowledged or dismissed"),
    ("linked_dispute_id",     "STRING",    "FK to billing_disputes.dispute_id"),
]:
    try:
        spark.sql(f"""
            ALTER TABLE {catalog}.{schema}.billing_anomalies
            ADD COLUMN IF NOT EXISTS {col_name} {col_type} COMMENT '{comment}'
        """)
        print(f"  Column {col_name} ensured")
    except Exception as e:
        print(f"  Could not add {col_name}: {e}")

# COMMAND ----------

# DBTITLE 1,Create billing_disputes
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.billing_disputes (
  dispute_id           STRING     NOT NULL,
  customer_id          BIGINT     NOT NULL,
  anomaly_id           STRING,
  event_month          STRING,
  dispute_type         STRING     NOT NULL,
  status               STRING     NOT NULL,
  description          STRING     NOT NULL,
  resolution_notes     STRING,
  disputed_amount_usd  DOUBLE,
  resolved_amount_usd  DOUBLE,
  created_by           STRING     NOT NULL,
  created_at           TIMESTAMP  NOT NULL,
  updated_at           TIMESTAMP  NOT NULL,
  resolved_at          TIMESTAMP,
  assigned_to          STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'delta.columnMapping.mode' = 'name')
COMMENT 'Billing dispute records created by the AI agent or human agents'
""")
print("billing_disputes created/verified")

# COMMAND ----------

# DBTITLE 1,Create billing_write_audit
# Uses two-INSERT pattern (PENDING then SUCCESS/FAILED) — no UPDATEs needed
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.billing_write_audit (
  audit_id             STRING     NOT NULL,
  action_type          STRING     NOT NULL,
  target_table         STRING     NOT NULL,
  target_record_id     STRING,
  customer_id          BIGINT,
  agent_session_id     STRING,
  executed_by          STRING     NOT NULL,
  payload_json         STRING,
  sql_statement        STRING,
  result_status        STRING     NOT NULL,
  result_message       STRING,
  error_detail         STRING,
  executed_at          TIMESTAMP  NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Immutable write audit log. Uses two-INSERT pattern: PENDING before write, then SUCCESS/FAILED after.'
""")
print("billing_write_audit created/verified")

# COMMAND ----------

# DBTITLE 1,Validation
for tbl in ["billing_anomalies", "billing_disputes", "billing_write_audit"]:
    try:
        count = spark.table(f"{catalog}.{schema}.{tbl}").count()
        print(f"  {tbl}: {count} existing rows")
    except Exception as e:
        print(f"  {tbl}: ERROR — {e}")

print("\nWrite-back infrastructure setup complete.")
