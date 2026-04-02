# Databricks notebook source
# MAGIC %md
# MAGIC # System Table Telemetry Ingestion
# MAGIC
# MAGIC Materializes Databricks system tables into the user catalog as governed Bronze/Silver/Gold
# MAGIC telemetry tables. System tables (`system.*`) are read-only and cannot be added to Genie
# MAGIC Spaces directly — this notebook snapshots them into the user catalog for analytics.
# MAGIC
# MAGIC ## Pipeline
# MAGIC
# MAGIC ```
# MAGIC system.billing.usage            → telemetry_dbu_usage (Bronze) → telemetry_dbu_daily (Silver)
# MAGIC system.lakeflow.job_run_timeline → telemetry_job_runs (Bronze)  → telemetry_job_reliability (Silver)
# MAGIC system.query.history            → telemetry_query_history (Bronze) → telemetry_warehouse_utilization (Silver)
# MAGIC                                                                      ↓
# MAGIC                                                          telemetry_operational_kpis (Gold)
# MAGIC ```
# MAGIC
# MAGIC ## Graceful Degradation
# MAGIC
# MAGIC System table availability depends on workspace tier and admin enablement. Each source
# MAGIC is probed before ingestion. Unavailable sources are skipped, and the Gold KPI table
# MAGIC writes NULL for the missing fields.

# COMMAND ----------

# DBTITLE 1,Guard: Confirm system catalog access
try:
    accessible = spark.sql("SHOW SCHEMAS IN system").count()
    print(f"system catalog accessible: {accessible} schemas found")
except Exception as e:
    raise RuntimeError(
        f"system catalog is not accessible in this workspace: {e}\n"
        "Ensure 'system' catalog is enabled for this workspace in Account Console "
        "under Settings > System Tables."
    ) from e

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Parameters and date range
import pyspark.sql.functions as F
from datetime import datetime, timedelta, timezone
from delta.tables import DeltaTable

dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD, blank = 30 days ago)")
dbutils.widgets.text("end_date",   "", "End date   (YYYY-MM-DD, blank = today)")

raw_start = dbutils.widgets.get("start_date").strip()
raw_end   = dbutils.widgets.get("end_date").strip()

today = datetime.now(timezone.utc).date()
start_date = datetime.strptime(raw_start, "%Y-%m-%d").date() if raw_start else (today - timedelta(days=30))
end_date   = datetime.strptime(raw_end,   "%Y-%m-%d").date() if raw_end   else today

catalog = config['catalog']
schema  = config['database']

print(f"Ingestion window: {start_date} -> {end_date}")

# COMMAND ----------

# DBTITLE 1,Derive workspace_id
# Resolve workspace_id — needed to filter system tables to this workspace.
# spark.conf clusterUsageTags don't exist on serverless, so we cascade:
#   1. Notebook context (works on all compute types incl. serverless)
#   2. Databricks SDK
#   3. spark.conf fallback (works on some runtimes)
workspace_id = ""

# Method 1: notebook context
try:
    workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None) or ""
except Exception:
    pass

# Method 2: Databricks SDK
if not workspace_id:
    try:
        from databricks.sdk import WorkspaceClient
        workspace_id = str(WorkspaceClient().get_workspace_id())
    except Exception:
        pass

# Method 3: spark conf (classic clusters)
if not workspace_id:
    try:
        workspace_id = spark.conf.get("spark.databricks.workspaceId", "")
    except Exception:
        pass

if workspace_id:
    print(f"Workspace ID: {workspace_id}")
else:
    print("WARNING: Could not determine workspace_id. System tables won't be filtered by workspace.")

# COMMAND ----------

# DBTITLE 1,Probe system table availability
def probe_system_table(fqtn):
    try:
        spark.table(fqtn).limit(0).collect()
        return True
    except Exception:
        return False

SYSTEM_TABLES = {
    "billing_usage":       "system.billing.usage",
    "billing_list_prices": "system.billing.list_prices",
    "jobs":                "system.lakeflow.jobs",
    "job_run_timeline":    "system.lakeflow.job_run_timeline",
    "query_history":       "system.query.history",
    "clusters":            "system.compute.clusters",
}

availability = {}
for alias, fqtn in SYSTEM_TABLES.items():
    ok = probe_system_table(fqtn)
    availability[alias] = ok
    print(f"{'OK' if ok else 'UNAVAILABLE':>12}: {fqtn}")

# COMMAND ----------

# DBTITLE 1,Create Bronze tables (idempotent DDL)

# telemetry_dbu_usage
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.telemetry_dbu_usage (
  record_id         STRING,
  workspace_id      STRING,
  usage_date        DATE,
  usage_start_time  TIMESTAMP,
  usage_end_time    TIMESTAMP,
  sku_name          STRING,
  usage_quantity    DOUBLE,
  usage_unit        STRING,
  billing_origin_product STRING,
  job_id            STRING,
  cluster_id        STRING,
  endpoint_name     STRING,
  warehouse_id      STRING,
  usage_type        STRING,
  ingested_at       TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Bronze: snapshot of system.billing.usage for this workspace'
""")

# telemetry_job_runs
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.telemetry_job_runs (
  workspace_id          STRING,
  job_id                STRING,
  job_name              STRING,
  run_id                STRING,
  run_name              STRING,
  run_type              STRING,
  start_time            TIMESTAMP,
  end_time              TIMESTAMP,
  result_state          STRING,
  run_duration_minutes  DOUBLE,
  trigger_type          STRING,
  creator_user_name     STRING,
  ingested_at           TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Bronze: snapshot of system.lakeflow.job_run_timeline joined with job names'
""")

# telemetry_query_history
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.telemetry_query_history (
  statement_id          STRING,
  workspace_id          STRING,
  warehouse_id          STRING,
  execution_status      STRING,
  statement_type        STRING,
  total_duration_ms     BIGINT,
  execution_duration_ms BIGINT,
  waiting_at_capacity_ms BIGINT,
  read_rows             BIGINT,
  produced_rows         BIGINT,
  start_time            TIMESTAMP,
  end_time              TIMESTAMP,
  client_application    STRING,
  ingested_at           TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Bronze: snapshot of system.query.history for configured warehouse'
""")

# Silver tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.telemetry_dbu_daily (
  usage_date          DATE,
  sku_name            STRING,
  usage_type          STRING,
  dbu_quantity        DOUBLE,
  estimated_cost_usd  DOUBLE,
  job_id              STRING,
  warehouse_id        STRING,
  endpoint_name       STRING
)
USING DELTA
COMMENT 'Silver: daily DBU aggregates by SKU and usage type'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.telemetry_job_reliability (
  job_id                STRING,
  job_name              STRING,
  run_count_30d         BIGINT,
  success_count_30d     BIGINT,
  failure_count_30d     BIGINT,
  success_rate_pct      DOUBLE,
  avg_duration_minutes  DOUBLE,
  p95_duration_minutes  DOUBLE,
  last_run_state        STRING,
  last_run_ts           TIMESTAMP,
  is_billing_pipeline   BOOLEAN
)
USING DELTA
COMMENT 'Silver: rolling 30-day job reliability metrics'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.telemetry_warehouse_utilization (
  warehouse_id        STRING,
  hour_bucket         TIMESTAMP,
  query_count         BIGINT,
  failed_query_count  BIGINT,
  p50_duration_ms     BIGINT,
  p95_duration_ms     BIGINT,
  p99_duration_ms     BIGINT,
  total_queuing_ms    BIGINT,
  genie_query_count   BIGINT,
  agent_query_count   BIGINT
)
USING DELTA
COMMENT 'Silver: hourly warehouse utilization and query performance'
""")

# Gold table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.telemetry_operational_kpis (
  kpi_date                      DATE,
  total_dbu_consumed            DOUBLE,
  estimated_daily_cost_usd      DOUBLE,
  billing_pipeline_success_rate DOUBLE,
  avg_genie_query_latency_ms    BIGINT,
  genie_query_count             BIGINT,
  warehouse_queuing_pct         DOUBLE,
  anomaly_detection_ran         BOOLEAN,
  anomaly_detection_state       STRING,
  dbu_vs_prior_7d_pct           DOUBLE,
  cost_anomaly_flag             BOOLEAN
)
USING DELTA
COMMENT 'Gold: daily operational health KPIs for the billing platform'
""")

print("All target tables created (idempotent)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Ingestion

# COMMAND ----------

# DBTITLE 1,Bronze: telemetry_dbu_usage
dbu_count = 0
if availability["billing_usage"]:
    dbu_bronze = (
        spark.table("system.billing.usage")
        .filter(
            (F.to_date("usage_start_time") >= F.lit(str(start_date))) &
            (F.to_date("usage_start_time") <= F.lit(str(end_date)))
        )
    )
    if workspace_id:
        dbu_bronze = dbu_bronze.filter(F.col("workspace_id") == workspace_id)

    dbu_bronze = dbu_bronze.select(
        "record_id", "workspace_id",
        F.to_date("usage_start_time").alias("usage_date"),
        "usage_start_time", "usage_end_time",
        "sku_name", "usage_quantity", "usage_unit",
        "billing_origin_product",
        F.col("usage_metadata")["job_id"].alias("job_id"),
        F.col("usage_metadata")["cluster_id"].alias("cluster_id"),
        F.col("usage_metadata")["endpoint_name"].alias("endpoint_name"),
        F.col("usage_metadata")["warehouse_id"].alias("warehouse_id"),
        "usage_type",
        F.current_timestamp().alias("ingested_at"),
    )

    dbu_count = dbu_bronze.count()

    target = DeltaTable.forName(spark, f"{catalog}.{schema}.telemetry_dbu_usage")
    target.alias("t").merge(
        dbu_bronze.alias("s"), "t.record_id = s.record_id"
    ).whenNotMatchedInsertAll().execute()

    print(f"telemetry_dbu_usage: {dbu_count} records")
else:
    print("SKIP: system.billing.usage not accessible")

# COMMAND ----------

from pyspark.sql import functions as F

jobs_count = 0
if availability["job_run_timeline"] and availability["jobs"]:
    # 1. Alias the dataframes immediately to prevent resolution errors
    runs = spark.table("system.lakeflow.job_run_timeline").alias("r").filter(
        (F.to_date("period_start_time") >= F.lit(str(start_date))) &
        (F.to_date("period_start_time") <= F.lit(str(end_date)))
    )
    
    if workspace_id:
        runs = runs.filter(F.col("workspace_id") == workspace_id)

    jobs_meta = spark.table("system.lakeflow.jobs").alias("j")
    if workspace_id:
        jobs_meta = jobs_meta.filter(F.col("workspace_id") == workspace_id)
    
    # Keep the necessary columns for the join and final select
    jobs_meta = jobs_meta.select("job_id", "name", "creator_user_name")

    job_runs = (
        runs.join(jobs_meta, "job_id", "left")
        .select(
            # 2. Use F.col() to explicitly reference the columns
            F.col("r.workspace_id"), 
            F.col("job_id"),
            F.col("name").alias("job_name"),
            F.col("run_id"), 
            F.col("run_name"), 
            F.col("run_type"),
            F.col("period_start_time").alias("start_time"), 
            F.col("period_end_time").alias("end_time"), 
            F.col("result_state"),
            (F.col("run_duration_seconds") / 60).alias("run_duration_minutes"),
            F.col("r.trigger_type"), 
            F.col("creator_user_name"),
            F.current_timestamp().alias("ingested_at")
        )
    )

    jobs_count = job_runs.count()

    target = DeltaTable.forName(spark, f"{catalog}.{schema}.telemetry_job_runs")
    target.alias("t").merge(
        job_runs.alias("s"), "t.run_id = s.run_id"
    ).whenNotMatchedInsertAll().execute()

    print(f"telemetry_job_runs: {jobs_count} records")
else:
    print("SKIP: system.lakeflow.* not accessible")

# COMMAND ----------

# DBTITLE 1,Bronze: telemetry_query_history
queries_count = 0
warehouse_id_filter = config.get("warehouse_id", "")
if availability["query_history"] and warehouse_id_filter:
    queries = (
        spark.table("system.query.history")
        .filter(
            (F.to_date("start_time") >= F.lit(str(start_date))) &
            (F.to_date("start_time") <= F.lit(str(end_date))) &
            (F.col("compute.warehouse_id") == warehouse_id_filter)
        )
    )
    if workspace_id:
        queries = queries.filter(F.col("workspace_id") == workspace_id)

    queries = queries.select(
        "statement_id", "workspace_id",
        F.col("compute.warehouse_id").alias("warehouse_id"),
        "execution_status", "statement_type",
        "total_duration_ms", "execution_duration_ms",
        F.col("waiting_at_capacity_duration_ms").alias("waiting_at_capacity_ms"),
        "read_rows", "produced_rows",
        "start_time", "end_time",
        "client_application",
        F.current_timestamp().alias("ingested_at"),
    )

    queries_count = queries.count()

    target = DeltaTable.forName(spark, f"{catalog}.{schema}.telemetry_query_history")
    target.alias("t").merge(
        queries.alias("s"), "t.statement_id = s.statement_id"
    ).whenNotMatchedInsertAll().execute()

    print(f"telemetry_query_history: {queries_count} records")
elif not warehouse_id_filter:
    print("SKIP: warehouse_id not set in config")
else:
    print("SKIP: system.query.history not accessible")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Aggregations

# COMMAND ----------

# DBTITLE 1,Silver: telemetry_dbu_daily
if dbu_count > 0:
    dbu_daily = (
        spark.table(f"{catalog}.{schema}.telemetry_dbu_usage")
        .filter(
            (F.col("usage_date") >= F.lit(str(start_date))) &
            (F.col("usage_date") <= F.lit(str(end_date)))
        )
        .groupBy("usage_date", "sku_name", "usage_type", "job_id", "warehouse_id", "endpoint_name")
        .agg(
            F.sum("usage_quantity").alias("dbu_quantity"),
        )
        .withColumn("estimated_cost_usd", F.lit(None).cast("double"))
    )

    # Attempt list_prices join for cost estimation
    if availability["billing_list_prices"]:
        try:
            prices = spark.table("system.billing.list_prices").select(
                "sku_name",
                F.col("pricing.default").alias("list_price_per_dbu"),
            ).filter(F.col("list_price_per_dbu").isNotNull())

            dbu_daily = (
                dbu_daily.drop("estimated_cost_usd")
                .join(prices, "sku_name", "left")
                .withColumn("estimated_cost_usd",
                            F.col("dbu_quantity") * F.coalesce(F.col("list_price_per_dbu"), F.lit(0.0)))
                .drop("list_price_per_dbu")
            )
        except Exception as e:
            print(f"Could not join list_prices (pricing column schema may differ by account): {e}")

    dbu_daily.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.telemetry_dbu_daily")
    print(f"telemetry_dbu_daily: {dbu_daily.count()} rows")
else:
    print("SKIP: telemetry_dbu_daily (no bronze data)")

# COMMAND ----------

# DBTITLE 1,Silver: telemetry_job_reliability
if jobs_count > 0:
    BILLING_JOB_NAMES = [
        "telco_billing_anomaly_detection",
        config.get("agent_name", "ai_billing_agent"),
    ]

    from pyspark.sql.window import Window as W

    runs_30d = (
        spark.table(f"{catalog}.{schema}.telemetry_job_runs")
        .filter(F.to_date("start_time") >= F.lit(str(today - timedelta(days=30))))
    )

    # Get the most recent result_state per job using a window ordered by start_time
    w_latest = W.partitionBy("job_id").orderBy(F.col("start_time").desc())
    latest_state = (
        runs_30d
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .select("job_id", F.col("result_state").alias("last_run_state"))
    )

    reliability = (
        runs_30d
        .groupBy("job_id", "job_name")
        .agg(
            F.count("*").alias("run_count_30d"),
            F.sum(F.when(F.col("result_state") == "SUCCESS", 1).otherwise(0)).alias("success_count_30d"),
            F.sum(F.when(F.col("result_state").isin("FAILED", "TIMED_OUT"), 1).otherwise(0)).alias("failure_count_30d"),
            F.avg("run_duration_minutes").alias("avg_duration_minutes"),
            F.percentile_approx("run_duration_minutes", 0.95).alias("p95_duration_minutes"),
            F.max("end_time").alias("last_run_ts"),
        )
        .join(latest_state, "job_id", "left")
        .withColumn("success_rate_pct", F.round(F.col("success_count_30d") / F.col("run_count_30d") * 100, 1))
        .withColumn("is_billing_pipeline", F.col("job_name").isin(BILLING_JOB_NAMES))
    )

    # Full overwrite is intentional — this is a complete 30-day rolling recalculation
    reliability.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.telemetry_job_reliability")
    print(f"telemetry_job_reliability: {reliability.count()} jobs")
else:
    print("SKIP: telemetry_job_reliability (no bronze data)")

# COMMAND ----------

# DBTITLE 1,Silver: telemetry_warehouse_utilization
if queries_count > 0:
    wh_util = (
        spark.table(f"{catalog}.{schema}.telemetry_query_history")
        .filter(
            (F.to_date("start_time") >= F.lit(str(start_date))) &
            (F.to_date("start_time") <= F.lit(str(end_date)))
        )
        .withColumn("hour_bucket", F.date_trunc("hour", "start_time"))
        .groupBy("warehouse_id", "hour_bucket")
        .agg(
            F.count("*").alias("query_count"),
            F.sum(F.when(F.col("execution_status") != "FINISHED", 1).otherwise(0)).alias("failed_query_count"),
            F.percentile_approx("total_duration_ms", 0.50).alias("p50_duration_ms"),
            F.percentile_approx("total_duration_ms", 0.95).alias("p95_duration_ms"),
            F.percentile_approx("total_duration_ms", 0.99).alias("p99_duration_ms"),
            F.sum(F.coalesce("total_duration_ms", F.lit(0))).alias("total_duration_ms_sum"),
            F.sum(F.coalesce("waiting_at_capacity_ms", F.lit(0))).alias("total_queuing_ms"),
            F.sum(F.when(F.col("client_application") == "Genie", 1).otherwise(0)).alias("genie_query_count"),
            F.sum(F.when(F.col("client_application").contains("serving"), 1).otherwise(0)).alias("agent_query_count"),
        )
    )

    # Full overwrite is intentional — recalculates all hourly buckets in the window
    wh_util.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.telemetry_warehouse_utilization")
    print(f"telemetry_warehouse_utilization: {wh_util.count()} hour buckets")
else:
    print("SKIP: telemetry_warehouse_utilization (no bronze data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Operational KPIs

# COMMAND ----------

# DBTITLE 1,Gold: telemetry_operational_kpis
from pyspark.sql.window import Window
from pyspark.sql import Row

# Generate one row per date in the window
date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
dates_df = spark.createDataFrame([Row(kpi_date=d) for d in date_range])

# DBU metrics (may be NULL)
dbu_metrics = None
if dbu_count > 0:
    dbu_metrics = (
        spark.table(f"{catalog}.{schema}.telemetry_dbu_daily")
        .groupBy(F.col("usage_date").alias("kpi_date"))
        .agg(
            F.sum("dbu_quantity").alias("total_dbu_consumed"),
            F.sum("estimated_cost_usd").alias("estimated_daily_cost_usd"),
        )
    )

# Job reliability metrics (may be NULL)
pipeline_rate = None
anomaly_job_status = None
if jobs_count > 0:
    billing_jobs = spark.table(f"{catalog}.{schema}.telemetry_job_reliability").filter(
        F.col("is_billing_pipeline") == True
    )
    if billing_jobs.count() > 0:
        pipeline_rate = billing_jobs.agg(
            F.avg("success_rate_pct").alias("billing_pipeline_success_rate")
        ).collect()[0]["billing_pipeline_success_rate"]

    # Check if anomaly detection ran on each date
    anomaly_job_status = (
        spark.table(f"{catalog}.{schema}.telemetry_job_runs")
        .filter(F.col("job_name").contains("anomaly"))
        .withColumn("run_date", F.to_date("start_time"))
        .groupBy(F.col("run_date").alias("kpi_date"))
        .agg(
            F.lit(True).alias("anomaly_detection_ran"),
            F.last("result_state").alias("anomaly_detection_state"),
        )
    )

# Warehouse metrics (may be NULL)
wh_metrics = None
if queries_count > 0:
    wh_metrics = (
        spark.table(f"{catalog}.{schema}.telemetry_warehouse_utilization")
        .withColumn("util_date", F.to_date("hour_bucket"))
        .groupBy(F.col("util_date").alias("kpi_date"))
        .agg(
            F.avg(F.when(F.col("genie_query_count") > 0, F.col("p50_duration_ms"))).alias("avg_genie_query_latency_ms"),
            F.sum("genie_query_count").alias("genie_query_count"),
            F.round(
                F.sum("total_queuing_ms") / F.greatest(F.sum("total_duration_ms_sum"), F.lit(1)) * 100, 2
            ).alias("warehouse_queuing_pct"),
        )
    )

# Build the KPI table
kpis = dates_df

if dbu_metrics:
    kpis = kpis.join(dbu_metrics, "kpi_date", "left")
else:
    kpis = kpis.withColumn("total_dbu_consumed", F.lit(None).cast("double"))
    kpis = kpis.withColumn("estimated_daily_cost_usd", F.lit(None).cast("double"))

# Current 30-day rolling rate as of pipeline run time (same value for all dates in window)
kpis = kpis.withColumn("billing_pipeline_success_rate", F.lit(pipeline_rate).cast("double"))

if anomaly_job_status:
    kpis = kpis.join(anomaly_job_status, "kpi_date", "left")
else:
    kpis = kpis.withColumn("anomaly_detection_ran", F.lit(None).cast("boolean"))
    kpis = kpis.withColumn("anomaly_detection_state", F.lit(None).cast("string"))

if wh_metrics:
    kpis = kpis.join(wh_metrics, "kpi_date", "left")
else:
    kpis = kpis.withColumn("avg_genie_query_latency_ms", F.lit(None).cast("bigint"))
    kpis = kpis.withColumn("genie_query_count", F.lit(None).cast("bigint"))
    kpis = kpis.withColumn("warehouse_queuing_pct", F.lit(None).cast("double"))

# DBU vs prior 7-day average
w7d = Window.orderBy("kpi_date").rowsBetween(-7, -1)
kpis = kpis.withColumn("avg_dbu_7d", F.avg("total_dbu_consumed").over(w7d))
kpis = kpis.withColumn(
    "dbu_vs_prior_7d_pct",
    F.when(
        F.col("avg_dbu_7d") > 0,
        F.round((F.col("total_dbu_consumed") - F.col("avg_dbu_7d")) / F.col("avg_dbu_7d") * 100, 1)
    )
)
kpis = kpis.withColumn("cost_anomaly_flag", F.col("dbu_vs_prior_7d_pct") > 50)
kpis = kpis.drop("avg_dbu_7d")

# Fill NULLs for anomaly fields
kpis = kpis.fillna({"anomaly_detection_ran": False, "cost_anomaly_flag": False})

kpis.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.telemetry_operational_kpis")
kpi_count = kpis.count()
print(f"telemetry_operational_kpis: {kpi_count} days")

# COMMAND ----------

# DBTITLE 1,Summary
try:
    _kpi_count = spark.table(f"{catalog}.{schema}.telemetry_operational_kpis").count()
except Exception:
    _kpi_count = 0

print(f"""
System Table Ingestion Complete
================================
Window: {start_date} -> {end_date}

Bronze: telemetry_dbu_usage={dbu_count}, telemetry_job_runs={jobs_count}, telemetry_query_history={queries_count}
Silver: telemetry_dbu_daily, telemetry_job_reliability, telemetry_warehouse_utilization
Gold:   telemetry_operational_kpis={_kpi_count} days
""")