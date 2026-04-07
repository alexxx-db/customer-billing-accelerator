# Databricks notebook source
# MAGIC %md
# MAGIC # Seed Upstream Tables (shared helper)
# MAGIC
# MAGIC Generates 8 synthetic upstream tables required by the agent tools:
# MAGIC billing_monitoring_summary, telemetry_operational_kpis, telemetry_job_reliability,
# MAGIC silver_customer_account_dims, gold_revenue_attribution, gold_finance_operations_summary,
# MAGIC billing_disputes, billing_write_audit.
# MAGIC
# MAGIC **Usage:** Called via `%run` from notebooks 00 and 02. Expects `catalog` and `schema`
# MAGIC variables to be set in the calling notebook's scope before invocation.

# COMMAND ----------

from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import random

random.seed(42)
fq = lambda t: f"{catalog}.{schema}.{t}"

# ── 1. billing_monitoring_summary ──
schema_bms = StructType([
    StructField("event_month", StringType()),
    StructField("anomaly_type", StringType()),
    StructField("total_anomalies", LongType()),
    StructField("alerted_count", LongType()),
    StructField("pending_alert_count", LongType()),
    StructField("last_detection_ts", TimestampType()),
    StructField("last_alert_ts", TimestampType()),
])
rows_bms = []
for m in ["2026-01", "2026-02", "2026-03"]:
    for atype in ["SPIKE_USAGE", "DUPLICATE_CHARGE", "PLAN_MISMATCH", "ROAMING_OUTLIER"]:
        total = random.randint(2, 25)
        alerted = random.randint(1, total)
        det_ts = datetime(2026, int(m[-2:]), random.randint(1, 28), random.randint(0, 23), random.randint(0, 59))
        alert_ts = det_ts + timedelta(hours=random.randint(1, 12))
        rows_bms.append((m, atype, total, alerted, total - alerted, det_ts, alert_ts))
spark.createDataFrame(rows_bms, schema_bms).write.mode("overwrite").saveAsTable(fq("billing_monitoring_summary"))

# ── 2. telemetry_operational_kpis ──
schema_tok = StructType([
    StructField("kpi_date", DateType()),
    StructField("total_dbu_consumed", DoubleType()),
    StructField("estimated_daily_cost_usd", DoubleType()),
    StructField("billing_pipeline_success_rate", DoubleType()),
    StructField("avg_genie_query_latency_ms", LongType()),
    StructField("genie_query_count", LongType()),
    StructField("warehouse_queuing_pct", DoubleType()),
    StructField("anomaly_detection_ran", BooleanType()),
    StructField("anomaly_detection_state", StringType()),
    StructField("dbu_vs_prior_7d_pct", DoubleType()),
    StructField("cost_anomaly_flag", BooleanType()),
])
rows_tok = []
for d in range(30):
    dt = date(2026, 3, 30) - timedelta(days=d)
    dbu = round(random.uniform(80, 200), 2)
    cost = round(dbu * 0.22, 2)
    rows_tok.append((dt, dbu, cost, round(random.uniform(0.92, 1.0), 4),
                     random.randint(180, 600), random.randint(20, 150),
                     round(random.uniform(0.0, 0.15), 4), True,
                     random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "FAILED"]),
                     round(random.uniform(-0.15, 0.25), 4), random.random() > 0.85))
spark.createDataFrame(rows_tok, schema_tok).write.mode("overwrite").saveAsTable(fq("telemetry_operational_kpis"))

# ── 3. telemetry_job_reliability ──
schema_tjr = StructType([
    StructField("job_id", StringType()),
    StructField("job_name", StringType()),
    StructField("run_count_30d", LongType()),
    StructField("success_count_30d", LongType()),
    StructField("failure_count_30d", LongType()),
    StructField("success_rate_pct", DoubleType()),
    StructField("avg_duration_minutes", DoubleType()),
    StructField("p95_duration_minutes", DoubleType()),
    StructField("last_run_state", StringType()),
    StructField("last_run_ts", TimestampType()),
    StructField("is_billing_pipeline", BooleanType()),
])
jobs = [
    ("job-101", "billing_ingest_raw", True), ("job-102", "billing_transform_silver", True),
    ("job-103", "billing_aggregate_gold", True), ("job-104", "anomaly_detection", True),
    ("job-201", "marketing_etl", False), ("job-202", "crm_sync", False),
]
rows_tjr = []
for jid, jname, is_bill in jobs:
    runs = random.randint(25, 60)
    fails = random.randint(0, 4)
    succ = runs - fails
    rows_tjr.append((jid, jname, runs, succ, fails, round(succ / runs * 100, 2),
                     round(random.uniform(2, 25), 2), round(random.uniform(15, 45), 2),
                     random.choice(["SUCCESS", "SUCCESS", "FAILED"]),
                     datetime(2026, 3, 30, random.randint(0, 23), random.randint(0, 59)), is_bill))
spark.createDataFrame(rows_tjr, schema_tjr).write.mode("overwrite").saveAsTable(fq("telemetry_job_reliability"))

# ── 4. silver_customer_account_dims ──
schema_cad = StructType([
    StructField("customer_id", LongType()),
    StructField("erp_account_id", StringType()),
    StructField("account_type", StringType()),
    StructField("credit_rating", StringType()),
    StructField("payment_terms_days", IntegerType()),
    StructField("account_status", StringType()),
    StructField("ar_balance_usd", DoubleType()),
    StructField("overdue_balance_usd", DoubleType()),
    StructField("erp_segment", StringType()),
    StructField("erp_source_system", StringType()),
])
rows_cad = []
for cid in range(1000, 1020):
    rows_cad.append((cid, f"ERP-{cid}", random.choice(["CONSUMER", "BUSINESS", "ENTERPRISE"]),
                     random.choice(["A", "A", "B", "B", "C"]), random.choice([15, 30, 45, 60]),
                     random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "SUSPENDED"]),
                     round(random.uniform(0, 500), 2), round(random.uniform(0, 120), 2),
                     random.choice(["RETAIL", "SMB", "CORPORATE"]),
                     random.choice(["SAP", "ORACLE_ERP"])))
spark.createDataFrame(rows_cad, schema_cad).write.mode("overwrite").saveAsTable(fq("silver_customer_account_dims"))

# ── 5. gold_revenue_attribution ──
schema_gra = StructType([
    StructField("customer_id", LongType()),
    StructField("event_month", StringType()),
    StructField("billed_total_usd", DoubleType()),
    StructField("erp_recognized_revenue_usd", DoubleType()),
    StructField("erp_collected_revenue_usd", DoubleType()),
    StructField("erp_overdue_revenue_usd", DoubleType()),
    StructField("revenue_variance_usd", DoubleType()),
    StructField("revenue_variance_pct", DoubleType()),
])
rows_gra = []
for cid in range(1000, 1020):
    for m in ["2026-01", "2026-02", "2026-03"]:
        billed = round(random.uniform(30, 250), 2)
        recognized = round(billed * random.uniform(0.92, 1.02), 2)
        collected = round(recognized * random.uniform(0.80, 1.0), 2)
        overdue = round(max(0, recognized - collected), 2)
        variance = round(billed - recognized, 2)
        rows_gra.append((cid, m, billed, recognized, collected, overdue, variance,
                         round(variance / billed * 100, 2) if billed else 0.0))
spark.createDataFrame(rows_gra, schema_gra).write.mode("overwrite").saveAsTable(fq("gold_revenue_attribution"))

# ── 6. gold_finance_operations_summary ──
schema_gfo = StructType([
    StructField("event_month", StringType()),
    StructField("account_type", StringType()),
    StructField("erp_segment", StringType()),
    StructField("customer_count", LongType()),
    StructField("total_billed_usd", DoubleType()),
    StructField("total_erp_revenue_usd", DoubleType()),
    StructField("total_overdue_usd", DoubleType()),
    StructField("arpu_usd", DoubleType()),
    StructField("total_roaming_revenue_usd", DoubleType()),
    StructField("total_intl_revenue_usd", DoubleType()),
    StructField("total_opex_usd", DoubleType()),
    StructField("opex_ratio_pct", DoubleType()),
    StructField("overdue_ar_ratio_pct", DoubleType()),
])
rows_gfo = []
for m in ["2025-10", "2025-11", "2025-12", "2026-01", "2026-02", "2026-03"]:
    for atype in ["CONSUMER", "BUSINESS", "ENTERPRISE"]:
        for seg in ["RETAIL", "SMB", "CORPORATE"]:
            cnt = random.randint(50, 500)
            billed = round(cnt * random.uniform(40, 120), 2)
            erp_rev = round(billed * random.uniform(0.93, 1.0), 2)
            overdue = round(billed * random.uniform(0.02, 0.12), 2)
            arpu = round(billed / cnt, 2)
            roaming = round(billed * random.uniform(0.05, 0.15), 2)
            intl = round(billed * random.uniform(0.03, 0.10), 2)
            opex = round(billed * random.uniform(0.15, 0.30), 2)
            rows_gfo.append((m, atype, seg, cnt, billed, erp_rev, overdue, arpu,
                             roaming, intl, opex,
                             round(opex / billed * 100, 2) if billed else 0.0,
                             round(overdue / billed * 100, 2) if billed else 0.0))
spark.createDataFrame(rows_gfo, schema_gfo).write.mode("overwrite").saveAsTable(fq("gold_finance_operations_summary"))

# ── 7. billing_disputes ──
schema_bd = StructType([
    StructField("dispute_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("dispute_type", StringType()),
    StructField("status", StringType()),
    StructField("description", StringType()),
    StructField("disputed_amount_usd", DoubleType()),
    StructField("created_at", TimestampType()),
    StructField("anomaly_id", StringType()),
])
rows_bd = []
statuses = ["OPEN", "UNDER_REVIEW", "ESCALATED", "RESOLVED_CREDIT", "RESOLVED_NO_ACTION", "CLOSED"]
dispute_types = ["OVERCHARGE", "DUPLICATE_BILLING", "PLAN_MISMATCH", "ROAMING_DISPUTE", "CREDIT_NOT_APPLIED"]
for i in range(30):
    cid = random.randint(1000, 1019)
    rows_bd.append((f"DSP-{10000+i}", cid, random.choice(dispute_types), random.choice(statuses),
                    f"Customer {cid} reported billing discrepancy",
                    round(random.uniform(5, 200), 2),
                    datetime(2026, 3, random.randint(1, 30), random.randint(8, 20), random.randint(0, 59)),
                    f"ANM-{random.randint(5000,5999)}" if random.random() > 0.3 else None))
spark.createDataFrame(rows_bd, schema_bd).write.mode("overwrite").saveAsTable(fq("billing_disputes"))

# ── 8. billing_write_audit ──
schema_bwa = StructType([
    StructField("audit_id", StringType()),
    StructField("action_type", StringType()),
    StructField("target_record_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("result_status", StringType()),
    StructField("result_message", StringType()),
    StructField("executed_at", TimestampType()),
])
rows_bwa = []
actions = ["APPLY_CREDIT", "RESOLVE_DISPUTE", "ADJUST_PLAN", "OVERRIDE_CHARGE", "ESCALATE_DISPUTE"]
for i in range(40):
    cid = random.randint(1000, 1019)
    action = random.choice(actions)
    status = random.choice(["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"])
    rows_bwa.append((f"AUD-{20000+i}", action, f"DSP-{random.randint(10000,10029)}", cid,
                     status, f"{action} {'completed' if status=='SUCCESS' else 'failed'} for customer {cid}",
                     datetime(2026, 3, random.randint(1, 30), random.randint(0, 23), random.randint(0, 59))))
spark.createDataFrame(rows_bwa, schema_bwa).write.mode("overwrite").saveAsTable(fq("billing_write_audit"))

print(f"All 8 upstream tables seeded in {catalog}.{schema}")
