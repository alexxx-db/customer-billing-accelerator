# Databricks notebook source
# MAGIC %md
# MAGIC # External Data Ingestion: Silver and Gold Transformations
# MAGIC
# MAGIC Reads from `ext_*` views (pointing at either federated or simulated data)
# MAGIC and produces Silver and Gold analytical assets.
# MAGIC
# MAGIC ## Pipeline
# MAGIC ```
# MAGIC ext_erp_accounts + customers       → silver_customer_account_dims
# MAGIC ext_fx_rates                       → silver_fx_daily
# MAGIC ext_procurement_costs              → silver_procurement_monthly
# MAGIC (reference)                        → silver_conformed_kpi_defs
# MAGIC invoice + Silver + billing_anomalies → gold_revenue_attribution
# MAGIC gold_revenue_attribution + Silver    → gold_finance_operations_summary
# MAGIC ```
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run `08a_erp_data_simulation` (Track B) or `08_federation_setup` (Track A) first
# MAGIC - Run `00_data_preparation` (invoice, customers, billing_plans tables must exist)

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Guard: Verify ext_* views exist
import pyspark.sql.functions as F

catalog = config['catalog']
schema = config['database']

for view in ["ext_erp_accounts", "ext_erp_orders", "ext_procurement_costs", "ext_fx_rates"]:
    try:
        spark.table(f"{catalog}.{schema}.{view}").limit(1).count()
        print(f"OK: {view}")
    except Exception as e:
        raise RuntimeError(
            f"View {view} not found. Run 08a_erp_data_simulation (Track B) "
            f"or 08_federation_setup (Track A) first.\nError: {e}"
        ) from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Customer Account Dimensions

# COMMAND ----------

# DBTITLE 1,Silver: silver_customer_account_dims
silver_accounts = (
    spark.table(f"{catalog}.{schema}.customers")
    .join(
        spark.table(f"{catalog}.{schema}.ext_erp_accounts"),
        on="customer_id", how="left"
    )
    .select(
        "customer_id", "plan", "contract_start_dt",
        F.coalesce("erp_account_id", F.concat(F.lit("ACC-UNKNOWN-"), F.col("customer_id").cast("string"))).alias("erp_account_id"),
        F.coalesce("account_type", F.lit("UNKNOWN")).alias("account_type"),
        F.coalesce("credit_rating", F.lit("UNRATED")).alias("credit_rating"),
        F.coalesce("payment_terms_days", F.lit(30)).alias("payment_terms_days"),
        F.coalesce("account_status", F.lit("ACTIVE")).alias("account_status"),
        F.coalesce("ar_balance_usd", F.lit(0.0)).alias("ar_balance_usd"),
        F.coalesce("overdue_balance_usd", F.lit(0.0)).alias("overdue_balance_usd"),
        F.coalesce("lifetime_value_usd", F.lit(0.0)).alias("lifetime_value_usd"),
        F.coalesce("erp_region", F.lit("UNKNOWN")).alias("erp_region"),
        F.coalesce("erp_segment", F.lit("UNKNOWN")).alias("erp_segment"),
        F.coalesce("erp_source_system", F.lit("UNKNOWN")).alias("erp_source_system"),
        F.current_timestamp().alias("silver_updated_at"),
    )
)

silver_accounts.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.silver_customer_account_dims")
print(f"silver_customer_account_dims: {silver_accounts.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: FX Daily Rates

# COMMAND ----------

# DBTITLE 1,Silver: silver_fx_daily
silver_fx = (
    spark.table(f"{catalog}.{schema}.ext_fx_rates")
    .withColumn("usd_per_unit",
                F.when(F.col("from_currency") == "USD", 1.0 / F.col("exchange_rate"))
                .otherwise(F.col("exchange_rate")))
    .select(
        "rate_date", "to_currency",
        F.col("exchange_rate").alias("usd_to_foreign"),
        "usd_per_unit", "rate_source",
        F.current_timestamp().alias("silver_updated_at"),
    )
)

silver_fx.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.silver_fx_daily")
print(f"silver_fx_daily: {silver_fx.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Procurement Monthly

# COMMAND ----------

# DBTITLE 1,Silver: silver_procurement_monthly
silver_proc = (
    spark.table(f"{catalog}.{schema}.ext_procurement_costs")
    .filter(F.col("approval_status") == "APPROVED")
    .groupBy("cost_month", "cost_category", "cost_type")
    .agg(
        F.sum("cost_usd").alias("total_cost_usd"),
        F.count("po_number").alias("po_count"),
        F.countDistinct("vendor_name").alias("vendor_count"),
    )
    .withColumnRenamed("cost_month", "event_month")
    .withColumn("silver_updated_at", F.current_timestamp())
)

silver_proc.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.silver_procurement_monthly")
print(f"silver_procurement_monthly: {silver_proc.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Conformed KPI Definitions

# COMMAND ----------

# DBTITLE 1,Silver: silver_conformed_kpi_defs
kpi_defs = [
    ("ARPU_MONTHLY",       "Average Revenue Per User",    "total_charges / customer_count",           "BILLING",    "USD", "HIGHER_BETTER"),
    ("COLLECTION_RATE",    "Revenue Collection Rate",     "collected_revenue / billed_revenue * 100", "AR",         "PCT", "HIGHER_BETTER"),
    ("OPEX_RATIO",         "Operational Cost Ratio",      "total_opex / total_revenue * 100",         "FINANCE",    "PCT", "LOWER_BETTER"),
    ("ROAMING_REV_PCT",    "Roaming Revenue Share",       "roaming_revenue / total_revenue * 100",    "BILLING",    "PCT", "NEUTRAL"),
    ("INTL_REV_PCT",       "International Revenue Share", "intl_revenue / total_revenue * 100",       "BILLING",    "PCT", "NEUTRAL"),
    ("OVERDUE_AR_RATIO",   "Overdue AR Ratio",            "overdue_balance / total_ar * 100",         "AR",         "PCT", "LOWER_BETTER"),
]

kpi_schema = ["kpi_code", "kpi_name", "formula_description", "owning_domain", "unit", "direction"]
df_kpi_defs = spark.createDataFrame(kpi_defs, kpi_schema).withColumn("silver_updated_at", F.current_timestamp())
df_kpi_defs.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.silver_conformed_kpi_defs")
print(f"silver_conformed_kpi_defs: {df_kpi_defs.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Revenue Attribution

# COMMAND ----------

# DBTITLE 1,Gold: gold_revenue_attribution
invoice = spark.table(f"{catalog}.{schema}.invoice")
erp_orders = spark.table(f"{catalog}.{schema}.ext_erp_orders")
acct_dims = spark.table(f"{catalog}.{schema}.silver_customer_account_dims")

# Pre-aggregate ERP orders by customer+month to avoid cross-product
erp_monthly = (
    erp_orders.groupBy("customer_id", "order_month")
    .agg(
        F.sum("recognized_revenue_usd").alias("erp_recognized_revenue_usd"),
        F.sum(F.when(F.col("collection_status") == "COLLECTED", F.col("recognized_revenue_usd"))).alias("erp_collected_revenue_usd"),
        F.sum(F.when(F.col("collection_status") == "OVERDUE", F.col("recognized_revenue_usd"))).alias("erp_overdue_revenue_usd"),
    )
)

gold_rev = (
    invoice
    .join(acct_dims.select("customer_id", "account_type", "credit_rating", "erp_segment"),
          on="customer_id", how="left")
    .join(erp_monthly,
          on=[invoice.customer_id == erp_monthly.customer_id,
              invoice.event_month == erp_monthly.order_month],
          how="left")
    .select(
        invoice.customer_id, invoice.event_month, invoice.plan_name,
        F.coalesce("account_type", F.lit("UNKNOWN")).alias("account_type"),
        F.coalesce("erp_segment", F.lit("UNKNOWN")).alias("erp_segment"),
        F.coalesce("credit_rating", F.lit("UNRATED")).alias("credit_rating"),
        invoice.total_charges.alias("billed_total_usd"),
        invoice.monthly_charges.alias("billed_base_usd"),
        (F.col("roaming_data_charges") + F.col("roaming_call_charges") + F.col("roaming_text_charges")).alias("billed_roaming_usd"),
        (F.col("international_call_charges") + F.col("international_text_charges")).alias("billed_intl_usd"),
        F.coalesce("erp_recognized_revenue_usd", F.lit(0.0)).alias("erp_recognized_revenue_usd"),
        F.coalesce("erp_collected_revenue_usd", F.lit(0.0)).alias("erp_collected_revenue_usd"),
        F.coalesce("erp_overdue_revenue_usd", F.lit(0.0)).alias("erp_overdue_revenue_usd"),
        (invoice.total_charges - F.coalesce("erp_recognized_revenue_usd", invoice.total_charges)).alias("revenue_variance_usd"),
        F.when(F.coalesce("erp_recognized_revenue_usd", F.lit(0.0)) > 0,
               (invoice.total_charges - F.col("erp_recognized_revenue_usd")) / F.col("erp_recognized_revenue_usd") * 100
        ).alias("revenue_variance_pct"),
        F.current_timestamp().alias("gold_updated_at"),
    )
)

gold_rev.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.gold_revenue_attribution")

# Cross-product assertion
invoice_count = invoice.count()
gold_count = spark.table(f"{catalog}.{schema}.gold_revenue_attribution").count()
assert gold_count <= invoice_count * 1.05, (
    f"gold_revenue_attribution ({gold_count}) >> invoice ({invoice_count}). Check join keys."
)
print(f"gold_revenue_attribution: {gold_count} rows (invoice: {invoice_count})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Finance Operations Summary

# COMMAND ----------

# DBTITLE 1,Gold: gold_finance_operations_summary
rev_agg = (
    spark.table(f"{catalog}.{schema}.gold_revenue_attribution")
    .groupBy("event_month", "account_type", "erp_segment")
    .agg(
        F.count("customer_id").alias("customer_count"),
        F.sum("billed_total_usd").alias("total_billed_usd"),
        F.sum("erp_recognized_revenue_usd").alias("total_erp_revenue_usd"),
        F.sum("erp_overdue_revenue_usd").alias("total_overdue_usd"),
        F.avg("billed_total_usd").alias("arpu_usd"),
        F.sum("billed_roaming_usd").alias("total_roaming_revenue_usd"),
        F.sum("billed_intl_usd").alias("total_intl_revenue_usd"),
    )
)

proc_agg = (
    spark.table(f"{catalog}.{schema}.silver_procurement_monthly")
    .groupBy("event_month")
    .agg(F.sum("total_cost_usd").alias("total_opex_usd"))
)

gold_fin = (
    rev_agg
    .join(proc_agg, on="event_month", how="left")
    .withColumn("opex_ratio_pct",
                F.when(F.col("total_billed_usd") > 0,
                       F.col("total_opex_usd") / F.col("total_billed_usd") * 100))
    .withColumn("overdue_ar_ratio_pct",
                F.when(F.col("total_erp_revenue_usd") > 0,
                       F.col("total_overdue_usd") / F.col("total_erp_revenue_usd") * 100))
    .withColumn("gold_updated_at", F.current_timestamp())
)

gold_fin.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.gold_finance_operations_summary")
print(f"gold_finance_operations_summary: {gold_fin.count()} rows")
