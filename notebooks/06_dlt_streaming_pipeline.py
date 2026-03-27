# Databricks notebook source
# MAGIC %md
# MAGIC ## Delta Live Tables Pipeline Definition
# MAGIC This notebook is a Delta Live Tables pipeline definition.
# MAGIC It must be executed via a DLT pipeline (notebook 06a), NOT run directly.
# MAGIC Direct execution will fail with `ModuleNotFoundError: No module named 'dlt'`.
# MAGIC This is expected and correct.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# DLT pipelines cannot use %run or dbutils; config is passed via pipeline parameters
catalog = spark.conf.get("pipeline.catalog", "cme_demos_alex_barreto")
schema  = spark.conf.get("pipeline.schema",  "telco_billing_db")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze/Silver Layer: Enriched Billing Events
# MAGIC Streaming read of billing_items joined with customer and plan metadata.

# COMMAND ----------

@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_device_id", "device_id IS NOT NULL")
@dlt.expect("valid_event_type", "event_type IS NOT NULL")
@dlt.table(
    name="billing_events_streaming",
    comment="Near-real-time enriched billing events joined with customer and plan metadata",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "true",
    },
)
def billing_events_streaming():
    billing_items = dlt.read_stream(f"{catalog}.{schema}.billing_items")
    customers     = dlt.read(f"{catalog}.{schema}.customers")
    plans         = dlt.read(f"{catalog}.{schema}.billing_plans")

    customer_plans = customers.join(plans, customers.plan == plans.Plan_key, "left")

    return (
        billing_items
        .join(customer_plans, billing_items.device_id == customer_plans.device_id, "left")
        .select(
            billing_items.device_id.cast("bigint").alias("device_id"),
            customer_plans.customer_id.cast("bigint").alias("customer_id"),
            F.concat(
                F.year("event_ts").cast("string"),
                F.lit("-"),
                F.lpad(F.month("event_ts").cast("string"), 2, "0")
            ).alias("event_month"),
            billing_items.event_type,
            billing_items.minutes.cast("double").alias("minutes"),
            billing_items.bytes_transferred.cast("double").alias("bytes_transferred"),
            billing_items.event_ts,
            F.current_timestamp().alias("ingestion_ts"),
            customer_plans.Plan_name.alias("plan_name"),
            customer_plans.monthly_charges_dollars.cast("double").alias("monthly_charges"),
            customer_plans.Roam_Data_charges_per_MB,
            customer_plans.Roam_Call_charges_per_min,
            customer_plans.International_call_charge_per_min,
            customer_plans.Data_Outside_Allowance_Per_MB,
            customer_plans.Data_Limit_GB,
        )
        # NULL customer_id rows are dropped by @dlt.expect_or_drop above
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer: Running Monthly Charge Accumulators
# MAGIC Per-customer per-month charge estimates from streaming events.

# COMMAND ----------

@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_event_month", "event_month IS NOT NULL")
@dlt.expect("positive_total", "estimated_total_charge >= 0")
@dlt.table(
    name="billing_monthly_running",
    comment="Running per-customer monthly charge estimates from streaming events",
    table_properties={
        "quality": "gold",
        "pipelines.reset.allowed": "true",
    }
)
def billing_monthly_running():
    events = dlt.read_stream("billing_events_streaming")

    return (
        events
        .groupBy("customer_id", "device_id", "event_month", "plan_name",
                 "monthly_charges", "Roam_Data_charges_per_MB",
                 "Roam_Call_charges_per_min", "International_call_charge_per_min",
                 "Data_Outside_Allowance_Per_MB", "Data_Limit_GB")
        .agg(
            F.sum(F.when(F.col("event_type") == "data_local",
                         F.col("bytes_transferred") / (1024 * 1024)).otherwise(0)
                  ).alias("data_local_mb"),
            F.sum(F.when(F.col("event_type") == "data_roaming",
                         F.col("bytes_transferred") / (1024 * 1024)).otherwise(0)
                  ).alias("data_roaming_mb"),
            F.sum(F.when(F.col("event_type") == "call_mins_roaming",
                         F.ceil(F.col("minutes"))).otherwise(0)
                  ).alias("call_mins_roaming"),
            F.sum(F.when(F.col("event_type") == "call_mins_international",
                         F.ceil(F.col("minutes"))).otherwise(0)
                  ).alias("call_mins_international"),
            F.sum(F.when(F.col("event_type") == "texts_roaming", 1).otherwise(0)
                  ).alias("texts_roaming"),
            F.sum(F.when(F.col("event_type") == "texts_international", 1).otherwise(0)
                  ).alias("texts_international"),
            F.max("event_ts").alias("last_event_ts"),
            F.count("*").alias("record_count"),
        )
        .withColumn(
            "estimated_overage_charge",
            F.when(
                F.col("Data_Limit_GB") != "UNLIMITED",
                F.greatest(
                    F.lit(0.0),
                    (F.col("data_local_mb") - F.col("Data_Limit_GB").cast("double") * 1024)
                    * F.col("Data_Outside_Allowance_Per_MB")
                )
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "estimated_roaming_charge",
            F.col("data_roaming_mb") * F.col("Roam_Data_charges_per_MB")
            + F.col("call_mins_roaming") * F.col("Roam_Call_charges_per_min")
        )
        .withColumn(
            "estimated_intl_charge",
            F.col("call_mins_international") * F.col("International_call_charge_per_min")
        )
        .withColumn(
            "estimated_total_charge",
            F.col("monthly_charges")
            + F.col("estimated_overage_charge")
            + F.col("estimated_roaming_charge")
            + F.col("estimated_intl_charge")
        )
        .drop("monthly_charges", "Roam_Data_charges_per_MB", "Roam_Call_charges_per_min",
              "International_call_charge_per_min", "Data_Outside_Allowance_Per_MB", "Data_Limit_GB")
    )
