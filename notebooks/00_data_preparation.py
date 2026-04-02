# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# dependencies = [
#   "dbldatagen",
# ]
# ///
# MAGIC %md ##Data Generation for the Demo
# MAGIC
# MAGIC For this solution accelerator, we will use data generator. The datasets include:
# MAGIC
# MAGIC - billing_plans: Telcos provide multiple billing plans to their consumers. These are representative plans for this solution accelarator.
# MAGIC - customers: Telco customer master data including attributes like customer_id, name, phone number, email etc
# MAGIC - billing_items: The detailed billing items including device_id, event type (data, call or text), call duration, data transferred, event timestamp etc
# MAGIC - invoice: Telco billing invoice for the customer
# MAGIC   
# MAGIC The invoice is based on:
# MAGIC   - cost per MB of internet activity
# MAGIC   - cost per minute of call for each of the call categories
# MAGIC   - cost per message
# MAGIC   
# MAGIC Internet activitity will be priced per MB transferred
# MAGIC
# MAGIC Phone calls will be priced per minute or partial minute.
# MAGIC
# MAGIC Messages will be priced per actual counts
# MAGIC
# MAGIC For simplicity, we'll ignore the free data, messages and calls threshold in most plans and the complexity
# MAGIC of matching devices to customers and telecoms operators - our goal here is to show generation of join
# MAGIC ready data, rather than full modelling of phone usage invoicing.

# COMMAND ----------

# MAGIC %md ## Databricks Labs Data Generator 
# MAGIC  Generating synthetic data is complex, however we have leveraged [Databricks Labs Data Generator ](https://github.com/databrickslabs/dbldatagen/tree/master)for the data generation. In fact, the labs project had a good [example](https://github.com/databrickslabs/dbldatagen/blob/master/dbldatagen/datasets/multi_table_telephony_provider.py) for telco which is reused for this solution. We highly recommend to use the labs project for synthetic data generation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install data generator package

# COMMAND ----------

import socket
import requests

domains = [
    "pypi.org", 
    "files.pythonhosted.org", 
    "google.com",
    "github.com"
]

print(f"{'Domain':<25} | {'DNS Lookup':<12} | {'HTTP Connect'}")
print("-" * 55)

for domain in domains:
    # 1. Test DNS Resolution
    try:
        ip = socket.gethostbyname(domain)
        dns_status = "✅ OK"
    except socket.gaierror:
        dns_status = "❌ FAILED"
        ip = None
    
    # 2. Test Connection (if DNS worked)
    http_status = "Skipped"
    if ip:
        try:
            response = requests.get(f"https://{domain}", timeout=5)
            http_status = f"✅ {response.status_code}"
        except Exception as e:
            http_status = "❌ TIMEOUT/BLOCKED"

    print(f"{domain:<25} | {dns_status:<12} | {http_status}")

# COMMAND ----------

# MAGIC %pip install --quiet dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a catalog and a database
# MAGIC We create a catalog and a database (schema) to store the delta tables for our data.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

catalog = config['catalog']
db = config['database']

# Ensure the catalog exists, create it if it does not
# _ = spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
# Ensure the schema exists within the specified catalog, create it if it does not
_ = spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")
# _ = spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{db}.{volume}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations for data generation

# COMMAND ----------

UNIQUE_CUSTOMERS = config['UNIQUE_CUSTOMERS']
CUSTOMER_MIN_VALUE = config['CUSTOMER_MIN_VALUE']
DEVICE_MIN_VALUE = config['DEVICE_MIN_VALUE']
SUBSCRIBER_NUM_MIN_VALUE = config['SUBSCRIBER_NUM_MIN_VALUE']
UNIQUE_PLANS = config['UNIQUE_PLANS']
PLAN_MIN_VALUE = config['PLAN_MIN_VALUE']

AVG_EVENTS_PER_CUSTOMER = config['AVG_EVENTS_PER_CUSTOMER']

shuffle_partitions_requested = config['shuffle_partitions_requested']
partitions_requested = config['partitions_requested']
NUM_DAYS=config['NUM_DAYS'] 
MB_100 = config['MB_100']
K_1 = config['K_1']
start_dt=config['start_dt']
end_dt=config['end_dt']

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create billing plans table 
# MAGIC Billing plans represent various subscription options offered by the telecom provider to their customers. This dataset specifically focuses on plans available under contractual agreements. Due to its small size, the sample dataset has been uploaded to Git and imported into a Delta table.

# COMMAND ----------

# Use the billing_plans.json file located in the notebook-relative data/ directory

# COMMAND ----------

import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType

# Define the schema for the JSON data
schema = StructType([
    StructField("Plan_key", IntegerType(), True),
    StructField("Plan_id", StringType(), True),
    StructField("Plan_name", StringType(), True),
    StructField("contract_in_months", IntegerType(), True),
    StructField("monthly_charges_dollars", DoubleType(), True),
    StructField("Calls_Text", StringType(), True),
    StructField("Internet_Speed_MBPS", StringType(), True),
    StructField("Data_Limit_GB", StringType(), True),
    StructField("Data_Outside_Allowance_Per_MB", DoubleType(), True),
    StructField("Roam_Data_charges_per_MB", DoubleType(), True),
    StructField("Roam_Call_charges_per_min", DoubleType(), True),
    StructField("Roam_text_charges", DoubleType(), True),
    StructField("International_call_charge_per_min", DoubleType(), True),
    StructField("International_text_charge", DoubleType(), True)
])

# Get the billing dataset path and import the data into a delta table with the specified schema
current_workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
data_dir_path = "/".join(current_workspace_path.split("/")[:-2]) + "/data"
json_path = "/Workspace" + data_dir_path + "/billing_plans.json"

# Read JSON Lines file with Python (avoids Spark's dbfs path resolution on serverless)
with open(json_path, "r") as f:
    plans_data = [json.loads(line) for line in f if line.strip()]

df_plans = spark.createDataFrame(plans_data, schema=schema)

# COMMAND ----------

# Write the DataFrame to a Delta table
df_plans.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{db}.billing_plans")

# COMMAND ----------

display(df_plans)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Generate customer, billing items and invoice datasets 
# MAGIC Customer, Billing items and Invoice tables are generated using data generator and loaded in the delta tables

# COMMAND ----------

# MAGIC %md ###Lets model our customers
# MAGIC
# MAGIC Device is used as the the foreign key. For more details around the data generation, please refer Databricks Labs project.
# MAGIC

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)


shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = UNIQUE_CUSTOMERS

customer_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
            .withColumn("customer_id","decimal(10)", minValue=CUSTOMER_MIN_VALUE, uniqueValues=UNIQUE_CUSTOMERS)
            .withColumn("customer_name", template=r"\\w \\w|\\w a. \\w")  
           
            # use the following for a simple sequence
            #.withColumn("device_id","decimal(10)", minValue=DEVICE_MIN_VALUE, uniqueValues=UNIQUE_CUSTOMERS)
                     
            .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE, 
                        baseColumn="customer_id", baseColumnType="hash")

            .withColumn("phone_number","decimal(10)",  minValue=SUBSCRIBER_NUM_MIN_VALUE, 
                        baseColumn=["customer_id", "customer_name"], baseColumnType="hash")

            # for email, we'll just use the formatted phone number
            .withColumn("email","string",  format="subscriber_%s@myoperator.com", baseColumn="phone_number")
            .withColumn("plan", "int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS, random=True)
            .withColumn("contract_start_dt", "date", data_range=dg.DateRange("2023-01-01 00:00:00",
                                                                             "2024-12-31 11:59:59",
                                                                             "days=1"),
                        random=True)
            )

df_customers = (customer_dataspec.build()
                .dropDuplicates(["device_id"])
                .dropDuplicates(["phone_number"])
                .orderBy("customer_id")
               )

# Write the DataFrame to a Delta table
df_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{db}.customers")

# COMMAND ----------

# MAGIC %md ###Lets model our billing events
# MAGIC
# MAGIC Billing events like data usage, calls and texts are generated for this solution. 

# COMMAND ----------

import dbldatagen as dg



data_rows = AVG_EVENTS_PER_CUSTOMER * UNIQUE_CUSTOMERS * NUM_DAYS

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)


# use random seed method of 'hash_fieldname' for better spread - default in later builds
events_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested, randomSeed=42,
                                    randomSeedMethod="hash_fieldname")
             # use same logic as per customers dataset to ensure matching keys - but make them random
            .withColumn("device_id_base", "decimal(10)", minValue=CUSTOMER_MIN_VALUE, uniqueValues=UNIQUE_CUSTOMERS,
                        random=True, omit=True)
            .withColumn("device_id", "decimal(10)",  minValue=DEVICE_MIN_VALUE,
                        baseColumn="device_id_base", baseColumnType="hash")

            # use specific random seed to get better spread of values
            .withColumn("event_type", "string",  values=["call_mins_local", "data_local", "data_roaming", "call_mins_roaming", "texts_roaming", "call_mins_international", "texts_local", "texts_international"],
                                                weights=[100, 100, 1, 0.5, 0.6, 2,20,2], random=True)

            # use Gamma distribution for skew towards short calls
            .withColumn("base_minutes","decimal(7,2)",  minValue=1.0, maxValue=5.0, step=0.1,
                        distribution=dg.distributions.Gamma(shape=1.5, scale=2.0), random=True, omit=True)
                   
            # use Gamma distribution for skew towards short transfers
            .withColumn("base_bytes_transferred","decimal(12)",  minValue=K_1, maxValue=MB_100, 
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0), random=True, omit=True)
                   
            .withColumn("minutes", "decimal(7,2)", baseColumn=["event_type", "base_minutes"],
                        expr="""
                              case when event_type in ("call_mins_local", "call_mins_roaming", "call_mins_international") then base_minutes
                              else 0
                              end
                               """)
            .withColumn("bytes_transferred", "decimal(12)", baseColumn=["event_type", "base_bytes_transferred"],
                        expr="""
                              case when event_type in ("data_local", "data_roaming") then base_bytes_transferred
                              else 0
                              end
                               """)
                   
            .withColumn("event_ts", "timestamp", data_range=dg.DateRange(start_dt,
                                                                             end_dt,
                                                                             "seconds=1"),
                        random=True)
                   
            )

df_events = (events_dataspec.build()
               )

# COMMAND ----------

# MAGIC %md ###Billing Items
# MAGIC
# MAGIC Now generate events for generating billing items by joining the datasets customer, billing plans and events.

# COMMAND ----------

# Join the customer dataframe with the billing plans based on plan_key
df_customer_pricing = df_customers.join(df_plans, df_plans.Plan_key == df_customers.plan)

# COMMAND ----------

# remove events before the contract start date
df_billing_items = df_events.alias("events") \
    .join(df_customer_pricing.alias("pricing"), df_events.device_id == df_customer_pricing.device_id) \
    .where(df_events.event_ts >= df_customer_pricing.contract_start_dt) \
    .select("events.*", "pricing.contract_start_dt") 
    
# Write the DataFrame to a Delta table
df_billing_items.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{db}.billing_items")

# COMMAND ----------

# MAGIC %md ###Billing Invoices
# MAGIC
# MAGIC Now generate monthly invoices based on the billing items and other master datasets

# COMMAND ----------

# MAGIC %md let's compute our summary information

# COMMAND ----------

import pyspark.sql.functions as F


# # lets compute the summary minutes messages and bytes transferred
df_enriched_events = (df_billing_items
                      .withColumn("texts_roaming", F.expr("case when event_type='texts_roaming' then 1 else 0 end"))
                      .withColumn("texts_international", F.expr("case when event_type='texts_international' then 1 else 0 end"))
                      .withColumn("call_mins_roaming", F.expr("case when event_type='call_mins_roaming' then cast(ceil(minutes) as decimal(18,3)) else 0.0 end"))
                      .withColumn("call_mins_international", F.expr("case when event_type='call_mins_international' then cast(ceil(minutes) as decimal(18,3)) else 0.0 end"))
                      .withColumn("data_local", F.expr("case when event_type='data_local' then cast(ceil(bytes_transferred) as decimal(30,3)) else 0.0 end"))
                      .withColumn("data_roaming", F.expr("case when event_type='data_roaming' then cast(ceil(bytes_transferred) as decimal(30,3)) else 0.0 end"))
                     )

# ["data_local", "data_roaming", "call_mins_roaming", "texts_roaming", "call_mins_international", "texts_international"]
df_enriched_events.createOrReplaceTempView("telephony_events")

df_summary = spark.sql("""select device_id, 
                                 concat(extract(year FROM event_ts),"-",lpad(extract(month FROM event_ts),2,'0')) as event_month,
                                 round(sum(data_local) / (1024*1024), 3) as data_local_mb, 
                                 round(sum(data_roaming) / (1024*1024), 3) as data_roaming_mb, 
                                 sum(texts_roaming) as texts_roaming,
                                 sum(texts_international) as texts_international,
                                 sum(call_mins_roaming) as call_mins_roaming,
                                 sum(call_mins_international) as call_mins_international, 
                                 count(device_id) as event_count
                                 from telephony_events
                                 group by 1,2
                          
""")
# .write.format("delta").mode("overwrite").saveAsTable()

df_summary.createOrReplaceTempView("event_summary")


# COMMAND ----------

# MAGIC %md let's create a summary temp view

# COMMAND ----------

df_customer_summary = (df_customer_pricing.join(df_summary,df_customer_pricing.device_id == df_summary.device_id )
                       .createOrReplaceTempView("customer_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now generate the invoices

# COMMAND ----------

df_invoices = spark.sql(
    """select customer_id, 
       customer_name, 
       event_month, 
       phone_number, 
       email, 
       plan_name,      
       contract_start_dt, 
       contract_in_months, 
       monthly_charges_dollars as monthly_charges, 
       Calls_Text, 
       Internet_Speed_MBPS, 
       Data_Limit_GB, 
       Data_Outside_Allowance_Per_MB, 
       Roam_Data_charges_per_MB, 
       Roam_Call_charges_per_min, 
       Roam_text_charges, 
       International_text_charge,
       International_call_charge_per_min,
       data_local_mb,
       data_roaming_mb,
       call_mins_roaming,
       texts_roaming,
       call_mins_international,
       texts_international,
       case 
           when Data_Limit_GB != 'UNLIMITED' 
           then case 
                    when (data_local_mb - cast(Data_Limit_GB as double) * 1024) > 0
                    then cast((data_local_mb - cast(Data_Limit_GB as double) * 1024) * Data_Outside_Allowance_Per_MB as decimal(18,2))   
                    else 0 
                end
           else 0 
       end as data_charges_outside_allowance,
       case 
           when data_roaming_mb > 0 
           then cast(data_roaming_mb * Roam_Data_charges_per_MB as decimal(18,2))
           else 0 
       end as roaming_data_charges,
       case 
           when call_mins_roaming > 0 
           then cast(ceiling(call_mins_roaming) * Roam_Call_charges_per_min as decimal(18,2))
           else 0 
       end as roaming_call_charges,
       case 
           when texts_roaming > 0 
           then cast(texts_roaming * Roam_text_charges as decimal(18,2))
           else 0 
       end as roaming_text_charges,
       case 
           when call_mins_international > 0 
           then cast(ceiling(call_mins_international) * International_call_charge_per_min as decimal(18,2))
           else 0 
       end as international_call_charges,
       case 
           when texts_international > 0 
           then cast(texts_international * International_text_charge as decimal(18,2))
           else 0 
       end as international_text_charges,
       monthly_charges_dollars + data_charges_outside_allowance + roaming_data_charges + roaming_call_charges + roaming_text_charges + international_call_charges + international_text_charges as total_charges
from customer_summary

"""
)

# COMMAND ----------

# Write the DataFrame to a Delta table
df_invoices.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{db}.invoice")

# COMMAND ----------

from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

random.seed(42)
fq = lambda t: f"{CATALOG}.{SCHEMA}.{t}"

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
from datetime import date
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

print("✅ All 8 upstream tables created successfully:")
for t in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect():
    print(f"  - {t.tableName}")