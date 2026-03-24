# Databricks notebook source
# MAGIC %md
# MAGIC # ERP Data Simulation (Track B)
# MAGIC
# MAGIC Generates synthetic ERP accounts/orders, procurement costs, and FX rates using `dbldatagen`.
# MAGIC Creates the `ext_*` view abstraction layer that makes simulation data transparent to
# MAGIC downstream Silver/Gold transformations and the agent.
# MAGIC
# MAGIC Run this when no real ERP credentials are configured (Track B).

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F
from pyspark.sql import Row
from datetime import date, timedelta
import random

catalog = config['catalog']
schema = config['database']
CUSTOMER_MIN = config['CUSTOMER_MIN_VALUE']
UNIQUE_CUSTOMERS = config['UNIQUE_CUSTOMERS']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulated ERP Accounts Receivable

# COMMAND ----------

# DBTITLE 1,Generate AR Master Data
ar_dataspec = (
    dg.DataGenerator(spark, rows=UNIQUE_CUSTOMERS, partitions=8)
    .withColumn("customer_id", "decimal(10)", minValue=CUSTOMER_MIN, uniqueValues=UNIQUE_CUSTOMERS)
    .withColumn("erp_account_id", "string", format="ACC-%07d", baseColumn="customer_id")
    .withColumn("account_type", "string",
                values=["RESIDENTIAL", "BUSINESS_SMB", "BUSINESS_ENTERPRISE", "GOVERNMENT"],
                weights=[65, 25, 8, 2], random=True)
    .withColumn("credit_rating", "string",
                values=["AAA", "AA", "A", "BBB", "BB", "B", "UNRATED"],
                weights=[5, 10, 20, 30, 20, 10, 5], random=True)
    .withColumn("payment_terms_days", "int", values=[7, 14, 30, 45, 60], weights=[10, 20, 40, 20, 10], random=True)
    .withColumn("account_status", "string",
                values=["ACTIVE", "SUSPENDED", "CLOSED", "PENDING"],
                weights=[88, 5, 5, 2], random=True)
    .withColumn("ar_balance_usd", "decimal(12,2)", minValue=0.0, maxValue=500.0, random=True)
    .withColumn("overdue_balance_usd", "decimal(12,2)", minValue=0.0, maxValue=200.0, random=True)
    .withColumn("lifetime_value_usd", "decimal(14,2)", minValue=100.0, maxValue=50000.0, random=True)
    .withColumn("erp_region", "string", values=["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"], random=True)
    .withColumn("erp_segment", "string",
                values=["MASS_MARKET", "VALUE", "PREMIUM", "ENTERPRISE"],
                weights=[40, 30, 25, 5], random=True)
    .withColumn("erp_source_system", "string",
                values=["SAP_S4HANA", "ORACLE_FUSION", "WORKDAY"],
                weights=[50, 35, 15], random=True)
    .withColumn("ingested_at", "timestamp", expr="current_timestamp()")
)

df_ar = ar_dataspec.build()
df_ar.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.simulated_erp_accounts")
print(f"simulated_erp_accounts: {df_ar.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulated ERP Orders / Revenue

# COMMAND ----------

# DBTITLE 1,Generate Order Revenue Data
MONTHS = [f"2024-{str(m).zfill(2)}" for m in range(1, 13)]

order_dataspec = (
    dg.DataGenerator(spark, rows=UNIQUE_CUSTOMERS * 8, partitions=8)
    .withColumn("customer_id", "decimal(10)", minValue=CUSTOMER_MIN, uniqueValues=UNIQUE_CUSTOMERS)
    .withColumn("order_month", "string", values=MONTHS, random=True)
    .withColumn("erp_order_id", "string", format="ORD-%010d", baseColumn=["customer_id", "order_month"])
    .withColumn("recognized_revenue_usd", "decimal(12,2)", minValue=10.0, maxValue=300.0, random=True)
    .withColumn("order_type", "string",
                values=["RECURRING", "ONE_TIME", "ADJUSTMENT", "CREDIT"],
                weights=[75, 10, 10, 5], random=True)
    .withColumn("revenue_category", "string",
                values=["SUBSCRIPTION", "DATA_SERVICES", "ROAMING", "INTERNATIONAL", "OTHER"],
                weights=[50, 20, 15, 10, 5], random=True)
    .withColumn("collection_status", "string",
                values=["COLLECTED", "PENDING", "OVERDUE", "WRITTEN_OFF"],
                weights=[80, 12, 6, 2], random=True)
    .withColumn("ingested_at", "timestamp", expr="current_timestamp()")
)

df_orders = order_dataspec.build().dropDuplicates(["customer_id", "order_month", "erp_order_id"])
df_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.simulated_erp_orders")
print(f"simulated_erp_orders: {df_orders.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulated Procurement Costs

# COMMAND ----------

# DBTITLE 1,Generate Procurement Data
COST_CATEGORIES = [
    "NETWORK_OPEX", "ROAMING_INTERCONNECT", "SPECTRUM_LICENSE",
    "CUSTOMER_ACQUISITION", "BILLING_PROCESSING", "REGULATORY_COMPLIANCE",
    "IT_INFRASTRUCTURE", "FIELD_OPERATIONS",
]

proc_rows = [
    Row(
        cost_month=m, cost_category=cat,
        po_number=f"PO-{m}-{cat[:4]}-{i:04d}",
        vendor_name=f"Vendor_{cat[:8]}_{i % 10:02d}",
        cost_usd=round(random.uniform(50_000, 2_000_000), 2),
        cost_type=random.choice(["CAPEX", "OPEX"]),
        approval_status=random.choice(["APPROVED", "APPROVED", "APPROVED", "PENDING"]),
    )
    for m in MONTHS
    for i, cat in enumerate(COST_CATEGORIES)
]

df_proc = spark.createDataFrame(proc_rows).withColumn("ingested_at", F.current_timestamp())
df_proc.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.simulated_procurement")
print(f"simulated_procurement: {df_proc.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulated FX Rates

# COMMAND ----------

# DBTITLE 1,Generate FX Rate Data
CURRENCIES = ["EUR", "GBP", "JPY", "CAD", "AUD", "MXN", "BRL", "INR", "SGD", "CHF"]
BASE_RATES = {
    "EUR": 0.92, "GBP": 0.79, "JPY": 150.5, "CAD": 1.36,
    "AUD": 1.52, "MXN": 17.2, "BRL": 4.97, "INR": 83.1,
    "SGD": 1.34, "CHF": 0.88,
}

fx_rows = []
start = date(2024, 1, 1)
for day_offset in range(366):
    d = start + timedelta(days=day_offset)
    for ccy in CURRENCIES:
        rate = BASE_RATES[ccy] * (1 + random.gauss(0, 0.005))
        fx_rows.append(Row(
            rate_date=d, from_currency="USD", to_currency=ccy,
            exchange_rate=round(rate, 6), rate_source="SIMULATED_ECB",
        ))

df_fx = spark.createDataFrame(fx_rows).withColumn("ingested_at", F.current_timestamp())
df_fx.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.simulated_fx_rates")
print(f"simulated_fx_rates: {df_fx.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create View Abstraction Layer

# COMMAND ----------

# DBTITLE 1,Create ext_* Views (Track B: point at simulation tables)
views = {
    "ext_erp_accounts": f"""
        SELECT CAST(customer_id AS BIGINT) AS customer_id, erp_account_id, account_type, credit_rating,
               payment_terms_days, account_status, CAST(ar_balance_usd AS DOUBLE) AS ar_balance_usd,
               CAST(overdue_balance_usd AS DOUBLE) AS overdue_balance_usd,
               CAST(lifetime_value_usd AS DOUBLE) AS lifetime_value_usd, erp_region,
               erp_segment, erp_source_system, ingested_at
        FROM {catalog}.{schema}.simulated_erp_accounts
    """,
    "ext_erp_orders": f"""
        SELECT CAST(customer_id AS BIGINT) AS customer_id, order_month, erp_order_id,
               CAST(recognized_revenue_usd AS DOUBLE) AS recognized_revenue_usd,
               order_type, revenue_category, collection_status, ingested_at
        FROM {catalog}.{schema}.simulated_erp_orders
    """,
    "ext_procurement_costs": f"""
        SELECT cost_month, cost_category, po_number, vendor_name,
               cost_usd, cost_type, approval_status, ingested_at
        FROM {catalog}.{schema}.simulated_procurement
    """,
    "ext_fx_rates": f"""
        SELECT rate_date, from_currency, to_currency,
               exchange_rate, rate_source, ingested_at
        FROM {catalog}.{schema}.simulated_fx_rates
    """,
}

for view_name, view_sql in views.items():
    spark.sql(f"CREATE OR REPLACE VIEW {catalog}.{schema}.{view_name} AS {view_sql}")
    print(f"Created view: {catalog}.{schema}.{view_name}")
