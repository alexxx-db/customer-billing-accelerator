# Databricks notebook source
# MAGIC %md
# MAGIC # 📡 Building Tools for a Billing Agent for a Telco Provider
# MAGIC
# MAGIC In this notebook, we define the *tools* that will be made available to the agent. These tools allow the agent to retrieve key billing and customer data, enabling it to answer questions, support customer interactions, and provide useful insights.
# MAGIC
# MAGIC ### 🛠️ Tools Defined in This Notebook
# MAGIC
# MAGIC | Tool Name                     | Description |
# MAGIC |------------------------------|-------------|
# MAGIC | `lookup_customer(input_id)`  | Retrieves customer details including device ID, plan, contact info, and contract start date. |
# MAGIC | `lookup_billing_items(input_id)` | Fetches all billing event records (e.g., call minutes, data usage) for a given device. |
# MAGIC | `lookup_billing_plans()`     | Lists all available billing plans along with pricing, contract duration, and allowances. |
# MAGIC | `lookup_billing(input_customer)` | Provides an aggregated monthly billing summary with detailed charges for a specific customer. |
# MAGIC | `billing_faq(question)`      | Uses vector search to retrieve answers from a frequently asked questions (FAQ) knowledge base. |
# MAGIC
# MAGIC Each of these tools is a modular function designed to be easily callable by the agent during runtime, whether to answer natural language queries or generate insights during workflows.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install and Update Required Python Packages
# Install required packages
# MAGIC %pip install -U -qqqq mlflow-skinny langchain==0.2.16 langgraph-checkpoint==1.0.12 langchain_core langchain-community==0.2.16 langgraph==0.3.4 pydantic langchain_databricks unitycatalog-langchain unitycatalog-ai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Set Working Catalog and Schema
# Set working catalog and schema
CATALOG = config['catalog']
SCHEMA = config['database']
INDEX_NAME = config['vector_search_index']

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions for Customer Insights
# MAGIC ### Tool `lookup_customer`
# MAGIC Fetch customer metadata using their customer_id.

# COMMAND ----------

# DBTITLE 1,Create Customer Lookup Function Create Customer Lookup Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_customer;")

sqlstr_lkp_customer  = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_customer(
  input_id STRING COMMENT 'Input customer id'
)
RETURNS TABLE (
    customer_id BIGINT,
    customer_name STRING,
    device_id BIGINT,
    phone_number BIGINT,
    email STRING,
    plan BIGINT,
    contract_start_dt DATE
)
COMMENT 'Returns the customer data of the customer given the customer_id'
RETURN (
  SELECT 
    customer_id,
    customer_name,
    device_id,
    phone_number,
    email,
    plan,
    contract_start_dt
  FROM {CATALOG}.{SCHEMA}.customers
  WHERE customer_id = CAST(input_id AS DECIMAL)
);
"""
spark.sql(sqlstr_lkp_customer)

# COMMAND ----------

# Test the function
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_customer('4401');"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_billing_items`
# MAGIC Retrieve all billing events for a specific device_id.

# COMMAND ----------

# DBTITLE 1,Function to Retrieve Billing Items Info
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_billing_items;")

sqlstr_lkp_bill_items  = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_billing_items(
  input_id STRING COMMENT 'Input device_id. Use lookup_customer first to get the device_id for a customer.'
)
RETURNS TABLE (
    device_id BIGINT,
    event_type STRING,
    minutes DOUBLE,
    bytes_transferred BIGINT,
    event_ts TIMESTAMP,
    contract_start_dt DATE
)
COMMENT 'Returns all billing items information for a device. Requires device_id obtained from lookup_customer.'
RETURN (
  SELECT 
    device_id,
    event_type,
    minutes,
    bytes_transferred,
    event_ts,
    contract_start_dt 
  FROM {CATALOG}.{SCHEMA}.billing_items
  WHERE device_id = CAST(input_id AS DECIMAL)
  ORDER BY event_ts DESC
);
"""
spark.sql(sqlstr_lkp_bill_items)


# COMMAND ----------

# DBTITLE 1,Test Function
# Test the function
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_billing_items('9862259275');"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_billing_plans`
# MAGIC List available billing plans and associated metadata.

# COMMAND ----------

# DBTITLE 1,Lookup Billing Plans Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_billing_plans;")

sqlstr_lkp_bill_plans  = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_billing_plans()
RETURNS TABLE (
    Plan_key BIGINT,
    Plan_id STRING,
    Plan_name STRING,
    contract_in_months BIGINT,
    monthly_charges_dollars BIGINT,
    Calls_Text STRING,
    Internet_Speed_MBPS STRING,
    Data_Limit_GB STRING,
    Data_Outside_Allowance_Per_MB DOUBLE,
    Roam_Data_charges_per_MB DOUBLE,
    Roam_Call_charges_per_min DOUBLE,
    Roam_text_charges DOUBLE,
    International_call_charge_per_min DOUBLE,
    International_text_charge DOUBLE
)
COMMENT 'Returns billing plan details'
RETURN (
  SELECT
    Plan_key,
    Plan_id,
    Plan_name,
    contract_in_months,
    monthly_charges_dollars,
    Calls_Text,
    Internet_Speed_MBPS,
    Data_Limit_GB,
    Data_Outside_Allowance_Per_MB,
    Roam_Data_charges_per_MB,
    Roam_Call_charges_per_min,
    Roam_text_charges,
    International_call_charge_per_min,
    International_text_charge 
  FROM {CATALOG}.{SCHEMA}.billing_plans
);
"""
spark.sql(sqlstr_lkp_bill_plans)

# COMMAND ----------

# DBTITLE 1,Test Function
# Test the function
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_billing_plans()"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_billing`
# MAGIC Returns monthly aggregated billing summary for a given customer.

# COMMAND ----------

# DBTITLE 1,Billing Information Lookup Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_billing;")

sqlstr_lkp_billing  = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_billing(
    input_customer STRING COMMENT "the customer to lookup for" 
)
RETURNS TABLE (
    customer_id BIGINT,
    event_month STRING,
    plan_name STRING,
    monthly_charges DOUBLE,
    data_charges_outside_allowance DOUBLE,
    roaming_data_charges DOUBLE,
    roaming_call_charges DOUBLE,
    roaming_text_charges DOUBLE,
    international_call_charges DOUBLE,
    international_text_charges DOUBLE,
    total_charges DOUBLE
)
COMMENT "Returns billing information for the customer given the customer_id. Does not return PII fields."
RETURN
SELECT
    customer_id,
    event_month,
    plan_name,
    monthly_charges,
    data_charges_outside_allowance,
    roaming_data_charges,
    roaming_call_charges,
    roaming_text_charges,
    international_call_charges,
    international_text_charges,
    total_charges
FROM {CATALOG}.{SCHEMA}.invoice
WHERE  customer_id = CAST(input_customer AS DECIMAL)
ORDER BY event_month DESC;
"""
spark.sql(sqlstr_lkp_billing)

# COMMAND ----------

# DBTITLE 1,Test Function
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_billing('4401');"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Vector Search for FAQ Support
# MAGIC Search the faq_index for answers to frequently asked billing questions using vector similarity.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Query FAQ Index for Change Bill Due Date
result = spark.sql(f"""
SELECT * 
FROM vector_search(index => '{CATALOG}.{SCHEMA}.{INDEX_NAME}', query => 'Can I change my bill due date', num_results => 5)
""")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `billing_faq`
# MAGIC Wrapper function to perform vector-based FAQ lookups.

# COMMAND ----------

# DBTITLE 1,Create Billing FAQ Search Function
sqlstr_billing_faq  = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.billing_faq(question STRING COMMENT "FAQ search, the question to ask is a frequently asked question about billing")
RETURNS STRING
LANGUAGE SQL
COMMENT 'FAQ answer' 
RETURN SELECT concat_ws('\n', collect_list(faq)) from vector_search(index => '{CATALOG}.{SCHEMA}.{INDEX_NAME}', query => question, num_results => 1);
"""
spark.sql(sqlstr_billing_faq)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `get_monitoring_status`
# MAGIC Returns the current monitoring state: what's been detected, what's been alerted,
# MAGIC and what's pending review. Use for questions like "what's new since yesterday?"

# COMMAND ----------

# DBTITLE 1,Create get_monitoring_status Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.get_monitoring_status;")

sqlstr_monitoring_status = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_monitoring_status(
  since_hours INT COMMENT 'Look back this many hours. Use 24 for daily summary, 168 for weekly. Use 0 for all time.'
)
RETURNS TABLE (
  event_month          STRING,
  anomaly_type         STRING,
  total_anomalies      BIGINT,
  alerted_count        BIGINT,
  pending_alert_count  BIGINT,
  last_detection_ts    TIMESTAMP,
  last_alert_ts        TIMESTAMP
)
COMMENT 'Returns current monitoring summary. Use to answer: how many anomalies exist, which are new, which have been alerted. For what is new since yesterday use since_hours=24.'
RETURN (
  SELECT *
  FROM {CATALOG}.{SCHEMA}.billing_monitoring_summary
  WHERE since_hours = 0
     OR last_detection_ts >= CURRENT_TIMESTAMP - MAKE_INTERVAL(0, 0, 0, 0, since_hours, 0, 0)
  ORDER BY event_month DESC
);
"""
spark.sql(sqlstr_monitoring_status)

# COMMAND ----------

# DBTITLE 1,Test get_monitoring_status
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.get_monitoring_status(0);"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_operational_kpis`
# MAGIC Returns daily operational KPIs: DBU consumption, cost, pipeline health, Genie performance.

# COMMAND ----------

# DBTITLE 1,Create lookup_operational_kpis Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_operational_kpis;")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_operational_kpis(
  lookback_days INT COMMENT 'Number of days to look back (e.g. 7 for last week, 30 for last month)'
)
RETURNS TABLE (
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
COMMENT 'Returns daily operational KPIs for the billing platform: DBU consumption, estimated cost, pipeline health, Genie usage, and warehouse performance.'
RETURN (
  SELECT *
  FROM {CATALOG}.{SCHEMA}.telemetry_operational_kpis
  WHERE kpi_date >= CURRENT_DATE - lookback_days
  ORDER BY kpi_date DESC
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_job_reliability`
# MAGIC Returns rolling 30-day reliability metrics for Databricks jobs.

# COMMAND ----------

# DBTITLE 1,Create lookup_job_reliability Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_job_reliability;")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_job_reliability(
  billing_pipelines_only BOOLEAN COMMENT 'If true, return only billing accelerator jobs. If false, return all tracked jobs.'
)
RETURNS TABLE (
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
COMMENT 'Returns rolling 30-day reliability metrics for Databricks jobs. Use for questions about job health, failure rates, and runtimes.'
RETURN (
  SELECT *
  FROM {CATALOG}.{SCHEMA}.telemetry_job_reliability
  WHERE billing_pipelines_only = false OR is_billing_pipeline = true
  ORDER BY is_billing_pipeline DESC, success_rate_pct ASC
);
""")

# COMMAND ----------

# DBTITLE 1,Test Telemetry Functions
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_operational_kpis(7);"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_job_reliability(true);"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_customer_erp_profile`
# MAGIC Returns ERP account profile for a customer.

# COMMAND ----------

# DBTITLE 1,Create lookup_customer_erp_profile Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_customer_erp_profile;")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_customer_erp_profile(
  input_customer_id STRING COMMENT 'Customer ID to look up ERP account profile'
)
RETURNS TABLE (
  customer_id BIGINT, erp_account_id STRING, account_type STRING, credit_rating STRING,
  payment_terms_days INT, account_status STRING, ar_balance_usd DOUBLE,
  overdue_balance_usd DOUBLE, erp_segment STRING, erp_source_system STRING
)
COMMENT 'Returns ERP account profile for a customer: AR status, credit rating, payment terms. Do NOT expose ar_balance or overdue_balance to the customer directly.'
RETURN (
  SELECT customer_id, erp_account_id, account_type, credit_rating,
         payment_terms_days, account_status, ar_balance_usd,
         overdue_balance_usd, erp_segment, erp_source_system
  FROM {CATALOG}.{SCHEMA}.silver_customer_account_dims
  WHERE customer_id = CAST(input_customer_id AS BIGINT)
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_revenue_attribution`
# MAGIC Returns revenue reconciliation between billing and ERP.

# COMMAND ----------

# DBTITLE 1,Create lookup_revenue_attribution Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_revenue_attribution;")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_revenue_attribution(
  input_customer_id STRING COMMENT 'Customer ID',
  month_filter STRING COMMENT 'Month in YYYY-MM format. Pass empty string for all months.'
)
RETURNS TABLE (
  event_month STRING, billed_total_usd DOUBLE, erp_recognized_revenue_usd DOUBLE,
  erp_collected_revenue_usd DOUBLE, erp_overdue_revenue_usd DOUBLE,
  revenue_variance_usd DOUBLE, revenue_variance_pct DOUBLE
)
COMMENT 'Returns revenue reconciliation between billing and ERP for a customer. Use for billing dispute investigation.'
RETURN (
  SELECT event_month, billed_total_usd, erp_recognized_revenue_usd,
         erp_collected_revenue_usd, erp_overdue_revenue_usd,
         revenue_variance_usd, revenue_variance_pct
  FROM {CATALOG}.{SCHEMA}.gold_revenue_attribution
  WHERE customer_id = CAST(input_customer_id AS BIGINT)
    AND (month_filter = '' OR event_month = month_filter)
  ORDER BY event_month DESC
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `get_finance_operations_summary`
# MAGIC Returns monthly finance operations KPI summary.

# COMMAND ----------

# DBTITLE 1,Create get_finance_operations_summary Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.get_finance_operations_summary;")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.get_finance_operations_summary(
  lookback_months INT COMMENT 'Number of months to include (e.g. 3, 6, 12)'
)
RETURNS TABLE (
  event_month STRING, account_type STRING, erp_segment STRING,
  customer_count BIGINT, total_billed_usd DOUBLE, total_erp_revenue_usd DOUBLE,
  total_overdue_usd DOUBLE, arpu_usd DOUBLE, total_roaming_revenue_usd DOUBLE,
  total_intl_revenue_usd DOUBLE, total_opex_usd DOUBLE,
  opex_ratio_pct DOUBLE, overdue_ar_ratio_pct DOUBLE
)
COMMENT 'Returns monthly finance operations KPI summary: revenue, ARPU, AR health, OPEX ratios. Segmented by account type and ERP segment.'
RETURN (
  SELECT event_month, account_type, erp_segment, customer_count,
         total_billed_usd, total_erp_revenue_usd, total_overdue_usd,
         arpu_usd, total_roaming_revenue_usd, total_intl_revenue_usd,
         total_opex_usd, opex_ratio_pct, overdue_ar_ratio_pct
  FROM {CATALOG}.{SCHEMA}.gold_finance_operations_summary
  WHERE event_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -lookback_months), 'yyyy-MM')
  ORDER BY event_month DESC, total_billed_usd DESC
);
""")

# COMMAND ----------

# DBTITLE 1,Test External Data Functions
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_customer_erp_profile('4401');"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_revenue_attribution('4401', '');"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.get_finance_operations_summary(3);"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_open_disputes`
# MAGIC Returns open billing disputes.

# COMMAND ----------

# DBTITLE 1,Create lookup_open_disputes Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_open_disputes;")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_open_disputes(
  input_customer_id STRING COMMENT 'Customer ID, or empty string for all open disputes'
)
RETURNS TABLE (
  dispute_id STRING, customer_id BIGINT, dispute_type STRING, status STRING,
  description STRING, disputed_amount_usd DOUBLE, created_at TIMESTAMP, anomaly_id STRING
)
COMMENT 'Returns open billing disputes for a customer or all customers.'
RETURN (
  SELECT dispute_id, customer_id, dispute_type, status,
         description, disputed_amount_usd, created_at, anomaly_id
  FROM {CATALOG}.{SCHEMA}.billing_disputes
  WHERE (input_customer_id = '' OR customer_id = CAST(input_customer_id AS BIGINT))
    AND status NOT IN ('RESOLVED_CREDIT', 'RESOLVED_NO_ACTION', 'CLOSED')
  ORDER BY created_at DESC LIMIT 50
);
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_write_audit`
# MAGIC Returns the write audit trail.

# COMMAND ----------

# DBTITLE 1,Create lookup_write_audit Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_write_audit;")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_write_audit(
  lookback_hours INT COMMENT 'Look back this many hours. Use 24 for today, 168 for last week.'
)
RETURNS TABLE (
  audit_id STRING, action_type STRING, target_record_id STRING,
  customer_id BIGINT, result_status STRING, result_message STRING, executed_at TIMESTAMP
)
COMMENT 'Returns the write audit trail for the given lookback window.'
RETURN (
  SELECT audit_id, action_type, target_record_id, customer_id,
         result_status, result_message, executed_at
  FROM {CATALOG}.{SCHEMA}.billing_write_audit
  WHERE executed_at >= CURRENT_TIMESTAMP - MAKE_INTERVAL(0, 0, 0, 0, lookback_hours, 0, 0)
  ORDER BY executed_at DESC LIMIT 100
);
""")