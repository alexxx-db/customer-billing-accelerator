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
%pip install -U -qqqq mlflow-skinny langchain==0.2.16 langgraph-checkpoint==1.0.12 langchain_core langchain-community==0.2.16 langgraph==0.2.16 pydantic langchain_databricks unitycatalog-langchain unitycatalog-ai
dbutils.library.restartPython()

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
  input_id STRING COMMENT 'Input customer id'
)
RETURNS TABLE (
    device_id BIGINT,
    event_type STRING,
    minutes DOUBLE,
    bytes_transferred BIGINT,
    event_ts TIMESTAMP,
    contract_start_dt DATE
)
COMMENT 'Returns all billing items information for the customer.You need device_id from the lookup_customer function'
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
    customer_name STRING,
    event_month DATE,
    phone_number BIGINT,
    data_charges_outside_allowance DOUBLE,
    roaming_data_charges DOUBLE,
    roaming_call_charges DOUBLE,
    roaming_text_charges DOUBLE,
    international_call_charges DOUBLE,
    international_text_charges DOUBLE,
    total_charges DOUBLE
)
COMMENT "Returns billing information for the customer given the customer_id"
RETURN
SELECT 
    customer_id,
    customer_name,
    event_month,
    phone_number,
    data_charges_outside_allowance,
    roaming_data_charges,
    roaming_call_charges,
    roaming_text_charges,
    international_call_charges,
    international_text_charges,
    total_charges
FROM {CATALOG}.{SCHEMA}.invoice
WHERE  customer_id = input_customer
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
COMMENT 'fqa answer' 
RETURN SELECT string(collect_set(faq)) from vector_search(index => '{CATALOG}.{SCHEMA}.{INDEX_NAME}', query => question, num_results => 1);
"""
spark.sql(sqlstr_billing_faq)