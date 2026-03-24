# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Parameters
# MAGIC Please change as required.

# COMMAND ----------

# DBTITLE 1,Initialize Config Dictionary If Not Present
if 'config' not in locals():
  config = {}

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data Catalog configs

# COMMAND ----------

# DBTITLE 1,Set Catalog and Database in Config Dictionary
# Catalog and database
# Change the Catalog and database name as per your requirements

config['catalog'] = 'cme_demos_alex_barreto'
config['database'] = 'telco_billing_db'

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Key configurations for the data generation 

# COMMAND ----------

# DBTITLE 1,Set Data Generation Configuration Parameters
# Data Generation Configs

config['UNIQUE_CUSTOMERS'] = 50000
config['CUSTOMER_MIN_VALUE'] = 1000
config['DEVICE_MIN_VALUE'] = 1000000000
config['SUBSCRIBER_NUM_MIN_VALUE'] = 1000000000
config['UNIQUE_PLANS'] = 10 # Number of unique plans are 10 in the Github dataset. If you need to change this value, you will need to change the billing plan dataset as well.
config['PLAN_MIN_VALUE'] = 1


config['AVG_EVENTS_PER_CUSTOMER'] = 10

config['shuffle_partitions_requested'] = 8
config['partitions_requested'] = 8
config['NUM_DAYS']=365 # number of days to generate data for
config['MB_100'] = 50000000 # Max bytes transferred
config['K_1'] = 100000 # Min bytes transferred
config['start_dt']="2024-01-01 00:00:00" 
config['end_dt']="2024-12-31 11:59:59"




# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent configuration parameters

# COMMAND ----------

# DBTITLE 1,Set Agent Configuration Parameters in Config Dictionary
# Agent Configs
config['agent_name'] = 'ai_billing_agent'
config['VECTOR_SEARCH_ENDPOINT_NAME'] = 'vector-search-telco-billing'
config['vector_search_index'] = 'faq_indx1'
config['embedding_model_endpoint_name'] = 'databricks-gte-large-en'  # This is default enbedding model and needs to be updated for your environment
config['llm_endpoint']="databricks-claude-3-7-sonnet" # This is default token based pricing endpoint and needs to be updated based on your requirement
config['warehouse_id']="148ccb90800933a1" # This is the warehouse id and need to be updated for your environment

# Tools 
config['tools_billing_faq'] = config['catalog']+'.'+config['database']+'.billing_faq'
config['tools_billing'] = config['catalog']+'.'+config['database']+'.lookup_billing'
config['tools_items'] = config['catalog']+'.'+config['database']+'.lookup_billing_items'
config['tools_plans'] = config['catalog']+'.'+config['database']+'.lookup_billing_plans'
config['tools_customer'] = config['catalog']+'.'+config['database']+'.lookup_customer'
config['tools_anomalies'] = config['catalog']+'.'+config['database']+'.lookup_billing_anomalies'
config['tools_monitoring_status'] = config['catalog']+'.'+config['database']+'.get_monitoring_status'

# Continuous monitoring
config['dlt_pipeline_id'] = ''
config['monitoring_state_table'] = config['catalog']+'.'+config['database']+'.billing_monitoring_state'
config['monitoring_summary_view'] = config['catalog']+'.'+config['database']+'.billing_monitoring_summary'
config['dlt_events_table'] = config['catalog']+'.'+config['database']+'.billing_events_streaming'
config['dlt_running_table'] = config['catalog']+'.'+config['database']+'.billing_monthly_running'

# System table telemetry
config['telemetry_dbu_table'] = config['catalog']+'.'+config['database']+'.telemetry_dbu_usage'
config['telemetry_jobs_table'] = config['catalog']+'.'+config['database']+'.telemetry_job_runs'
config['telemetry_queries_table'] = config['catalog']+'.'+config['database']+'.telemetry_query_history'
config['telemetry_dbu_daily_table'] = config['catalog']+'.'+config['database']+'.telemetry_dbu_daily'
config['telemetry_job_reliability_table'] = config['catalog']+'.'+config['database']+'.telemetry_job_reliability'
config['telemetry_wh_utilization_table'] = config['catalog']+'.'+config['database']+'.telemetry_warehouse_utilization'
config['telemetry_kpis_table'] = config['catalog']+'.'+config['database']+'.telemetry_operational_kpis'
config['tools_operational_kpis'] = config['catalog']+'.'+config['database']+'.lookup_operational_kpis'
config['tools_job_reliability'] = config['catalog']+'.'+config['database']+'.lookup_job_reliability'

# Genie Space
config['genie_space_name'] = 'Telco Billing Analytics'
config['genie_space_description'] = (
    'Natural language analytics over telco billing data. '
    'Includes invoice_analytics (monthly charges per customer), billing_plans (pricing and allowances), '
    'billing_anomalies (detected charge spikes and roaming spikes), '
    'billing_monitoring_state (alert dispatch audit log with severity and delivery status), '
    'and billing_monthly_running (real-time streaming charge estimates per customer per month). '
    'Tables join via plan_name and customer_id. PII fields are excluded. '
    'telemetry_dbu_daily tracks DBU consumption and estimated cost by SKU and usage type. '
    'telemetry_job_reliability tracks 30-day rolling success rates for platform jobs. '
    'telemetry_warehouse_utilization tracks hourly query performance for the SQL warehouse. '
    'telemetry_operational_kpis is the daily summary of platform health and cost.'
)
config['genie_space_tables'] = [
    # Domain data
    config['catalog'] + '.' + config['database'] + '.invoice_analytics',
    config['catalog'] + '.' + config['database'] + '.billing_plans',
    # Anomaly and monitoring
    config['catalog'] + '.' + config['database'] + '.billing_anomalies',
    config['catalog'] + '.' + config['database'] + '.billing_monitoring_state',
    config['catalog'] + '.' + config['database'] + '.billing_monthly_running',
    # Operational telemetry (Silver + Gold only — no Bronze)
    config['catalog'] + '.' + config['database'] + '.telemetry_dbu_daily',
    config['catalog'] + '.' + config['database'] + '.telemetry_job_reliability',
    config['catalog'] + '.' + config['database'] + '.telemetry_warehouse_utilization',
    config['catalog'] + '.' + config['database'] + '.telemetry_operational_kpis',
]
config['genie_space_sample_questions'] = [
    "What is the average monthly total charge across all customers?",
    "Which billing plan has the highest average roaming data charges?",
    "How many customers are on each billing plan?",
    "What are the top 10 customers by total charges in the last 3 months?",
    "What is the month-over-month trend in international call charges?",
    "Which customers have total charges above $100 in any single month?",
    "Compare average total charges between 12-month and 24-month contract plans",
    "How many billing anomalies were detected by type?",
    "Which customers have the most billing anomalies?",
    "How many anomalies have been alerted vs how many are still pending notification?",
    "What is the real-time estimated total charge for the top 10 highest-spending customers this month?",
    "Which customers have anomalies that have not been alerted yet?",
    "Show the trend of estimated charges vs actual billed charges for the last 3 months",
    "What was the total DBU consumption and estimated cost for the last 7 days?",
    "Is the anomaly detection job currently healthy? What is its 30-day success rate?",
    "On which days did we have cost anomaly flags raised?",
    "What is the average Genie query latency compared to last week?",
    "Which billing pipeline jobs have failed in the last 30 days?",
]
config['genie_space_id'] = None  # Set by 03a_create_genie_space after creation

# Agent Bricks (Supervisor Agent)
config['ka_name'] = 'Telco Billing FAQ'
config['ka_description'] = 'Answers frequently asked billing questions about bill calculation, payments, autopay, disputes, refunds, late fees, data usage, roaming charges, and due date changes.'
config['ka_instructions'] = (
    'Answer billing questions using the FAQ documents. '
    'Be concise and cite the relevant FAQ when possible. '
    'If the question is not covered by the FAQ, say so and suggest contacting customer support.'
)
config['ka_volume_path'] = f"/Volumes/{config['catalog']}/{config['database']}/billing_faq_docs"
config['ka_tile_id'] = None  # Set by 04_agent_bricks_deployment after creation

config['mas_name'] = 'Telco Billing Support'
config['mas_description'] = (
    'Telco billing support supervisor that routes queries to specialized agents: '
    'a FAQ Knowledge Assistant for general billing questions and a Genie Space '
    'for ad-hoc billing analytics across the customer base.'
)
config['mas_instructions'] = """Route queries as follows:
- General billing questions, how-to, policy/procedure -> billing_faq_agent
- Data analysis, charge trends, plan comparisons, aggregations, top-N -> billing_analytics_agent
- Questions spanning both -> chain FAQ for explanation, then Analytics for data; synthesize one answer.

If the query requires a specific customer's billing details, inform the user that individual lookups require the dedicated customer care tools.
If unclear, ask the user to clarify."""
config['mas_tile_id'] = None  # Set by 04_agent_bricks_deployment after creation
