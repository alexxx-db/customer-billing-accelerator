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

config['catalog'] = 'alexxx'
config['database'] = 'customer_billing_accelerator'

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


config['AVG_EVENTS_PER_CUSTOMER'] = 20

config['shuffle_partitions_requested'] = 8
config['partitions_requested'] = 8
config['NUM_DAYS']=365 # number of days to generate data for
config['MB_100'] = 50000000 # Max bytes transferred
config['K_1'] = 100000 # Min bytes transferred
config['start_dt']="2024-01-01 00:00:00" 
config['end_dt']="2026-12-31 11:59:59"




# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent configuration parameters

# COMMAND ----------

# DBTITLE 1,Set Agent Configuration Parameters in Config Dictionary
# Agent Configs
config['agent_name'] = 'ai_customer_billing_agent'
config['VECTOR_SEARCH_ENDPOINT_NAME'] = 'vector-search-customer-billing'
config['vector_search_index'] = 'faq_indx1'
config['embedding_model_endpoint_name'] = 'databricks-gte-large-en'  # This is default enbedding model and needs to be updated for your environment
config['llm_endpoint']="databricks-claude-sonnet-5"  # Verified present 2026-09-01. Pay-per-token endpoint; change per workspace.
config['warehouse_id']="b50e5cec5077ea22"  # entrada_serverless_sql_databricks_warehouse, verified live 2026-09-01

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

# External data integration (Gap #5)
# Track A — real federation (leave empty to use Track B simulation)
config['erp_connection_host'] = ''
config['erp_connection_port'] = '5432'
config['erp_connection_user'] = ''
# WARNING: Do NOT commit real passwords. Use Databricks Secret Scope:
# config['erp_connection_password'] = dbutils.secrets.get(scope="billing-erp", key="password")
config['erp_connection_password'] = ''
config['erp_connection_database'] = 'erp'
config['erp_uc_connection'] = ''
config['erp_foreign_catalog'] = ''

# UC tools — external data
# Write-back (Gap #6)
config['disputes_table'] = config['catalog']+'.'+config['database']+'.billing_disputes'
config['write_audit_table'] = config['catalog']+'.'+config['database']+'.billing_write_audit'
# Domain abstraction (Gap #7)
config['active_domain'] = 'Customer'
config['domain_display_name'] = 'Telecommunications'
config['domain_industry'] = 'Telecommunications'
config['domain_charge_labels'] = {}
config['domain_agent_prompt_section'] = ''
config['v_billing_summary'] = config['catalog']+'.'+config['database']+'.v_billing_summary'
config['v_customer_profile'] = config['catalog']+'.'+config['database']+'.v_customer_profile'
config['v_service_catalog'] = config['catalog']+'.'+config['database']+'.v_service_catalog'
config['v_billing_events'] = config['catalog']+'.'+config['database']+'.v_billing_events'

# Multi-persona (Gap #8)
config['default_persona'] = 'customer_care'
config['persona_config_path'] = ''
config['personas'] = {}

config['tools_open_disputes'] = config['catalog']+'.'+config['database']+'.lookup_open_disputes'
config['tools_write_audit'] = config['catalog']+'.'+config['database']+'.lookup_write_audit'
config['tools_customer_360'] = config['catalog']+'.'+config['database']+'.lookup_customer_360'
config['tools_customer_hierarchy'] = config['catalog']+'.'+config['database']+'.lookup_customer_hierarchy'
config['tools_pricing_drift'] = config['catalog']+'.'+config['database']+'.detect_pricing_drift'
config['tools_pricing_history'] = config['catalog']+'.'+config['database']+'.lookup_pricing_history'
config['tools_product_pricing'] = config['catalog']+'.'+config['database']+'.lookup_product_pricing'
config['tools_customer_contract'] = config['catalog']+'.'+config['database']+'.lookup_customer_contract'
config['tools_overage'] = config['catalog']+'.'+config['database']+'.detect_overage'
config['tools_usage_history'] = config['catalog']+'.'+config['database']+'.lookup_usage_history'
config['tools_plan_entitlement'] = config['catalog']+'.'+config['database']+'.lookup_plan_entitlement'
config['tools_inventory_assets'] = config['catalog']+'.'+config['database']+'.lookup_inventory_assets'
config['tools_usage_forecast'] = config['catalog']+'.'+config['database']+'.lookup_usage_forecast'
config['tools_order_reconcile'] = config['catalog']+'.'+config['database']+'.reconcile_order_to_cash'
config['tools_revenue_leakage'] = config['catalog']+'.'+config['database']+'.detect_revenue_leakage'
config['tools_dso_region'] = config['catalog']+'.'+config['database']+'.lookup_dso_by_region'
config['tools_order_lines'] = config['catalog']+'.'+config['database']+'.lookup_order_line_items'
config['tools_pos_gap'] = config['catalog']+'.'+config['database']+'.detect_pos_reconciliation_gap'
config['tools_store_compare'] = config['catalog']+'.'+config['database']+'.compare_stores_by_region'
config['tools_store_profile'] = config['catalog']+'.'+config['database']+'.lookup_store_profile'
config['tools_store_hierarchy'] = config['catalog']+'.'+config['database']+'.lookup_store_hierarchy'
config['tools_plan_upgrade'] = config['catalog']+'.'+config['database']+'.recommend_plan_upgrade'
config['writeback_param_mode'] = 'parameters'

# UC tools — external data
config['tools_customer_erp_profile'] = config['catalog']+'.'+config['database']+'.lookup_customer_erp_profile'
config['tools_revenue_attribution'] = config['catalog']+'.'+config['database']+'.lookup_revenue_attribution'
config['tools_finance_ops_summary'] = config['catalog']+'.'+config['database']+'.get_finance_operations_summary'

# Genie Space
config['genie_space_name'] = 'Customer Billing Analytics'
config['genie_space_description'] = (
    'Natural language analytics over customer billing data. '
    'Includes invoice_analytics (monthly charges per customer), billing_plans (pricing and allowances), '
    'billing_anomalies (detected charge spikes and roaming spikes), '
    'billing_monitoring_state (alert dispatch audit log with severity and delivery status), '
    'and billing_monthly_running (real-time streaming charge estimates per customer per month). '
    'Tables join via plan_name and customer_id. PII fields are excluded. '
    'telemetry_dbu_daily tracks DBU consumption and estimated cost by SKU and usage type. '
    'telemetry_job_reliability tracks 30-day rolling success rates for platform jobs. '
    'telemetry_warehouse_utilization tracks hourly query performance for the SQL warehouse. '
    'telemetry_operational_kpis is the daily summary of platform health and cost. '
    'silver_customer_account_dims joins ERP AR data with retail customers. '
    'gold_revenue_attribution reconciles billing revenue with ERP recognized revenue. '
    'gold_finance_operations_summary is the monthly finance ops KPI roll-up.'
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
    # External data (Silver + Gold only — no simulated Bronze or ext_* views)
    config['catalog'] + '.' + config['database'] + '.silver_customer_account_dims',
    config['catalog'] + '.' + config['database'] + '.silver_fx_daily',
    config['catalog'] + '.' + config['database'] + '.silver_procurement_monthly',
    config['catalog'] + '.' + config['database'] + '.silver_conformed_kpi_defs',
    config['catalog'] + '.' + config['database'] + '.gold_revenue_attribution',
    config['catalog'] + '.' + config['database'] + '.gold_finance_operations_summary',
    # Write-back tables (Gap #6)
    config['catalog'] + '.' + config['database'] + '.billing_disputes',
    config['catalog'] + '.' + config['database'] + '.billing_write_audit',
    # Canonical views (universal across all domains)
    config['catalog'] + '.' + config['database'] + '.v_billing_summary',
    config['catalog'] + '.' + config['database'] + '.v_customer_profile',
    config['catalog'] + '.' + config['database'] + '.v_service_catalog',
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
    "What is the total billed revenue vs ERP recognized revenue for the last 3 months?",
    "Which customer segments have the highest overdue AR ratio?",
    "What is the OPEX ratio trend for the last 6 months?",
    "Show revenue variance between billing and ERP by account type",
    "What are the top procurement cost categories and how have they trended?",
    "Show me the FX rate for EUR over the last 90 days",
    "How many open billing disputes are there by type?",
    "What is the total disputed amount across all open disputes?",
    "Show me all write operations the AI agent has performed in the last 24 hours",
    "Which customers have disputes open for more than 7 days?",
]
config['genie_space_id'] = None  # Set by 03a_create_genie_space after creation

# Agent Bricks (Supervisor Agent)
config['ka_name'] = 'Customer Billing FAQ'
config['ka_description'] = 'Answers frequently asked billing questions about bill calculation, payments, autopay, disputes, refunds, late fees, data usage, roaming charges, and due date changes.'
config['ka_instructions'] = (
    'Answer billing questions using the FAQ documents. '
    'Be concise and cite the relevant FAQ when possible. '
    'If the question is not covered by the FAQ, say so and suggest contacting customer support.'
)
config['ka_volume_path'] = f"/Volumes/{config['catalog']}/{config['database']}/billing_faq_docs"
config['ka_tile_id'] = None  # Set by 04_agent_bricks_deployment after creation

config['mas_name'] = 'Customer Billing Support'
config['mas_description'] = (
    'Customer billing support supervisor that routes queries to specialized agents: '
    'a FAQ Knowledge Assistant for general billing questions and a Genie Space '
    'for ad-hoc billing analytics across the customer base.'
)
config['mas_instructions'] = """Route queries as follows:
- General billing questions, how-to, policy/procedure -> billing_faq_agent
- Data analysis, charge trends, plan comparisons, aggregations, top-N -> billing_analytics_agent
- Questions spanning both -> chain FAQ for explanation, then Analytics for data; synthesize one answer.

This system provides fleet-wide analytics and FAQ answers only. If the query requires individual customer account lookups, write operations (disputes, anomaly acknowledgement), or customer-specific ERP data, explain that these capabilities require the full Billing Agent deployment (LangGraph tier) and suggest a fleet-wide alternative instead.
If unclear, ask the user to clarify."""
config['mas_tile_id'] = None  # Set by 04_agent_bricks_deployment after creation

# Lakebase (Track C: operational write-back store)
config['lakebase_enabled'] = False   # Set True by 08c_lakebase_setup when instance is ready
config['lakebase_instance'] = 'billing-accelerator'
config['lakebase_database'] = 'databricks_postgres'
config['lakebase_schema'] = 'billing_ops'
config['lakebase_host'] = ''        # Auto-populated by 08c or PGHOST env var
config['lakebase_port'] = '5432'

# COMMAND ----------

# DBTITLE 1,Validate Required Configuration
_required_keys = [
    ('catalog',                        'Data catalog name'),
    ('database',                       'Schema / database name'),
    ('llm_endpoint',                   'LLM model serving endpoint'),
    ('embedding_model_endpoint_name',  'Embedding model endpoint'),
    ('warehouse_id',                   'SQL warehouse ID'),
]
_missing = [
    f"  - {key}: {desc}"
    for key, desc in _required_keys
    if not config.get(key)
]
if _missing:
    _msg = "Missing required configuration:\n" + "\n".join(_missing)
    print(f"WARNING: {_msg}")
else:
    print(f"Config validated: catalog={config['catalog']}, schema={config['database']}")