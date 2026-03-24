# Databricks notebook source
# MAGIC %md
# MAGIC # Domain Configuration Adapter
# MAGIC
# MAGIC Reads a domain YAML config and creates canonical views over domain source tables,
# MAGIC regenerates UC function tools with domain-appropriate DDL, and updates config.yaml.
# MAGIC
# MAGIC Run with the `domain` widget set to: `telco` | `saas` | `utility` | your custom domain.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Load Domain Config
import yaml
import os

dbutils.widgets.text("domain", "telco", "Domain name (telco | saas | utility)")
domain_name = dbutils.widgets.get("domain").strip().lower()

catalog = config['catalog']
schema = config['database']

# Load domain YAML
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebooks_dir = "/".join(current_path.split("/")[:-1])
domain_path = f"/Workspace{notebooks_dir}/domains/{domain_name}.yaml"

try:
    with open(domain_path) as f:
        dc = yaml.safe_load(f)
except FileNotFoundError:
    raise FileNotFoundError(
        f"Domain config not found: {domain_path}\n"
        f"Available: telco, saas, utility. Or create notebooks/domains/{domain_name}.yaml"
    )

# Validate structure
assert 'domain' in dc and 'tables' in dc, "Invalid domain YAML: missing 'domain' or 'tables'"
tables_cfg = dc['tables']
cm = tables_cfg['billing_summary']['charge_mappings']
assert len(cm) == 6, f"Must have exactly 6 charge_mappings, got {len(cm)}"

print(f"Loaded domain: {dc['domain']['display_name']} ({domain_name})")

# COMMAND ----------

# DBTITLE 1,Check Source Table Availability
table_status = {}
for section_name, section in tables_cfg.items():
    src = section.get('source_table', '')
    if not src:
        continue
    fqtn = f"{catalog}.{schema}.{src}"
    try:
        count = spark.table(fqtn).count()
        table_status[src] = True
        print(f"  OK: {fqtn} ({count} rows)")
    except Exception:
        table_status[src] = False
        print(f"  MISSING: {fqtn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Canonical Views

# COMMAND ----------

# DBTITLE 1,v_billing_summary
bs = tables_cfg['billing_summary']

charge_selects = ",\n".join([
    f"    COALESCE(CAST({v['source_col']} AS DOUBLE), 0.0) AS {k}"
    for k, v in cm.items()
])

if table_status.get(bs['source_table'], False):
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_billing_summary AS
    SELECT
        CAST({bs['customer_id_col']} AS BIGINT) AS customer_id,
        {bs['event_month_col']} AS event_month,
        COALESCE(CAST({bs['base_charge_col']} AS DOUBLE), 0.0) AS base_charge,
        {charge_selects},
        COALESCE(CAST({bs['total_charges_col']} AS DOUBLE), 0.0) AS total_charges,
        {bs['plan_name_col']} AS plan_name
    FROM {catalog}.{schema}.{bs['source_table']}
    """)
else:
    # Stub view for missing source table
    null_charges = ",\n".join([f"        CAST(NULL AS DOUBLE) AS {k}" for k in cm.keys()])
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_billing_summary AS
    SELECT CAST(NULL AS BIGINT) AS customer_id, CAST(NULL AS STRING) AS event_month,
           CAST(NULL AS DOUBLE) AS base_charge,
           {null_charges},
           CAST(NULL AS DOUBLE) AS total_charges, CAST(NULL AS STRING) AS plan_name
    WHERE 1=0
    """)
    print(f"  Stub view created (source '{bs['source_table']}' missing)")

print(f"v_billing_summary created")

# COMMAND ----------

# DBTITLE 1,v_customer_profile
cp = tables_cfg['customer_profile']

if table_status.get(cp['source_table'], False):
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_customer_profile AS
    SELECT
        CAST({cp['customer_id_col']} AS BIGINT) AS customer_id,
        CAST({cp['customer_name_col']} AS STRING) AS customer_name,
        CAST({cp['account_identifier_col']} AS STRING) AS account_identifier,
        CAST({cp['product_identifier_col']} AS STRING) AS product_identifier,
        CAST({cp['plan_key_col']} AS BIGINT) AS plan_key,
        CAST({cp['subscription_start_col']} AS DATE) AS subscription_start_date
    FROM {catalog}.{schema}.{cp['source_table']}
    """)
else:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_customer_profile AS
    SELECT CAST(NULL AS BIGINT) AS customer_id, CAST(NULL AS STRING) AS customer_name,
           CAST(NULL AS STRING) AS account_identifier, CAST(NULL AS STRING) AS product_identifier,
           CAST(NULL AS BIGINT) AS plan_key, CAST(NULL AS DATE) AS subscription_start_date
    WHERE 1=0
    """)

print(f"v_customer_profile created")

# COMMAND ----------

# DBTITLE 1,v_service_catalog
sc = tables_cfg['service_catalog']

if table_status.get(sc['source_table'], False):
    premium_2 = f"CAST({sc['premium_rate_2_col']} AS DOUBLE)" if sc.get('premium_rate_2_col') else "NULL"
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_service_catalog AS
    SELECT
        CAST({sc['plan_key_col']} AS BIGINT) AS plan_key,
        CAST({sc['plan_id_col']} AS STRING) AS plan_id,
        CAST({sc['plan_name_col']} AS STRING) AS plan_name,
        CAST({sc['contract_months_col']} AS BIGINT) AS contract_months,
        CAST({sc['base_charge_col']} AS DOUBLE) AS base_monthly_charge,
        '{sc['feature_1_label']}' AS feature_1_label,
        CAST({sc['feature_1_col']} AS STRING) AS feature_1_value,
        '{sc['feature_2_label']}' AS feature_2_label,
        CAST({sc['feature_2_col']} AS STRING) AS feature_2_value,
        CAST({sc['overage_rate_col']} AS DOUBLE) AS overage_rate,
        CAST({sc['premium_rate_1_col']} AS DOUBLE) AS premium_rate_1,
        {premium_2} AS premium_rate_2
    FROM {catalog}.{schema}.{sc['source_table']}
    """)
else:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_service_catalog AS
    SELECT CAST(NULL AS BIGINT) AS plan_key, CAST(NULL AS STRING) AS plan_id,
           CAST(NULL AS STRING) AS plan_name, CAST(NULL AS BIGINT) AS contract_months,
           CAST(NULL AS DOUBLE) AS base_monthly_charge,
           CAST(NULL AS STRING) AS feature_1_label, CAST(NULL AS STRING) AS feature_1_value,
           CAST(NULL AS STRING) AS feature_2_label, CAST(NULL AS STRING) AS feature_2_value,
           CAST(NULL AS DOUBLE) AS overage_rate, CAST(NULL AS DOUBLE) AS premium_rate_1,
           CAST(NULL AS DOUBLE) AS premium_rate_2
    WHERE 1=0
    """)

print(f"v_service_catalog created")

# COMMAND ----------

# DBTITLE 1,v_billing_events
be = tables_cfg['billing_events']

if table_status.get(be['source_table'], False):
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_billing_events AS
    SELECT
        CAST({be['product_identifier_col']} AS STRING) AS product_identifier,
        CAST({be['event_category_col']} AS STRING) AS event_category,
        COALESCE(CAST({be['usage_quantity_col']} AS DOUBLE), 0.0) AS usage_quantity,
        COALESCE(CAST({be['data_quantity_col']} AS DOUBLE), 0.0) AS data_quantity,
        CAST({be['event_ts_col']} AS TIMESTAMP) AS event_ts,
        CAST({be['subscription_start_col']} AS DATE) AS subscription_start
    FROM {catalog}.{schema}.{be['source_table']}
    """)
else:
    spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.{schema}.v_billing_events AS
    SELECT CAST(NULL AS STRING) AS product_identifier, CAST(NULL AS STRING) AS event_category,
           CAST(NULL AS DOUBLE) AS usage_quantity, CAST(NULL AS DOUBLE) AS data_quantity,
           CAST(NULL AS TIMESTAMP) AS event_ts, CAST(NULL AS DATE) AS subscription_start
    WHERE 1=0
    """)

print(f"v_billing_events created")

# COMMAND ----------

# DBTITLE 1,Update config.yaml with domain settings
ag = dc['agent']
charge_labels = {k: v['label'] for k, v in cm.items()}
conf_fields = dc.get('confidential_fields', [])

domain_prompt = (
    f"\n  Domain context ({dc['domain']['display_name']}):\n"
    f"  - You are a {ag['role_name']} for a {dc['domain']['display_name']} provider.\n"
    f"  - Refer to customers as \"{ag['customer_noun']}\".\n"
    f"  - The billing service is called a \"{ag['service_noun']}\".\n"
    f"  - Charge categories: "
    + ", ".join(f"{k}={v['label']}" for k, v in cm.items()) + "\n"
    f"  - NEVER share these fields: {', '.join(conf_fields)}.\n"
)

with open("config.yaml", "r") as f:
    cfg_yaml = yaml.safe_load(f)

cfg_yaml['active_domain'] = domain_name
cfg_yaml['domain_display_name'] = dc['domain']['display_name']
cfg_yaml['domain_industry'] = dc['domain']['industry']
cfg_yaml['domain_charge_labels'] = charge_labels
cfg_yaml['domain_agent_prompt_section'] = domain_prompt

with open("config.yaml", "w") as f:
    yaml.dump(cfg_yaml, f, default_flow_style=False, sort_keys=False)

print(f"config.yaml updated for domain '{domain_name}'")

# COMMAND ----------

# DBTITLE 1,Summary
missing = [t for t, ok in table_status.items() if not ok]
print(f"""
Domain Configuration Complete
==============================
Domain:    {dc['domain']['display_name']} ({domain_name})
Industry:  {dc['domain']['industry']}
Views:     v_billing_summary, v_customer_profile, v_service_catalog, v_billing_events
Missing:   {missing if missing else 'None (all source tables available)'}

Next: Re-run 03a (Genie Space) and 03 (agent deployment) to apply domain changes.
""")
