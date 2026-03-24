# Databricks notebook source
# MAGIC %md
# MAGIC # Domain Validation
# MAGIC
# MAGIC Verifies that a deployed domain configuration is working correctly:
# MAGIC canonical views exist, UC tools return results, and charge columns align.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

catalog = config['catalog']
schema = config['database']
domain = config.get('active_domain', 'telco')

print(f"Validating domain: {domain}")

# COMMAND ----------

# DBTITLE 1,Check Canonical Views
for view in ["v_billing_summary", "v_customer_profile", "v_service_catalog", "v_billing_events"]:
    try:
        count = spark.table(f"{catalog}.{schema}.{view}").count()
        cols = spark.table(f"{catalog}.{schema}.{view}").columns
        print(f"  OK: {view} — {count} rows, {len(cols)} columns")
    except Exception as e:
        print(f"  FAIL: {view} — {e}")

# COMMAND ----------

# DBTITLE 1,Validate Column Names
required = {
    "v_billing_summary": ["customer_id", "event_month", "base_charge", "charge_category_1", "total_charges", "plan_name"],
    "v_customer_profile": ["customer_id", "customer_name", "account_identifier", "product_identifier", "plan_key"],
    "v_service_catalog": ["plan_key", "plan_id", "plan_name", "base_monthly_charge", "overage_rate"],
    "v_billing_events": ["product_identifier", "event_category", "usage_quantity", "event_ts"],
}

for view, cols in required.items():
    try:
        actual = spark.table(f"{catalog}.{schema}.{view}").columns
        missing = [c for c in cols if c not in actual]
        print(f"  {'OK' if not missing else 'FAIL'}: {view} — missing: {missing}" if missing else f"  OK: {view} schema valid")
    except Exception:
        print(f"  SKIP: {view} not available")

# COMMAND ----------

# DBTITLE 1,Test UC Functions
try:
    first = spark.table(f"{catalog}.{schema}.v_customer_profile").select("customer_id").limit(1).collect()
    if first:
        cid = first[0][0]
        pid = spark.table(f"{catalog}.{schema}.v_customer_profile").select("product_identifier").limit(1).collect()[0][0]

        for fn, sql in [
            ("lookup_customer", f"SELECT * FROM {catalog}.{schema}.lookup_customer('{cid}')"),
            ("lookup_billing", f"SELECT * FROM {catalog}.{schema}.lookup_billing('{cid}')"),
            ("lookup_billing_plans", f"SELECT * FROM {catalog}.{schema}.lookup_billing_plans()"),
            ("lookup_billing_items", f"SELECT * FROM {catalog}.{schema}.lookup_billing_items('{pid}')"),
        ]:
            try:
                count = spark.sql(sql).count()
                print(f"  OK: {fn} — {count} rows")
            except Exception as e:
                print(f"  FAIL: {fn} — {e}")
    else:
        print("  SKIP: No customers found (template domain)")
except Exception as e:
    print(f"  SKIP: {e}")

# COMMAND ----------

# DBTITLE 1,Charge Column Alignment
canonical_charges = [f"charge_category_{i}" for i in range(1, 7)]
try:
    view_cols = spark.table(f"{catalog}.{schema}.v_billing_summary").columns
    aligned = all(c in view_cols for c in canonical_charges)
    print(f"  {'OK' if aligned else 'FAIL'}: All 6 charge categories present in v_billing_summary")
except Exception as e:
    print(f"  FAIL: {e}")

print(f"\nValidation complete for domain '{domain}'.")
