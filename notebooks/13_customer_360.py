# Databricks notebook source
# MAGIC %md
# MAGIC # 👤 Customer 360 — ORDM Unified Customer View
# MAGIC
# MAGIC Extends the billing agent from *"what's my bill?"* to *"what is my relationship with you?"*
# MAGIC by projecting the accelerator's billing data onto the
# MAGIC [Open Retail Data Model](https://github.com/databricks-industry-solutions/open-retail-data-model)
# MAGIC **Customer** canonical core, then rolling billing, dispute and anomaly signals up an
# MAGIC account hierarchy.
# MAGIC
# MAGIC ### What this notebook creates
# MAGIC
# MAGIC | Object | Layer | Purpose |
# MAGIC |--------|-------|---------|
# MAGIC | `ordm_customer` | Canonical core | ORDM-aligned customer attributes. PII-free by design (PM-001). |
# MAGIC | `ordm_customer_hierarchy` | Canonical core | Parent / subsidiary account relationships. |
# MAGIC | `customer_360_profile` | Gold | Unified profile: ORDM attributes + 12-month billing, dispute and anomaly rollups. |
# MAGIC | `lookup_customer_360` | UC function | Agent tool — one customer's unified profile. |
# MAGIC | `lookup_customer_hierarchy` | UC function | Agent tool — the customer's whole org, ranked. |
# MAGIC
# MAGIC ### Databricks features applied
# MAGIC
# MAGIC - **Liquid clustering** (`CLUSTER BY`) on all three tables, keyed on the access path the
# MAGIC   UC functions actually use (`customer_id`, `org_key`). No partitioning, so no skew from
# MAGIC   high-activity accounts.
# MAGIC - **Predictive optimization** enabled per table, so `OPTIMIZE` / `VACUUM` / `ANALYZE` are
# MAGIC   managed by the platform rather than by a scheduled job.
# MAGIC - **Change Data Feed** on the gold profile, so downstream consumers read incrementally.
# MAGIC - **Definer-rights UC functions** — the agent service principal executes these functions
# MAGIC   and holds no `SELECT` on the underlying tables. Both functions are reads only; writes in
# MAGIC   this accelerator go through the Statement Execution API confirmation flow in `agent.py`,
# MAGIC   because a Unity Catalog SQL function body is a query expression and cannot have side effects.
# MAGIC - **Bounded result sets** — every function has a `LIMIT` and a primary-key or org-key filter.
# MAGIC
# MAGIC ### Synthetic attributes
# MAGIC
# MAGIC The source `customers` table has no region, account type or hierarchy. This notebook derives
# MAGIC them deterministically from `customer_id` so the demo is reproducible and re-runnable. They
# MAGIC are clearly labelled below — replace these derivations with your real ORDM feed before using
# MAGIC any of this against production data.
# MAGIC
# MAGIC **Prerequisites:** `000-config` → `00_data_preparation`. Optionally `05_billing_anomaly_detection`
# MAGIC and `09_writeback_setup` — if those tables are absent the corresponding rollups are simply zero.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Set Working Catalog and Schema
CATALOG = config['catalog']
SCHEMA = config['database']

FQ = lambda t: f"{CATALOG}.{SCHEMA}.{t}"

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Probe Optional Upstream Tables
# 05_billing_anomaly_detection and 09_writeback_setup are optional in the run order.
# Probe rather than assume, so this notebook is runnable on a fresh workspace.
HAS_DISPUTES = spark.catalog.tableExists(FQ("billing_disputes"))
HAS_ANOMALIES = spark.catalog.tableExists(FQ("billing_anomalies"))

for name, present in [("billing_disputes", HAS_DISPUTES), ("billing_anomalies", HAS_ANOMALIES)]:
    print(f"{name:<20} {'found' if present else 'absent — rollup will be 0'}")

# COMMAND ----------

# DBTITLE 1,Helper — Enable Predictive Optimization
def enable_predictive_optimization(table: str) -> None:
    """Hand OPTIMIZE/VACUUM/ANALYZE to the platform.

    Only valid for Unity Catalog managed tables, and the account or catalog must permit
    the override. Failing that is not fatal — the table is still correct, just not
    auto-maintained — so report and continue.
    """
    try:
        spark.sql(f"ALTER TABLE {table} ENABLE PREDICTIVE OPTIMIZATION")
        print(f"predictive optimization enabled: {table}")
    except Exception as e:
        print(f"predictive optimization NOT enabled for {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ORDM Canonical Core — `ordm_customer`
# MAGIC
# MAGIC Projects `customers` onto the ORDM Customer domain. Name, email and phone are excluded
# MAGIC here exactly as they are in `lookup_customer` (PM-001) — the agent-facing path never
# MAGIC carries PII. `_internal.lookup_customer_pii` remains the only route to those fields.
# MAGIC
# MAGIC `customer_type` and `region` are **synthetic**, derived from a stable hash of `customer_id`.

# COMMAND ----------

# DBTITLE 1,Create ordm_customer
spark.sql(f"""
CREATE OR REPLACE TABLE {FQ('ordm_customer')}
CLUSTER BY (customer_id)
COMMENT 'ORDM Customer canonical core, projected from the billing customers table. PII excluded by design (PM-001). customer_type and region are synthetic, derived deterministically from customer_id.'
AS
SELECT
  customer_id,
  CONCAT('CUST-', LPAD(CAST(customer_id AS STRING), 8, '0'))       AS customer_key,
  device_id,
  plan                                                             AS plan_id,
  contract_start_dt                                                AS acquisition_dt,
  -- synthetic: replace with the real ORDM customer feed
  CASE PMOD(HASH(customer_id), 3)
    WHEN 0 THEN 'Enterprise'
    WHEN 1 THEN 'SMB'
    ELSE        'Consumer'
  END                                                              AS customer_type,
  CASE PMOD(HASH(customer_id, 'region'), 4)
    WHEN 0 THEN 'West'
    WHEN 1 THEN 'East'
    WHEN 2 THEN 'Central'
    ELSE        'International'
  END                                                              AS region,
  MONTHS_BETWEEN(CURRENT_DATE(), contract_start_dt)                AS tenure_months
FROM {FQ('customers')}
""")

enable_predictive_optimization(FQ("ordm_customer"))
display(spark.sql(f"SELECT * FROM {FQ('ordm_customer')} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ORDM Canonical Core — `ordm_customer_hierarchy`
# MAGIC
# MAGIC Groups customers into parent organisations so the agent can answer *"which subsidiary has
# MAGIC the highest dispute rate?"*. The lowest `customer_id` in each group becomes the parent.
# MAGIC
# MAGIC Entirely **synthetic** — replace with your real ORDM hierarchy feed.

# COMMAND ----------

# DBTITLE 1,Create ordm_customer_hierarchy
# Size the org count so groups average ~8 members, which gives a hierarchy that is
# interesting to query without any single org dominating the rollups.
TARGET_ORG_SIZE = 8
customer_count = spark.table(FQ("ordm_customer")).count()
n_orgs = max(1, customer_count // TARGET_ORG_SIZE)
print(f"{customer_count} customers -> {n_orgs} synthetic organisations (~{TARGET_ORG_SIZE} each)")

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ('ordm_customer_hierarchy')}
CLUSTER BY (customer_id, org_key)
COMMENT 'ORDM Customer hierarchy — parent/subsidiary account relationships. Synthetic: groups are derived from a stable hash of customer_id. Replace with the real ORDM hierarchy feed.'
AS
WITH org_assign AS (
  SELECT
    customer_id,
    PMOD(HASH(customer_id, 'org'), {n_orgs}) AS org_group
  FROM {FQ('ordm_customer')}
),
org_root AS (
  SELECT org_group, MIN(customer_id) AS root_customer_id
  FROM org_assign
  GROUP BY org_group
)
SELECT
  a.customer_id,
  CONCAT('ORG-', LPAD(CAST(a.org_group AS STRING), 5, '0'))                       AS org_key,
  r.root_customer_id,
  CASE WHEN a.customer_id = r.root_customer_id
       THEN CAST(NULL AS BIGINT) ELSE r.root_customer_id END                      AS parent_customer_id,
  CASE WHEN a.customer_id = r.root_customer_id
       THEN 'Parent' ELSE 'Subsidiary' END                                        AS relationship_type,
  CASE WHEN a.customer_id = r.root_customer_id THEN 0 ELSE 1 END                  AS hierarchy_level
FROM org_assign a
JOIN org_root r ON a.org_group = r.org_group
""")

enable_predictive_optimization(FQ("ordm_customer_hierarchy"))
display(spark.sql(f"""
  SELECT org_key, COUNT(*) AS members, MIN(root_customer_id) AS root
  FROM {FQ('ordm_customer_hierarchy')}
  GROUP BY org_key ORDER BY members DESC LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Gold — `customer_360_profile`
# MAGIC
# MAGIC The unified view the agent and Genie both read. One row per customer: ORDM attributes,
# MAGIC hierarchy position, and 12-month billing / dispute / anomaly rollups.
# MAGIC
# MAGIC Change Data Feed is on so downstream consumers can read incrementally rather than
# MAGIC re-scanning the table.

# COMMAND ----------

# DBTITLE 1,Create customer_360_profile
# Optional upstreams degrade to empty rather than failing the build.
disputes_cte = f"""
  SELECT
    customer_id,
    COUNT(*)                                                       AS dispute_count_12m,
    SUM(CASE WHEN UPPER(status) = 'OPEN' THEN 1 ELSE 0 END)        AS open_dispute_count,
    COALESCE(SUM(disputed_amount_usd), 0.0)                        AS disputed_amount_12m
  FROM {FQ('billing_disputes')}
  WHERE created_at >= ADD_MONTHS(CURRENT_DATE(), -12)
  GROUP BY customer_id
""" if HAS_DISPUTES else """
  SELECT CAST(NULL AS BIGINT) AS customer_id,
         CAST(NULL AS BIGINT) AS dispute_count_12m,
         CAST(NULL AS BIGINT) AS open_dispute_count,
         CAST(NULL AS DOUBLE) AS disputed_amount_12m
  LIMIT 0
"""

anomalies_cte = f"""
  SELECT customer_id, COUNT(*) AS anomaly_count_12m
  FROM {FQ('billing_anomalies')}
  WHERE event_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -12), 'yyyy-MM')
  GROUP BY customer_id
""" if HAS_ANOMALIES else """
  SELECT CAST(NULL AS BIGINT) AS customer_id,
         CAST(NULL AS BIGINT) AS anomaly_count_12m
  LIMIT 0
"""

spark.sql(f"""
CREATE OR REPLACE TABLE {FQ('customer_360_profile')}
CLUSTER BY (customer_id, org_key)
COMMENT 'ORDM Unified Customer View — one row per customer combining ORDM attributes, account hierarchy position, and 12-month billing, dispute and anomaly rollups. No PII.'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
WITH billing_12m AS (
  SELECT
    customer_id,
    SUM(total_charges)                    AS total_billed_12m,
    AVG(total_charges)                    AS avg_monthly_charges_12m,
    COUNT(*)                              AS invoice_count_12m,
    MAX(event_month)                      AS last_invoice_month,
    MAX_BY(plan_name, event_month)        AS current_plan_name
  FROM {FQ('invoice')}
  WHERE event_month >= DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -12), 'yyyy-MM')
  GROUP BY customer_id
),
disputes_12m AS ({disputes_cte}),
anomalies_12m AS ({anomalies_cte})
SELECT
  c.customer_id,
  c.customer_key,
  c.customer_type,
  c.region,
  c.plan_id,
  c.acquisition_dt,
  c.tenure_months,

  h.org_key,
  h.root_customer_id,
  h.parent_customer_id,
  h.relationship_type,
  h.hierarchy_level,

  COALESCE(b.total_billed_12m, 0.0)        AS total_billed_12m,
  COALESCE(b.avg_monthly_charges_12m, 0.0) AS avg_monthly_charges_12m,
  COALESCE(b.invoice_count_12m, 0)         AS invoice_count_12m,
  b.last_invoice_month,
  b.current_plan_name,

  COALESCE(d.dispute_count_12m, 0)         AS dispute_count_12m,
  COALESCE(d.open_dispute_count, 0)        AS open_dispute_count,
  COALESCE(d.disputed_amount_12m, 0.0)     AS disputed_amount_12m,
  COALESCE(a.anomaly_count_12m, 0)         AS anomaly_count_12m,

  -- disputes as a share of invoices issued; 0 when the customer has no invoices
  ROUND(
    COALESCE(d.dispute_count_12m, 0) * 100.0 / NULLIF(COALESCE(b.invoice_count_12m, 0), 0),
    2
  )                                        AS dispute_rate_pct,

  -- demo thresholds — retune against your own revenue distribution
  CASE
    WHEN COALESCE(b.total_billed_12m, 0.0) >= 2000 THEN 'High'
    WHEN COALESCE(b.total_billed_12m, 0.0) >=  500 THEN 'Medium'
    WHEN COALESCE(b.total_billed_12m, 0.0) >     0 THEN 'Low'
    ELSE 'Inactive'
  END                                      AS value_segment,

  CURRENT_TIMESTAMP()                      AS profile_refreshed_at
FROM {FQ('ordm_customer')} c
LEFT JOIN {FQ('ordm_customer_hierarchy')} h ON c.customer_id = h.customer_id
LEFT JOIN billing_12m  b ON c.customer_id = b.customer_id
LEFT JOIN disputes_12m d ON c.customer_id = d.customer_id
LEFT JOIN anomalies_12m a ON c.customer_id = a.customer_id
""")

enable_predictive_optimization(FQ("customer_360_profile"))
display(spark.sql(f"SELECT * FROM {FQ('customer_360_profile')} ORDER BY total_billed_12m DESC LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. UC Function — `lookup_customer_360`
# MAGIC
# MAGIC The flagship tool. One bounded row: everything the agent needs to answer
# MAGIC *"show me my billing and my company structure"* in a single call.

# COMMAND ----------

# DBTITLE 1,Create lookup_customer_360
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_customer_360')}")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ('lookup_customer_360')}(
  input_customer STRING COMMENT 'Customer ID to build the unified profile for'
)
RETURNS TABLE (
  customer_id             BIGINT,
  customer_key            STRING,
  customer_type           STRING,
  region                  STRING,
  value_segment           STRING,
  current_plan_name       STRING,
  tenure_months           DOUBLE,
  org_key                 STRING,
  relationship_type       STRING,
  parent_customer_id      BIGINT,
  total_billed_12m        DOUBLE,
  avg_monthly_charges_12m DOUBLE,
  invoice_count_12m       BIGINT,
  last_invoice_month      STRING,
  dispute_count_12m       BIGINT,
  open_dispute_count      BIGINT,
  disputed_amount_12m     DOUBLE,
  dispute_rate_pct        DOUBLE,
  anomaly_count_12m       BIGINT
)
COMMENT 'Unified ORDM Customer 360 profile for one customer: account attributes, hierarchy position, and 12-month billing, dispute and anomaly rollups. Returns at most one row. No PII.'
RETURN
SELECT
  customer_id, customer_key, customer_type, region, value_segment,
  current_plan_name, tenure_months,
  org_key, relationship_type, parent_customer_id,
  total_billed_12m, avg_monthly_charges_12m, invoice_count_12m, last_invoice_month,
  dispute_count_12m, open_dispute_count, disputed_amount_12m, dispute_rate_pct,
  anomaly_count_12m
FROM {FQ('customer_360_profile')}
WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
LIMIT 1
""")

print(f"created {FQ('lookup_customer_360')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. UC Function — `lookup_customer_hierarchy`
# MAGIC
# MAGIC Returns the requested customer's entire organisation, ranked by spend, with the requested
# MAGIC customer flagged. This is what lets the agent answer the drill-down:
# MAGIC *"which subsidiary has the highest dispute rate?"*

# COMMAND ----------

# DBTITLE 1,Create lookup_customer_hierarchy
spark.sql(f"DROP FUNCTION IF EXISTS {FQ('lookup_customer_hierarchy')}")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ('lookup_customer_hierarchy')}(
  input_customer STRING COMMENT 'Customer ID whose parent organisation should be returned'
)
RETURNS TABLE (
  customer_id          BIGINT,
  customer_key         STRING,
  org_key              STRING,
  relationship_type    STRING,
  hierarchy_level      INT,
  root_customer_id     BIGINT,
  region               STRING,
  value_segment        STRING,
  total_billed_12m     DOUBLE,
  dispute_count_12m    BIGINT,
  dispute_rate_pct     DOUBLE,
  is_requested_customer BOOLEAN
)
COMMENT 'Returns every account in the requested customer parent organisation, ranked by 12-month spend, with the requested customer flagged. Use to answer questions about account structure, subsidiaries, and which entity in a group drives disputes or spend. Capped at 100 rows. No PII.'
RETURN
SELECT
  p.customer_id,
  p.customer_key,
  p.org_key,
  p.relationship_type,
  p.hierarchy_level,
  p.root_customer_id,
  p.region,
  p.value_segment,
  p.total_billed_12m,
  p.dispute_count_12m,
  p.dispute_rate_pct,
  p.customer_id = TRY_CAST(input_customer AS DECIMAL) AS is_requested_customer
FROM {FQ('customer_360_profile')} p
WHERE p.org_key = (
  SELECT org_key
  FROM {FQ('customer_360_profile')}
  WHERE customer_id = TRY_CAST(input_customer AS DECIMAL)
  LIMIT 1
)
ORDER BY p.hierarchy_level, p.total_billed_12m DESC
LIMIT 100
""")

print(f"created {FQ('lookup_customer_hierarchy')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation
# MAGIC
# MAGIC Exercises both functions against a customer that actually has billing history, and asserts
# MAGIC the two invariants that matter for an agent tool: results are bounded, and no PII column
# MAGIC can leak through the returned schema.

# COMMAND ----------

# DBTITLE 1,Validate Both Functions
sample_customer = (
    spark.sql(f"""
        SELECT customer_id FROM {FQ('customer_360_profile')}
        WHERE invoice_count_12m > 0
        ORDER BY total_billed_12m DESC LIMIT 1
    """).collect()
)
assert sample_customer, "No customer has billing history — run 00_data_preparation first."
sample_id = sample_customer[0]["customer_id"]
print(f"Validating against customer_id={sample_id}\n")

# -- lookup_customer_360 --------------------------------------------------------
df_360 = spark.sql(f"SELECT * FROM {FQ('lookup_customer_360')}('{sample_id}')")
assert df_360.count() == 1, "lookup_customer_360 must return exactly one row"
display(df_360)

# -- lookup_customer_hierarchy --------------------------------------------------
df_org = spark.sql(f"SELECT * FROM {FQ('lookup_customer_hierarchy')}('{sample_id}')")
org_rows = df_org.count()
assert 0 < org_rows <= 100, f"hierarchy result must be bounded to 100 rows, got {org_rows}"
assert df_org.filter("is_requested_customer").count() == 1, \
    "the requested customer must appear exactly once in its own organisation"
print(f"lookup_customer_hierarchy returned {org_rows} accounts in the organisation")
display(df_org)

# -- no PII may reach the agent -------------------------------------------------
PII_COLUMNS = {"customer_name", "email", "phone_number"}
for fn_name, df in [("lookup_customer_360", df_360), ("lookup_customer_hierarchy", df_org)]:
    leaked = PII_COLUMNS & {c.lower() for c in df.columns}
    assert not leaked, f"{fn_name} leaks PII columns: {leaked}"
print("\nno PII in either function signature")

# -- unknown customer returns empty, not an error -------------------------------
assert spark.sql(f"SELECT * FROM {FQ('lookup_customer_360')}('not-a-customer')").count() == 0, \
    "unknown customer must return zero rows"
print("unknown customer returns empty result")

print("\nAll checks passed.")

# COMMAND ----------

# DBTITLE 1,Grant Execute to the Agent Service Principal
# MAGIC %md
# MAGIC The agent service principal needs `EXECUTE` on the two new functions and must **not** be
# MAGIC granted `SELECT` on `customer_360_profile` — definer rights are what keep the tables closed.
# MAGIC Run this with the principal name used elsewhere in your deployment:
# MAGIC
# MAGIC ```sql
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_customer_360        TO `<agent-sp>`;
# MAGIC GRANT EXECUTE ON FUNCTION <catalog>.<schema>.lookup_customer_hierarchy  TO `<agent-sp>`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC 1. Re-run `03_agent_deployment_and_evaluation` to pick up the two new tools and redeploy.
# MAGIC 2. Optionally add `customer_360_profile` to the Genie Space in `03a_create_genie_space` so
# MAGIC    analysts get the same rollups in natural language.
# MAGIC
# MAGIC ### Demo script
# MAGIC
# MAGIC | Ask | Tool the agent should select |
# MAGIC |-----|------------------------------|
# MAGIC | *"Show me my billing history and my account details."* | `lookup_customer_360` |
# MAGIC | *"What does my company structure look like?"* | `lookup_customer_hierarchy` |
# MAGIC | *"Which subsidiary has the highest dispute rate?"* | `lookup_customer_hierarchy`, then reason over `dispute_rate_pct` |
# MAGIC | *"Is my spend unusual for my segment?"* | `lookup_customer_360` + `lookup_billing_anomalies` |
