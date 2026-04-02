# Databricks notebook source
# MAGIC %md
# MAGIC # Create Genie Space for Telco Billing Analytics
# MAGIC
# MAGIC This notebook creates a **Databricks Genie Space** that provides a natural language SQL interface
# MAGIC over the telco billing data. The Genie Space enables ad-hoc analytical questions such as:
# MAGIC
# MAGIC - Revenue and charge trends across customers and plans
# MAGIC - Plan comparisons and customer segmentation
# MAGIC - Aggregations across the full billing dataset
# MAGIC
# MAGIC The Genie Space complements the existing UC function tools:
# MAGIC - **UC function tools** handle individual customer lookups (single customer's bill, plan, etc.)
# MAGIC - **Genie Space** handles cross-customer analytics (trends, averages, top-N, comparisons)
# MAGIC
# MAGIC ## Tables Exposed
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `invoice_analytics` | PII-safe view of monthly billing aggregates per customer (excludes names, emails, phone numbers) |
# MAGIC | `billing_plans` | Plan catalog with pricing, data limits, and allowances |
# MAGIC | `billing_anomalies` | Detected billing anomalies: charge spikes, roaming spikes, data overage spikes (added by notebook 05) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Prerequisites**: Run notebooks `000-config`, `00_data_preparation`, `01_create_vector_search`, and `02_define_uc_tools` first.

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install -U -qqqq databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Create PII-Safe Invoice View for Genie
catalog = config['catalog']
db = config['database']

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog}.{db}.invoice_analytics AS
SELECT
    customer_id,
    event_month,
    plan_name,
    contract_start_dt,
    contract_in_months,
    monthly_charges,
    Calls_Text,
    Internet_Speed_MBPS,
    Data_Limit_GB,
    data_local_mb,
    data_roaming_mb,
    call_mins_roaming,
    texts_roaming,
    call_mins_international,
    texts_international,
    data_charges_outside_allowance,
    roaming_data_charges,
    roaming_call_charges,
    roaming_text_charges,
    international_call_charges,
    international_text_charges,
    total_charges
FROM {catalog}.{db}.invoice
""")
print(f"Created view {catalog}.{db}.invoice_analytics (PII columns excluded)")

# COMMAND ----------

# DBTITLE 1,Create or Update Genie Space
import json
import uuid
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

space_name = config['genie_space_name']
space_description = config['genie_space_description']
table_identifiers = config['genie_space_tables']
warehouse_id = config['warehouse_id']
sample_questions = config['genie_space_sample_questions']

# Validate that referenced tables exist (Genie API rejects missing tables)
catalog = config['catalog']
db = config['database']
_existing = {r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.{db}").collect()}
_valid = [t for t in table_identifiers if t.split('.')[-1] in _existing]
_skipped = [t for t in table_identifiers if t.split('.')[-1] not in _existing]
if _skipped:
    print(f"Skipping {len(_skipped)} tables not yet created: {', '.join(t.split('.')[-1] for t in _skipped)}")
table_identifiers = sorted(_valid)
assert table_identifiers, "No valid tables found — create the upstream tables first."
print(f"Creating Genie Space with {len(table_identifiers)} tables")

# Build the serialized_space payload in GenieSpaceExport format
serialized_space = json.dumps({
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": uuid.uuid4().hex, "question": [q]}
            for q in sample_questions
        ],
    },
    "data_sources": {
        "tables": [
            {"identifier": t} for t in table_identifiers
        ],
    },
})

# Check if a Genie Space with this name already exists
existing_space_id = None
try:
    spaces_resp = w.genie.list_spaces()
    if hasattr(spaces_resp, 'spaces') and spaces_resp.spaces:
        for space in spaces_resp.spaces:
            if space.title == space_name:
                existing_space_id = space.space_id
                print(f"Found existing Genie Space '{space_name}' with ID: {existing_space_id}")
                break
except Exception as e:
    print(f"Could not list existing spaces: {e}. Will create a new one.")

if existing_space_id:
    result = w.genie.update_space(
        space_id=existing_space_id,
        title=space_name,
        description=space_description,
        warehouse_id=warehouse_id,
        serialized_space=serialized_space,
    )
    space_id = existing_space_id
    print(f"Updated existing Genie Space: {space_id}")
else:
    result = w.genie.create_space(
        warehouse_id=warehouse_id,
        serialized_space=serialized_space,
        title=space_name,
        description=space_description,
    )
    space_id = result.space_id
    print(f"Created new Genie Space: {space_id}")

config['genie_space_id'] = space_id

# COMMAND ----------

# DBTITLE 1,Test the Genie Space
import time

# Send a test question to verify the space is working
test_question = "What is the average monthly total charge across all customers?"

print(f"Testing Genie Space with question: '{test_question}'")
response = w.genie.start_conversation(
    space_id=space_id,
    content=test_question,
)
print(f"Conversation ID: {response.conversation_id}")
print(f"Message ID: {response.message_id}")

# Poll for result
max_attempts = 30
result = None
for attempt in range(max_attempts):
    result = w.genie.get_message(
        space_id=space_id,
        conversation_id=response.conversation_id,
        message_id=response.message_id,
    )
    if hasattr(result, 'status') and result.status in ("COMPLETED", "FAILED"):
        break
    time.sleep(2)

if result is None:
    print("Test timed out — no result received.")
elif hasattr(result, 'status'):
    print(f"Status: {result.status}")
    if hasattr(result, 'attachments') and result.attachments:
        for att in result.attachments:
            if hasattr(att, 'text') and att.text:
                print(f"Response: {att.text.content}")
            if hasattr(att, 'query') and att.query:
                print(f"Generated SQL: {att.query.query}")

# COMMAND ----------

# DBTITLE 1,Display Genie Space Link
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
print(f"Genie Space URL: https://{host}/genie/spaces/{space_id}")
print(f"\nSpace ID for config: {space_id}")
print(f"\nThis space_id is stored in config['genie_space_id'] and will be picked up by notebook 03.")