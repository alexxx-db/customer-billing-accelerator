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
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

space_name = config['genie_space_name']
space_description = config['genie_space_description']
table_identifiers = config['genie_space_tables']
warehouse_id = config['warehouse_id']
sample_questions = config['genie_space_sample_questions']

# Check if a Genie Space with this name already exists
existing_space_id = None
try:
    spaces = w.genie.list_spaces()
    for space in spaces:
        if space.title == space_name:
            existing_space_id = space.space_id
            print(f"Found existing Genie Space '{space_name}' with ID: {existing_space_id}")
            break
except Exception as e:
    print(f"Could not list existing spaces: {e}. Will create a new one.")

if existing_space_id:
    # Update the existing space
    genie_space = w.genie.update_space(
        space_id=existing_space_id,
        title=space_name,
        description=space_description,
        table_identifiers=table_identifiers,
        warehouse_id=warehouse_id,
        sample_questions=sample_questions,
    )
    space_id = existing_space_id
    print(f"Updated existing Genie Space: {space_id}")
else:
    # Create a new space
    genie_space = w.genie.create_space(
        title=space_name,
        description=space_description,
        table_identifiers=table_identifiers,
        warehouse_id=warehouse_id,
        sample_questions=sample_questions,
    )
    space_id = genie_space.space_id
    print(f"Created new Genie Space: {space_id}")

config['genie_space_id'] = space_id

# COMMAND ----------

# DBTITLE 1,Test the Genie Space
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
import time

max_attempts = 30
for attempt in range(max_attempts):
    result = w.genie.get_message(
        space_id=space_id,
        conversation_id=response.conversation_id,
        message_id=response.message_id,
    )
    if hasattr(result, 'status') and result.status in ("COMPLETED", "FAILED"):
        break
    time.sleep(2)

if hasattr(result, 'status'):
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
