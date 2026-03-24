# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Bricks Deployment: Supervisor Agent for Telco Billing
# MAGIC
# MAGIC This notebook deploys the telco billing support system as a **Databricks Agent Bricks** Supervisor Agent
# MAGIC (Multi-Agent Supervisor) that orchestrates two specialized agents:
# MAGIC
# MAGIC | Agent | Type | Purpose |
# MAGIC |-------|------|---------|
# MAGIC | **Billing FAQ** | Knowledge Assistant (KA) | Answers general billing questions from FAQ documents |
# MAGIC | **Billing Analytics** | Genie Space | Runs ad-hoc SQL analytics across the billing dataset |
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC User Query
# MAGIC     ↓
# MAGIC Supervisor Agent (MAS)
# MAGIC     ├─→ Billing FAQ Agent (KA) — "How is my bill calculated?"
# MAGIC     └─→ Billing Analytics Agent (Genie) — "Average charges by plan?"
# MAGIC ```
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Run these notebooks first:
# MAGIC 1. `000-config` — Configuration
# MAGIC 2. `00_data_preparation` — Generate billing data
# MAGIC 3. `01_create_vector_search` — Create FAQ dataset
# MAGIC 4. `02_define_uc_tools` — Define UC functions
# MAGIC 5. `03a_create_genie_space` — Create Genie Space (sets `genie_space_id` in config)

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install -U -qqqq databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare FAQ Documents for Knowledge Assistant
# MAGIC
# MAGIC The Knowledge Assistant requires documents in a Unity Catalog Volume.
# MAGIC We write the billing FAQ entries as individual text files to a Volume.

# COMMAND ----------

# DBTITLE 1,Create Volume and Write FAQ Documents
catalog = config['catalog']
db = config['database']
volume_path = config['ka_volume_path']

# Create the volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{db}.billing_faq_docs")

# Read FAQ data from the Delta table
faq_df = spark.table(f"{catalog}.{db}.billing_faq_dataset").collect()

import json

for row in faq_df:
    idx = row['index']
    faq_text = row['faq']

    # Extract question and answer from the FAQ text
    if "Q:" in faq_text and "A:" in faq_text:
        parts = faq_text.split("A:", 1)
        question = parts[0].replace("Q:", "").strip()
        answer = parts[1].strip()
    else:
        question = f"FAQ {idx}"
        answer = faq_text

    # Write the FAQ as a text file
    doc_content = f"# Billing FAQ {idx}\n\n**Question:** {question}\n\n**Answer:** {answer}\n"
    file_path = f"{volume_path}/faq_{idx:03d}.txt"
    dbutils.fs.put(file_path.replace("/Volumes/", "dbfs:/Volumes/"), doc_content, True)

    # Write a companion JSON file with question/guideline pairs for evaluation
    example = {
        "question": question,
        "guideline": f"Should answer using FAQ {idx}: {answer[:100]}..."
    }
    json_path = f"{volume_path}/faq_{idx:03d}.json"
    dbutils.fs.put(json_path.replace("/Volumes/", "dbfs:/Volumes/"), json.dumps(example), True)

print(f"Wrote {len(faq_df)} FAQ documents + JSON examples to {volume_path}")

# COMMAND ----------

# DBTITLE 1,Verify Volume Contents
files = dbutils.fs.ls(volume_path.replace("/Volumes/", "dbfs:/Volumes/"))
for f in files:
    print(f"  {f.name} ({f.size} bytes)")
print(f"\nTotal files: {len(files)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Knowledge Assistant
# MAGIC
# MAGIC Create a Knowledge Assistant that indexes the billing FAQ documents.
# MAGIC Uses the `manage_ka` MCP tool via the Databricks SDK.

# COMMAND ----------

# DBTITLE 1,Create or Update Knowledge Assistant
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

ka_name = config['ka_name']
ka_description = config['ka_description']
ka_instructions = config['ka_instructions']
ka_volume_path = config['ka_volume_path']

# Check if KA already exists
existing_ka = None
try:
    tiles = w.agent_bricks.list_tiles()
    for tile in tiles:
        if hasattr(tile, 'name') and tile.name == ka_name.replace(' ', '_'):
            existing_ka = tile
            print(f"Found existing KA '{ka_name}' with tile_id: {tile.tile_id}")
            break
except Exception as e:
    print(f"Could not check existing KAs: {e}")

if existing_ka:
    ka_tile = w.agent_bricks.update_tile(
        tile_id=existing_ka.tile_id,
        name=ka_name,
        description=ka_description,
        instructions=ka_instructions,
        volume_path=ka_volume_path,
    )
    ka_tile_id = existing_ka.tile_id
    print(f"Updated existing KA: {ka_tile_id}")
else:
    ka_tile = w.agent_bricks.create_ka(
        name=ka_name,
        description=ka_description,
        instructions=ka_instructions,
        volume_path=ka_volume_path,
    )
    ka_tile_id = ka_tile.tile_id
    print(f"Created new KA: {ka_tile_id}")

config['ka_tile_id'] = ka_tile_id

# COMMAND ----------

# DBTITLE 1,Wait for KA Endpoint to Provision
import time

print(f"Waiting for KA endpoint to provision (tile_id: {ka_tile_id})...")
max_wait = 600  # 10 minutes
elapsed = 0

while elapsed < max_wait:
    try:
        status = w.agent_bricks.get_tile(tile_id=ka_tile_id)
        endpoint_status = getattr(status, 'endpoint_status', 'UNKNOWN')
        print(f"  Status: {endpoint_status} ({elapsed}s elapsed)")
        if endpoint_status == "ONLINE":
            print("KA endpoint is ONLINE!")
            break
        if endpoint_status == "FAILED":
            print("ERROR: KA endpoint provisioning failed.")
            break
    except Exception as e:
        print(f"  Error checking status: {e}")
    time.sleep(15)
    elapsed += 15

if elapsed >= max_wait:
    print(f"WARNING: KA endpoint did not come online within {max_wait}s. Check the Databricks UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Supervisor Agent (MAS)
# MAGIC
# MAGIC Create a Supervisor Agent that combines the FAQ Knowledge Assistant
# MAGIC with the Billing Analytics Genie Space.

# COMMAND ----------

# DBTITLE 1,Verify Genie Space ID is Available
genie_space_id = config.get('genie_space_id')
if not genie_space_id:
    raise ValueError(
        "genie_space_id is not set in config. "
        "Run notebook 03a_create_genie_space first."
    )
print(f"Genie Space ID: {genie_space_id}")
print(f"KA Tile ID: {ka_tile_id}")

# COMMAND ----------

# DBTITLE 1,Create or Update Supervisor Agent
mas_name = config['mas_name']
mas_description = config['mas_description']
mas_instructions = config['mas_instructions']

agents = [
    {
        "name": "billing_faq_agent",
        "ka_tile_id": ka_tile_id,
        "description": config['ka_description'],
    },
    {
        "name": "billing_analytics_agent",
        "genie_space_id": genie_space_id,
        "description": (
            "Runs SQL analytics on billing data: charge trends, plan comparisons, "
            "customer segmentation, top-N rankings, and month-over-month analysis. "
            "Uses invoice_analytics (PII-safe monthly charges per customer) and "
            "billing_plans (plan pricing and allowances)."
        ),
    },
]

examples = [
    {"question": "How is my bill calculated?", "guideline": "Should be routed to billing_faq_agent"},
    {"question": "What is the average monthly charge across all plans?", "guideline": "Should be routed to billing_analytics_agent"},
    {"question": "How do I set up autopay?", "guideline": "Should be routed to billing_faq_agent"},
    {"question": "Which plan has the highest roaming charges?", "guideline": "Should be routed to billing_analytics_agent"},
    {"question": "What are the top 10 customers by total charges?", "guideline": "Should be routed to billing_analytics_agent"},
    {"question": "Can I change my bill due date?", "guideline": "Should be routed to billing_faq_agent"},
    {"question": "Compare charges between 12-month and 24-month plans", "guideline": "Should be routed to billing_analytics_agent"},
]

# Check if MAS already exists
existing_mas = None
try:
    tiles = w.agent_bricks.list_tiles()
    for tile in tiles:
        if hasattr(tile, 'name') and tile.name == mas_name.replace(' ', '_'):
            existing_mas = tile
            print(f"Found existing MAS '{mas_name}' with tile_id: {tile.tile_id}")
            break
except Exception as e:
    print(f"Could not check existing MAS tiles: {e}")

if existing_mas:
    mas_tile = w.agent_bricks.update_tile(
        tile_id=existing_mas.tile_id,
        name=mas_name,
        agents=agents,
        description=mas_description,
        instructions=mas_instructions,
        examples=examples,
    )
    mas_tile_id = existing_mas.tile_id
    print(f"Updated existing MAS: {mas_tile_id}")
else:
    mas_tile = w.agent_bricks.create_mas(
        name=mas_name,
        agents=agents,
        description=mas_description,
        instructions=mas_instructions,
        examples=examples,
    )
    mas_tile_id = mas_tile.tile_id
    print(f"Created new MAS: {mas_tile_id}")

config['mas_tile_id'] = mas_tile_id

# COMMAND ----------

# DBTITLE 1,Wait for MAS Endpoint to Provision
print(f"Waiting for MAS endpoint to provision (tile_id: {mas_tile_id})...")
elapsed = 0

while elapsed < max_wait:
    try:
        status = w.agent_bricks.get_tile(tile_id=mas_tile_id)
        endpoint_status = getattr(status, 'endpoint_status', 'UNKNOWN')
        print(f"  Status: {endpoint_status} ({elapsed}s elapsed)")
        if endpoint_status == "ONLINE":
            print("MAS endpoint is ONLINE!")
            break
        if endpoint_status == "FAILED":
            print("ERROR: MAS endpoint provisioning failed.")
            break
    except Exception as e:
        print(f"  Error checking status: {e}")
    time.sleep(15)
    elapsed += 15

if elapsed >= max_wait:
    print(f"WARNING: MAS endpoint did not come online within {max_wait}s. Check the Databricks UI.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Supervisor Agent

# COMMAND ----------

# DBTITLE 1,Test FAQ Routing
print("Testing FAQ routing...")
test_faq = "How can I set up autopay for my bill?"
print(f"Question: {test_faq}")

try:
    response = w.serving_endpoints.query(
        name=f"mas-{mas_tile_id}-endpoint",
        input={"messages": [{"role": "user", "content": test_faq}]},
    )
    if hasattr(response, 'choices') and response.choices:
        print(f"Response: {response.choices[0].message.content[:500]}")
    elif hasattr(response, 'messages') and response.messages:
        print(f"Response: {response.messages[-1].content[:500]}")
    else:
        print(f"Response: {response}")
except Exception as e:
    print(f"Test query failed (endpoint may still be provisioning): {e}")

# COMMAND ----------

# DBTITLE 1,Test Analytics Routing
print("Testing analytics routing...")
test_analytics = "What is the average monthly total charge across all billing plans?"
print(f"Question: {test_analytics}")

try:
    response = w.serving_endpoints.query(
        name=f"mas-{mas_tile_id}-endpoint",
        input={"messages": [{"role": "user", "content": test_analytics}]},
    )
    if hasattr(response, 'choices') and response.choices:
        print(f"Response: {response.choices[0].message.content[:500]}")
    elif hasattr(response, 'messages') and response.messages:
        print(f"Response: {response.messages[-1].content[:500]}")
    else:
        print(f"Response: {response}")
except Exception as e:
    print(f"Test query failed (endpoint may still be provisioning): {e}")

# COMMAND ----------

# DBTITLE 1,Display Summary
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

print("=" * 60)
print("Agent Bricks Deployment Summary")
print("=" * 60)
print(f"\nKnowledge Assistant (FAQ):")
print(f"  Tile ID:  {ka_tile_id}")
print(f"  Endpoint: ka-{ka_tile_id}-endpoint")
print(f"  Volume:   {config['ka_volume_path']}")

print(f"\nGenie Space (Analytics):")
print(f"  Space ID: {genie_space_id}")
print(f"  URL:      https://{host}/genie/spaces/{genie_space_id}")

print(f"\nSupervisor Agent (MAS):")
print(f"  Tile ID:  {mas_tile_id}")
print(f"  Endpoint: mas-{mas_tile_id}-endpoint")

print(f"\nTo use in the Dash app, update SERVING_ENDPOINT to: mas-{mas_tile_id}-endpoint")
print("=" * 60)
