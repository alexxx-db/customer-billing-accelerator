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
# MAGIC
# MAGIC ## Note on Agent Bricks API
# MAGIC
# MAGIC Agent Bricks are managed via the Databricks REST API (`/api/2.0/agent-bricks/`).
# MAGIC This notebook uses `WorkspaceClient().api_client` for direct REST calls since
# MAGIC the Agent Bricks SDK surface may not be available in all SDK versions.

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
import json

catalog = config['catalog']
db = config['database']
volume_path = config['ka_volume_path']

# Create the volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{db}.billing_faq_docs")

# Read FAQ data from the Delta table
faq_df = spark.table(f"{catalog}.{db}.billing_faq_dataset").collect()

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
# MAGIC Create a Knowledge Assistant that indexes the billing FAQ documents
# MAGIC using the Agent Bricks REST API.

# COMMAND ----------

# DBTITLE 1,Agent Bricks API Helpers
import time
import requests
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Max wait time for endpoint provisioning (seconds)
MAX_WAIT = 600
POLL_INTERVAL = 15


def _ab_api(method, path, body=None):
    """Call Agent Bricks REST API via the workspace API client."""
    api_url = f"{w.config.host}/api/2.0/agent-bricks{path}"
    headers = {"Authorization": f"Bearer {w.config.token}"}
    if method == "GET":
        resp = requests.get(api_url, headers=headers)
    elif method == "POST":
        resp = requests.post(api_url, headers=headers, json=body)
    elif method == "PUT":
        resp = requests.put(api_url, headers=headers, json=body)
    elif method == "DELETE":
        resp = requests.delete(api_url, headers=headers)
    else:
        raise ValueError(f"Unsupported method: {method}")
    resp.raise_for_status()
    return resp.json() if resp.text else {}


def find_tile_by_name(name):
    """Find an Agent Bricks tile by name. Returns tile dict or None."""
    sanitized = name.replace(' ', '_')
    try:
        tiles = _ab_api("GET", "/tiles")
        for tile in tiles.get("tiles", []):
            if tile.get("name") == sanitized or tile.get("name") == name:
                return tile
    except Exception as e:
        print(f"Could not list tiles: {e}")
    return None


def wait_for_tile(tile_id, label="tile"):
    """Poll until a tile's endpoint is ONLINE, FAILED, or timeout."""
    print(f"Waiting for {label} endpoint to provision (tile_id: {tile_id})...")
    elapsed = 0
    while elapsed < MAX_WAIT:
        try:
            tile = _ab_api("GET", f"/tiles/{tile_id}")
            status = tile.get("endpoint_status", "UNKNOWN")
            print(f"  Status: {status} ({elapsed}s elapsed)")
            if status == "ONLINE":
                print(f"{label} endpoint is ONLINE!")
                return tile
            if status == "FAILED":
                print(f"ERROR: {label} endpoint provisioning failed.")
                return tile
        except Exception as e:
            print(f"  Error checking status: {e}")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    print(f"WARNING: {label} endpoint did not come online within {MAX_WAIT}s. Check the Databricks UI.")
    return None

# COMMAND ----------

# DBTITLE 1,Create or Update Knowledge Assistant
ka_name = config['ka_name']
ka_volume_path = config['ka_volume_path']

existing_ka = find_tile_by_name(ka_name)

ka_payload = {
    "name": ka_name,
    "description": config['ka_description'],
    "instructions": config['ka_instructions'],
    "volume_path": ka_volume_path,
    "add_examples_from_volume": True,
}

if existing_ka:
    ka_tile_id = existing_ka["tile_id"]
    _ab_api("PUT", f"/tiles/{ka_tile_id}", ka_payload)
    print(f"Updated existing KA '{ka_name}': {ka_tile_id}")
else:
    result = _ab_api("POST", "/ka", ka_payload)
    ka_tile_id = result["tile_id"]
    print(f"Created new KA '{ka_name}': {ka_tile_id}")

config['ka_tile_id'] = ka_tile_id

# COMMAND ----------

# DBTITLE 1,Wait for KA Endpoint to Provision
wait_for_tile(ka_tile_id, label="KA")

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

existing_mas = find_tile_by_name(mas_name)

mas_payload = {
    "name": mas_name,
    "agents": agents,
    "description": config['mas_description'],
    "instructions": config['mas_instructions'],
    "examples": examples,
}

if existing_mas:
    mas_tile_id = existing_mas["tile_id"]
    _ab_api("PUT", f"/tiles/{mas_tile_id}", mas_payload)
    print(f"Updated existing MAS '{mas_name}': {mas_tile_id}")
else:
    result = _ab_api("POST", "/mas", mas_payload)
    mas_tile_id = result["tile_id"]
    print(f"Created new MAS '{mas_name}': {mas_tile_id}")

config['mas_tile_id'] = mas_tile_id

# COMMAND ----------

# DBTITLE 1,Wait for MAS Endpoint to Provision
wait_for_tile(mas_tile_id, label="MAS")

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
