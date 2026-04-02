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
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.knowledgeassistants import (
    KnowledgeAssistant,
    KnowledgeSource,
    FilesSpec,
)

w = WorkspaceClient()

MAX_WAIT = 600
POLL_INTERVAL = 15


def find_ka_by_name(display_name: str):
    """Find an existing Knowledge Assistant by display_name."""
    try:
        for ka in w.knowledge_assistants.list_knowledge_assistants():
            if ka.display_name == display_name:
                return ka
    except Exception as e:
        print(f"Could not list knowledge assistants: {e}")
    return None


def wait_for_ka(ka_resource_name: str, label: str = "KA"):
    """Poll until a KA endpoint is ONLINE. ka_resource_name: knowledge-assistants/{id}"""
    print(f"Waiting for {label} to provision ({ka_resource_name})...")
    elapsed = 0
    while elapsed < MAX_WAIT:
        try:
            ka = w.knowledge_assistants.get_knowledge_assistant(ka_resource_name)
            state = ka.state.value if ka.state else "UNKNOWN"
            print(f"  State: {state} ({elapsed}s elapsed)")
            if state in ("ONLINE", "ACTIVE"):
                print(f"{label} is ready! Endpoint: {ka.endpoint_name}")
                return ka
            if state in ("FAILED", "ERROR"):
                print(f"ERROR: {label} provisioning failed: {ka.error_info}")
                return ka
        except Exception as e:
            print(f"  Error checking status: {e}")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    print(f"WARNING: {label} did not come online within {MAX_WAIT}s.")
    return None


def find_genie_space_by_name(space_name: str):
    """Find an existing Genie Space by title."""
    try:
        resp = w.genie.list_spaces()
        if hasattr(resp, 'spaces') and resp.spaces:
            for s in resp.spaces:
                if s.title == space_name:
                    return s.space_id
    except Exception as e:
        print(f"Could not list Genie spaces: {e}")
    return None

print("SDK helpers loaded.")

# COMMAND ----------

# DBTITLE 1,Create or Update Knowledge Assistant
ka_name = config['ka_name']
ka_volume_path = config['ka_volume_path']

existing_ka = find_ka_by_name(ka_name)

if existing_ka:
    ka_id = existing_ka.id
    ka_resource_name = existing_ka.name  # format: knowledge-assistants/{id}
    print(f"Found existing KA '{ka_name}': {ka_id} (endpoint: {existing_ka.endpoint_name})")
else:
    created = w.knowledge_assistants.create_knowledge_assistant(
        knowledge_assistant=KnowledgeAssistant(
            display_name=ka_name,
            description=config['ka_description'],
            instructions=config['ka_instructions'],
        )
    )
    ka_id = created.id
    ka_resource_name = created.name
    print(f"Created new KA '{ka_name}': {ka_id}")

# Add knowledge source (UC Volume with FAQ docs) if not already present
existing_sources = list(w.knowledge_assistants.list_knowledge_sources(ka_resource_name))
volume_source = None
for src in existing_sources:
    if src.files and src.files.path == ka_volume_path:
        volume_source = src
        break

if not volume_source:
    ks = w.knowledge_assistants.create_knowledge_source(
        parent=ka_resource_name,
        knowledge_source=KnowledgeSource(
            display_name="Billing FAQ Documents",
            description="FAQ documents covering billing questions, payment methods, and plan details.",
            source_type="files",
            files=FilesSpec(path=ka_volume_path),
        ),
    )
    print(f"Added knowledge source: {ks.id} ({ka_volume_path})")
else:
    print(f"Knowledge source already exists: {volume_source.id}")

# Sync knowledge sources to trigger indexing
w.knowledge_assistants.sync_knowledge_sources(ka_resource_name)
print("Triggered knowledge source sync.")

config['ka_id'] = ka_id

# COMMAND ----------

# DBTITLE 1,Wait for KA Endpoint to Provision
ka_result = wait_for_ka(ka_resource_name, label="Billing FAQ KA")
if ka_result and ka_result.endpoint_name:
    ka_endpoint_name = ka_result.endpoint_name
    config['ka_endpoint_name'] = ka_endpoint_name
    print(f"KA endpoint: {ka_endpoint_name}")
else:
    # KA may already be online from a previous run
    ka = w.knowledge_assistants.get_knowledge_assistant(ka_resource_name)
    ka_endpoint_name = ka.endpoint_name or '(not available)'
    config['ka_endpoint_name'] = ka_endpoint_name
    print(f"KA endpoint (from existing): {ka_endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Supervisor Agent (MAS)
# MAGIC
# MAGIC Create a Supervisor Agent that combines the FAQ Knowledge Assistant
# MAGIC with the Billing Analytics Genie Space.

# COMMAND ----------

# DBTITLE 1,Verify Genie Space ID is Available
genie_space_id = config.get('genie_space_id')

# If not in config, look up by name
if not genie_space_id:
    genie_space_name = config.get('genie_space_name', '')
    if genie_space_name:
        genie_space_id = find_genie_space_by_name(genie_space_name)
        if genie_space_id:
            config['genie_space_id'] = genie_space_id
            print(f"Found Genie Space '{genie_space_name}': {genie_space_id}")

if not genie_space_id:
    raise ValueError(
        "genie_space_id is not set in config and could not be found by name. "
        "Run notebook 03a_create_genie_space first."
    )

ka_endpoint_name = config.get('ka_endpoint_name', '')
if not ka_endpoint_name:
    ka = w.knowledge_assistants.get_knowledge_assistant(ka_resource_name)
    ka_endpoint_name = ka.endpoint_name or '(not yet provisioned)'

print(f"Genie Space ID:    {genie_space_id}")
print(f"KA Endpoint Name:  {ka_endpoint_name}")

# COMMAND ----------

# DBTITLE 1,Create or Update Supervisor Agent
host = w.config.host
mas_name = config['mas_name']

print("=" * 60)
print("Create the Supervisor Agent via the UI")
print("=" * 60)
print(f"\n1. Open: {host}/#/agents")
print("2. Click 'Supervisor Agent' → 'Build'")
print(f"3. Name: {mas_name}")
print(f"4. Description: {config['mas_description']}")
print(f"\n5. Add sub-agents:")
print(f"   a) Agent endpoint → '{ka_endpoint_name}'  (Billing FAQ KA)")
print(f"   b) Genie Space → ID: {genie_space_id}  (Billing Analytics)")
print(f"\n6. Instructions:")
print(f"   {config['mas_instructions'][:300]}...")
print(f"\n7. Add example questions:")
examples = [
    ("How is my bill calculated?", "billing_faq_agent"),
    ("Average monthly charge across all plans?", "billing_analytics_agent"),
    ("How do I set up autopay?", "billing_faq_agent"),
    ("Which plan has the highest roaming charges?", "billing_analytics_agent"),
    ("Top 10 customers by total charges?", "billing_analytics_agent"),
]
for q, agent in examples:
    print(f"   Q: {q:50s} → {agent}")

print(f"\n8. Click 'Create' to provision the supervisor endpoint.")
print("=" * 60)
print("\nNote: The Supervisor Agent API is UI-only; no programmatic SDK is available yet.")

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