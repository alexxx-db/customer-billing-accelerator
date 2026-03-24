# Databricks notebook source
# MAGIC %md
# MAGIC # Create and Start DLT Streaming Pipeline
# MAGIC
# MAGIC This notebook programmatically creates the DLT pipeline defined in
# MAGIC `06_dlt_streaming_pipeline.py` and starts it.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Run `06b_enable_streaming_prereqs` first (CDF must be enabled on billing_items)
# MAGIC - Run `000-config`, `00_data_preparation` (tables must exist)

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Preflight: Verify CDF is enabled
catalog = config['catalog']
schema  = config['database']

cdf_check = (spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.billing_items")
             .filter("col_name = 'delta.enableChangeDataFeed'")
             .collect())
if not cdf_check or cdf_check[0]["data_type"].lower() != "true":
    raise RuntimeError(
        "CDF is not enabled on billing_items. "
        "Run notebook 06b_enable_streaming_prereqs first."
    )
print("Preflight OK: CDF enabled on billing_items")

# COMMAND ----------

# DBTITLE 1,Create or Update DLT Pipeline
import yaml
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Resolve the path to the DLT pipeline definition notebook
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebooks_dir = "/".join(current_path.split("/")[:-1])
dlt_notebook_path = f"{notebooks_dir}/06_dlt_streaming_pipeline"

PIPELINE_NAME = f"telco_billing_streaming_{config['database']}"

# Check for existing pipeline
existing_pipeline_id = None
try:
    for pipeline in w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'"):
        if pipeline.name == PIPELINE_NAME:
            existing_pipeline_id = pipeline.pipeline_id
            print(f"Found existing pipeline: {existing_pipeline_id}")
            break
except Exception as e:
    print(f"Could not list pipelines: {e}")

# Pipeline spec
# continuous=True keeps the cluster alive for near-real-time processing.
# For cost-effective demo, set continuous=False and use a trigger interval instead.
pipeline_spec = {
    "name": PIPELINE_NAME,
    "continuous": True,
    "libraries": [
        {"notebook": {"path": dlt_notebook_path}}
    ],
    "clusters": [
        {
            "label": "default",
            "num_workers": 1,
        }
    ],
    "configuration": {
        "pipeline.catalog": config["catalog"],
        "pipeline.schema":  config["database"],
    },
    "catalog": config["catalog"],
    "target": config["database"],
    "development": True,
    "photon": False,
    "edition": "CORE",
}

if existing_pipeline_id:
    w.pipelines.update(pipeline_id=existing_pipeline_id, **pipeline_spec)
    pipeline_id = existing_pipeline_id
    print(f"Updated existing pipeline {pipeline_id}")
else:
    created = w.pipelines.create(**pipeline_spec)
    pipeline_id = created.pipeline_id
    print(f"Created pipeline {pipeline_id}")

# COMMAND ----------

# DBTITLE 1,Start Pipeline
state = w.pipelines.get(pipeline_id=pipeline_id)
print(f"Current pipeline state: {state.state}")

if str(state.state) not in ("RUNNING", "STARTING", "RESETTING"):
    w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
    print("Pipeline start triggered")
else:
    print("Pipeline already running — no action needed")

# COMMAND ----------

# DBTITLE 1,Save Pipeline ID to Config
with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)
cfg["dlt_pipeline_id"] = pipeline_id
with open("config.yaml", "w") as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

config['dlt_pipeline_id'] = pipeline_id

host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
print(f"\nPipeline URL: https://{host}/#joblist/pipelines/{pipeline_id}")
print(f"pipeline_id saved to config.yaml: {pipeline_id}")
