# Databricks notebook source
# MAGIC %md
# MAGIC # Persona Configuration
# MAGIC
# MAGIC Validates all persona YAML configs and serializes persona metadata to config.yaml.
# MAGIC
# MAGIC Four personas: Customer Care, Finance Operations, Executive Briefing, Platform Engineering.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Load and Validate Personas
import yaml

current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebooks_dir = "/".join(current_path.split("/")[:-1])
personas_dir = f"/Workspace{notebooks_dir}/personas"

PERSONA_NAMES = ["customer_care", "finance_ops", "executive", "technical"]

personas = {}
for name in PERSONA_NAMES:
    path = f"{personas_dir}/{name}.yaml"
    with open(path) as f:
        p = yaml.safe_load(f)

    for key in ["persona", "system_prompt", "tool_policy", "response_style"]:
        assert key in p, f"{name}.yaml missing: {key}"
    for key in ["write_access", "allowed_tools", "blocked_tools"]:
        assert key in p['tool_policy'], f"{name}.yaml tool_policy missing: {key}"

    personas[name] = p
    allowed = len(p['tool_policy']['allowed_tools'])
    blocked = len(p['tool_policy']['blocked_tools'])
    write = p['tool_policy']['write_access']
    print(f"  {name}: {allowed} allowed, {blocked} blocked, write={write}")

print(f"\nAll {len(personas)} personas validated.")

# COMMAND ----------

# DBTITLE 1,Serialize to config.yaml
with open("config.yaml", "r") as f:
    cfg = yaml.safe_load(f)

cfg['personas'] = {
    name: {
        "display_name": p['persona']['display_name'],
        "ui_label": p['persona']['ui_label'],
        "ui_description": p['persona']['ui_description'],
        "write_access": p['tool_policy']['write_access'],
        "allowed_tools": p['tool_policy']['allowed_tools'],
        "starter_prompts": p.get('starter_prompts', []),
    }
    for name, p in personas.items()
}
cfg['default_persona'] = 'customer_care'
cfg['persona_config_path'] = personas_dir

with open("config.yaml", "w") as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)

print("Persona configs serialized to config.yaml")
for name, p in personas.items():
    print(f"  {p['persona']['ui_label']}: {len(p['tool_policy']['allowed_tools'])} tools")
