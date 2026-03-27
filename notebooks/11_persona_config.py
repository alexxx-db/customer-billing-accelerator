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

# Discover persona files dynamically instead of hardcoding names
import os
PERSONA_NAMES = sorted([
    f.replace(".yaml", "")
    for f in os.listdir(personas_dir)
    if f.endswith(".yaml")
])
assert len(PERSONA_NAMES) > 0, f"No persona YAML files found in {personas_dir}"
print(f"Discovered personas: {PERSONA_NAMES}")

# Known tool names for cross-reference validation
KNOWN_UC_TOOLS = {v.split('.')[-1] for k, v in config.items() if k.startswith('tools_')}
# In-agent tools defined in agent.py
KNOWN_AGENT_TOOLS = {
    "request_write_confirmation", "acknowledge_anomaly", "create_billing_dispute",
    "update_dispute_status", "lookup_dispute_history", "ask_billing_analytics",
}
ALL_KNOWN_TOOLS = KNOWN_UC_TOOLS | KNOWN_AGENT_TOOLS

personas = {}
for name in PERSONA_NAMES:
    path = f"{personas_dir}/{name}.yaml"
    with open(path) as f:
        p = yaml.safe_load(f)

    for key in ["persona", "system_prompt", "tool_policy", "response_style"]:
        assert key in p, f"{name}.yaml missing: {key}"
    for key in ["write_access", "allowed_tools", "blocked_tools"]:
        assert key in p['tool_policy'], f"{name}.yaml tool_policy missing: {key}"

    # Cross-reference: warn on tool names that don't match any known tool
    for tool_name in p['tool_policy']['allowed_tools']:
        if tool_name not in ALL_KNOWN_TOOLS:
            print(f"  WARNING: {name}.yaml allowed_tools references unknown tool '{tool_name}'")
    for tool_name in p['tool_policy']['blocked_tools']:
        if tool_name not in ALL_KNOWN_TOOLS:
            print(f"  WARNING: {name}.yaml blocked_tools references unknown tool '{tool_name}'")

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
