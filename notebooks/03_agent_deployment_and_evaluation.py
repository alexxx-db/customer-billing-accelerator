# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Telco Billing Agent Deployment & Evaluation
# MAGIC
# MAGIC This notebook orchestrates the **deployment pipeline** for an AI agent designed to answer customer billing queries for a telecommunications provider.
# MAGIC
# MAGIC It extends the auto-generated code from the Databricks AI Playground with an additional **synthetic evaluation component**, allowing for a more rigorous and repeatable assessment of the agent’s quality using a curated FAQ knowledge base.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Notebook Overview
# MAGIC
# MAGIC This notebook performs the following steps:
# MAGIC
# MAGIC 1. **Define the agent**
# MAGIC 2. **Log the agent**: Wraps the agent (from the `agent` notebook) as an MLflow model along with configuration and resource bindings.
# MAGIC 3. **Synthetic Evaluation**: Generates a set of realistic and adversarial queries from the FAQ dataset to evaluate agent performance using the Agent Evaluation framework.
# MAGIC 4. **Register to Unity Catalog**: Stores the model centrally for governance and discovery.
# MAGIC 5. **Deploy to Model Serving**: Deploys the agent to a Databricks model serving endpoint for interactive and production use.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Evaluation with Synthetic Questions
# MAGIC
# MAGIC Using the `generate_evals_df()` API, this notebook constructs a diverse set of evaluation prompts from the billing FAQ knowledge base. These include:
# MAGIC
# MAGIC - Realistic billing-related user queries
# MAGIC - Edge cases like irrelevant or sensitive questions (to test refusal behavior)
# MAGIC - Multiple user personas (e.g., customers, agents)
# MAGIC
# MAGIC This process helps verify that the deployed agent:
# MAGIC
# MAGIC - Understands the domain context (billing)
# MAGIC - Retrieves accurate answers from the knowledge base
# MAGIC - Appropriately ignores irrelevant or sensitive queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 Prerequisites
# MAGIC
# MAGIC Before running this notebook:
# MAGIC
# MAGIC - Update and validate the `config.yaml` to define agent tools, LLM endpoints, and system prompt.
# MAGIC - Run the `agent` notebook to define and test your agent.
# MAGIC - Ensure the `faq_index` and `billing_faq_dataset` are available and correctly formatted in Unity Catalog.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Outputs
# MAGIC
# MAGIC - A registered and deployed agent model in Unity Catalog
# MAGIC - A set of evaluation metrics stored in MLflow
# MAGIC - A live model endpoint ready for testing in AI Playground or production integration
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install Libraries
# MAGIC %pip install -U -qqqq mlflow-skinny[databricks] langgraph==0.3.4 databricks-langchain databricks-agents==1.0.1 uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

import yaml

class LiteralString(str):
    pass

def literal_str_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='|')

yaml.add_representer(LiteralString, literal_str_representer)

# COMMAND ----------

# Prompt
system_prompt = """You are a Billing Support Agent assisting users with billing inquiries.

Guidelines:
- First, check FAQ Search before requesting any details.
- If an FAQ answer exists, return it immediately.
- If no FAQ match, request the customer_id before retrieving billing details.
- NEVER disclose confidential information in your responses, including: customer_name, email, device_id, or phone_number. These fields are for internal lookup only.
- If a user asks for confidential fields, politely decline and explain that you cannot share that information.
- For ad-hoc analytical questions that span multiple customers or require aggregations
  (e.g., trends, averages, comparisons across plans, top-N rankings), use the
  ask_billing_analytics tool. This delegates to a Genie Space that can write SQL
  over the full billing dataset.
- For individual customer lookups (specific customer's bill, plan, etc.), use the
  dedicated lookup tools.
- To check for billing anomalies (charge spikes, roaming spikes, international spikes,
  data overage spikes), use the lookup_billing_anomalies tool. You can look up anomalies for
  a specific customer or pass an empty string to see recent anomalies across all customers.
- When asked "what's new?", "any updates?", "what happened since yesterday?",
  call get_monitoring_status(24) first, then summarize conversationally.
  Do not ask for a customer_id for these fleet-wide monitoring questions.
- When asked about monitoring coverage ("are all anomalies being tracked?",
  "how many are unalerted?"), use get_monitoring_status(0) for all-time state.
- The billing_monthly_running table contains real-time charge estimates from
  the streaming pipeline. If a customer asks "how much have I spent so far this
  month?" use ask_billing_analytics to query billing_monthly_running.
- For operational health questions about the billing platform itself (costs, job status,
  pipeline health, warehouse performance), use lookup_operational_kpis or
  lookup_job_reliability — NOT the billing domain tools which are for customer data.
- "How much does the platform cost?" -> lookup_operational_kpis(30).
- "Is the anomaly detection job working?" -> lookup_job_reliability(true).
- Operational questions are about the Databricks infrastructure, not customer billing.
  Never share raw DBU record IDs, cluster IDs, or workspace IDs with end users.
- For questions about a customer's account standing, credit risk, or payment history,
  use lookup_customer_erp_profile. Do NOT share ar_balance or overdue_balance directly.
- For billing dispute investigation or revenue reconciliation, use lookup_revenue_attribution.
- For finance leadership questions about revenue, ARPU, AR health, OPEX ratios,
  use get_finance_operations_summary.
- ERP data may have a 24-hour lag. Revenue variance < 10% is normal timing difference.

Write-Back Operations (require explicit user confirmation):
- You CAN acknowledge billing anomalies, create billing disputes, and update
  dispute statuses. These are write operations — they modify data permanently.
- ALWAYS call request_write_confirmation BEFORE any write operation. This stages
  the action and asks the user to confirm. Do not call acknowledge_anomaly,
  create_billing_dispute, or update_dispute_status directly without staging first.
- After staging, explain what will happen and ask the user to reply CONFIRM or CANCEL.
- When acknowledging an anomaly, always provide a reason.
- To check existing disputes, use lookup_dispute_history (in-agent) or
  lookup_open_disputes (UC function).
- Never acknowledge anomalies in bulk. Handle one at a time.

Process:
1. Run FAQ Search -> If an answer exists, return it.
2. If no FAQ match, ask for the customer_id and use the relevant tool(s) to fetch billing details.
3. For analytical questions across multiple customers, use ask_billing_analytics.
4. When asked about unusual charges or billing anomalies, use lookup_billing_anomalies.
5. When asked about monitoring status or what's new, use get_monitoring_status.
6. For platform health or cost questions, use lookup_operational_kpis or lookup_job_reliability.
7. For ERP/finance questions, use lookup_customer_erp_profile, lookup_revenue_attribution, or get_finance_operations_summary.
8. For write operations (acknowledge, dispute, escalate), use request_write_confirmation first, then execute after user confirms.
9. If missing details (e.g., timeframe), ask clarifying questions.

Keep responses polite, professional, and concise.
"""

# COMMAND ----------

catalog = config['catalog']
schema = config['database']
llm_endpoint = config['llm_endpoint']
embedding_model_endpoint_name = config['embedding_model_endpoint_name']
warehouse_id = config['warehouse_id']
vector_search_index = f"{config['catalog']}.{config['database']}.{config['vector_search_index']}"
tools_billing_faq = config['tools_billing_faq'] 
tools_billing = config['tools_billing']
tools_items = config['tools_items']
tools_plans = config['tools_plans']
tools_customer = config['tools_customer']
tools_anomalies = config['tools_anomalies']
tools_monitoring_status = config['tools_monitoring_status']
tools_operational_kpis = config['tools_operational_kpis']
tools_job_reliability = config['tools_job_reliability']
tools_customer_erp_profile = config['tools_customer_erp_profile']
tools_revenue_attribution = config['tools_revenue_attribution']
tools_finance_ops_summary = config['tools_finance_ops_summary']
tools_open_disputes = config['tools_open_disputes']
tools_write_audit = config['tools_write_audit']
agent_name = config['agent_name']
genie_space_id = config.get('genie_space_id', '') or ''
agent_prompt = LiteralString(system_prompt)

# COMMAND ----------

yaml_data = {
    "catalog": catalog,
    "schema": schema,
    "llm_endpoint": llm_endpoint,
    "embedding_model_endpoint_name": embedding_model_endpoint_name,
    "warehouse_id": warehouse_id,
    "vector_search_index": vector_search_index,
    "tools_billing_faq": tools_billing_faq,
    "tools_billing": tools_billing,
    "tools_items": tools_items,
    "tools_plans": tools_plans,
    "tools_customer": tools_customer,
    "tools_anomalies": tools_anomalies,
    "tools_monitoring_status": tools_monitoring_status,
    "tools_operational_kpis": tools_operational_kpis,
    "tools_job_reliability": tools_job_reliability,
    "tools_customer_erp_profile": tools_customer_erp_profile,
    "tools_revenue_attribution": tools_revenue_attribution,
    "tools_finance_ops_summary": tools_finance_ops_summary,
    "tools_open_disputes": tools_open_disputes,
    "tools_write_audit": tools_write_audit,
    "agent_name": agent_name,
    "genie_space_id": genie_space_id,
    "agent_prompt": agent_prompt
}

with open("config.yaml", "w") as f:
    yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)


# COMMAND ----------

# MAGIC %md ## Define the agent in code
# MAGIC Below we define our agent code in a single cell, enabling us to easily write it to a local Python file for subsequent logging and deployment using the `%%writefile` magic command.
# MAGIC
# MAGIC For more examples of tools to add to your agent, see [docs](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/agent-tool).

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC from typing import Any, Generator, Optional, Sequence, Union
# MAGIC import time
# MAGIC import json
# MAGIC import uuid
# MAGIC from datetime import datetime, timezone
# MAGIC
# MAGIC import mlflow
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks_langchain import (
# MAGIC     ChatDatabricks,
# MAGIC     VectorSearchRetrieverTool,
# MAGIC     DatabricksFunctionClient,
# MAGIC     UCFunctionToolkit,
# MAGIC     set_uc_function_client,
# MAGIC )
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool, tool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.graph import CompiledGraph
# MAGIC from langgraph.graph.state import CompiledStateGraph
# MAGIC from langgraph.prebuilt.tool_node import ToolNode
# MAGIC from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk,
# MAGIC     ChatAgentMessage,
# MAGIC     ChatAgentResponse,
# MAGIC     ChatContext,
# MAGIC )
# MAGIC from mlflow.models import ModelConfig
# MAGIC
# MAGIC mlflow.langchain.autolog()
# MAGIC
# MAGIC client = DatabricksFunctionClient()
# MAGIC set_uc_function_client(client)
# MAGIC
# MAGIC config = ModelConfig(development_config="config.yaml").to_dict()
# MAGIC
# MAGIC
# MAGIC ############################################
# MAGIC # Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC llm = ChatDatabricks(endpoint=config['llm_endpoint'])
# MAGIC
# MAGIC # Inject domain-aware context into the system prompt
# MAGIC _base_prompt = config.get('agent_prompt', '')
# MAGIC _domain_section = config.get('domain_agent_prompt_section', '')
# MAGIC if _domain_section and _domain_section.strip() not in _base_prompt:
# MAGIC     system_prompt = _base_prompt + "\n" + _domain_section
# MAGIC else:
# MAGIC     system_prompt = _base_prompt
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Write-Back Infrastructure
# MAGIC ###############################################################################
# MAGIC
# MAGIC WRITE_PENDING_PREFIX = "WRITE_PENDING"
# MAGIC
# MAGIC CONFIRM_PHRASES = {"confirm", "yes", "proceed", "approve", "go ahead", "do it"}
# MAGIC CANCEL_PHRASES  = {"cancel", "stop", "abort", "never mind", "don't"}
# MAGIC
# MAGIC # Module-level WorkspaceClient for write operations (reused across calls)
# MAGIC _write_client = None
# MAGIC
# MAGIC def _get_write_client():
# MAGIC     global _write_client
# MAGIC     if _write_client is None:
# MAGIC         _write_client = WorkspaceClient()
# MAGIC     return _write_client
# MAGIC
# MAGIC
# MAGIC def _sanitize_sql_str(val: str) -> str:
# MAGIC     """Sanitize a string for SQL injection prevention beyond single-quote escaping."""
# MAGIC     if val is None:
# MAGIC         return ""
# MAGIC     # Replace single quotes, backslashes, and null bytes
# MAGIC     return val.replace("\\", "\\\\").replace("'", "''").replace("\x00", "")
# MAGIC
# MAGIC
# MAGIC def _execute_sql(sql: str, warehouse_id: str, action_type: str,
# MAGIC                  payload: dict, customer_id: int | None,
# MAGIC                  session_id: str | None) -> dict:
# MAGIC     """Execute a SQL write via Statement Execution API with audit-first pattern.
# MAGIC     Writes two audit records (PENDING before, then SUCCESS/FAILED after) — pure append."""
# MAGIC     from databricks.sdk.service.sql import StatementState
# MAGIC
# MAGIC     w = _get_write_client()
# MAGIC     audit_id = str(uuid.uuid4())
# MAGIC     executed_at = datetime.now(timezone.utc).isoformat()
# MAGIC     cat = config['catalog']
# MAGIC     sch = config['schema']
# MAGIC
# MAGIC     # Audit record 1: PENDING (before business write)
# MAGIC     audit_sql = f"""
# MAGIC     INSERT INTO {cat}.{sch}.billing_write_audit
# MAGIC     (audit_id, action_type, target_table, target_record_id, customer_id,
# MAGIC      agent_session_id, executed_by, payload_json, sql_statement,
# MAGIC      result_status, result_message, error_detail, executed_at)
# MAGIC     VALUES (
# MAGIC       '{audit_id}', '{action_type}',
# MAGIC       '{payload.get("target_table", "unknown")}',
# MAGIC       '{payload.get("record_id", "")}',
# MAGIC       {customer_id if customer_id is not None else 'NULL'},
# MAGIC       {f"'{session_id}'" if session_id else 'NULL'},
# MAGIC       'ai_billing_agent',
# MAGIC       '{_sanitize_sql_str(json.dumps(payload))}',
# MAGIC       '{_sanitize_sql_str(sql)}',
# MAGIC       'PENDING', 'Audit pre-written before business SQL.', NULL,
# MAGIC       TIMESTAMP '{executed_at}'
# MAGIC     )
# MAGIC     """
# MAGIC     try:
# MAGIC         resp = w.statement_execution.execute_statement(
# MAGIC             statement=audit_sql, warehouse_id=warehouse_id, wait_timeout="15s")
# MAGIC         if resp.status.state not in (StatementState.SUCCEEDED, StatementState.RUNNING):
# MAGIC             return {"success": False, "audit_id": audit_id,
# MAGIC                     "message": f"Write aborted: audit creation failed ({resp.status.state})."}
# MAGIC     except Exception as e:
# MAGIC         return {"success": False, "audit_id": audit_id,
# MAGIC                 "message": f"Write aborted: audit creation failed ({e})."}
# MAGIC
# MAGIC     # Execute the business SQL
# MAGIC     try:
# MAGIC         resp = w.statement_execution.execute_statement(
# MAGIC             statement=sql, warehouse_id=warehouse_id, wait_timeout="30s")
# MAGIC         if resp.status.state == StatementState.SUCCEEDED:
# MAGIC             # Audit record 2: SUCCESS
# MAGIC             w.statement_execution.execute_statement(
# MAGIC                 statement=f"""INSERT INTO {cat}.{sch}.billing_write_audit
# MAGIC                 (audit_id, action_type, target_table, target_record_id, customer_id,
# MAGIC                  agent_session_id, executed_by, payload_json, sql_statement,
# MAGIC                  result_status, result_message, error_detail, executed_at)
# MAGIC                 VALUES ('{str(uuid.uuid4())}', '{action_type}',
# MAGIC                 '{payload.get("target_table", "")}', '{payload.get("record_id", "")}',
# MAGIC                 {customer_id if customer_id is not None else 'NULL'},
# MAGIC                 {f"'{session_id}'" if session_id else 'NULL'},
# MAGIC                 'ai_billing_agent', NULL, NULL, 'SUCCESS',
# MAGIC                 'Write completed successfully.', NULL,
# MAGIC                 TIMESTAMP '{datetime.now(timezone.utc).isoformat()}')""",
# MAGIC                 warehouse_id=warehouse_id, wait_timeout="10s")
# MAGIC             return {"success": True, "message": "Write completed successfully.", "audit_id": audit_id}
# MAGIC         else:
# MAGIC             error = str(resp.status.error) if resp.status.error else "Unknown error"
# MAGIC             w.statement_execution.execute_statement(
# MAGIC                 statement=f"""INSERT INTO {cat}.{sch}.billing_write_audit
# MAGIC                 (audit_id, action_type, target_table, target_record_id, customer_id,
# MAGIC                  agent_session_id, executed_by, payload_json, sql_statement,
# MAGIC                  result_status, result_message, error_detail, executed_at)
# MAGIC                 VALUES ('{str(uuid.uuid4())}', '{action_type}',
# MAGIC                 '{payload.get("target_table", "")}', '{payload.get("record_id", "")}',
# MAGIC                 {customer_id if customer_id is not None else 'NULL'},
# MAGIC                 {f"'{session_id}'" if session_id else 'NULL'},
# MAGIC                 'ai_billing_agent', NULL, NULL, 'FAILED',
# MAGIC                 'Business SQL failed.', '{_sanitize_sql_str(error[:500])}',
# MAGIC                 TIMESTAMP '{datetime.now(timezone.utc).isoformat()}')""",
# MAGIC                 warehouse_id=warehouse_id, wait_timeout="10s")
# MAGIC             return {"success": False, "message": f"Write failed: {error}", "audit_id": audit_id}
# MAGIC     except Exception as e:
# MAGIC         return {"success": False, "message": f"Write exception: {e}", "audit_id": audit_id}
# MAGIC
# MAGIC
# MAGIC def _extract_pending_write(messages: list[dict]) -> dict | None:
# MAGIC     for msg in reversed(messages[-4:]):
# MAGIC         content = msg.get("content", "")
# MAGIC         if isinstance(content, str) and content.startswith(WRITE_PENDING_PREFIX):
# MAGIC             try:
# MAGIC                 return json.loads(content[len(WRITE_PENDING_PREFIX) + 1:])
# MAGIC             except Exception:
# MAGIC                 pass
# MAGIC     return None
# MAGIC
# MAGIC
# MAGIC def _get_user_intent(messages: list[dict]) -> str:
# MAGIC     """Returns 'confirm', 'cancel', or 'unclear' based on the most recent user message.
# MAGIC     Cancel takes priority over confirm if both match (safety-first)."""
# MAGIC     for msg in reversed(messages[-2:]):
# MAGIC         if msg.get("role") == "user":
# MAGIC             text = (msg.get("content", "") or "").lower().strip()
# MAGIC             has_cancel = any(phrase in text for phrase in CANCEL_PHRASES)
# MAGIC             has_confirm = any(phrase in text for phrase in CONFIRM_PHRASES)
# MAGIC             if has_cancel:
# MAGIC                 return "cancel"
# MAGIC             if has_confirm:
# MAGIC                 return "confirm"
# MAGIC             return "unclear"
# MAGIC     return "unclear"
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Define tools
# MAGIC ###############################################################################
# MAGIC catalog = config['catalog']
# MAGIC schema = config['schema']
# MAGIC
# MAGIC tools = []
# MAGIC
# MAGIC # UC function tools (read-only)
# MAGIC uc_tool_names = [
# MAGIC     config['tools_billing_faq'],
# MAGIC     config['tools_billing'],
# MAGIC     config['tools_items'],
# MAGIC     config['tools_plans'],
# MAGIC     config['tools_customer'],
# MAGIC     config['tools_anomalies'],
# MAGIC     config['tools_monitoring_status'],
# MAGIC     config['tools_operational_kpis'],
# MAGIC     config['tools_job_reliability'],
# MAGIC     config['tools_customer_erp_profile'],
# MAGIC     config['tools_revenue_attribution'],
# MAGIC     config['tools_finance_ops_summary'],
# MAGIC     config['tools_open_disputes'],
# MAGIC     config['tools_write_audit'],
# MAGIC     ]
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
# MAGIC tools.extend(uc_toolkit.tools)
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Genie Space tool
# MAGIC ###############################################################################
# MAGIC genie_space_id = config.get('genie_space_id', '')
# MAGIC
# MAGIC if genie_space_id:
# MAGIC     _genie_client = WorkspaceClient()
# MAGIC
# MAGIC     @tool
# MAGIC     def ask_billing_analytics(question: str) -> str:
# MAGIC         """Ask an ad-hoc billing analytics question using natural language.
# MAGIC         Use this tool for complex analytical questions that span multiple customers
# MAGIC         or require aggregations across the billing dataset, such as:
# MAGIC         - Revenue and charge trends over time
# MAGIC         - Plan comparisons and averages
# MAGIC         - Top-N customer rankings
# MAGIC         - Customer segmentation by charges or plan type
# MAGIC         - Month-over-month or period-over-period analysis
# MAGIC
# MAGIC         Do NOT use this for individual customer lookups — use the dedicated
# MAGIC         lookup_customer, lookup_billing, or lookup_billing_items tools instead.
# MAGIC         """
# MAGIC         response = _genie_client.genie.start_conversation(
# MAGIC             space_id=genie_space_id, content=question)
# MAGIC
# MAGIC         conversation_id = response.conversation_id
# MAGIC         message_id = response.message_id
# MAGIC
# MAGIC         max_attempts = 30
# MAGIC         result = None
# MAGIC         for _ in range(max_attempts):
# MAGIC             result = _genie_client.genie.get_message(
# MAGIC                 space_id=genie_space_id,
# MAGIC                 conversation_id=conversation_id, message_id=message_id)
# MAGIC             if hasattr(result, 'status') and result.status in ("COMPLETED", "FAILED"):
# MAGIC                 break
# MAGIC             time.sleep(2)
# MAGIC
# MAGIC         if result is None or not hasattr(result, 'status'):
# MAGIC             return "The analytics query timed out. Please try a simpler question."
# MAGIC         if result.status == "FAILED":
# MAGIC             return "The analytics query could not be completed. Please try rephrasing your question."
# MAGIC         if result.status != "COMPLETED":
# MAGIC             return "The analytics query timed out. Please try a simpler question."
# MAGIC
# MAGIC         if hasattr(result, 'attachments') and result.attachments:
# MAGIC             parts = []
# MAGIC             for att in result.attachments:
# MAGIC                 if hasattr(att, 'text') and att.text:
# MAGIC                     parts.append(att.text.content)
# MAGIC                 if hasattr(att, 'query') and att.query:
# MAGIC                     if att.query.description:
# MAGIC                         parts.append(att.query.description)
# MAGIC                     if att.query.query:
# MAGIC                         parts.append(f"SQL: {att.query.query}")
# MAGIC             if parts:
# MAGIC                 return "\n\n".join(parts)
# MAGIC
# MAGIC         return "No results found for your analytics question."
# MAGIC
# MAGIC     tools.append(ask_billing_analytics)
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Write-back tools
# MAGIC ###############################################################################
# MAGIC
# MAGIC @tool
# MAGIC def request_write_confirmation(action_type: str, payload_json: str,
# MAGIC                                human_readable_summary: str) -> str:
# MAGIC     """Stage a write action for human confirmation. Call this BEFORE any write.
# MAGIC     action_type: acknowledge_anomaly, create_billing_dispute, update_dispute_status.
# MAGIC     payload_json: JSON string with all fields for the action.
# MAGIC     human_readable_summary: what you will show the user.
# MAGIC     Returns a WRITE_PENDING sentinel. Then ask the user to reply CONFIRM or CANCEL.
# MAGIC     """
# MAGIC     try:
# MAGIC         payload = json.loads(payload_json)
# MAGIC     except Exception as e:
# MAGIC         return f"ERROR: payload_json is not valid JSON: {e}"
# MAGIC
# MAGIC     pending = {"action_type": action_type, "payload": payload,
# MAGIC                "staged_at": datetime.now(timezone.utc).isoformat()}
# MAGIC     return f"{WRITE_PENDING_PREFIX}|{json.dumps(pending)}"
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def acknowledge_anomaly(anomaly_id: str, reason: str, customer_id: str) -> str:
# MAGIC     """Acknowledge a billing anomaly. ONLY call after user confirmed via request_write_confirmation.
# MAGIC     Sets billing_anomalies row as acknowledged with the given reason.
# MAGIC     """
# MAGIC     warehouse_id = config.get("warehouse_id", "")
# MAGIC     if not warehouse_id:
# MAGIC         return "ERROR: warehouse_id not configured."
# MAGIC
# MAGIC     cat, sch = config["catalog"], config["schema"]
# MAGIC     now_ts = datetime.now(timezone.utc).isoformat()
# MAGIC
# MAGIC     sql = f"""
# MAGIC         UPDATE {cat}.{sch}.billing_anomalies
# MAGIC         SET acknowledged_by = 'ai_billing_agent',
# MAGIC             acknowledged_at = TIMESTAMP '{now_ts}',
# MAGIC             acknowledgement_reason = '{_sanitize_sql_str(reason)}'
# MAGIC         WHERE CONCAT(CAST(customer_id AS STRING), '-', event_month, '-', anomaly_type) = '{anomaly_id}'
# MAGIC     """
# MAGIC     result = _execute_sql(sql=sql, warehouse_id=warehouse_id,
# MAGIC                           action_type="ACKNOWLEDGE_ANOMALY",
# MAGIC                           payload={"target_table": f"{cat}.{sch}.billing_anomalies",
# MAGIC                                    "record_id": anomaly_id, "reason": reason},
# MAGIC                           customer_id=int(customer_id) if customer_id else None,
# MAGIC                           session_id=None)
# MAGIC     if result["success"]:
# MAGIC         return f"SUCCESS: Anomaly {anomaly_id} acknowledged. Audit ID: {result['audit_id']}."
# MAGIC     return f"FAILED: {result['message']} (Audit ID: {result['audit_id']})"
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def create_billing_dispute(customer_id: str, dispute_type: str, description: str,
# MAGIC                            disputed_amount_usd: str, anomaly_id: str = "",
# MAGIC                            event_month: str = "") -> str:
# MAGIC     """Create a billing dispute. ONLY call after user confirmed.
# MAGIC     dispute_type: BILLING_ERROR, ROAMING_DISPUTE, PLAN_MISMATCH, OVERCHARGE, UNAUTHORIZED_CHARGE.
# MAGIC     """
# MAGIC     warehouse_id = config.get("warehouse_id", "")
# MAGIC     if not warehouse_id:
# MAGIC         return "ERROR: warehouse_id not configured."
# MAGIC
# MAGIC     valid = {"BILLING_ERROR", "ROAMING_DISPUTE", "PLAN_MISMATCH", "OVERCHARGE", "UNAUTHORIZED_CHARGE"}
# MAGIC     if dispute_type not in valid:
# MAGIC         return f"ERROR: Invalid dispute_type. Must be one of: {sorted(valid)}"
# MAGIC
# MAGIC     try:
# MAGIC         amount = float(disputed_amount_usd)
# MAGIC     except ValueError:
# MAGIC         return f"ERROR: disputed_amount_usd must be numeric, got '{disputed_amount_usd}'"
# MAGIC
# MAGIC     cat, sch = config["catalog"], config["schema"]
# MAGIC     dispute_id = str(uuid.uuid4())
# MAGIC     now_ts = datetime.now(timezone.utc).isoformat()
# MAGIC
# MAGIC     sql = f"""
# MAGIC         INSERT INTO {cat}.{sch}.billing_disputes
# MAGIC         (dispute_id, customer_id, anomaly_id, event_month, dispute_type, status,
# MAGIC          description, disputed_amount_usd, created_by, created_at, updated_at)
# MAGIC         VALUES ('{dispute_id}', {int(customer_id)},
# MAGIC           {f"'{anomaly_id}'" if anomaly_id else 'NULL'},
# MAGIC           {f"'{event_month}'" if event_month else 'NULL'},
# MAGIC           '{dispute_type}', 'OPEN', '{_sanitize_sql_str(description)}',
# MAGIC           {amount}, 'ai_billing_agent', TIMESTAMP '{now_ts}', TIMESTAMP '{now_ts}')
# MAGIC     """
# MAGIC     result = _execute_sql(sql=sql, warehouse_id=warehouse_id,
# MAGIC                           action_type="CREATE_DISPUTE",
# MAGIC                           payload={"target_table": f"{cat}.{sch}.billing_disputes",
# MAGIC                                    "record_id": dispute_id, "customer_id": customer_id},
# MAGIC                           customer_id=int(customer_id), session_id=None)
# MAGIC     if result["success"]:
# MAGIC         return f"SUCCESS: Dispute {dispute_id} created (OPEN). Audit ID: {result['audit_id']}."
# MAGIC     return f"FAILED: {result['message']} (Audit ID: {result['audit_id']})"
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def update_dispute_status(dispute_id: str, new_status: str, resolution_notes: str,
# MAGIC                           resolved_amount_usd: str = "") -> str:
# MAGIC     """Update dispute status. ONLY call after user confirmed.
# MAGIC     new_status: UNDER_REVIEW, RESOLVED_CREDIT, RESOLVED_NO_ACTION, ESCALATED, CLOSED.
# MAGIC     """
# MAGIC     warehouse_id = config.get("warehouse_id", "")
# MAGIC     if not warehouse_id:
# MAGIC         return "ERROR: warehouse_id not configured."
# MAGIC
# MAGIC     valid = {"UNDER_REVIEW", "RESOLVED_CREDIT", "RESOLVED_NO_ACTION", "ESCALATED", "CLOSED"}
# MAGIC     if new_status not in valid:
# MAGIC         return f"ERROR: Invalid status. Must be one of: {sorted(valid)}"
# MAGIC     if new_status == "RESOLVED_CREDIT" and not resolved_amount_usd:
# MAGIC         return "ERROR: resolved_amount_usd required for RESOLVED_CREDIT."
# MAGIC
# MAGIC     cat, sch = config["catalog"], config["schema"]
# MAGIC     now_ts = datetime.now(timezone.utc).isoformat()
# MAGIC     is_terminal = new_status in ("RESOLVED_CREDIT", "RESOLVED_NO_ACTION", "CLOSED")
# MAGIC
# MAGIC     sql = f"""
# MAGIC         UPDATE {cat}.{sch}.billing_disputes
# MAGIC         SET status = '{new_status}',
# MAGIC             resolution_notes = '{_sanitize_sql_str(resolution_notes)}',
# MAGIC             updated_at = TIMESTAMP '{now_ts}'
# MAGIC             {f", resolved_at = TIMESTAMP '{now_ts}'" if is_terminal else ""}
# MAGIC             {f", resolved_amount_usd = {float(resolved_amount_usd)}" if resolved_amount_usd else ""}
# MAGIC         WHERE dispute_id = '{dispute_id}'
# MAGIC     """
# MAGIC     result = _execute_sql(sql=sql, warehouse_id=warehouse_id,
# MAGIC                           action_type="UPDATE_DISPUTE",
# MAGIC                           payload={"target_table": f"{cat}.{sch}.billing_disputes",
# MAGIC                                    "record_id": dispute_id, "new_status": new_status},
# MAGIC                           customer_id=None, session_id=None)
# MAGIC     if result["success"]:
# MAGIC         return f"SUCCESS: Dispute {dispute_id} -> {new_status}. Audit ID: {result['audit_id']}."
# MAGIC     return f"FAILED: {result['message']} (Audit ID: {result['audit_id']})"
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def lookup_dispute_history(customer_id: str) -> str:
# MAGIC     """Look up all disputes for a customer. READ-ONLY — no confirmation needed."""
# MAGIC     warehouse_id = config.get("warehouse_id", "")
# MAGIC     if not warehouse_id:
# MAGIC         return "ERROR: warehouse_id not configured."
# MAGIC
# MAGIC     from databricks.sdk.service.sql import StatementState
# MAGIC     w = _get_write_client()
# MAGIC     resp = w.statement_execution.execute_statement(
# MAGIC         statement=f"""
# MAGIC             SELECT dispute_id, dispute_type, status, disputed_amount_usd,
# MAGIC                    resolved_amount_usd, description, created_at
# MAGIC             FROM {config['catalog']}.{config['schema']}.billing_disputes
# MAGIC             WHERE customer_id = {int(customer_id)} ORDER BY created_at DESC LIMIT 20
# MAGIC         """,
# MAGIC         warehouse_id=warehouse_id, wait_timeout="15s")
# MAGIC
# MAGIC     if resp.status.state != StatementState.SUCCEEDED:
# MAGIC         return f"ERROR: {resp.status.error}"
# MAGIC     if not resp.result or not resp.result.data_array:
# MAGIC         return f"No disputes found for customer {customer_id}."
# MAGIC
# MAGIC     lines = [f"Disputes for customer {customer_id}:"]
# MAGIC     for row in resp.result.data_array:
# MAGIC         lines.append(f"  [{row[0]}] {row[1]} | {row[2]} | ${row[3]} disputed | {row[6]}")
# MAGIC     return "\n".join(lines)
# MAGIC
# MAGIC
# MAGIC # Register write-back tools
# MAGIC tools.extend([
# MAGIC     request_write_confirmation,
# MAGIC     acknowledge_anomaly,
# MAGIC     create_billing_dispute,
# MAGIC     update_dispute_status,
# MAGIC     lookup_dispute_history,
# MAGIC ])
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Define agent logic — 3-node graph with write confirmation
# MAGIC ###############################################################################
# MAGIC
# MAGIC
# MAGIC def create_tool_calling_agent(
# MAGIC     model: LanguageModelLike,
# MAGIC     tools: Union[Sequence[BaseTool], ToolNode],
# MAGIC     system_prompt: Optional[str] = None,
# MAGIC ) -> CompiledGraph:
# MAGIC     model = model.bind_tools(tools)
# MAGIC
# MAGIC     system_message = [{"role": "system", "content": system_prompt}] if system_prompt else []
# MAGIC
# MAGIC     def call_model(state: ChatAgentState, config: RunnableConfig):
# MAGIC         messages = state["messages"]
# MAGIC         if system_message and (not messages or messages[0].get("role") != "system"):
# MAGIC             messages = system_message + messages
# MAGIC         return {"messages": [model.invoke(messages, config)]}
# MAGIC
# MAGIC     def should_continue(state: ChatAgentState) -> str:
# MAGIC         last = state["messages"][-1]
# MAGIC         return "continue" if last.get("tool_calls") else "end"
# MAGIC
# MAGIC     def route_after_tools(state: ChatAgentState) -> str:
# MAGIC         for msg in reversed(state["messages"][-3:]):
# MAGIC             content = msg.get("content", "")
# MAGIC             if isinstance(content, str) and content.startswith(WRITE_PENDING_PREFIX):
# MAGIC                 return "pending"
# MAGIC         return "agent"
# MAGIC
# MAGIC     # Capture module-level config for use in confirm_or_cancel closure
# MAGIC     config_ = config
# MAGIC
# MAGIC     def confirm_or_cancel(state: ChatAgentState, cfg: RunnableConfig):
# MAGIC         messages = state["messages"]
# MAGIC         pending = _extract_pending_write(messages)
# MAGIC
# MAGIC         if pending is None:
# MAGIC             # Routing inconsistency — WRITE_PENDING was detected by route_after_tools
# MAGIC             # but _extract_pending_write couldn't parse it. Route back to agent.
# MAGIC             return {"messages": []}
# MAGIC
# MAGIC         intent = _get_user_intent(messages)
# MAGIC
# MAGIC         if intent == "cancel":
# MAGIC             action_type = pending.get("action_type", "unknown")
# MAGIC             # Write CANCELLED audit
# MAGIC             wh = config_.get("warehouse_id", "")
# MAGIC             if wh:
# MAGIC                 try:
# MAGIC                     w_local = _get_write_client()
# MAGIC                     w_local.statement_execution.execute_statement(
# MAGIC                         statement=f"""INSERT INTO {config_['catalog']}.{config_['schema']}.billing_write_audit
# MAGIC                         (audit_id, action_type, target_table, target_record_id, customer_id,
# MAGIC                          agent_session_id, executed_by, payload_json, sql_statement,
# MAGIC                          result_status, result_message, error_detail, executed_at)
# MAGIC                         VALUES ('{str(uuid.uuid4())}', '{action_type}', 'N/A', '', NULL, NULL,
# MAGIC                         'ai_billing_agent', '{_sanitize_sql_str(json.dumps(pending.get("payload", {})))}',
# MAGIC                         NULL, 'CANCELLED', 'User cancelled the pending action.', NULL,
# MAGIC                         TIMESTAMP '{datetime.now(timezone.utc).isoformat()}')""",
# MAGIC                         warehouse_id=wh, wait_timeout="10s")
# MAGIC                 except Exception:
# MAGIC                     pass
# MAGIC             return {"messages": [{"role": "tool", "content":
# MAGIC                     f"CANCELLED: The pending '{action_type}' action was cancelled. No data was modified.",
# MAGIC                     "tool_call_id": "cancelled_write"}]}
# MAGIC
# MAGIC         if intent == "confirm":
# MAGIC             return {"messages": [{"role": "tool", "content":
# MAGIC                     f"CONFIRMED: User approved '{pending.get('action_type')}'. Proceed with execution.",
# MAGIC                     "tool_call_id": "confirmed_write"}]}
# MAGIC
# MAGIC         # intent == "unclear"
# MAGIC         return {"messages": [{"role": "tool", "content":
# MAGIC                 "AWAITING_CONFIRMATION: Reply CONFIRM to proceed or CANCEL to abort.",
# MAGIC                 "tool_call_id": "awaiting_confirmation"}]}
# MAGIC
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.add_node("tools", ChatAgentToolNode(tools))
# MAGIC     workflow.add_node("confirm_or_cancel", RunnableLambda(confirm_or_cancel))
# MAGIC
# MAGIC     workflow.set_entry_point("agent")
# MAGIC     workflow.add_conditional_edges("agent", should_continue,
# MAGIC                                   {"continue": "tools", "end": END})
# MAGIC     workflow.add_conditional_edges("tools", route_after_tools,
# MAGIC                                   {"pending": "confirm_or_cancel", "agent": "agent"})
# MAGIC     workflow.add_edge("confirm_or_cancel", "agent")
# MAGIC
# MAGIC     return workflow.compile(recursion_limit=30)
# MAGIC
# MAGIC
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     def __init__(self, agent: CompiledStateGraph):
# MAGIC         self.agent = agent
# MAGIC
# MAGIC     def predict(self, messages: list[ChatAgentMessage],
# MAGIC                 context: Optional[ChatContext] = None,
# MAGIC                 custom_inputs: Optional[dict[str, Any]] = None) -> ChatAgentResponse:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         messages = []
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 messages.extend(
# MAGIC                     ChatAgentMessage(**msg) for msg in node_data.get("messages", []))
# MAGIC         return ChatAgentResponse(messages=messages)
# MAGIC
# MAGIC     def predict_stream(self, messages: list[ChatAgentMessage],
# MAGIC                        context: Optional[ChatContext] = None,
# MAGIC                        custom_inputs: Optional[dict[str, Any]] = None
# MAGIC                        ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC             for node_data in event.values():
# MAGIC                 yield from (
# MAGIC                     ChatAgentChunk(**{"delta": msg}) for msg in node_data.get("messages", []))
# MAGIC
# MAGIC
# MAGIC agent = create_tool_calling_agent(llm, tools, system_prompt)
# MAGIC AGENT = LangGraphChatAgent(agent)
# MAGIC mlflow.models.set_model(AGENT)


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.
# MAGIC
# MAGIC Replace this placeholder input with an appropriate domain-specific example for your agent.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from agent import AGENT

AGENT.predict({"messages": [{"role": "user", "content": "Hello, how can I pay my bill?"}]})

# COMMAND ----------

from agent import AGENT

AGENT.predict({"messages": [{"role": "user", "content": "Based on my usage in the last six months and my current contract, would you recommend keeping this plan or changing to another? My customer id is 4401"}]})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Determine Databricks resources to specify for automatic auth passthrough at deployment time
# MAGIC - **TODO**: If your Unity Catalog Function queries a [vector search index](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/unstructured-retrieval-tools) or leverages [external functions](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/external-connection-tools), you need to include the dependent vector search index and UC connection objects, respectively, as resources. See [docs](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/log-agent#specify-resources-for-automatic-authentication-passthrough) for more details.
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).

# COMMAND ----------


import yaml
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from agent import tools
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
)
from pkg_resources import get_distribution
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

resources = [
    DatabricksServingEndpoint(endpoint_name=config['llm_endpoint']),
    DatabricksVectorSearchIndex(index_name=config['vector_search_index']),
]

# Add Genie Space resource for auth passthrough if configured
if config.get('genie_space_id'):
    resources.append(DatabricksGenieSpace(genie_space_id=config['genie_space_id']))

for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "Based on my usage in the last six months and my current contract, would you recommend keeping this plan or changing to another? My customer id is 4401"
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name=config['agent_name'],
        python_model="agent.py",
        model_config='config.yaml',
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            f"databricks-connect=={get_distribution('databricks-connect').version}",
            f"databricks-sdk=={get_distribution('databricks-sdk').version}",
            f"mlflow=={get_distribution('mlflow').version}",
            f"databricks-langchain=={get_distribution('databricks-langchain').version}",
            f"langgraph=={get_distribution('langgraph').version}",
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://learn.microsoft.com/azure/databricks/generative-ai/agent-evaluation/)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.

# COMMAND ----------

# TODO: Change to your FAQ table name
faq_table = (f"{config['catalog']}.{config['schema']}.billing_faq_dataset")
display(faq_table)

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Evals with AI Assistant
# Use the synthetic eval generation API to get some evals
from databricks.agents.evals import generate_evals_df

# "Ghost text" for agent description and question guidelines - feel free to modify as you see fit.
agent_description = f"""
The agent is an AI assistant that answers questions about billing. Questions unrelated to billing are irrelevant. Include questions that are irrelevant or ask for sensitive data too to the test that the agent ignores them.  
"""
question_guidelines = f"""
# User personas
- Customer of a telco provider
- Customer support agent

# Example questions
- How can I set up autopay for my bill?

# Additional Guidelines
- Questions should be succinct, and human-like
"""

docs_df = (
    spark.table(faq_table)
    .withColumnRenamed("faq", "content")  
)
pandas_docs_df = docs_df.toPandas()
pandas_docs_df["doc_uri"] = pandas_docs_df["index"].astype(str)
evals = generate_evals_df(
    docs=pandas_docs_df,  # Pass your docs. They should be in a Pandas or Spark DataFrame with columns `content STRING` and `doc_uri STRING`.
    num_evals=20,  # How many synthetic evaluations to generate
    agent_description=agent_description,
    question_guidelines=question_guidelines,
)
display(evals)

# COMMAND ----------

import mlflow
from mlflow.genai.scorers import RelevanceToQuery, Safety, RetrievalRelevance, RetrievalGroundedness

eval_results = mlflow.genai.evaluate(
    data=evals,
    predict_fn=lambda messages: AGENT.predict({"messages": messages}),
    scorers=[RelevanceToQuery(), Safety()], # add more scorers here if they're applicable
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Register UC Model with MLflow in Databricks
mlflow.set_registry_uri("databricks-uc")

# TODO: define the catalog, schema, and model name for your UC model
UC_MODEL_NAME = f"{config['catalog']}.{config['schema']}.{config['agent_name']}" 

# register the model to UC
uc_registered_model_info = mlflow.register_model(model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

# DBTITLE 1,Deploy Model to Review App and Serving Endpoint
from databricks import agents
import mlflow

# Deploy the model to the review app and a model serving endpoint
agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version)

# COMMAND ----------


