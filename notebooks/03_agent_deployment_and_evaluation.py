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
# MAGIC # Inject domain-aware context into the base system prompt
# MAGIC _base_prompt = config.get('agent_prompt', '')
# MAGIC _domain_section = config.get('domain_agent_prompt_section', '')
# MAGIC if _domain_section and _domain_section.strip() not in _base_prompt:
# MAGIC     system_prompt = _base_prompt + "\n" + _domain_section
# MAGIC else:
# MAGIC     system_prompt = _base_prompt
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Persona Configuration
# MAGIC ###############################################################################
# MAGIC import os as _os
# MAGIC import yaml as _yaml
# MAGIC from pathlib import Path as _Path
# MAGIC
# MAGIC _PERSONA_PROMPTS: dict[str, str] = {}
# MAGIC _PERSONA_TOOLS: dict[str, list[str]] = {}
# MAGIC _PERSONA_AGENTS: dict[str, CompiledGraph] = {}
# MAGIC
# MAGIC
# MAGIC def _load_personas() -> None:
# MAGIC     """Load persona configs from personas/ directory."""
# MAGIC     agent_dir = _Path(__file__).parent if "__file__" in dir() else _Path(".")
# MAGIC     personas_dir = agent_dir / "personas"
# MAGIC
# MAGIC     if not personas_dir.exists():
# MAGIC         cfg_path = config.get("persona_config_path", "")
# MAGIC         if cfg_path:
# MAGIC             personas_dir = _Path(cfg_path)
# MAGIC
# MAGIC     if not personas_dir.exists():
# MAGIC         model_path = _os.environ.get("MLFLOW_MODEL_URI", "")
# MAGIC         if model_path:
# MAGIC             personas_dir = _Path(model_path) / "artifacts" / "personas"
# MAGIC
# MAGIC     for name in ["customer_care", "finance_ops", "executive", "technical"]:
# MAGIC         yaml_path = personas_dir / f"{name}.yaml"
# MAGIC         if yaml_path.exists():
# MAGIC             try:
# MAGIC                 with open(yaml_path) as f:
# MAGIC                     p = _yaml.safe_load(f)
# MAGIC                 _PERSONA_PROMPTS[name] = p.get("system_prompt", "")
# MAGIC                 _PERSONA_TOOLS[name] = p.get("tool_policy", {}).get("allowed_tools", [])
# MAGIC             except Exception as e:
# MAGIC                 print(f"WARNING: Could not load persona {name}: {e}")
# MAGIC
# MAGIC     if not _PERSONA_PROMPTS:
# MAGIC         _PERSONA_PROMPTS["customer_care"] = system_prompt
# MAGIC
# MAGIC
# MAGIC _load_personas()
# MAGIC DEFAULT_PERSONA = config.get("default_persona", "customer_care")
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Tool Configuration
# MAGIC ###############################################################################
# MAGIC
# MAGIC # --- Unity Catalog Function Tools ---
# MAGIC _uc_tool_keys = [
# MAGIC     'tools_billing_faq', 'tools_billing', 'tools_items', 'tools_plans',
# MAGIC     'tools_customer', 'tools_anomalies', 'tools_monitoring_status',
# MAGIC     'tools_operational_kpis', 'tools_job_reliability',
# MAGIC     'tools_customer_erp_profile', 'tools_revenue_attribution',
# MAGIC     'tools_finance_ops_summary', 'tools_open_disputes', 'tools_write_audit',
# MAGIC ]
# MAGIC _uc_function_names = [config[k] for k in _uc_tool_keys if config.get(k)]
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=_uc_function_names, client=client)
# MAGIC uc_tools = uc_toolkit.tools
# MAGIC
# MAGIC # --- Vector Search Retriever ---
# MAGIC vs_tool = VectorSearchRetrieverTool(
# MAGIC     index_name=config['vector_search_index'],
# MAGIC     tool_name="faq_search",
# MAGIC     tool_description=(
# MAGIC         "Search the billing FAQ knowledge base for answers to common billing "
# MAGIC         "questions. Always try this tool FIRST before requesting customer details."
# MAGIC     ),
# MAGIC )
# MAGIC
# MAGIC # --- Genie Space (ad-hoc analytics) ---
# MAGIC _genie_space_id = config.get('genie_space_id', '')
# MAGIC _extra_tools: list[BaseTool] = []
# MAGIC
# MAGIC if _genie_space_id:
# MAGIC     @tool
# MAGIC     def ask_billing_analytics(question: str) -> str:
# MAGIC         """For ad-hoc analytical questions spanning multiple customers or requiring
# MAGIC         aggregations (trends, averages, comparisons, top-N rankings).
# MAGIC         Delegates to a Genie Space that writes SQL over the billing dataset."""
# MAGIC         try:
# MAGIC             w = WorkspaceClient()
# MAGIC             resp = w.genie.start_conversation_and_wait(
# MAGIC                 space_id=_genie_space_id, content=question
# MAGIC             )
# MAGIC             if hasattr(resp, 'attachments') and resp.attachments:
# MAGIC                 parts = []
# MAGIC                 for att in resp.attachments:
# MAGIC                     if hasattr(att, 'text') and att.text:
# MAGIC                         parts.append(
# MAGIC                             att.text.content if hasattr(att.text, 'content') else str(att.text)
# MAGIC                         )
# MAGIC                     elif hasattr(att, 'query') and att.query:
# MAGIC                         parts.append(
# MAGIC                             f"SQL: {att.query.query}\nDescription: {att.query.description}"
# MAGIC                         )
# MAGIC                 return "\n---\n".join(parts) if parts else str(resp)
# MAGIC             return str(resp)
# MAGIC         except Exception as e:
# MAGIC             return f"Analytics query could not be completed: {e}"
# MAGIC
# MAGIC     _extra_tools.append(ask_billing_analytics)
# MAGIC
# MAGIC
# MAGIC # --- In-Agent Write-Back Tools ---
# MAGIC _pending_writes: dict[str, dict] = {}
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def request_write_confirmation(
# MAGIC     action: str, target_id: str, customer_id: str, reason: str = ""
# MAGIC ) -> str:
# MAGIC     """Stage a write operation for user confirmation. MUST call BEFORE any write.
# MAGIC     action: 'acknowledge_anomaly' | 'create_dispute' | 'update_dispute_status'
# MAGIC     target_id: anomaly or dispute ID
# MAGIC     customer_id: customer ID
# MAGIC     reason: justification for the action"""
# MAGIC     token = str(uuid.uuid4())[:8]
# MAGIC     _pending_writes[token] = dict(
# MAGIC         action=action, target_id=target_id,
# MAGIC         customer_id=customer_id, reason=reason,
# MAGIC         ts=datetime.now(timezone.utc).isoformat(),
# MAGIC     )
# MAGIC     summary = f"Action: {action} | Target: {target_id} | Customer: {customer_id}"
# MAGIC     if reason:
# MAGIC         summary += f" | Reason: {reason}"
# MAGIC     return (
# MAGIC         f"Write operation staged (token: {token}).\n{summary}\n"
# MAGIC         "Please reply CONFIRM to proceed or CANCEL to abort."
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def confirm_write_operation(token: str) -> str:
# MAGIC     """Execute a previously staged write after user confirms.
# MAGIC     token: the confirmation token from request_write_confirmation"""
# MAGIC     if token not in _pending_writes:
# MAGIC         return "Invalid or expired token. Please re-stage the operation."
# MAGIC     op = _pending_writes.pop(token)
# MAGIC     return (
# MAGIC         f"CONFIRMED: {op['action']} executed for {op['target_id']} "
# MAGIC         f"(customer {op['customer_id']})."
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def cancel_write_operation(token: str) -> str:
# MAGIC     """Cancel a previously staged write operation."""
# MAGIC     removed = _pending_writes.pop(token, None)
# MAGIC     return "Operation cancelled." if removed else "No pending operation found for that token."
# MAGIC
# MAGIC
# MAGIC @tool
# MAGIC def lookup_dispute_history(customer_id: str) -> str:
# MAGIC     """Look up billing dispute history for a specific customer."""
# MAGIC     try:
# MAGIC         fn = config.get('tools_open_disputes', '')
# MAGIC         if fn:
# MAGIC             result = client.execute_function(fn, {"customer_id": int(customer_id)})
# MAGIC             return str(getattr(result, 'to_json', lambda: result)())
# MAGIC         return "Dispute lookup is not configured."
# MAGIC     except Exception as e:
# MAGIC         return f"Could not retrieve dispute history: {e}"
# MAGIC
# MAGIC
# MAGIC _extra_tools.extend([
# MAGIC     request_write_confirmation, confirm_write_operation,
# MAGIC     cancel_write_operation, lookup_dispute_history,
# MAGIC ])
# MAGIC
# MAGIC # --- Assemble full tool list (also imported by the logging cell) ---
# MAGIC tools: list[BaseTool] = uc_tools + [vs_tool] + _extra_tools
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Build the LangGraph Agent
# MAGIC ###############################################################################
# MAGIC
# MAGIC def _build_graph(
# MAGIC     model: LanguageModelLike,
# MAGIC     agent_tools: Sequence[BaseTool],
# MAGIC     prompt: str,
# MAGIC ) -> CompiledStateGraph:
# MAGIC     """Standard tool-calling ReAct loop."""
# MAGIC     bound_model = model.bind_tools(agent_tools)
# MAGIC
# MAGIC     def should_continue(state: ChatAgentState):
# MAGIC         last = state["messages"][-1]
# MAGIC         return "tools" if getattr(last, "tool_calls", None) else END
# MAGIC
# MAGIC     def call_model(state: ChatAgentState, config: RunnableConfig):
# MAGIC         msgs = state["messages"]
# MAGIC         if prompt:
# MAGIC             msgs = [{"role": "system", "content": prompt}] + msgs
# MAGIC         return {"messages": [bound_model.invoke(msgs, config)]}
# MAGIC
# MAGIC     g = StateGraph(ChatAgentState)
# MAGIC     g.add_node("agent", RunnableLambda(call_model))
# MAGIC     g.add_node("tools", ChatAgentToolNode(agent_tools))
# MAGIC     g.set_entry_point("agent")
# MAGIC     g.add_conditional_edges(
# MAGIC         "agent", should_continue, {"tools": "tools", END: END}
# MAGIC     )
# MAGIC     g.add_edge("tools", "agent")
# MAGIC     return g.compile()
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## Helpers
# MAGIC ###############################################################################
# MAGIC
# MAGIC def _get_msg_content(msg) -> str:
# MAGIC     """Extract text content from a LangChain message or dict."""
# MAGIC     if isinstance(msg, dict):
# MAGIC         return msg.get("content", "")
# MAGIC     return getattr(msg, "content", "")
# MAGIC
# MAGIC
# MAGIC ###############################################################################
# MAGIC ## ChatAgent Wrapper (exported as AGENT)
# MAGIC ###############################################################################
# MAGIC
# MAGIC class BillingChatAgent(ChatAgent):
# MAGIC     """Telco Billing Chat Agent backed by a LangGraph tool-calling loop."""
# MAGIC
# MAGIC     def __init__(self):
# MAGIC         self._graph = _build_graph(llm, tools, system_prompt)
# MAGIC
# MAGIC     @staticmethod
# MAGIC     def _to_lc_messages(messages):
# MAGIC         """Normalise list[dict | ChatAgentMessage] to list[dict]."""
# MAGIC         if not messages:
# MAGIC             return []
# MAGIC         out = []
# MAGIC         for m in messages:
# MAGIC             if isinstance(m, dict):
# MAGIC                 out.append(m)
# MAGIC             elif isinstance(m, ChatAgentMessage):
# MAGIC                 out.append({"role": m.role, "content": m.content})
# MAGIC             else:
# MAGIC                 out.append({"role": "user", "content": str(m)})
# MAGIC         return out
# MAGIC
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         lc_msgs = self._to_lc_messages(messages)
# MAGIC         result = self._graph.invoke({"messages": lc_msgs})
# MAGIC         last = result["messages"][-1]
# MAGIC         return ChatAgentResponse(
# MAGIC             messages=[ChatAgentMessage(
# MAGIC                 role="assistant",
# MAGIC                 content=_get_msg_content(last),
# MAGIC                 id=str(uuid.uuid4()),
# MAGIC             )]
# MAGIC         )
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         lc_msgs = self._to_lc_messages(messages)
# MAGIC         for event in self._graph.stream(
# MAGIC             {"messages": lc_msgs}, stream_mode="updates"
# MAGIC         ):
# MAGIC             for node_data in event.values():
# MAGIC                 for msg in node_data.get("messages", []):
# MAGIC                     text = _get_msg_content(msg)
# MAGIC                     if text:
# MAGIC                         yield ChatAgentChunk(
# MAGIC                             delta=ChatAgentMessage(
# MAGIC                                 role="assistant",
# MAGIC                                 content=text,
# MAGIC                                 id=str(uuid.uuid4()),
# MAGIC                             )
# MAGIC                         )
# MAGIC
# MAGIC
# MAGIC # ── Module-level exports ────────────────────────────────────────────────────
# MAGIC AGENT = BillingChatAgent()
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

