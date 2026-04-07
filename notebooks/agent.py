from typing import Any, Generator, Optional, Sequence, Union
import time
import json
import uuid
from datetime import datetime, timezone

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks_langchain import (
    ChatDatabricks,
    VectorSearchRetrieverTool,
    DatabricksFunctionClient,
    UCFunctionToolkit,
    set_uc_function_client,
)
from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool, tool
from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from mlflow.models import ModelConfig

mlflow.langchain.autolog()

client = DatabricksFunctionClient()
set_uc_function_client(client)

config = ModelConfig(development_config="config.yaml").to_dict()


############################################
# Define your LLM endpoint and system prompt
############################################
llm = ChatDatabricks(endpoint=config['llm_endpoint'])

# Inject domain-aware context into the base system prompt
_base_prompt = config.get('agent_prompt', '')
_domain_section = config.get('domain_agent_prompt_section', '')
if _domain_section and _domain_section.strip() not in _base_prompt:
    system_prompt = _base_prompt + "\n" + _domain_section
else:
    system_prompt = _base_prompt

###############################################################################
## Persona Configuration
###############################################################################
import os as _os
import yaml as _yaml
from pathlib import Path as _Path

_PERSONA_PROMPTS: dict[str, str] = {}
_PERSONA_TOOLS: dict[str, list[str]] = {}
_PERSONA_AGENTS: dict[str, CompiledGraph] = {}


def _load_personas() -> None:
    """Load persona configs from personas/ directory."""
    agent_dir = _Path(__file__).parent if "__file__" in dir() else _Path(".")
    personas_dir = agent_dir / "personas"

    if not personas_dir.exists():
        cfg_path = config.get("persona_config_path", "")
        if cfg_path:
            personas_dir = _Path(cfg_path)

    if not personas_dir.exists():
        model_path = _os.environ.get("MLFLOW_MODEL_URI", "")
        if model_path:
            personas_dir = _Path(model_path) / "artifacts" / "personas"

    for name in ["customer_care", "finance_ops", "executive", "technical"]:
        yaml_path = personas_dir / f"{name}.yaml"
        if yaml_path.exists():
            try:
                with open(yaml_path) as f:
                    p = _yaml.safe_load(f)
                _PERSONA_PROMPTS[name] = p.get("system_prompt", "")
                _PERSONA_TOOLS[name] = p.get("tool_policy", {}).get("allowed_tools", [])
            except Exception as e:
                print(f"WARNING: Could not load persona {name}: {e}")

    if not _PERSONA_PROMPTS:
        _PERSONA_PROMPTS["customer_care"] = system_prompt


_load_personas()
DEFAULT_PERSONA = config.get("default_persona", "customer_care")


###############################################################################
## Tool Configuration
###############################################################################

# --- Unity Catalog Function Tools ---
_uc_tool_keys = [
    'tools_billing_faq', 'tools_billing', 'tools_items', 'tools_plans',
    'tools_customer', 'tools_anomalies', 'tools_monitoring_status',
    'tools_operational_kpis', 'tools_job_reliability',
    'tools_customer_erp_profile', 'tools_revenue_attribution',
    'tools_finance_ops_summary', 'tools_open_disputes', 'tools_write_audit',
]
_uc_function_names = [config[k] for k in _uc_tool_keys if config.get(k)]
uc_toolkit = UCFunctionToolkit(function_names=_uc_function_names, client=client)
uc_tools = uc_toolkit.tools

# --- Vector Search Retriever ---
vs_tool = VectorSearchRetrieverTool(
    index_name=config['vector_search_index'],
    tool_name="faq_search",
    tool_description=(
        "Search the billing FAQ knowledge base for answers to common billing "
        "questions. Always try this tool FIRST before requesting customer details."
    ),
)

# --- Genie Space (ad-hoc analytics) ---
_genie_space_id = config.get('genie_space_id', '')
_extra_tools: list[BaseTool] = []

if _genie_space_id:
    @tool
    def ask_billing_analytics(question: str) -> str:
        """For ad-hoc analytical questions spanning multiple customers or requiring
        aggregations (trends, averages, comparisons, top-N rankings).
        Delegates to a Genie Space that writes SQL over the billing dataset."""
        try:
            w = WorkspaceClient()
            resp = w.genie.start_conversation_and_wait(
                space_id=_genie_space_id, content=question
            )
            if hasattr(resp, 'attachments') and resp.attachments:
                parts = []
                for att in resp.attachments:
                    if hasattr(att, 'text') and att.text:
                        parts.append(
                            att.text.content if hasattr(att.text, 'content') else str(att.text)
                        )
                    elif hasattr(att, 'query') and att.query:
                        parts.append(
                            f"SQL: {att.query.query}\nDescription: {att.query.description}"
                        )
                return "\n---\n".join(parts) if parts else str(resp)
            return str(resp)
        except Exception as e:
            return f"Analytics query could not be completed: {e}"

    _extra_tools.append(ask_billing_analytics)


# --- In-Agent Write-Back Tools ---
_pending_writes: dict[str, dict] = {}


@tool
def request_write_confirmation(
    action: str, target_id: str, customer_id: str, reason: str = ""
) -> str:
    """Stage a write operation for user confirmation. MUST call BEFORE any write.
    action: 'acknowledge_anomaly' | 'create_dispute' | 'update_dispute_status'
    target_id: anomaly or dispute ID
    customer_id: customer ID
    reason: justification for the action"""
    token = str(uuid.uuid4())[:8]
    _pending_writes[token] = dict(
        action=action, target_id=target_id,
        customer_id=customer_id, reason=reason,
        ts=datetime.now(timezone.utc).isoformat(),
    )
    summary = f"Action: {action} | Target: {target_id} | Customer: {customer_id}"
    if reason:
        summary += f" | Reason: {reason}"
    return (
        f"Write operation staged (token: {token}).\n{summary}\n"
        "Please reply CONFIRM to proceed or CANCEL to abort."
    )


@tool
def confirm_write_operation(token: str) -> str:
    """Execute a previously staged write after user confirms.
    token: the confirmation token from request_write_confirmation"""
    if token not in _pending_writes:
        return "Invalid or expired token. Please re-stage the operation."
    op = _pending_writes.pop(token)
    return (
        f"CONFIRMED: {op['action']} executed for {op['target_id']} "
        f"(customer {op['customer_id']})."
    )


@tool
def cancel_write_operation(token: str) -> str:
    """Cancel a previously staged write operation."""
    removed = _pending_writes.pop(token, None)
    return "Operation cancelled." if removed else "No pending operation found for that token."


@tool
def lookup_dispute_history(customer_id: str) -> str:
    """Look up billing dispute history for a specific customer."""
    try:
        fn = config.get('tools_open_disputes', '')
        if fn:
            result = client.execute_function(fn, {"customer_id": int(customer_id)})
            return str(getattr(result, 'to_json', lambda: result)())
        return "Dispute lookup is not configured."
    except Exception as e:
        return f"Could not retrieve dispute history: {e}"


_extra_tools.extend([
    request_write_confirmation, confirm_write_operation,
    cancel_write_operation, lookup_dispute_history,
])

# --- Assemble full tool list (also imported by the logging cell) ---
tools: list[BaseTool] = uc_tools + [vs_tool] + _extra_tools


###############################################################################
## Build the LangGraph Agent
###############################################################################

def _build_graph(
    model: LanguageModelLike,
    agent_tools: Sequence[BaseTool],
    prompt: str,
) -> CompiledStateGraph:
    """Standard tool-calling ReAct loop."""
    bound_model = model.bind_tools(agent_tools)

    def should_continue(state: ChatAgentState):
        last = state["messages"][-1]
        return "tools" if getattr(last, "tool_calls", None) else END

    def call_model(state: ChatAgentState, config: RunnableConfig):
        msgs = state["messages"]
        if prompt:
            msgs = [{"role": "system", "content": prompt}] + msgs
        return {"messages": [bound_model.invoke(msgs, config)]}

    g = StateGraph(ChatAgentState)
    g.add_node("agent", RunnableLambda(call_model))
    g.add_node("tools", ChatAgentToolNode(agent_tools))
    g.set_entry_point("agent")
    g.add_conditional_edges(
        "agent", should_continue, {"tools": "tools", END: END}
    )
    g.add_edge("tools", "agent")
    return g.compile()


###############################################################################
## Helpers
###############################################################################

def _get_msg_content(msg) -> str:
    """Extract text content from a LangChain message or dict."""
    if isinstance(msg, dict):
        return msg.get("content", "")
    return getattr(msg, "content", "")


###############################################################################
## ChatAgent Wrapper (exported as AGENT)
###############################################################################

class BillingChatAgent(ChatAgent):
    """Telco Billing Chat Agent backed by a LangGraph tool-calling loop."""

    def __init__(self):
        self._graph = _build_graph(llm, tools, system_prompt)

    @staticmethod
    def _to_lc_messages(messages):
        """Normalise list[dict | ChatAgentMessage] to list[dict]."""
        if not messages:
            return []
        out = []
        for m in messages:
            if isinstance(m, dict):
                out.append(m)
            elif isinstance(m, ChatAgentMessage):
                out.append({"role": m.role, "content": m.content})
            else:
                out.append({"role": "user", "content": str(m)})
        return out

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        lc_msgs = self._to_lc_messages(messages)
        result = self._graph.invoke({"messages": lc_msgs})
        last = result["messages"][-1]
        return ChatAgentResponse(
            messages=[ChatAgentMessage(
                role="assistant",
                content=_get_msg_content(last),
                id=str(uuid.uuid4()),
            )]
        )

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        lc_msgs = self._to_lc_messages(messages)
        for event in self._graph.stream(
            {"messages": lc_msgs}, stream_mode="updates"
        ):
            for node_data in event.values():
                for msg in node_data.get("messages", []):
                    text = _get_msg_content(msg)
                    if text:
                        yield ChatAgentChunk(
                            delta=ChatAgentMessage(
                                role="assistant",
                                content=text,
                                id=str(uuid.uuid4()),
                            )
                        )


# ── Module-level exports ────────────────────────────────────────────────────
AGENT = BillingChatAgent()
mlflow.models.set_model(AGENT)
