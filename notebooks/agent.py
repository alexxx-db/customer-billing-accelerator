from typing import Any, Generator, Optional, Sequence, Union
import time
import json
import uuid
import logging
from datetime import datetime, timezone

import mlflow

logger = logging.getLogger(__name__)

# --- EchoStar Identity Propagation ---
try:
    from identity_utils import (
        RequestContext, validate_request_context, check_tool_authorization,
        require_user_context, resolve_asset_policy, get_identity_secret,
        validate_persona_for_user, IdentityError, AuthorizationError,
    )
    _IDENTITY_AVAILABLE = True
except ImportError:
    _IDENTITY_AVAILABLE = False
    logger.warning("identity_utils not found — identity propagation disabled")
    # Provide stub types so except clauses don't crash
    class IdentityError(Exception): pass
    class AuthorizationError(Exception): pass
    RequestContext = None  # type: ignore
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
# Thread-safe per-token store. Tokens are 8-char UUIDs, so collisions across
# concurrent sessions are negligible. TTL cleanup runs on each staging call
# to prevent unbounded growth from abandoned tokens.
import threading as _threading

_pending_writes: dict[str, dict] = {}
_pending_writes_lock = _threading.Lock()
_TOKEN_TTL_SECONDS = 600  # 10-minute expiry for unconfirmed tokens

# Thread-local storage for the current request's identity context.
# Set in predict(), read by tools during the same request.
_request_context_local = _threading.local()


def _set_request_context(ctx: Optional[RequestContext]) -> None:
    """Store the validated RequestContext for the current request thread."""
    _request_context_local.ctx = ctx


def _get_request_context() -> Optional[RequestContext]:
    """Retrieve the current request's RequestContext, or None."""
    return getattr(_request_context_local, "ctx", None)


def _cleanup_expired_tokens() -> None:
    now = datetime.now(timezone.utc)
    expired = [
        k for k, v in _pending_writes.items()
        if (now - datetime.fromisoformat(v["ts"])).total_seconds() > _TOKEN_TTL_SECONDS
    ]
    for k in expired:
        _pending_writes.pop(k, None)


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
    with _pending_writes_lock:
        _cleanup_expired_tokens()
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
    # HARD GUARD 1: require valid pending-write token
    with _pending_writes_lock:
        if token not in _pending_writes:
            return "BLOCKED: Invalid or expired token. Stage the operation again with request_write_confirmation."
        op = _pending_writes.pop(token)

    # HARD GUARD 2: user identity context required for writes
    ctx = _get_request_context()
    if ctx is None:
        # Return token to store — don't consume without executing
        with _pending_writes_lock:
            _pending_writes[token] = op
        logger.warning("Write blocked: no user identity context")
        return "BLOCKED: Authenticated user context required for write operations."

    return _execute_write(
        op,
        initiating_user=ctx.user_email,
        executing_principal="billing-agent-sp",
        session_id=ctx.session_id,
        request_id=ctx.request_id,
        persona=ctx.persona,
        user_groups=json.dumps(ctx.user_groups),
    )


def _execute_write(
    op: dict,
    initiating_user: str = "UNKNOWN",
    executing_principal: str = "billing-agent-sp",
    session_id: str = "",
    request_id: str = "",
    persona: str = "",
    user_groups: str = "[]",
) -> str:
    """Execute a write operation via Statement Execution API and log to audit."""
    catalog_name = config.get("catalog", "")
    schema_name = config.get("schema", config.get("database", ""))
    warehouse_id = config.get("warehouse_id", "")

    if not warehouse_id:
        return "ERROR: warehouse_id not configured. Cannot execute write operations."

    action = op["action"]
    target_id = op["target_id"]
    customer_id = op["customer_id"]
    reason = op.get("reason", "")
    now_ts = datetime.now(timezone.utc).isoformat()
    audit_id = str(uuid.uuid4())

    # Build the SQL for the business write
    if action == "acknowledge_anomaly":
        sql = (
            f"UPDATE {catalog_name}.{schema_name}.billing_anomalies "
            f"SET acknowledged_by = 'agent', "
            f"acknowledged_at = TIMESTAMP '{now_ts}', "
            f"acknowledgement_reason = '{reason.replace(chr(39), chr(39)+chr(39))}' "
            f"WHERE anomaly_id = '{target_id}'"
        )
    elif action == "create_dispute":
        dispute_id = f"DSP-{str(uuid.uuid4())[:8]}"
        escaped_reason = reason.replace("'", "''")
        sql = (
            f"INSERT INTO {catalog_name}.{schema_name}.billing_disputes "
            f"(dispute_id, customer_id, dispute_type, status, description, "
            f"created_by, created_at, updated_at) VALUES "
            f"('{dispute_id}', {customer_id}, 'AGENT_CREATED', 'OPEN', "
            f"'{escaped_reason}', 'agent', "
            f"TIMESTAMP '{now_ts}', TIMESTAMP '{now_ts}')"
        )
    elif action == "update_dispute_status":
        escaped_reason = reason.replace("'", "''")
        sql = (
            f"UPDATE {catalog_name}.{schema_name}.billing_disputes "
            f"SET status = '{escaped_reason}', "
            f"updated_at = TIMESTAMP '{now_ts}' "
            f"WHERE dispute_id = '{target_id}'"
        )
    else:
        return f"ERROR: Unknown action '{action}'."

    w = WorkspaceClient()

    identity_degraded = initiating_user == "UNKNOWN"

    # Audit record: PENDING
    w.statement_execution.execute_statement(
        statement=(
            f"INSERT INTO {catalog_name}.{schema_name}.billing_write_audit "
            f"(audit_id, action_type, target_table, target_record_id, customer_id, "
            f"agent_session_id, executed_by, result_status, result_message, executed_at, "
            f"initiating_user, executing_principal, persona, request_id, "
            f"identity_degraded, user_groups) VALUES "
            f"('{audit_id}', '{action}', "
            f"'{catalog_name}.{schema_name}.billing_disputes', "
            f"'{target_id}', {customer_id}, '{session_id}', 'agent', 'PENDING', "
            f"'Staged by confirm_write_operation', TIMESTAMP '{now_ts}', "
            f"'{initiating_user}', '{executing_principal}', '{persona}', "
            f"'{request_id}', {str(identity_degraded).lower()}, "
            f"'{user_groups.replace(chr(39), chr(39)+chr(39))}')"
        ),
        warehouse_id=warehouse_id,
        wait_timeout="10s",
    )

    # Execute the business write
    try:
        resp = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=warehouse_id,
            wait_timeout="30s",
        )
        if resp.status.state == StatementState.SUCCEEDED:
            result_status = "SUCCESS"
            result_msg = f"{action} completed for {target_id} (customer {customer_id})."
        else:
            result_status = "FAILED"
            error_detail = resp.status.error.message if resp.status.error else "Unknown error"
            result_msg = f"{action} failed for {target_id}: {error_detail}"
    except Exception as e:
        result_status = "FAILED"
        result_msg = f"{action} failed for {target_id}: {e}"

    # Audit record: result
    w.statement_execution.execute_statement(
        statement=(
            f"INSERT INTO {catalog_name}.{schema_name}.billing_write_audit "
            f"(audit_id, action_type, target_table, target_record_id, customer_id, "
            f"agent_session_id, executed_by, sql_statement, result_status, result_message, "
            f"executed_at, initiating_user, executing_principal, persona, request_id, "
            f"identity_degraded, user_groups) VALUES "
            f"('{str(uuid.uuid4())}', '{action}', "
            f"'{catalog_name}.{schema_name}.billing_disputes', "
            f"'{target_id}', {customer_id}, '{session_id}', 'agent', "
            f"'{sql.replace(chr(39), chr(39)+chr(39))}', "
            f"'{result_status}', '{result_msg.replace(chr(39), chr(39)+chr(39))}', "
            f"TIMESTAMP '{datetime.now(timezone.utc).isoformat()}', "
            f"'{initiating_user}', '{executing_principal}', '{persona}', "
            f"'{request_id}', {str(identity_degraded).lower()}, "
            f"'{user_groups.replace(chr(39), chr(39)+chr(39))}')"
        ),
        warehouse_id=warehouse_id,
        wait_timeout="10s",
    )

    prefix = "CONFIRMED" if result_status == "SUCCESS" else "FAILED"
    return f"{prefix}: {result_msg}"


@tool
def cancel_write_operation(token: str) -> str:
    """Cancel a previously staged write operation."""
    with _pending_writes_lock:
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

def _filter_tools_for_persona(persona: str) -> list[BaseTool]:
    """Return the subset of tools allowed for the given persona.

    If a RequestContext is present, validates that the user's groups
    authorize the requested persona. Falls back to customer_care on denial.
    """
    # Persona-group validation (if identity propagation is active)
    if _IDENTITY_AVAILABLE:
        ctx = _get_request_context()
        if ctx:
            persona_groups = config.get("persona_group_map", {})
            if persona_groups and not validate_persona_for_user(
                persona, ctx.user_groups, persona_groups
            ):
                logger.warning(
                    f"Persona denied: user={ctx.user_email} "
                    f"groups={ctx.user_groups} persona={persona}"
                )
                persona = DEFAULT_PERSONA

    allowed = _PERSONA_TOOLS.get(persona)
    if not allowed:
        return tools
    tool_name_set = set(allowed)
    filtered = [t for t in tools if t.name in tool_name_set]
    return filtered if filtered else tools


class BillingChatAgent(ChatAgent):
    """Telco Billing Chat Agent backed by a LangGraph tool-calling loop."""

    def __init__(self):
        # No default graph with all tools.
        # Persona-specific graphs are built on first use.
        self._persona_graphs: dict[str, CompiledStateGraph] = {}

    def _get_graph(self, custom_inputs: Optional[dict[str, Any]] = None) -> CompiledStateGraph:
        """Return a compiled graph for the requested persona."""
        persona = (custom_inputs or {}).get("persona", DEFAULT_PERSONA)
        if persona not in self._persona_graphs:
            persona_tools = _filter_tools_for_persona(persona)
            persona_prompt = _PERSONA_PROMPTS.get(persona, system_prompt)
            self._persona_graphs[persona] = _build_graph(llm, persona_tools, persona_prompt)
        return self._persona_graphs[persona]

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
        # --- Identity propagation ---
        ctx = None
        if _IDENTITY_AVAILABLE:
            try:
                raw_ctx = (custom_inputs or {}).get("request_context")
                if raw_ctx:
                    secret = get_identity_secret()
                    ctx = validate_request_context(raw_ctx, secret)
                    logger.info(
                        f"Identity validated: user={ctx.user_email} "
                        f"persona={ctx.persona} request_id={ctx.request_id}"
                    )
            except IdentityError as e:
                logger.warning(f"Identity validation failed: {e}")
            except Exception as e:
                logger.error(f"Identity error: {e}", exc_info=True)
        _set_request_context(ctx)

        lc_msgs = self._to_lc_messages(messages)
        graph = self._get_graph(custom_inputs)
        try:
            result = graph.invoke({"messages": lc_msgs})
            last = result["messages"][-1]
            content = _get_msg_content(last)
        except AuthorizationError as e:
            content = str(e)
            logger.warning(f"Authorization denied: {e}")
        except Exception as e:
            content = (
                "I'm sorry, I encountered an error processing your request. "
                "Please try again or rephrase your question."
            )
            logger.error(f"Error in predict: {e}", exc_info=True)
        finally:
            _set_request_context(None)  # Clean up thread-local

        return ChatAgentResponse(
            messages=[ChatAgentMessage(
                role="assistant",
                content=content,
                id=str(uuid.uuid4()),
            )]
        )

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        # --- Identity propagation ---
        ctx = None
        if _IDENTITY_AVAILABLE:
            try:
                raw_ctx = (custom_inputs or {}).get("request_context")
                if raw_ctx:
                    secret = get_identity_secret()
                    ctx = validate_request_context(raw_ctx, secret)
                    logger.info(
                        f"Identity validated: user={ctx.user_email} "
                        f"persona={ctx.persona} request_id={ctx.request_id}"
                    )
            except IdentityError as e:
                logger.warning(f"Identity validation failed: {e}")
            except Exception as e:
                logger.error(f"Identity error: {e}", exc_info=True)
        _set_request_context(ctx)

        lc_msgs = self._to_lc_messages(messages)
        graph = self._get_graph(custom_inputs)
        try:
            for event in graph.stream(
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
        except AuthorizationError as e:
            logger.warning(f"Authorization denied in stream: {e}")
            yield ChatAgentChunk(
                delta=ChatAgentMessage(
                    role="assistant", content=str(e), id=str(uuid.uuid4()),
                )
            )
        except Exception as e:
            logger.error(f"Error in predict_stream: {e}", exc_info=True)
            yield ChatAgentChunk(
                delta=ChatAgentMessage(
                    role="assistant",
                    content=(
                        "I'm sorry, I encountered an error processing your request. "
                        "Please try again or rephrase your question."
                    ),
                    id=str(uuid.uuid4()),
                )
            )
        finally:
            _set_request_context(None)  # Clean up thread-local


# ── Module-level exports ────────────────────────────────────────────────────
AGENT = BillingChatAgent()
mlflow.models.set_model(AGENT)
