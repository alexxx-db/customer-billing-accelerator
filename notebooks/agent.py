from typing import Any, Generator, Optional, Sequence, Union
import logging
import time
import json
import uuid
from datetime import datetime, timezone

import mlflow
from databricks.sdk import WorkspaceClient
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

logger = logging.getLogger(__name__)

mlflow.langchain.autolog()

client = DatabricksFunctionClient()
set_uc_function_client(client)

config = ModelConfig(development_config="config.yaml").to_dict()


############################################
# Define your LLM endpoint and system prompt
############################################
MAX_AGENT_TOKENS = int(config.get('max_agent_tokens', 4096))
MAX_HISTORY_TURNS = int(config.get('max_history_turns', 20))

llm = ChatDatabricks(endpoint=config['llm_endpoint'], max_tokens=MAX_AGENT_TOKENS)

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
_PERSONA_TOOLS: dict[str, list[str] | None] = {}  # None = no policy (all tools); [] = explicitly empty
_PERSONA_AGENTS: dict[str, CompiledGraph] = {}


def _load_personas() -> None:
    """Load persona configs dynamically from personas/ directory."""
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

    # Discover all persona YAML files dynamically instead of hardcoding names
    if personas_dir.exists():
        for yaml_path in sorted(personas_dir.glob("*.yaml")):
            name = yaml_path.stem
            try:
                with open(yaml_path) as f:
                    p = _yaml.safe_load(f)
                _PERSONA_PROMPTS[name] = p.get("system_prompt", "")
                tool_policy = p.get("tool_policy")
                # None means no tool_policy section at all → all tools allowed
                # Empty list means explicitly restricted to zero tools
                _PERSONA_TOOLS[name] = tool_policy.get("allowed_tools", []) if tool_policy else None
                tool_count = len(_PERSONA_TOOLS[name]) if _PERSONA_TOOLS[name] is not None else "all"
                logger.info(f"Loaded persona: {name} ({tool_count} tools)")
            except Exception as e:
                logger.warning(f"Could not load persona {name}: {e}")

    if not _PERSONA_PROMPTS:
        _PERSONA_PROMPTS["customer_care"] = system_prompt
        logger.info("No persona files found; using default customer_care prompt")


_load_personas()
DEFAULT_PERSONA = config.get("default_persona", "customer_care")


def _tool_name(t) -> str:
    """Extract string name from a tool object."""
    if hasattr(t, "name"):
        return t.name
    if hasattr(t, "__name__"):
        return t.__name__
    if hasattr(t, "uc_function_name"):
        return t.uc_function_name.split(".")[-1]
    return str(t)

###############################################################################
## Write-Back Infrastructure
###############################################################################

WRITE_PENDING_PREFIX = "WRITE_PENDING"

# Module-level pending write state — set by request_write_confirmation, cleared after execution.
# This provides a code-level guard that write tools cannot be called without staging first.
_pending_write_state: dict | None = None

CONFIRM_PHRASES = {"confirm", "yes", "proceed", "approve", "go ahead", "do it"}
CANCEL_PHRASES  = {"cancel", "stop", "abort", "never mind", "don't"}

# Module-level WorkspaceClient for write operations (reused across calls)
_write_client = None

def _get_write_client():
    global _write_client
    if _write_client is None:
        _write_client = WorkspaceClient()
    return _write_client


def _sanitize_sql_str(val: str) -> str:
    """Sanitize a string for SQL injection prevention beyond single-quote escaping."""
    if val is None:
        return ""
    # Replace single quotes, backslashes, and null bytes
    return val.replace("\\", "\\\\").replace("'", "''").replace("\x00", "")


def _execute_sql(sql: str, warehouse_id: str, action_type: str,
                 payload: dict, customer_id: int | None,
                 session_id: str | None) -> dict:
    """Execute a SQL write via Statement Execution API with audit-first pattern.
    Writes two audit records (PENDING before, then SUCCESS/FAILED after) — pure append."""
    from databricks.sdk.service.sql import StatementState

    w = _get_write_client()
    audit_id = str(uuid.uuid4())
    executed_at = datetime.now(timezone.utc).isoformat()
    cat = config['catalog']
    sch = config['schema']

    # Audit record 1: PENDING (before business write)
    audit_sql = f"""
    INSERT INTO {cat}.{sch}.billing_write_audit
    (audit_id, action_type, target_table, target_record_id, customer_id,
     agent_session_id, executed_by, payload_json, sql_statement,
     result_status, result_message, error_detail, executed_at)
    VALUES (
      '{audit_id}', '{action_type}',
      '{payload.get("target_table", "unknown")}',
      '{payload.get("record_id", "")}',
      {customer_id if customer_id is not None else 'NULL'},
      {f"'{session_id}'" if session_id else 'NULL'},
      'ai_billing_agent',
      '{_sanitize_sql_str(json.dumps(payload))}',
      '{_sanitize_sql_str(sql)}',
      'PENDING', 'Audit pre-written before business SQL.', NULL,
      TIMESTAMP '{executed_at}'
    )
    """
    try:
        resp = w.statement_execution.execute_statement(
            statement=audit_sql, warehouse_id=warehouse_id, wait_timeout="15s")
        if resp.status.state not in (StatementState.SUCCEEDED, StatementState.RUNNING):
            return {"success": False, "audit_id": audit_id,
                    "message": f"Write aborted: audit creation failed ({resp.status.state})."}
    except Exception as e:
        return {"success": False, "audit_id": audit_id,
                "message": f"Write aborted: audit creation failed ({e})."}

    # Execute the business SQL
    try:
        resp = w.statement_execution.execute_statement(
            statement=sql, warehouse_id=warehouse_id, wait_timeout="30s")
        if resp.status.state == StatementState.SUCCEEDED:
            # Audit record 2: SUCCESS
            w.statement_execution.execute_statement(
                statement=f"""INSERT INTO {cat}.{sch}.billing_write_audit
                (audit_id, action_type, target_table, target_record_id, customer_id,
                 agent_session_id, executed_by, payload_json, sql_statement,
                 result_status, result_message, error_detail, executed_at)
                VALUES ('{str(uuid.uuid4())}', '{action_type}',
                '{payload.get("target_table", "")}', '{payload.get("record_id", "")}',
                {customer_id if customer_id is not None else 'NULL'},
                {f"'{session_id}'" if session_id else 'NULL'},
                'ai_billing_agent', NULL, NULL, 'SUCCESS',
                'Write completed successfully.', NULL,
                TIMESTAMP '{datetime.now(timezone.utc).isoformat()}')""",
                warehouse_id=warehouse_id, wait_timeout="10s")
            return {"success": True, "message": "Write completed successfully.", "audit_id": audit_id}
        else:
            error = str(resp.status.error) if resp.status.error else "Unknown error"
            w.statement_execution.execute_statement(
                statement=f"""INSERT INTO {cat}.{sch}.billing_write_audit
                (audit_id, action_type, target_table, target_record_id, customer_id,
                 agent_session_id, executed_by, payload_json, sql_statement,
                 result_status, result_message, error_detail, executed_at)
                VALUES ('{str(uuid.uuid4())}', '{action_type}',
                '{payload.get("target_table", "")}', '{payload.get("record_id", "")}',
                {customer_id if customer_id is not None else 'NULL'},
                {f"'{session_id}'" if session_id else 'NULL'},
                'ai_billing_agent', NULL, NULL, 'FAILED',
                'Business SQL failed.', '{_sanitize_sql_str(error[:500])}',
                TIMESTAMP '{datetime.now(timezone.utc).isoformat()}')""",
                warehouse_id=warehouse_id, wait_timeout="10s")
            return {"success": False, "message": f"Write failed: {error}", "audit_id": audit_id}
    except Exception as e:
        return {"success": False, "message": f"Write exception: {e}", "audit_id": audit_id}


def _extract_pending_write(messages: list[dict]) -> dict | None:
    """Search full message history (newest-first) for the most recent WRITE_PENDING sentinel."""
    for msg in reversed(messages):
        content = msg.get("content", "")
        if isinstance(content, str) and content.startswith(WRITE_PENDING_PREFIX):
            try:
                return json.loads(content[len(WRITE_PENDING_PREFIX) + 1:])
            except Exception:
                pass
    return None


def _get_user_intent(messages: list[dict]) -> str:
    """Returns 'confirm', 'cancel', or 'unclear' based on the most recent user message.
    Cancel takes priority over confirm if both match (safety-first)."""
    for msg in reversed(messages[-2:]):
        if msg.get("role") == "user":
            text = (msg.get("content", "") or "").lower().strip()
            has_cancel = any(phrase in text for phrase in CANCEL_PHRASES)
            has_confirm = any(phrase in text for phrase in CONFIRM_PHRASES)
            if has_cancel:
                return "cancel"
            if has_confirm:
                return "confirm"
            return "unclear"
    return "unclear"


###############################################################################
## Define tools
###############################################################################
catalog = config['catalog']
schema = config['schema']

tools = []

# UC function tools (read-only)
uc_tool_names = [
    config['tools_billing_faq'],
    config['tools_billing'],
    config['tools_items'],
    config['tools_plans'],
    config['tools_customer'],
    config['tools_anomalies'],
    config['tools_monitoring_status'],
    config['tools_operational_kpis'],
    config['tools_job_reliability'],
    config['tools_customer_erp_profile'],
    config['tools_revenue_attribution'],
    config['tools_finance_ops_summary'],
    config['tools_open_disputes'],
    config['tools_write_audit'],
    ]
uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
tools.extend(uc_toolkit.tools)

###############################################################################
## Genie Space tool
###############################################################################
genie_space_id = config.get('genie_space_id', '')

if genie_space_id:
    _genie_client = WorkspaceClient()

    @tool
    def ask_billing_analytics(question: str) -> str:
        """Ask an ad-hoc billing analytics question using natural language.
        Use this tool for complex analytical questions that span multiple customers
        or require aggregations across the billing dataset, such as:
        - Revenue and charge trends over time
        - Plan comparisons and averages
        - Top-N customer rankings
        - Customer segmentation by charges or plan type
        - Month-over-month or period-over-period analysis

        Do NOT use this for individual customer lookups — use the dedicated
        lookup_customer, lookup_billing, or lookup_billing_items tools instead.
        """
        try:
            response = _genie_client.genie.start_conversation(
                space_id=genie_space_id, content=question)
        except Exception as e:
            logger.error(f"Genie Space conversation start failed: {e}")
            return "The analytics service is temporarily unavailable. Please try again later."

        conversation_id = response.conversation_id
        message_id = response.message_id

        max_attempts = 30
        base_sleep = 1.0  # seconds; increases with exponential backoff
        result = None
        for attempt in range(max_attempts):
            try:
                result = _genie_client.genie.get_message(
                    space_id=genie_space_id,
                    conversation_id=conversation_id, message_id=message_id)
                if hasattr(result, 'status') and result.status in ("COMPLETED", "FAILED"):
                    break
            except Exception as e:
                logger.warning(f"Genie Space poll attempt {attempt + 1} failed: {e}")
                if attempt >= max_attempts - 1:
                    return "The analytics service encountered an error. Please try again later."
            # Exponential backoff: 1s, 1.5s, 2.25s, ... capped at 5s
            sleep_time = min(base_sleep * (1.5 ** attempt), 5.0)
            time.sleep(sleep_time)

        if result is None or not hasattr(result, 'status'):
            return "The analytics query timed out. Please try a simpler question."
        if result.status == "FAILED":
            return "The analytics query could not be completed. Please try rephrasing your question."
        if result.status != "COMPLETED":
            return "The analytics query timed out. Please try a simpler question."

        if hasattr(result, 'attachments') and result.attachments:
            parts = []
            for att in result.attachments:
                if hasattr(att, 'text') and att.text:
                    parts.append(att.text.content)
                if hasattr(att, 'query') and att.query:
                    if att.query.description:
                        parts.append(att.query.description)
                    if att.query.query:
                        parts.append(f"SQL: {att.query.query}")
            if parts:
                return "\n\n".join(parts)

        return "No results found for your analytics question."

    tools.append(ask_billing_analytics)

###############################################################################
## Write-back tools
###############################################################################

@tool
def request_write_confirmation(action_type: str, payload_json: str,
                               human_readable_summary: str) -> str:
    """Stage a write action for human confirmation. Call this BEFORE any write.
    action_type: acknowledge_anomaly, create_billing_dispute, update_dispute_status.
    payload_json: JSON string with all fields for the action.
    human_readable_summary: what you will show the user.
    Returns a WRITE_PENDING sentinel. Then ask the user to reply CONFIRM or CANCEL.
    """
    global _pending_write_state
    try:
        payload = json.loads(payload_json)
    except Exception as e:
        return f"ERROR: payload_json is not valid JSON: {e}"

    pending = {"action_type": action_type, "payload": payload,
               "staged_at": datetime.now(timezone.utc).isoformat()}
    _pending_write_state = pending
    return f"{WRITE_PENDING_PREFIX}|{json.dumps(pending)}"


@tool
def acknowledge_anomaly(anomaly_id: str, reason: str, customer_id: str) -> str:
    """Acknowledge a billing anomaly. ONLY call after user confirmed via request_write_confirmation.
    Sets billing_anomalies row as acknowledged with the given reason.
    """
    global _pending_write_state
    if _pending_write_state is None or _pending_write_state.get("action_type") != "acknowledge_anomaly":
        return ("BLOCKED: This action requires confirmation. "
                "Call request_write_confirmation with action_type='acknowledge_anomaly' first.")

    warehouse_id = config.get("warehouse_id", "")
    if not warehouse_id:
        return "ERROR: warehouse_id not configured."

    try:
        cust_id_int = int(customer_id) if customer_id else None
    except ValueError:
        return f"ERROR: customer_id must be numeric, got '{customer_id}'"

    cat, sch = config["catalog"], config["schema"]
    now_ts = datetime.now(timezone.utc).isoformat()

    sql = f"""
        UPDATE {cat}.{sch}.billing_anomalies
        SET acknowledged_by = 'ai_billing_agent',
            acknowledged_at = TIMESTAMP '{now_ts}',
            acknowledgement_reason = '{_sanitize_sql_str(reason)}'
        WHERE anomaly_id = '{_sanitize_sql_str(anomaly_id)}'
    """
    result = _execute_sql(sql=sql, warehouse_id=warehouse_id,
                          action_type="ACKNOWLEDGE_ANOMALY",
                          payload={"target_table": f"{cat}.{sch}.billing_anomalies",
                                   "record_id": anomaly_id, "reason": reason},
                          customer_id=cust_id_int,
                          session_id=None)
    _pending_write_state = None  # Clear after execution
    if result["success"]:
        return f"SUCCESS: Anomaly {anomaly_id} acknowledged. Audit ID: {result['audit_id']}."
    return f"FAILED: {result['message']} (Audit ID: {result['audit_id']})"


@tool
def create_billing_dispute(customer_id: str, dispute_type: str, description: str,
                           disputed_amount_usd: str, anomaly_id: str = "",
                           event_month: str = "") -> str:
    """Create a billing dispute. ONLY call after user confirmed.
    dispute_type: BILLING_ERROR, ROAMING_DISPUTE, PLAN_MISMATCH, OVERCHARGE, UNAUTHORIZED_CHARGE.
    """
    global _pending_write_state
    if _pending_write_state is None or _pending_write_state.get("action_type") != "create_billing_dispute":
        return ("BLOCKED: This action requires confirmation. "
                "Call request_write_confirmation with action_type='create_billing_dispute' first.")

    warehouse_id = config.get("warehouse_id", "")
    if not warehouse_id:
        return "ERROR: warehouse_id not configured."

    valid = {"BILLING_ERROR", "ROAMING_DISPUTE", "PLAN_MISMATCH", "OVERCHARGE", "UNAUTHORIZED_CHARGE"}
    if dispute_type not in valid:
        return f"ERROR: Invalid dispute_type. Must be one of: {sorted(valid)}"

    try:
        amount = float(disputed_amount_usd)
    except ValueError:
        return f"ERROR: disputed_amount_usd must be numeric, got '{disputed_amount_usd}'"

    cat, sch = config["catalog"], config["schema"]
    dispute_id = str(uuid.uuid4())
    now_ts = datetime.now(timezone.utc).isoformat()

    sql = f"""
        INSERT INTO {cat}.{sch}.billing_disputes
        (dispute_id, customer_id, anomaly_id, event_month, dispute_type, status,
         description, disputed_amount_usd, created_by, created_at, updated_at)
        VALUES ('{dispute_id}', {int(customer_id)},
          {f"'{anomaly_id}'" if anomaly_id else 'NULL'},
          {f"'{event_month}'" if event_month else 'NULL'},
          '{dispute_type}', 'OPEN', '{_sanitize_sql_str(description)}',
          {amount}, 'ai_billing_agent', TIMESTAMP '{now_ts}', TIMESTAMP '{now_ts}')
    """
    result = _execute_sql(sql=sql, warehouse_id=warehouse_id,
                          action_type="CREATE_DISPUTE",
                          payload={"target_table": f"{cat}.{sch}.billing_disputes",
                                   "record_id": dispute_id, "customer_id": customer_id},
                          customer_id=int(customer_id), session_id=None)
    _pending_write_state = None  # Clear after execution
    if result["success"]:
        return f"SUCCESS: Dispute {dispute_id} created (OPEN). Audit ID: {result['audit_id']}."
    return f"FAILED: {result['message']} (Audit ID: {result['audit_id']})"


@tool
def update_dispute_status(dispute_id: str, new_status: str, resolution_notes: str,
                          resolved_amount_usd: str = "") -> str:
    """Update dispute status. ONLY call after user confirmed.
    new_status: UNDER_REVIEW, RESOLVED_CREDIT, RESOLVED_NO_ACTION, ESCALATED, CLOSED.
    """
    global _pending_write_state
    if _pending_write_state is None or _pending_write_state.get("action_type") != "update_dispute_status":
        return ("BLOCKED: This action requires confirmation. "
                "Call request_write_confirmation with action_type='update_dispute_status' first.")

    warehouse_id = config.get("warehouse_id", "")
    if not warehouse_id:
        return "ERROR: warehouse_id not configured."

    valid = {"UNDER_REVIEW", "RESOLVED_CREDIT", "RESOLVED_NO_ACTION", "ESCALATED", "CLOSED"}
    if new_status not in valid:
        return f"ERROR: Invalid status. Must be one of: {sorted(valid)}"
    if new_status == "RESOLVED_CREDIT" and not resolved_amount_usd:
        return "ERROR: resolved_amount_usd required for RESOLVED_CREDIT."

    cat, sch = config["catalog"], config["schema"]
    now_ts = datetime.now(timezone.utc).isoformat()
    is_terminal = new_status in ("RESOLVED_CREDIT", "RESOLVED_NO_ACTION", "CLOSED")

    sql = f"""
        UPDATE {cat}.{sch}.billing_disputes
        SET status = '{new_status}',
            resolution_notes = '{_sanitize_sql_str(resolution_notes)}',
            updated_at = TIMESTAMP '{now_ts}'
            {f", resolved_at = TIMESTAMP '{now_ts}'" if is_terminal else ""}
            {f", resolved_amount_usd = {float(resolved_amount_usd)}" if resolved_amount_usd else ""}
        WHERE dispute_id = '{_sanitize_sql_str(dispute_id)}'
    """
    result = _execute_sql(sql=sql, warehouse_id=warehouse_id,
                          action_type="UPDATE_DISPUTE",
                          payload={"target_table": f"{cat}.{sch}.billing_disputes",
                                   "record_id": dispute_id, "new_status": new_status},
                          customer_id=None, session_id=None)
    _pending_write_state = None  # Clear after execution
    if result["success"]:
        return f"SUCCESS: Dispute {dispute_id} -> {new_status}. Audit ID: {result['audit_id']}."
    return f"FAILED: {result['message']} (Audit ID: {result['audit_id']})"


@tool
def lookup_dispute_history(customer_id: str) -> str:
    """Look up all disputes for a customer. READ-ONLY — no confirmation needed."""
    warehouse_id = config.get("warehouse_id", "")
    if not warehouse_id:
        return "ERROR: warehouse_id not configured."

    try:
        cust_id_int = int(customer_id)
    except (ValueError, TypeError):
        return f"ERROR: customer_id must be numeric, got '{customer_id}'"

    from databricks.sdk.service.sql import StatementState
    w = _get_write_client()
    resp = w.statement_execution.execute_statement(
        statement=f"""
            SELECT dispute_id, dispute_type, status, disputed_amount_usd,
                   resolved_amount_usd, description, created_at
            FROM {config['catalog']}.{config['schema']}.billing_disputes
            WHERE customer_id = {cust_id_int} ORDER BY created_at DESC LIMIT 20
        """,
        warehouse_id=warehouse_id, wait_timeout="15s")

    if resp.status.state != StatementState.SUCCEEDED:
        return f"ERROR: {resp.status.error}"
    if not resp.result or not resp.result.data_array:
        return f"No disputes found for customer {customer_id}."

    lines = [f"Disputes for customer {customer_id}:"]
    for row in resp.result.data_array:
        lines.append(f"  [{row[0]}] {row[1]} | {row[2]} | ${row[3]} disputed | {row[6]}")
    return "\n".join(lines)


# Register write-back tools
tools.extend([
    request_write_confirmation,
    acknowledge_anomaly,
    create_billing_dispute,
    update_dispute_status,
    lookup_dispute_history,
])

###############################################################################
## Define agent logic — 3-node graph with write confirmation
###############################################################################


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[Sequence[BaseTool], ToolNode],
    system_prompt: Optional[str] = None,
) -> CompiledGraph:
    model = model.bind_tools(tools)

    system_message = [{"role": "system", "content": system_prompt}] if system_prompt else []

    def call_model(state: ChatAgentState, config: RunnableConfig):
        messages = state["messages"]
        if system_message and (not messages or messages[0].get("role") != "system"):
            messages = system_message + messages
        return {"messages": [model.invoke(messages, config)]}

    def should_continue(state: ChatAgentState) -> str:
        last = state["messages"][-1]
        return "continue" if last.get("tool_calls") else "end"

    def route_after_tools(state: ChatAgentState) -> str:
        for msg in reversed(state["messages"][-3:]):
            content = msg.get("content", "")
            if isinstance(content, str) and content.startswith(WRITE_PENDING_PREFIX):
                return "pending"
        return "agent"

    # Capture module-level config for use in confirm_or_cancel closure
    config_ = config

    def confirm_or_cancel(state: ChatAgentState, cfg: RunnableConfig):
        global _pending_write_state
        messages = state["messages"]
        pending = _extract_pending_write(messages)

        if pending is None:
            # Routing inconsistency — WRITE_PENDING was detected by route_after_tools
            # but _extract_pending_write couldn't parse it. Route back to agent.
            return {"messages": []}

        intent = _get_user_intent(messages)

        if intent == "cancel":
            _pending_write_state = None  # Clear pending state on cancel
            action_type = pending.get("action_type", "unknown")
            # Write CANCELLED audit
            wh = config_.get("warehouse_id", "")
            if wh:
                try:
                    w_local = _get_write_client()
                    w_local.statement_execution.execute_statement(
                        statement=f"""INSERT INTO {config_['catalog']}.{config_['schema']}.billing_write_audit
                        (audit_id, action_type, target_table, target_record_id, customer_id,
                         agent_session_id, executed_by, payload_json, sql_statement,
                         result_status, result_message, error_detail, executed_at)
                        VALUES ('{str(uuid.uuid4())}', '{action_type}', 'N/A', '', NULL, NULL,
                        'ai_billing_agent', '{_sanitize_sql_str(json.dumps(pending.get("payload", {})))}',
                        NULL, 'CANCELLED', 'User cancelled the pending action.', NULL,
                        TIMESTAMP '{datetime.now(timezone.utc).isoformat()}')""",
                        warehouse_id=wh, wait_timeout="10s")
                except Exception:
                    pass
            return {"messages": [{"role": "tool", "content":
                    f"CANCELLED: The pending '{action_type}' action was cancelled. No data was modified.",
                    "tool_call_id": "cancelled_write"}]}

        if intent == "confirm":
            return {"messages": [{"role": "tool", "content":
                    f"CONFIRMED: User approved '{pending.get('action_type')}'. Proceed with execution.",
                    "tool_call_id": "confirmed_write"}]}

        # intent == "unclear"
        return {"messages": [{"role": "tool", "content":
                "AWAITING_CONFIRMATION: Reply CONFIRM to proceed or CANCEL to abort.",
                "tool_call_id": "awaiting_confirmation"}]}

    workflow = StateGraph(ChatAgentState)

    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ChatAgentToolNode(tools))
    workflow.add_node("confirm_or_cancel", RunnableLambda(confirm_or_cancel))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges("agent", should_continue,
                                  {"continue": "tools", "end": END})
    workflow.add_conditional_edges("tools", route_after_tools,
                                  {"pending": "confirm_or_cancel", "agent": "agent"})
    workflow.add_edge("confirm_or_cancel", "agent")

    return workflow.compile(recursion_limit=30)


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    def _get_persona_agent(self, persona_name: str) -> CompiledGraph:
        """Get or build a cached agent for the given persona."""
        if persona_name not in _PERSONA_AGENTS:
            active_prompt = _PERSONA_PROMPTS.get(persona_name, system_prompt)
            allowed = _PERSONA_TOOLS.get(persona_name)
            # None means persona had no tool_policy → use all tools (backwards compat)
            # Empty list means explicitly no tools allowed → filter to empty
            if allowed is None:
                active_tools = tools
            else:
                active_tools = [t for t in tools if _tool_name(t) in allowed]
            if not active_tools:
                logger.warning(f"Persona '{persona_name}' has 0 active tools")
            _PERSONA_AGENTS[persona_name] = create_tool_calling_agent(
                llm, active_tools, active_prompt)
        return _PERSONA_AGENTS[persona_name]

    @staticmethod
    def _trim_history(messages: list[dict], max_turns: int) -> list[dict]:
        """Keep system messages + first N_INITIAL non-system messages (initial tool call
        context) + last max_turns*2 non-system messages to bound context size while
        preserving the account context established early in the conversation."""
        N_INITIAL = 6  # Preserve first 3 tool call pairs (lookup_customer, lookup_billing, etc.)
        if max_turns <= 0 or len(messages) <= max_turns * 2 + 1:
            return messages
        system_msgs = [m for m in messages if m.get("role") == "system"]
        non_system = [m for m in messages if m.get("role") != "system"]
        tail_budget = max_turns * 2
        if len(non_system) <= N_INITIAL + tail_budget:
            return messages
        initial = non_system[:N_INITIAL]
        tail = non_system[-tail_budget:]
        return system_msgs + initial + tail

    def predict(self, messages: list[ChatAgentMessage],
                context: Optional[ChatContext] = None,
                custom_inputs: Optional[dict[str, Any]] = None) -> ChatAgentResponse:
        custom_inputs = custom_inputs or {}
        persona_name = custom_inputs.get("persona", DEFAULT_PERSONA)
        if persona_name not in _PERSONA_PROMPTS:
            logger.warning(f"Unknown persona '{persona_name}', falling back to '{DEFAULT_PERSONA}'")
            persona_name = DEFAULT_PERSONA

        persona_agent = self._get_persona_agent(persona_name)

        raw_messages = self._convert_messages_to_dict(messages)
        trimmed = self._trim_history(raw_messages, MAX_HISTORY_TURNS)
        request = {"messages": trimmed}
        out_messages = []
        for event in persona_agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                out_messages.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", []))
        return ChatAgentResponse(messages=out_messages)

    def predict_stream(self, messages: list[ChatAgentMessage],
                       context: Optional[ChatContext] = None,
                       custom_inputs: Optional[dict[str, Any]] = None
                       ) -> Generator[ChatAgentChunk, None, None]:
        custom_inputs = custom_inputs or {}
        persona_name = custom_inputs.get("persona", DEFAULT_PERSONA)
        if persona_name not in _PERSONA_PROMPTS:
            logger.warning(f"Unknown persona '{persona_name}', falling back to '{DEFAULT_PERSONA}'")
            persona_name = DEFAULT_PERSONA

        persona_agent = self._get_persona_agent(persona_name)

        raw_messages = self._convert_messages_to_dict(messages)
        trimmed = self._trim_history(raw_messages, MAX_HISTORY_TURNS)
        request = {"messages": trimmed}
        for event in persona_agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                yield from (
                    ChatAgentChunk(**{"delta": msg}) for msg in node_data.get("messages", []))


# Build the default persona agent (not the full 19-tool base agent).
# The LangGraphChatAgent always routes through _get_persona_agent() which filters
# tools per persona. Constructing a base agent with all tools is unnecessary waste.
_default_agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphChatAgent(_default_agent)
mlflow.models.set_model(AGENT)
