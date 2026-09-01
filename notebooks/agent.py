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

# --- Write action registry ---
try:
    from write_actions import (
        get_action as wa_get_action,
        action_permitted as wa_action_permitted,
        build_param_bag as wa_build_param_bag,
        render as wa_render,
        PARAM_TYPES as wa_PARAM_TYPES,
    )
    _WRITE_ACTIONS_AVAILABLE = True
except ImportError:
    _WRITE_ACTIONS_AVAILABLE = False
    logger.error("write_actions not found — all write operations are disabled")

# --- Per-turn tool scoping ---
try:
    from tool_domains import scope_tools as _scope_tools
    _TOOL_SCOPING_AVAILABLE = True
except ImportError:
    _TOOL_SCOPING_AVAILABLE = False
    logger.warning("tool_domains not found — every turn sees the full persona toolset")
    # Provide stub types so except clauses don't crash
    class IdentityError(Exception): pass
    class AuthorizationError(Exception): pass
    RequestContext = None  # type: ignore
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
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

# Shared WorkspaceClient — thread-safe, reused across tools (M2 fix)
_ws_client = WorkspaceClient()

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
_PERSONA_WRITE_ACCESS: dict[str, str] = {}


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
                _PERSONA_WRITE_ACCESS[name] = (
                    p.get("tool_policy", {}).get("write_access", "none")
                )
            except Exception as e:
                print(f"WARNING: Could not load persona {name}: {e}")

    # config.yaml is always packaged with the model; personas/ may not be. Fill any
    # gaps from the mirrored persona block so write-access enforcement survives
    # deployment even when the directory is absent.
    for _name, _p in (config.get("personas") or {}).items():
        _PERSONA_TOOLS.setdefault(_name, _p.get("allowed_tools", []))
        _PERSONA_WRITE_ACCESS.setdefault(_name, _p.get("write_access", "none"))

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
    'tools_customer_360', 'tools_customer_hierarchy',
    'tools_pricing_drift', 'tools_pricing_history',
    'tools_product_pricing', 'tools_customer_contract',
    'tools_overage', 'tools_usage_history', 'tools_plan_entitlement',
    'tools_inventory_assets', 'tools_usage_forecast', 'tools_plan_upgrade',
    'tools_order_reconcile', 'tools_revenue_leakage', 'tools_dso_region', 'tools_order_lines',
    'tools_pos_gap', 'tools_store_compare', 'tools_store_profile', 'tools_store_hierarchy',
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
            resp = _ws_client.genie.start_conversation_and_wait(
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
import contextvars as _contextvars

_pending_writes: dict[str, dict] = {}
_pending_writes_lock = _threading.Lock()
_TOKEN_TTL_SECONDS = 600  # 10-minute expiry for unconfirmed tokens

# Per-request identity context using contextvars.
# Unlike threading.local, ContextVar values are:
#   - isolated per-task in async frameworks
#   - copyable via copy_context(), so a generator snapshot stays consistent
#     even if the parent thread moves on to a new request
#   - never leak across requests on thread-pool reuse (each .set() is scoped)
_request_context_var: _contextvars.ContextVar[Optional["RequestContext"]] = (
    _contextvars.ContextVar("_request_context_var", default=None)
)


def _set_request_context(ctx) -> None:
    """Store the validated RequestContext for the current request."""
    _request_context_var.set(ctx)


def _get_request_context():
    """Retrieve the current request's RequestContext, or None."""
    return _request_context_var.get()


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
    action: str, target_id: str, customer_id: str, reason: str = "",
    extra: str = "{}",
) -> str:
    """Stage a write operation for user confirmation. MUST call BEFORE any write.
    action: 'acknowledge_anomaly' | 'create_dispute' | 'update_dispute_status'
            | 'submit_pricing_dispute' | 'apply_pricing_correction'
    target_id: anomaly or dispute ID
    customer_id: customer ID
    reason: justification for the action; for update_dispute_status this is the new status
    extra: JSON object of additional values the action needs, e.g.
           '{"event_month": "2026-07", "corrected_amount": 199.0}' for
           apply_pricing_correction. Keys the action does not declare are ignored."""
    token = str(uuid.uuid4())[:8]
    try:
        extra_values = json.loads(extra) if extra else {}
        if not isinstance(extra_values, dict):
            return "ERROR: `extra` must be a JSON object, e.g. {\"event_month\": \"2026-07\"}."
    except (ValueError, TypeError) as e:
        return f"ERROR: `extra` is not valid JSON: {e}"

    with _pending_writes_lock:
        _cleanup_expired_tokens()
        _pending_writes[token] = dict(
            action=action, target_id=target_id,
            customer_id=customer_id, reason=reason,
            extra=extra_values,
            ts=datetime.now(timezone.utc).isoformat(),
        )
    summary = f"Action: {action} | Target: {target_id} | Customer: {customer_id}"
    if reason:
        summary += f" | Reason: {reason}"
    if extra_values:
        summary += " | " + " | ".join(f"{k}: {v}" for k, v in sorted(extra_values.items()))
    return (
        f"Write operation staged (token: {token}).\n{summary}\n"
        "Please reply CONFIRM to proceed or CANCEL to abort."
    )


@tool
def confirm_write_operation(token: str) -> str:
    """Execute a previously staged write after user confirms.
    token: the confirmation token from request_write_confirmation"""
    # Check identity BEFORE popping token (avoids TOCTOU race on re-insert)
    ctx = _get_request_context()
    with _pending_writes_lock:
        if token not in _pending_writes:
            return "BLOCKED: Invalid or expired token. Stage the operation again with request_write_confirmation."
        if ctx is None:
            # Don't pop — leave token in store for retry after authentication
            logger.warning("Write blocked: no user identity context")
            return "BLOCKED: Authenticated user context required for write operations."
        op = _pending_writes.pop(token)

    return _execute_write(
        op,
        initiating_user=ctx.user_email,
        executing_principal="billing-agent-sp",
        session_id=ctx.session_id,
        request_id=ctx.request_id,
        persona=ctx.persona,
        user_groups=json.dumps(ctx.user_groups),
    )


def _sanitize_sql_value(val: str) -> str:
    """Escape a string for safe interpolation into SQL single-quoted literals."""
    return str(val).replace("'", "''")


def _validate_identifier(val: str, label: str) -> str:
    """Validate that a value looks like a safe identifier (alphanumeric + hyphens/underscores).
    Raises ValueError if it contains suspicious characters."""
    import re
    cleaned = str(val).strip()
    if not re.match(r'^[a-zA-Z0-9_\-\.]+$', cleaned):
        raise ValueError(f"Invalid {label}: contains disallowed characters: {cleaned!r}")
    return cleaned


# --- Audit record shape -----------------------------------------------------
# Parameter names are prefixed a_ so an audit column can never collide with a
# write action's own marker inside the shared atomic block.
_AUDIT_COLUMNS = [
    "audit_id", "action_type", "target_table", "target_record_id", "customer_id",
    "agent_session_id", "executed_by", "payload_json", "result_status",
    "result_message", "executed_at", "initiating_user", "executing_principal",
    "persona", "request_id", "identity_degraded", "user_groups",
]
_AUDIT_TYPES = {
    "a_audit_id": "STRING", "a_action_type": "STRING", "a_target_table": "STRING",
    "a_target_record_id": "STRING", "a_customer_id": "BIGINT",
    "a_agent_session_id": "STRING", "a_executed_by": "STRING",
    "a_sql_statement": "STRING", "a_result_status": "STRING",
    "a_result_message": "STRING", "a_executed_at": "TIMESTAMP",
    "a_initiating_user": "STRING", "a_executing_principal": "STRING",
    "a_persona": "STRING", "a_request_id": "STRING",
    "a_identity_degraded": "BOOLEAN", "a_user_groups": "STRING",
    "a_payload_json": "STRING",
}


def _audit_insert_sql(catalog_name: str, schema_name: str, with_sql: bool) -> str:
    cols = list(_AUDIT_COLUMNS)
    if with_sql:
        cols.insert(8, "sql_statement")  # immediately after payload_json
    markers = ", ".join(f":a_{c}" for c in cols)
    return (
        f"INSERT INTO {catalog_name}.{schema_name}.billing_write_audit "
        f"({', '.join(cols)}) VALUES ({markers})"
    )


def _as_sql_params(bag: dict, types: dict) -> list:
    """Convert a value bag into typed Statement Execution API parameters."""
    items = []
    for name, value in bag.items():
        sql_type = types.get(name, "STRING")
        if value is None:
            rendered = None
        elif sql_type == "BOOLEAN":
            rendered = "true" if value else "false"
        else:
            rendered = str(value)
        items.append(
            StatementParameterListItem(name=name, value=rendered, type=sql_type)
        )
    return items


def _run(statement: str, bag: dict, types: dict, mode: str, warehouse_id: str,
         timeout: str = "30s"):
    """Send one statement, binding values per the configured parameter mode."""
    if mode == "parameters":
        return _ws_client.statement_execution.execute_statement(
            statement=statement,
            parameters=_as_sql_params(bag, types),
            warehouse_id=warehouse_id,
            wait_timeout=timeout,
        )
    return _ws_client.statement_execution.execute_statement(
        statement=wa_render(statement, bag, mode, types),
        warehouse_id=warehouse_id,
        wait_timeout=timeout,
    )


def _audit_denial(
    *,
    catalog_name: str,
    schema_name: str,
    warehouse_id: str,
    param_mode: str,
    action_name: str,
    target_table: str,
    op: dict,
    persona: str,
    message: str,
    initiating_user: str,
    executing_principal: str,
    session_id: str,
    request_id: str,
    user_groups: str,
) -> None:
    """Record a write that was refused before any SQL ran.

    A refused attempt is exactly what an audit log should capture — it is the
    security-relevant event, not the successful writes. Best effort by design:
    if this insert fails the caller still returns the refusal to the user, so a
    logging problem can never turn a denial into a crash or, worse, into a pass.
    """
    if not warehouse_id or not _WRITE_ACTIONS_AVAILABLE:
        return

    # The attempt was refused, so nothing here has been validated. Coerce
    # defensively rather than raising.
    try:
        customer_id = int(op.get("customer_id"))
    except (TypeError, ValueError):
        customer_id = None
    target_id = str(op.get("target_id", ""))[:255]

    values = {
        "a_audit_id": str(uuid.uuid4()),
        "a_action_type": str(action_name)[:255],
        "a_target_table": target_table,
        "a_target_record_id": target_id,
        "a_customer_id": customer_id,
        "a_agent_session_id": session_id,
        "a_executed_by": "agent",
        "a_payload_json": json.dumps(
            {k: v for k, v in op.items() if k != "ts"}, default=str
        )[:8000],
        "a_result_status": "BLOCKED",
        "a_result_message": message[:1000],
        "a_executed_at": datetime.now(timezone.utc).isoformat(),
        "a_initiating_user": initiating_user,
        "a_executing_principal": executing_principal,
        "a_persona": persona,
        "a_request_id": request_id,
        "a_identity_degraded": initiating_user == "UNKNOWN",
        "a_user_groups": user_groups,
    }
    try:
        _run(
            _audit_insert_sql(catalog_name, schema_name, with_sql=False),
            values, _AUDIT_TYPES, param_mode, warehouse_id, timeout="10s",
        )
    except Exception as e:
        logger.error(f"Could not record BLOCKED audit for {action_name}: {e}")


def _execute_write(
    op: dict,
    initiating_user: str = "UNKNOWN",
    executing_principal: str = "billing-agent-sp",
    session_id: str = "",
    request_id: str = "",
    persona: str = "",
    user_groups: str = "[]",
) -> str:
    """Execute a registered write action atomically and record it in the audit log.

    Sequence:
      1. Resolve the action in the registry           — unknown action rejected
      2. Check persona write access                   — declared level is enforced
      3. Insert audit PENDING and verify it landed    — outside the transaction, so
                                                        intent survives a rollback
      4. BEGIN ATOMIC: business statements + audit SUCCESS
      5. On failure: audit FAILED

    Every PENDING row therefore resolves to exactly one outcome.
    """
    catalog_name = config.get("catalog", "")
    schema_name = config.get("schema", config.get("database", ""))
    warehouse_id = config.get("warehouse_id", "")
    param_mode = config.get("writeback_param_mode", "parameters")

    if not warehouse_id:
        return "ERROR: warehouse_id not configured. Cannot execute write operations."
    if not _WRITE_ACTIONS_AVAILABLE:
        return "ERROR: Write action registry unavailable. Writes are disabled."

    # --- 1. Resolve the action (closed allowlist — the LLM never composes SQL) ---
    action_name = op.get("action", "")
    action = wa_get_action(action_name)
    persona_name = persona or DEFAULT_PERSONA
    _denial_context = dict(
        catalog_name=catalog_name, schema_name=schema_name,
        warehouse_id=warehouse_id, param_mode=param_mode, op=op,
        persona=persona_name, initiating_user=initiating_user,
        executing_principal=executing_principal, session_id=session_id,
        request_id=request_id, user_groups=user_groups,
    )

    if action is None:
        message = f"Unknown action '{action_name}' — not in the write registry."
        _audit_denial(action_name=action_name, target_table="UNKNOWN",
                      message=message, **_denial_context)
        return f"ERROR: Unknown action '{_sanitize_sql_value(action_name)}'."

    # --- 2. Persona write-access gate ---
    level = _PERSONA_WRITE_ACCESS.get(persona_name, "none")
    if not wa_action_permitted(level, action):
        message = (
            f"Persona '{persona_name}' has write access '{level}', which does not "
            f"permit '{action.action}' (requires '{action.min_write_access}')."
        )
        logger.warning(f"Write blocked by persona policy: {message}")
        _audit_denial(
            action_name=action.action,
            target_table=",".join(
                f"{catalog_name}.{schema_name}.{t}" for t in action.target_tables
            ),
            message=message, **_denial_context,
        )
        return (
            f"BLOCKED: The {persona_name} persona has write access '{level}', which "
            f"does not permit '{action.action}' (requires '{action.min_write_access}')."
        )

    # --- Input validation ---
    try:
        customer_id_int = int(op["customer_id"])
    except (ValueError, TypeError):
        return f"ERROR: Invalid customer_id '{op.get('customer_id')}'. Must be numeric."

    try:
        target_id = _validate_identifier(op["target_id"], "target_id")
    except ValueError as e:
        return f"ERROR: {e}"

    now_ts = datetime.now(timezone.utc).isoformat()
    audit_id = str(uuid.uuid4())
    target_table = ",".join(
        f"{catalog_name}.{schema_name}.{t}" for t in action.target_tables
    )
    identity_degraded = initiating_user == "UNKNOWN"

    try:
        # Action-specific values arrive as `extra`. build_param_bag keeps only the
        # markers this action declares and type-checks numerics, so an unexpected
        # or malformed key cannot reach a statement.
        bag = wa_build_param_bag(
            action,
            actor="agent",
            now=now_ts,
            target_id=target_id,
            customer_id=customer_id_int,
            reason=op.get("reason", ""),
            **{k: v for k, v in (op.get("extra") or {}).items()
               if k not in ("actor", "now", "target_id", "customer_id", "reason")},
        )
    except KeyError as e:
        return (
            f"ERROR: {e}. Supply the missing values via the `extra` argument of "
            f"request_write_confirmation."
        )
    except ValueError as e:
        return f"ERROR: {e}"

    # For an action that creates a record, audit it under the new record's own id.
    audit_record_id = (
        str(bag[action.audit_record_param])
        if action.audit_record_param else target_id
    )

    def _audit_values(status, message, executed_at, sql_text=None):
        # Both rows of the two-INSERT pattern share one audit_id, so a PENDING
        # row can always be matched to its SUCCESS or FAILED resolution.
        values = {
            "a_audit_id": audit_id,
            "a_payload_json": json.dumps(bag, default=str),
            "a_action_type": action.action,
            "a_target_table": target_table,
            "a_target_record_id": audit_record_id,
            "a_customer_id": customer_id_int,
            "a_agent_session_id": session_id,
            "a_executed_by": "agent",
            "a_result_status": status,
            "a_result_message": message,
            "a_executed_at": executed_at,
            "a_initiating_user": initiating_user,
            "a_executing_principal": executing_principal,
            "a_persona": persona_name,
            "a_request_id": request_id,
            "a_identity_degraded": identity_degraded,
            "a_user_groups": user_groups,
        }
        if sql_text is not None:
            values["a_sql_statement"] = sql_text
        return values

    # --- 3. Audit PENDING, outside the transaction ---
    try:
        pending_resp = _run(
            _audit_insert_sql(catalog_name, schema_name, with_sql=False),
            _audit_values("PENDING", "Staged by confirm_write_operation", now_ts),
            _AUDIT_TYPES, param_mode, warehouse_id, timeout="10s",
        )
    except Exception as e:
        logger.error(f"Audit PENDING insert raised — aborting write for {target_id}: {e}")
        return "ERROR: Could not record audit trail. Write aborted for safety."

    if pending_resp.status and pending_resp.status.state != StatementState.SUCCEEDED:
        logger.error(f"Audit PENDING insert failed — aborting write for {target_id}")
        return "ERROR: Could not record audit trail. Write aborted for safety."

    # --- 4. Business statements and audit resolution, atomically ---
    business_sql = [
        wa_render(s.format(catalog=catalog_name, schema=schema_name),
                  bag, param_mode, wa_PARAM_TYPES)
        for s in action.statements
    ]
    # In 'parameters' mode sql_statement records the statement *template*; the
    # values live in the typed audit columns beside it. In 'literals' mode the
    # rendered statement is recorded, matching pre-Phase-0 behaviour.
    audit_success = _audit_values(
        "SUCCESS",
        f"{action.action} completed for {target_id} (customer {customer_id_int}).",
        now_ts, sql_text="; ".join(business_sql),
    )
    atomic_bag = {**bag, **audit_success}
    atomic_types = {**wa_PARAM_TYPES, **_AUDIT_TYPES}

    body = ";\n  ".join(
        business_sql
        + [wa_render(_audit_insert_sql(catalog_name, schema_name, with_sql=True),
                     audit_success, param_mode, _AUDIT_TYPES)]
    )
    atomic_stmt = f"BEGIN ATOMIC\n  {body};\nEND"

    try:
        resp = _run(atomic_stmt, atomic_bag, atomic_types, param_mode, warehouse_id)
        if resp.status and resp.status.state == StatementState.SUCCEEDED:
            return (
                f"{action.action} completed for {target_id} "
                f"(customer {customer_id_int}). Audit id {audit_id}."
            )
        error_detail = (
            resp.status.error.message
            if resp.status and resp.status.error else "Unknown error"
        )
        result_msg = f"{action.action} failed for {target_id}: {error_detail}"
    except Exception as e:
        result_msg = f"{action.action} failed for {target_id}: {e}"

    # --- 5. Nothing was written. Resolve the PENDING row to FAILED. ---
    logger.warning(f"Write transaction rolled back: {result_msg}")
    try:
        _run(
            _audit_insert_sql(catalog_name, schema_name, with_sql=True),
            _audit_values(
                "FAILED", result_msg,
                datetime.now(timezone.utc).isoformat(),
                sql_text="; ".join(business_sql),
            ),
            _AUDIT_TYPES, param_mode, warehouse_id, timeout="10s",
        )
    except Exception as e:
        logger.error(f"Could not record FAILED audit for {target_id}: {e}")

    return result_msg


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
    return g.compile(recursion_limit=config.get("recursion_limit", 30))


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
        # Keyed on (persona, frozenset(tool names)) — see _get_graph.
        self._persona_graphs: dict[tuple, CompiledStateGraph] = {}

    def _get_graph(
        self,
        custom_inputs: Optional[dict[str, Any]] = None,
        messages: Optional[list] = None,
    ) -> CompiledStateGraph:
        """Return a compiled graph scoped to this persona and this turn's topic.

        Persona filters by who is asking; the intent router filters by what they
        asked about. The router is permissive — when it cannot classify a turn it
        returns the full persona set, so scoping can narrow but never blank out.

        Graphs are cached on (persona, tool set). The number of distinct tool sets
        is bounded by the domain combinations that actually occur, which in
        practice is a handful.
        """
        persona = (custom_inputs or {}).get("persona", DEFAULT_PERSONA)
        persona_tools = _filter_tools_for_persona(persona)

        matched: set = set()
        if _TOOL_SCOPING_AVAILABLE and messages:
            available = {t.name for t in persona_tools}
            scoped_names, matched = _scope_tools(messages, available)
            if matched:
                persona_tools = [t for t in persona_tools if t.name in scoped_names]

        key = (persona, frozenset(t.name for t in persona_tools))
        if key not in self._persona_graphs:
            persona_prompt = _PERSONA_PROMPTS.get(persona, system_prompt)
            self._persona_graphs[key] = _build_graph(llm, persona_tools, persona_prompt)
            if matched:
                logger.info(
                    f"Tool scope: persona={persona} domains={sorted(matched)} "
                    f"tools={len(persona_tools)}"
                )
        return self._persona_graphs[key]

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
        graph = self._get_graph(custom_inputs, lc_msgs)
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

        # Snapshot the context so each generator step runs with the correct
        # identity even if the thread is reused for a new request before
        # this generator is fully consumed. Regular generators inherit
        # the caller's context on each next(), so we must wrap each step
        # in snapshot.run() to maintain isolation.
        snapshot = _contextvars.copy_context()
        inner = self._do_stream(self._to_lc_messages(messages), custom_inputs)
        try:
            while True:
                try:
                    yield snapshot.run(next, inner)
                except StopIteration:
                    return
        except GeneratorExit:
            inner.close()

    def _do_stream(
        self,
        lc_msgs: list[dict],
        custom_inputs: Optional[dict[str, Any]],
    ) -> Generator[ChatAgentChunk, None, None]:
        """Inner generator — each step is run inside a context snapshot by predict_stream."""
        graph = self._get_graph(custom_inputs, lc_msgs)
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


# ── Module-level exports ────────────────────────────────────────────────────
AGENT = BillingChatAgent()
mlflow.models.set_model(AGENT)
