"""Declarative registry of agent write actions.

Every write the agent can perform is a row in ``WRITE_ACTIONS``. The executor in
``agent.py`` resolves an action here, checks the caller's persona write-access
level against the action's declared minimum, binds values as typed parameters,
and runs the action's statements inside a single ``BEGIN ATOMIC`` block together
with the audit resolution row.

Adding a write to a new demo means adding a ``WriteAction`` here — not editing a
dispatcher in ``agent.py``. The closed allowlist is preserved: an action name
that is not in this registry is rejected before any SQL is composed, so the LLM
can never reach a statement that is not written here.

Every table an action writes to — and the audit table — must have catalog
commits enabled (``delta.feature.catalogManaged``), or ``BEGIN ATOMIC`` is
refused. ``catalog_managed_ddl()`` below emits the statements; ``09_writeback_setup``
runs them.

Two rendering modes are supported, selected by ``writeback_param_mode`` in
config.yaml:

``parameters``  Values bind through the Statement Execution API's typed
                parameter markers. Preferred — no SQL is built from user text.
``literals``    Values are escaped and interpolated. Fallback for the case where
                parameter markers do not bind inside a ``BEGIN ATOMIC`` block.
                See ``spike0_atomic_params.py``, which decides which mode to use.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

# --- persona write-access ordering -----------------------------------------
# A persona may perform an action when its own level is at least the action's
# declared minimum. Mirrors tool_policy.write_access in the persona YAML.
WRITE_ACCESS_ORDER: dict[str, int] = {
    "none": 0,
    "acknowledge_only": 1,
    "full": 2,
}

# SQL type for every parameter name any action may bind. Kept central so a new
# action cannot silently introduce an untyped (and therefore STRING) parameter.
PARAM_TYPES: dict[str, str] = {
    "actor": "STRING",
    "now": "TIMESTAMP",
    "target_id": "STRING",
    "customer_id": "BIGINT",
    "reason": "STRING",
    "dispute_id": "STRING",
    # Demo #2 — pricing dispute resolution
    "event_month": "STRING",
    "corrected_amount": "DOUBLE",
    "disputed_amount": "DOUBLE",
    # Demo #5 — usage-based billing
    "new_plan_key": "BIGINT",
    "new_product_id": "STRING",
    # Demo #3 — order-to-cash
    "adjustment_amount": "DOUBLE",
}

# Markers whose values must be numeric. Validated before binding so a
# non-numeric value is rejected by us, not discovered by the warehouse.
NUMERIC_TYPES = {"BIGINT", "INT", "DOUBLE", "DECIMAL"}

_MARKER_RE = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")


@dataclass(frozen=True)
class WriteAction:
    """One permitted write, declared rather than dispatched."""

    action: str
    target_tables: tuple[str, ...]   # unqualified; catalog/schema injected at render time
    min_write_access: str
    statements: tuple[str, ...]
    derived: dict[str, Callable[[], str]] = field(default_factory=dict)
    # For actions that create a record, the parameter holding its new id. The
    # audit row records that rather than the caller-supplied target_id, so a
    # created record is findable in the audit log by its own key.
    audit_record_param: Optional[str] = None
    description: str = ""

    @property
    def target_table(self) -> str:
        """Comma-joined table list, as recorded in the audit log."""
        return ",".join(self.target_tables)

    def markers(self) -> set[str]:
        """Parameter names referenced by this action's statements."""
        found: set[str] = set()
        for stmt in self.statements:
            found.update(_MARKER_RE.findall(stmt))
        return found

    def validate(self) -> None:
        """Fail loudly at import time rather than at write time."""
        if self.min_write_access not in WRITE_ACCESS_ORDER:
            raise ValueError(
                f"{self.action}: unknown write access level "
                f"'{self.min_write_access}'"
            )
        unknown = self.markers() - set(PARAM_TYPES)
        if unknown:
            raise ValueError(
                f"{self.action}: parameters with no declared type: {sorted(unknown)}"
            )
        if not self.target_tables:
            raise ValueError(f"{self.action}: at least one target table required")
        if self.audit_record_param and self.audit_record_param not in self.markers():
            raise ValueError(
                f"{self.action}: audit_record_param '{self.audit_record_param}' "
                f"is not a parameter of this action"
            )


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------
# NOTE ON update_dispute_status: the ``reason`` argument carries the *new
# status*, not a justification. That is the behaviour the agent and its prompts
# already rely on, so it is preserved verbatim here rather than corrected as a
# side effect of this refactor.

WRITE_ACTIONS: dict[str, WriteAction] = {
    "acknowledge_anomaly": WriteAction(
        action="acknowledge_anomaly",
        target_tables=("billing_anomalies",),
        min_write_access="acknowledge_only",
        description="Mark a detected billing anomaly as reviewed.",
        statements=(
            "UPDATE {catalog}.{schema}.billing_anomalies "
            "SET acknowledged_by = :actor, "
            "    acknowledged_at = :now, "
            "    acknowledgement_reason = :reason "
            "WHERE anomaly_id = :target_id",
        ),
    ),
    "create_dispute": WriteAction(
        action="create_dispute",
        target_tables=("billing_disputes",),
        min_write_access="full",
        description="Open a new billing dispute on behalf of a customer.",
        derived={"dispute_id": lambda: f"DSP-{str(uuid.uuid4())[:8]}"},
        audit_record_param="dispute_id",
        statements=(
            "INSERT INTO {catalog}.{schema}.billing_disputes "
            "(dispute_id, customer_id, dispute_type, status, description, "
            " created_by, created_at, updated_at) "
            "VALUES (:dispute_id, :customer_id, 'AGENT_CREATED', 'OPEN', "
            "        :reason, :actor, :now, :now)",
        ),
    ),
    "update_dispute_status": WriteAction(
        action="update_dispute_status",
        target_tables=("billing_disputes",),
        min_write_access="full",
        description="Move an existing dispute to a new status.",
        statements=(
            "UPDATE {catalog}.{schema}.billing_disputes "
            "SET status = :reason, "
            "    updated_at = :now "
            "WHERE dispute_id = :target_id",
        ),
    ),
    # --- Demo #2: pricing dispute resolution --------------------------------
    "submit_pricing_dispute": WriteAction(
        action="submit_pricing_dispute",
        target_tables=("billing_disputes",),
        min_write_access="full",
        description=(
            "Open a dispute for a pricing drift found by detect_pricing_drift. "
            "Pass event_month and disputed_amount in `extra`."
        ),
        derived={"dispute_id": lambda: f"DSP-{str(uuid.uuid4())[:8]}"},
        audit_record_param="dispute_id",
        statements=(
            "INSERT INTO {catalog}.{schema}.billing_disputes "
            "(dispute_id, customer_id, event_month, dispute_type, status, "
            " description, disputed_amount_usd, created_by, created_at, updated_at) "
            "VALUES (:dispute_id, :customer_id, :event_month, 'PRICING_DRIFT', 'OPEN', "
            "        :reason, :disputed_amount, :actor, :now, :now)",
        ),
    ),
    # The first genuinely multi-table action, and the reason Phase 0 exists: a
    # dispute cannot be marked corrected unless the invoice is corrected in the
    # same transaction. total_charges is adjusted by the delta because SET
    # expressions evaluate against the pre-update row.
    "apply_pricing_correction": WriteAction(
        action="apply_pricing_correction",
        target_tables=("billing_disputes", "invoice"),
        min_write_access="full",
        description=(
            "Resolve a pricing dispute and correct the invoice atomically. "
            "Pass event_month and corrected_amount in `extra`."
        ),
        statements=(
            "UPDATE {catalog}.{schema}.billing_disputes "
            "SET status = 'CORRECTED', "
            "    resolved_amount_usd = :corrected_amount, "
            "    resolution_notes = :reason, "
            "    resolved_at = :now, "
            "    updated_at = :now "
            "WHERE dispute_id = :target_id",
            "UPDATE {catalog}.{schema}.invoice "
            "SET total_charges = total_charges - monthly_charges + :corrected_amount, "
            "    monthly_charges = :corrected_amount "
            "WHERE customer_id = :customer_id AND event_month = :event_month",
        ),
    ),
    # --- Demo #5: usage-based billing ---------------------------------------
    # A plan change has to land in the customer record and the contract together.
    # Updating only one is precisely the state detect_pricing_drift reports as
    # ContractMismatch — the customer gets billed on a plan their contract does
    # not name. So this is atomic by necessity, not by preference.
    #
    # Any negotiated rate is cleared: it was negotiated against the old plan and
    # does not carry to the new one.
    #
    # Requires ordm_customer_contract from Demo #2 (14_pricing_dispute).
    "submit_auto_upgrade": WriteAction(
        action="submit_auto_upgrade",
        target_tables=("customers", "ordm_customer_contract"),
        min_write_access="full",
        description=(
            "Move a customer to a different plan. Pass new_plan_key and "
            "new_product_id in `extra`."
        ),
        statements=(
            "UPDATE {catalog}.{schema}.customers "
            "SET plan = :new_plan_key "
            "WHERE customer_id = :customer_id",
            "UPDATE {catalog}.{schema}.ordm_customer_contract "
            "SET product_id = :new_product_id, "
            "    negotiated_monthly_price = CAST(NULL AS DOUBLE) "
            "WHERE customer_id = :customer_id",
        ),
    ),
    # --- Demo #3: order-to-cash ---------------------------------------------
    # Correcting recognised revenue on an order and reissuing the affected
    # invoice have to land together: adjusting the order alone leaves the
    # customer owing the old amount, and reissuing alone leaves the order
    # overstated. Either half on its own is the revenue leakage
    # detect_revenue_leakage exists to report.
    #
    # Requires ordm_order_header and ordm_payment from Demo #3 (16_order_to_cash).
    "submit_revenue_adjustment": WriteAction(
        action="submit_revenue_adjustment",
        target_tables=("ordm_order_header", "ordm_payment"),
        min_write_access="full",
        description=(
            "Restate an order's recognised revenue and reissue the affected "
            "invoice. Pass event_month and adjustment_amount in `extra`."
        ),
        statements=(
            "UPDATE {catalog}.{schema}.ordm_order_header "
            "SET adjusted_total = :adjustment_amount, "
            "    adjustment_reason = :reason "
            "WHERE order_id = :target_id",
            "UPDATE {catalog}.{schema}.ordm_payment "
            "SET invoice_amount = :adjustment_amount, "
            "    amount_paid = 0.0, "
            "    payment_date = CAST(NULL AS DATE), "
            "    payment_status = 'Reissued' "
            "WHERE customer_id = :customer_id AND event_month = :event_month",
        ),
    ),
}

for _a in WRITE_ACTIONS.values():
    _a.validate()


# --- Catalog commits prerequisite -------------------------------------------
# Verified by spike0_atomic_params.py on 2026-08-31: BEGIN ATOMIC refuses any
# table without the `catalogManaged` feature, with
#   TRANSACTION_NOT_SUPPORTED.WRITE_NON_CATALOG_MANAGED_TABLE
# Existing tables can be upgraded in place; data is preserved.
CATALOG_MANAGED_PROPERTY = "delta.feature.catalogManaged"


def write_target_tables() -> set[str]:
    """Unqualified tables any registered action writes to.

    The audit table is included because the audit resolution row is written
    inside the same atomic block as the business statements.
    """
    return ({t for a in WRITE_ACTIONS.values() for t in a.target_tables}
            | {"billing_write_audit"})


def catalog_managed_ddl(catalog: str, schema: str) -> list[str]:
    """ALTER statements enabling catalog commits on every write target.

    Safe to re-run. Adding a write action for a new demo extends this list
    automatically, so the prerequisite cannot be forgotten.
    """
    return [
        f"ALTER TABLE {catalog}.{schema}.{t} "
        f"SET TBLPROPERTIES ('{CATALOG_MANAGED_PROPERTY}' = 'supported')"
        for t in sorted(write_target_tables())
    ]


def get_action(name: str) -> Optional[WriteAction]:
    """Resolve an action name. Returns None for anything not in the registry."""
    return WRITE_ACTIONS.get(name)


def action_permitted(persona_level: str, action: WriteAction) -> bool:
    """True when a persona at ``persona_level`` may perform ``action``."""
    have = WRITE_ACCESS_ORDER.get(persona_level, 0)
    need = WRITE_ACCESS_ORDER.get(action.min_write_access, 99)
    return have >= need


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def sql_literal(value, sql_type: str) -> str:
    """Render a value as a SQL literal. Used only in ``literals`` mode."""
    if value is None:
        return "NULL"
    if sql_type in ("BIGINT", "INT", "DOUBLE"):
        # Callers validate numerics before reaching here; this is the second gate.
        return str(int(value)) if sql_type in ("BIGINT", "INT") else str(float(value))
    if sql_type == "BOOLEAN":
        return "true" if value else "false"
    escaped = str(value).replace("'", "''")
    if sql_type == "TIMESTAMP":
        return f"TIMESTAMP '{escaped}'"
    if sql_type == "DATE":
        return f"DATE '{escaped}'"
    return f"'{escaped}'"


def build_param_bag(action: WriteAction, **base) -> dict[str, object]:
    """Assemble the parameter values an action needs.

    ``base`` supplies the common values (actor, now, target_id, customer_id,
    reason); the action's ``derived`` callables supply the rest.
    """
    bag = dict(base)
    for name, make in action.derived.items():
        bag[name] = make()
    missing = action.markers() - set(bag)
    if missing:
        raise KeyError(f"{action.action}: missing parameter values {sorted(missing)}")

    out = {}
    for name in action.markers():
        value = bag[name]
        sql_type = PARAM_TYPES[name]
        # Reject non-numerics here rather than letting the warehouse discover
        # them. Callers may pass values straight from an LLM tool argument.
        if sql_type in NUMERIC_TYPES:
            try:
                value = int(value) if sql_type in ("BIGINT", "INT") else float(value)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{action.action}: parameter '{name}' must be {sql_type}, "
                    f"got {value!r}"
                )
        out[name] = value
    return out


def render(statement: str, bag: dict, mode: str, types: dict[str, str]) -> str:
    """Return a statement ready to send, per the selected parameter mode."""
    if mode == "parameters":
        return statement
    if mode != "literals":
        raise ValueError(f"unknown writeback_param_mode '{mode}'")

    def sub(match: "re.Match") -> str:
        name = match.group(1)
        if name not in bag:
            raise KeyError(f"no value bound for :{name}")
        return sql_literal(bag[name], types.get(name, "STRING"))

    return _MARKER_RE.sub(sub, statement)
