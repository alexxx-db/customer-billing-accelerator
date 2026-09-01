"""Offline regression suite for the write action registry.

Runs anywhere with plain Python — no Spark, no workspace, no pytest:

    python3 test_write_actions.py

Covers what Phase 0 changed: the closed allowlist, persona write-access
enforcement, parameter binding in both modes, and behaviour parity with the
hard-coded statements this registry replaced.
"""

import sys

import write_actions as wa

_failures: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}" + (f"\n          {detail}" if detail else ""))
        _failures.append(label)


def section(title: str) -> None:
    print(f"\n{title}\n{'-' * len(title)}")


# ---------------------------------------------------------------------------
section("Registry integrity")

check("seven actions registered", len(wa.WRITE_ACTIONS) == 7, sorted(wa.WRITE_ACTIONS))
check("apply_pricing_correction spans two tables",
      wa.WRITE_ACTIONS["apply_pricing_correction"].target_tables == ("billing_disputes", "invoice"))
check("write targets are derived from the registry, not hand-maintained",
      {"invoice", "customers", "ordm_customer_contract"} <= wa.write_target_tables(),
      sorted(wa.write_target_tables()))
check("submit_auto_upgrade spans customer record and contract",
      wa.WRITE_ACTIONS["submit_auto_upgrade"].target_tables == ("customers", "ordm_customer_contract"))
check("creating actions audit under the new record's id",
      wa.WRITE_ACTIONS["submit_pricing_dispute"].audit_record_param == "dispute_id"
      and wa.WRITE_ACTIONS["create_dispute"].audit_record_param == "dispute_id")
check("updating actions have no audit_record_param",
      wa.WRITE_ACTIONS["apply_pricing_correction"].audit_record_param is None)
check(
    "every action validates at import",
    all(a.validate() is None for a in wa.WRITE_ACTIONS.values()),
)
check(
    "every marker has a declared SQL type",
    all(not (a.markers() - set(wa.PARAM_TYPES)) for a in wa.WRITE_ACTIONS.values()),
)
check(
    "acknowledge_anomaly targets billing_anomalies, not billing_disputes",
    wa.WRITE_ACTIONS["acknowledge_anomaly"].target_table == "billing_anomalies",
    "this is the audit misattribution the registry fixes",
)
check("unknown action resolves to None", wa.get_action("drop_everything") is None)
check("empty action name resolves to None", wa.get_action("") is None)


# ---------------------------------------------------------------------------
section("Persona write-access enforcement")

# Expected matrix mirrors tool_policy.write_access and blocked_tools in the
# persona YAML. finance_ops declares acknowledge_only and blocks the two
# dispute actions; before Phase 0 nothing enforced that.
EXPECTED = {
    ("full", "acknowledge_anomaly"): True,
    ("full", "create_dispute"): True,
    ("full", "update_dispute_status"): True,
    ("acknowledge_only", "acknowledge_anomaly"): True,
    ("acknowledge_only", "create_dispute"): False,
    ("acknowledge_only", "update_dispute_status"): False,
    ("none", "acknowledge_anomaly"): False,
    ("none", "create_dispute"): False,
    ("none", "update_dispute_status"): False,
    # Demo #2 writes are corrections to money — full access only.
    ("full", "submit_pricing_dispute"): True,
    ("full", "apply_pricing_correction"): True,
    ("acknowledge_only", "submit_pricing_dispute"): False,
    ("acknowledge_only", "apply_pricing_correction"): False,
    ("none", "apply_pricing_correction"): False,
    # A plan change is a billing change — full access only.
    ("full", "submit_auto_upgrade"): True,
    ("acknowledge_only", "submit_auto_upgrade"): False,
    ("none", "submit_auto_upgrade"): False,
    ("full", "submit_revenue_adjustment"): True,
    ("acknowledge_only", "submit_revenue_adjustment"): False,
}
for (level, action_name), expected in EXPECTED.items():
    actual = wa.action_permitted(level, wa.WRITE_ACTIONS[action_name])
    check(f"{level:<16} -> {action_name:<22} {'allow' if expected else 'deny'}",
          actual == expected, f"got {actual}")

check(
    "an unrecognised level is denied, not defaulted open",
    not wa.action_permitted("superuser", wa.WRITE_ACTIONS["acknowledge_anomaly"]),
)


# ---------------------------------------------------------------------------
section("Parameter bags")

BASE = dict(actor="agent", now="2026-08-31T12:00:00+00:00",
            target_id="ANM-001", customer_id=4401, reason="spike review")

bag = wa.build_param_bag(wa.WRITE_ACTIONS["acknowledge_anomaly"], **BASE)
check("bag contains exactly the markers used",
      set(bag) == wa.WRITE_ACTIONS["acknowledge_anomaly"].markers(), sorted(bag))
check("unused base values are dropped", "customer_id" not in bag)

bag_create = wa.build_param_bag(wa.WRITE_ACTIONS["create_dispute"], **BASE)
check("derived dispute_id is generated",
      bag_create["dispute_id"].startswith("DSP-") and len(bag_create["dispute_id"]) == 12,
      bag_create.get("dispute_id"))
check("two create_dispute bags get distinct ids",
      wa.build_param_bag(wa.WRITE_ACTIONS["create_dispute"], **BASE)["dispute_id"]
      != bag_create["dispute_id"])

try:
    wa.build_param_bag(wa.WRITE_ACTIONS["create_dispute"], actor="agent")
    check("missing values raise KeyError", False, "no exception raised")
except KeyError as e:
    check("missing values raise KeyError", "customer_id" in str(e), str(e))


# ---------------------------------------------------------------------------
section("Demo #2 — extra parameters and numeric validation")

CORRECTION = dict(BASE, target_id="DSP-abc123", event_month="2026-07", corrected_amount=199.0)
corr = wa.WRITE_ACTIONS["apply_pricing_correction"]
bag_corr = wa.build_param_bag(corr, **CORRECTION)
check("correction bag carries event_month and corrected_amount",
      bag_corr["event_month"] == "2026-07" and bag_corr["corrected_amount"] == 199.0, bag_corr)
check("corrected_amount is coerced to float",
      isinstance(wa.build_param_bag(corr, **dict(CORRECTION, corrected_amount="199"))["corrected_amount"], float))

for bad in ["not-a-number", "199; DROP TABLE x", None, ""]:
    try:
        wa.build_param_bag(corr, **dict(CORRECTION, corrected_amount=bad))
        check(f"non-numeric corrected_amount rejected: {bad!r}", False, "no exception")
    except ValueError:
        check(f"non-numeric corrected_amount rejected: {bad!r}", True)

# Keys the action does not declare must never reach a statement.
noise = dict(CORRECTION, injected_column="DROP TABLE users", status="HACKED")
bag_noise = wa.build_param_bag(corr, **noise)
check("undeclared extra keys are dropped",
      "injected_column" not in bag_noise and "status" not in bag_noise, sorted(bag_noise))

sub = wa.WRITE_ACTIONS["submit_pricing_dispute"]
bag_sub = wa.build_param_bag(sub, **dict(BASE, event_month="2026-07", disputed_amount=100.0))
check("submit_pricing_dispute derives its own dispute_id",
      bag_sub["dispute_id"].startswith("DSP-"), bag_sub.get("dispute_id"))
check("submit_pricing_dispute does not need target_id",
      "target_id" not in sub.markers(), sorted(sub.markers()))


# ---------------------------------------------------------------------------
section("Demo #5 — plan upgrade")

up = wa.WRITE_ACTIONS["submit_auto_upgrade"]
UPGRADE = dict(BASE, new_plan_key=7, new_product_id="PLAN007")
bag_up = wa.build_param_bag(up, **UPGRADE)
check("upgrade bag carries the new plan key and product id",
      bag_up["new_plan_key"] == 7 and bag_up["new_product_id"] == "PLAN007", bag_up)
check("new_plan_key is coerced to int",
      isinstance(wa.build_param_bag(up, **dict(UPGRADE, new_plan_key="7"))["new_plan_key"], int))
try:
    wa.build_param_bag(up, **dict(UPGRADE, new_plan_key="PLAN007"))
    check("a non-numeric plan key is rejected", False, "no exception")
except ValueError:
    check("a non-numeric plan key is rejected", True)

up_sql = [x.format(catalog="cat", schema="sch") for x in up.statements]
check("upgrade updates the customer record", "UPDATE cat.sch.customers" in up_sql[0], up_sql[0])
check("upgrade updates the contract", "UPDATE cat.sch.ordm_customer_contract" in up_sql[1], up_sql[1])
check("any negotiated rate is cleared on plan change",
      "negotiated_monthly_price = CAST(NULL AS DOUBLE)" in up_sql[1], up_sql[1])


# ---------------------------------------------------------------------------
section("Demo #3 — revenue adjustment")

adj = wa.WRITE_ACTIONS["submit_revenue_adjustment"]
ADJ = dict(BASE, target_id="ORD-0000004401", event_month="2026-07", adjustment_amount=1250.0)
bag_adj = wa.build_param_bag(adj, **ADJ)
check("adjustment spans the order and the payment",
      adj.target_tables == ("ordm_order_header", "ordm_payment"), adj.target_tables)
check("adjustment amount is coerced to float",
      isinstance(wa.build_param_bag(adj, **dict(ADJ, adjustment_amount="1250"))["adjustment_amount"], float))
try:
    wa.build_param_bag(adj, **dict(ADJ, adjustment_amount="lots"))
    check("a non-numeric adjustment is rejected", False, "no exception")
except ValueError:
    check("a non-numeric adjustment is rejected", True)
adj_sql = [x.format(catalog="cat", schema="sch") for x in adj.statements]
check("restates the order", "UPDATE cat.sch.ordm_order_header" in adj_sql[0], adj_sql[0])
check("reissues the invoice", "UPDATE cat.sch.ordm_payment" in adj_sql[1], adj_sql[1])
check("reissue clears prior settlement",
      "amount_paid = 0.0" in adj_sql[1] and "'Reissued'" in adj_sql[1], adj_sql[1])

check("order and payment tables joined the catalog-commit list",
      {"ordm_order_header", "ordm_payment"} <= wa.write_target_tables(),
      sorted(wa.write_target_tables()))


# ---------------------------------------------------------------------------
section("Rendering — parameters mode")

for name, action in sorted(wa.WRITE_ACTIONS.items()):
    stmt = action.statements[0].format(catalog="cat", schema="sch")
    rendered = wa.render(stmt, {}, "parameters", wa.PARAM_TYPES)
    check(f"{name}: statement passes through untouched", rendered == stmt)


# ---------------------------------------------------------------------------
section("Rendering — literals mode")

lit = wa.render(
    wa.WRITE_ACTIONS["acknowledge_anomaly"].statements[0].format(catalog="cat", schema="sch"),
    wa.build_param_bag(wa.WRITE_ACTIONS["acknowledge_anomaly"], **BASE),
    "literals", wa.PARAM_TYPES,
)
check("no unbound markers remain", not wa._MARKER_RE.search(lit), lit)
check("timestamp gets its TIMESTAMP prefix", "TIMESTAMP '2026-08-31T12:00:00+00:00'" in lit, lit)
check("target table is billing_anomalies", "cat.sch.billing_anomalies" in lit, lit)

# Injection: a quote in free text must not terminate the literal.
hostile = dict(BASE, reason="'; DROP TABLE cat.sch.billing_disputes; --")
lit_hostile = wa.render(
    wa.WRITE_ACTIONS["acknowledge_anomaly"].statements[0].format(catalog="cat", schema="sch"),
    wa.build_param_bag(wa.WRITE_ACTIONS["acknowledge_anomaly"], **hostile),
    "literals", wa.PARAM_TYPES,
)
check("embedded quote is doubled, not left to close the literal",
      "''; DROP TABLE" in lit_hostile and lit_hostile.count("DROP TABLE") == 1,
      lit_hostile)
check("the injected payload stays inside one string literal",
      lit_hostile.count("'") % 2 == 0 and not wa._MARKER_RE.search(lit_hostile),
      lit_hostile)

# Numeric parameters must not accept text at all.
lit_num = wa.render(
    wa.WRITE_ACTIONS["create_dispute"].statements[0].format(catalog="cat", schema="sch"),
    wa.build_param_bag(wa.WRITE_ACTIONS["create_dispute"], **BASE),
    "literals", wa.PARAM_TYPES,
)
check("customer_id renders as a bare integer", ", 4401," in lit_num, lit_num)

try:
    wa.sql_literal("4401; DROP TABLE x", "BIGINT")
    check("non-numeric BIGINT is rejected", False, "no exception raised")
except (ValueError, TypeError):
    check("non-numeric BIGINT is rejected", True)

try:
    wa.render("SELECT 1", {}, "yolo", wa.PARAM_TYPES)
    check("unknown parameter mode raises", False, "no exception raised")
except ValueError:
    check("unknown parameter mode raises", True)


# ---------------------------------------------------------------------------
section("Behaviour parity with the statements this replaced")

# The pre-Phase-0 executor built these by hand. Structure must be unchanged.
parity = {
    "acknowledge_anomaly": [
        "UPDATE cat.sch.billing_anomalies", "acknowledged_by", "acknowledged_at",
        "acknowledgement_reason", "WHERE anomaly_id",
    ],
    "create_dispute": [
        "INSERT INTO cat.sch.billing_disputes", "'AGENT_CREATED'", "'OPEN'",
        "dispute_id", "created_at", "updated_at",
    ],
    "update_dispute_status": [
        "UPDATE cat.sch.billing_disputes", "SET status", "updated_at",
        "WHERE dispute_id",
    ],
    "submit_pricing_dispute": [
        "INSERT INTO cat.sch.billing_disputes", "'PRICING_DRIFT'", "'OPEN'",
        "disputed_amount_usd", "event_month",
    ],
}

# The correction must touch both tables, and must adjust total_charges by the
# delta rather than overwriting it.
corr_sql = [x.format(catalog="cat", schema="sch")
            for x in wa.WRITE_ACTIONS["apply_pricing_correction"].statements]
check("correction updates the dispute", "UPDATE cat.sch.billing_disputes" in corr_sql[0], corr_sql[0])
check("correction updates the invoice", "UPDATE cat.sch.invoice" in corr_sql[1], corr_sql[1])
check("total_charges adjusted by delta, not overwritten",
      "total_charges - monthly_charges + :corrected_amount" in corr_sql[1], corr_sql[1])
for name, fragments in parity.items():
    stmt = wa.WRITE_ACTIONS[name].statements[0].format(catalog="cat", schema="sch")
    missing = [f for f in fragments if f not in stmt]
    check(f"{name}: statement shape preserved", not missing, f"missing {missing}")


# ---------------------------------------------------------------------------
print(f"\n{'=' * 58}")
if _failures:
    print(f"{len(_failures)} FAILED:")
    for f in _failures:
        print(f"  - {f}")
    sys.exit(1)
print("All checks passed.")
print("=" * 58)
