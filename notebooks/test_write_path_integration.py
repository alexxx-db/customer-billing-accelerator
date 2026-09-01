"""End-to-end integration test of the Phase 0 write path against a live warehouse.

Replicates agent.py's _execute_write SQL assembly exactly, using write_actions,
and exercises all three registered actions plus persona denial and a forced
rollback. Creates a throwaway schema and drops it.

    python3 test_write_path_integration.py [--warehouse ID] [--catalog CAT]

Needs the Databricks CLI authenticated (`databricks current-user me`). Defaults
come from config.yaml. Verified passing on 2026-08-31.
"""
import argparse, json, os, subprocess, sys, uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import write_actions as wa

try:
    import yaml
    _cfg = yaml.safe_load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")))
except Exception:
    _cfg = {}

_ap = argparse.ArgumentParser()
_ap.add_argument("--warehouse", default=_cfg.get("warehouse_id", ""))
_ap.add_argument("--catalog", default=_cfg.get("catalog", ""))
_args = _ap.parse_args()
WH = _args.warehouse
assert WH, "no warehouse: pass --warehouse or set warehouse_id in config.yaml"


def run(statement, parameters=None, timeout="50s"):
    payload = {"statement": statement, "warehouse_id": WH, "wait_timeout": timeout}
    if parameters:
        payload["parameters"] = parameters
    p = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(payload)],
        capture_output=True, text=True)
    try:
        d = json.loads(p.stdout)
    except Exception:
        return False, (p.stdout + p.stderr)[:300], None
    st = d.get("status", {})
    ok = st.get("state") == "SUCCEEDED"
    return ok, ("SUCCEEDED" if ok else st.get("error", {}).get("message", st.get("state", "?"))), d


def rows(statement, parameters=None):
    ok, detail, d = run(statement, parameters)
    return ((d.get("result", {}).get("data_array") or []), detail) if ok else (None, detail)

CAT, SCH = _args.catalog, "cba_phase0_test"
assert CAT, "no catalog: pass --catalog or set catalog in config.yaml"
NOW = "2026-08-31T12:00:00"
fails = []

def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}" + ("" if cond else f"\n          {detail}"))
    if not cond: fails.append(label)

# --- audit assembly, copied from agent.py ----------------------------------
AUDIT_COLUMNS = ["audit_id","action_type","target_table","target_record_id","customer_id",
    "agent_session_id","executed_by","payload_json","result_status","result_message","executed_at",
    "initiating_user","executing_principal","persona","request_id","identity_degraded","user_groups"]
AUDIT_TYPES = {"a_audit_id":"STRING","a_action_type":"STRING","a_target_table":"STRING",
    "a_target_record_id":"STRING","a_customer_id":"BIGINT","a_agent_session_id":"STRING",
    "a_executed_by":"STRING","a_sql_statement":"STRING","a_result_status":"STRING",
    "a_result_message":"STRING","a_executed_at":"TIMESTAMP","a_initiating_user":"STRING",
    "a_executing_principal":"STRING","a_persona":"STRING","a_request_id":"STRING",
    "a_identity_degraded":"BOOLEAN","a_user_groups":"STRING","a_payload_json":"STRING"}

def audit_sql(with_sql):
    cols = list(AUDIT_COLUMNS)
    if with_sql: cols.insert(8, "sql_statement")
    return (f"INSERT INTO {CAT}.{SCH}.billing_write_audit ({', '.join(cols)}) "
            f"VALUES ({', '.join(f':a_{c}' for c in cols)})")

def params_of(bag, types):
    out = []
    for k, v in bag.items():
        t = types.get(k, "STRING")
        val = None if v is None else ("true" if (t == "BOOLEAN" and v) else
                                      "false" if t == "BOOLEAN" else str(v))
        out.append({"name": k, "value": val, "type": t})
    return out

# --- setup ------------------------------------------------------------------
print("Setting up scratch schema...")
run(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
CM = "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.billing_anomalies")
run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.billing_disputes")
run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.billing_write_audit")
run(f"""CREATE TABLE {CAT}.{SCH}.billing_anomalies (
  anomaly_id STRING, customer_id BIGINT, anomaly_type STRING,
  acknowledged_by STRING, acknowledged_at TIMESTAMP, acknowledgement_reason STRING) USING DELTA {CM}""")
run(f"""CREATE TABLE {CAT}.{SCH}.billing_disputes (
  dispute_id STRING NOT NULL, customer_id BIGINT NOT NULL, anomaly_id STRING, event_month STRING,
  dispute_type STRING NOT NULL, status STRING NOT NULL, description STRING NOT NULL,
  resolution_notes STRING, disputed_amount_usd DOUBLE, resolved_amount_usd DOUBLE,
  created_by STRING NOT NULL, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
  resolved_at TIMESTAMP, assigned_to STRING) USING DELTA {CM}""")
run(f"""CREATE TABLE {CAT}.{SCH}.billing_write_audit (
  audit_id STRING NOT NULL, action_type STRING NOT NULL, target_table STRING NOT NULL,
  target_record_id STRING, customer_id BIGINT, agent_session_id STRING, executed_by STRING NOT NULL,
  payload_json STRING, sql_statement STRING, result_status STRING NOT NULL, result_message STRING,
  error_detail STRING, executed_at TIMESTAMP NOT NULL, initiating_user STRING,
  executing_principal STRING, persona STRING, request_id STRING, identity_degraded BOOLEAN,
  user_groups STRING) USING DELTA {CM}""")
run(f"INSERT INTO {CAT}.{SCH}.billing_anomalies VALUES ('ANM-001', 4401, 'total_charge_spike', NULL, NULL, NULL)")
run(f"""INSERT INTO {CAT}.{SCH}.billing_disputes VALUES
  ('DSP-seed01', 4401, NULL, '2026-07', 'AGENT_CREATED', 'OPEN', 'seed', NULL, NULL, NULL,
   'agent', TIMESTAMP '{NOW}', TIMESTAMP '{NOW}', NULL, NULL)""")
print("scratch schema ready\n")

# --- the executor, faithful to agent.py -------------------------------------
def execute_write(action_name, target_id, customer_id, reason, persona="customer_care",
                  level="full", force_failure=False):
    action = wa.get_action(action_name)
    if action is None: return "ERROR: unknown action"
    if not wa.action_permitted(level, action):
        # Mirrors _audit_denial in agent.py — a refused attempt is the security
        # event worth recording, so it leaves a BLOCKED row before returning.
        run(audit_sql(False), params_of({
            "a_audit_id": str(uuid.uuid4()), "a_action_type": action.action,
            "a_target_table": ",".join(f"{CAT}.{SCH}.{t}" for t in action.target_tables),
            "a_target_record_id": target_id, "a_customer_id": customer_id,
            "a_agent_session_id": "s", "a_executed_by": "agent",
            "a_payload_json": json.dumps({"action": action.action, "reason": reason}),
            "a_result_status": "BLOCKED",
            "a_result_message": f"Persona '{persona}' has write access '{level}', which does not permit '{action.action}'.",
            "a_executed_at": NOW, "a_initiating_user": "integration-test@example.com",
            "a_executing_principal": "sp", "a_persona": persona, "a_request_id": "r",
            "a_identity_degraded": False, "a_user_groups": "[]",
        }, AUDIT_TYPES), "20s")
        return (f"BLOCKED: The {persona} persona has write access '{level}', which does not "
                f"permit '{action.action}' (requires '{action.min_write_access}').")
    audit_id = str(uuid.uuid4())
    target_table = f"{CAT}.{SCH}.{action.target_table}"
    bag = wa.build_param_bag(action, actor="agent", now=NOW, target_id=target_id,
                             customer_id=customer_id, reason=reason)

    def av(status, msg, sql_text=None):
        v = {"a_audit_id":audit_id, "a_payload_json":json.dumps(bag, default=str),"a_action_type":action.action,"a_target_table":target_table,
             "a_target_record_id":target_id,"a_customer_id":customer_id,"a_agent_session_id":"sess-1",
             "a_executed_by":"agent","a_result_status":status,"a_result_message":msg,
             "a_executed_at":NOW,"a_initiating_user":"integration-test@example.com",
             "a_executing_principal":"billing-agent-sp","a_persona":persona,
             "a_request_id":"req-1","a_identity_degraded":False,"a_user_groups":"[]"}
        if sql_text is not None: v["a_sql_statement"] = sql_text
        return v

    # 3. audit PENDING, outside the transaction
    ok, det, _ = run(audit_sql(False), params_of(av("PENDING","Staged by confirm_write_operation"), AUDIT_TYPES), "20s")
    if not ok: return f"ERROR: Could not record audit trail. Write aborted for safety. ({det[:80]})"

    # 4. BEGIN ATOMIC
    business = [s.format(catalog=CAT, schema=SCH) for s in action.statements]
    if force_failure:
        business.append(f"INSERT INTO {CAT}.{SCH}.billing_disputes (dispute_id, customer_id, dispute_type, status, description, created_by, created_at, updated_at) SELECT 'X', 1/0, 'x','x','x','x', TIMESTAMP '{NOW}', TIMESTAMP '{NOW}'")
    succ = av("SUCCESS", f"{action.action} completed for {target_id} (customer {customer_id}).", "; ".join(business))
    body = ";\n  ".join(business + [audit_sql(True)])
    ok, det, _ = run(f"BEGIN ATOMIC\n  {body};\nEND",
                     params_of({**bag, **succ}, {**wa.PARAM_TYPES, **AUDIT_TYPES}))
    if ok:
        return f"{action.action} completed for {target_id} (customer {customer_id}). Audit id {audit_id}."
    msg = f"{action.action} failed for {target_id}: {det}"
    # 5. resolve PENDING to FAILED
    run(audit_sql(True), params_of(av("FAILED", msg[:400], "; ".join(business)), AUDIT_TYPES), "20s")
    return msg

# --- tests ------------------------------------------------------------------
print("Persona enforcement")
print("-" * 60)
r = execute_write("create_dispute", "ANM-001", 4401, "wrong charge", persona="finance_ops", level="acknowledge_only")
check("finance_ops (acknowledge_only) blocked from create_dispute", r.startswith("BLOCKED:"), r[:140])
r = execute_write("acknowledge_anomaly", "ANM-001", 4401, "reviewed", persona="finance_ops", level="acknowledge_only")
check("finance_ops permitted to acknowledge_anomaly", r.startswith("acknowledge_anomaly completed"), r[:140])
r = execute_write("acknowledge_anomaly", "ANM-001", 4401, "x", persona="executive", level="none")
check("executive (none) blocked from every write", r.startswith("BLOCKED:"), r[:140])

print("\nBusiness writes")
print("-" * 60)
rws, _ = rows(f"SELECT acknowledged_by, acknowledgement_reason FROM {CAT}.{SCH}.billing_anomalies WHERE anomaly_id='ANM-001'")
check("acknowledge_anomaly wrote billing_anomalies", rws and rws[0] == ["agent", "reviewed"], rws)

r = execute_write("create_dispute", "ANM-001", 4401, "billed twice for roaming")
check("create_dispute succeeded", r.startswith("create_dispute completed"), r[:160])
rws, _ = rows(f"SELECT dispute_id, status, description FROM {CAT}.{SCH}.billing_disputes WHERE description='billed twice for roaming'")
check("create_dispute wrote billing_disputes", rws and len(rws) == 1 and rws[0][1] == "OPEN", rws)
check("derived dispute_id landed", rws and rws[0][0].startswith("DSP-"), rws)

r = execute_write("update_dispute_status", "DSP-seed01", 4401, "RESOLVED")
check("update_dispute_status succeeded", r.startswith("update_dispute_status completed"), r[:160])
rws, _ = rows(f"SELECT status FROM {CAT}.{SCH}.billing_disputes WHERE dispute_id='DSP-seed01'")
check("status updated", rws and rws[0][0] == "RESOLVED", rws)

print("\nAudit trail")
print("-" * 60)
rws, _ = rows(f"SELECT action_type, target_table, result_status FROM {CAT}.{SCH}.billing_write_audit ORDER BY action_type, result_status")
check("3 successful writes produced 6 PENDING/SUCCESS rows",
      len([r for r in rws if r[2] in ("PENDING", "SUCCESS")]) == 6,
      f"{len(rws)} rows: {rws}")
ack = [r for r in rws if r[0] == "acknowledge_anomaly"]
check("acknowledge_anomaly audited against billing_anomalies (defect 2.1)",
      all(r[1].endswith(".billing_anomalies") for r in ack), ack)
disp = [r for r in rws if r[0] in ("create_dispute", "update_dispute_status")]
check("dispute actions audited against billing_disputes",
      all(r[1].endswith(".billing_disputes") for r in disp), disp)

print("\nDenied attempts are audited")
print("-" * 60)
blocked, _ = rows(f"""SELECT action_type, result_status, persona, result_message
                      FROM {CAT}.{SCH}.billing_write_audit
                      WHERE result_status = 'BLOCKED' ORDER BY persona""")
check("every refused attempt left a BLOCKED audit row", blocked and len(blocked) == 2,
      f"{len(blocked or [])} rows: {blocked}")
check("the refusal names the persona that was denied",
      blocked and {b[2] for b in blocked} == {"finance_ops", "executive"}, blocked)
check("the refusal says why", blocked and all("write access" in b[3] for b in blocked), blocked)
n_disputes, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.billing_disputes")
check("a refused attempt wrote no business row",
      int(n_disputes[0][0]) == 2, f"{n_disputes} (seed + the one legitimate create)")
for b in (blocked or []):
    print(f"    {b[1]:<8} {b[2]:<12} {b[0]:<22} {b[3][:58]}")

print("\nAtomicity — forced runtime failure")
print("-" * 60)
before_d, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.billing_disputes")
r = execute_write("create_dispute", "ANM-001", 4401, "SHOULD ROLL BACK", force_failure=True)
after_d, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.billing_disputes")
check("failed write reported failure", r.startswith("create_dispute failed"), r[:160])
check("no business row persisted", before_d[0][0] == after_d[0][0], f"{before_d[0][0]} -> {after_d[0][0]}")
rws, _ = rows(f"SELECT audit_id, result_status FROM {CAT}.{SCH}.billing_write_audit WHERE payload_json LIKE '%SHOULD ROLL BACK%' ORDER BY result_status")
check("the rolled-back write is findable by its payload_json", len(rws) == 2, rws)
check("PENDING resolved to FAILED, never stranded",
      sorted(x[1] for x in rws) == ["FAILED", "PENDING"], rws)
check("both rows share one audit_id, so the pair is correlatable",
      len(rws) == 2 and rws[0][0] == rws[1][0], rws)
rws, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.billing_write_audit WHERE result_status='SUCCESS' AND payload_json LIKE '%SHOULD ROLL BACK%'")
check("no SUCCESS audit row for the rolled-back write", int(rws[0][0]) == 0, rws)

rws, _ = rows(f"""SELECT COUNT(DISTINCT audit_id) FROM {CAT}.{SCH}.billing_write_audit
                   WHERE result_status <> 'BLOCKED'""")
check("one audit_id per executed operation", int(rws[0][0]) == 4, rws)
rws, _ = rows(f"""SELECT COUNT(*), COUNT(DISTINCT audit_id) FROM {CAT}.{SCH}.billing_write_audit
                   WHERE result_status = 'BLOCKED'""")
check("each refusal is a single standalone audit row",
      rws and int(rws[0][0]) == int(rws[0][1]) == 2, rws)

# --- teardown ---------------------------------------------------------------
print("\nCleaning up...")
run(f"DROP SCHEMA IF EXISTS {CAT}.{SCH} CASCADE")

print("\n" + "=" * 60)
if fails:
    print(f"{len(fails)} FAILED:")
    for f in fails: print(f"  - {f}")
    sys.exit(1)
print("Phase 0 write path verified end-to-end on a live warehouse.")
print("=" * 60)
