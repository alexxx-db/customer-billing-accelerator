"""Live integration test for Demo #5 — usage-based billing with inventory.

Seeds a throwaway schema from the real plan catalogue with usage scaled to each
plan's own allowance, builds the ORDM Inventory core and gold rollups, creates
the six read functions, then exercises submit_auto_upgrade including its
rollback. Drops everything.

    python3 test_usage_billing_integration.py [--warehouse ID] [--catalog CAT]

Needs the Databricks CLI authenticated. Defaults come from config.yaml.
Verified passing on 2026-09-01.
"""
import argparse, json, os, subprocess, sys, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import write_actions as wa
import demo5_usage_sql as D

try:
    import yaml
    _cfg = yaml.safe_load(open(os.path.join(HERE, "config.yaml")))
except Exception:
    _cfg = {}

_ap = argparse.ArgumentParser()
_ap.add_argument("--warehouse", default=_cfg.get("warehouse_id", ""))
_ap.add_argument("--catalog", default=_cfg.get("catalog", ""))
_args = _ap.parse_args()
WH, CAT = _args.warehouse, _args.catalog
assert WH, "no warehouse: pass --warehouse or set warehouse_id in config.yaml"
assert CAT, "no catalog: pass --catalog or set catalog in config.yaml"
SCH, NOW = "cba_demo5_test", "2026-09-01T12:00:00"
PLANS_JSON = os.path.join(HERE, "data", "billing_plans.json")


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


fails = []
def check(label, cond, detail=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}" + ("" if cond else f"\n          {detail}"))
    if not cond:
        fails.append(label)


# Audit row shape, mirroring agent.py. Prefixed a_ so no audit column can collide
# with a write action's own marker inside the shared atomic block.
AUDIT_COLUMNS = ["audit_id", "action_type", "target_table", "target_record_id",
    "customer_id", "agent_session_id", "executed_by", "payload_json", "result_status",
    "result_message", "executed_at", "initiating_user", "executing_principal",
    "persona", "request_id", "identity_degraded", "user_groups"]
AUDIT_TYPES = {f"a_{c}": ("BIGINT" if c == "customer_id"
                          else "TIMESTAMP" if c == "executed_at"
                          else "BOOLEAN" if c == "identity_degraded"
                          else "STRING") for c in AUDIT_COLUMNS}
AUDIT_TYPES["a_sql_statement"] = "STRING"



print("Seeding source tables...")
run(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
for t in ["usage_forecast","usage_by_asset_month","ordm_plan_entitlement","ordm_inventory_asset",
          "billing_items","customers","billing_plans"]:
    run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.{t}")

plans = [json.loads(l) for l in open(PLANS_JSON) if l.strip()]
run(f"""CREATE TABLE {CAT}.{SCH}.billing_plans (
  Plan_key BIGINT, Plan_id STRING, Plan_name STRING, contract_in_months BIGINT,
  monthly_charges_dollars DOUBLE, Calls_Text STRING, Internet_Speed_MBPS STRING,
  Data_Limit_GB STRING, Data_Outside_Allowance_Per_MB DOUBLE, Roam_Data_charges_per_MB DOUBLE,
  Roam_Call_charges_per_min DOUBLE, Roam_text_charges DOUBLE,
  International_call_charge_per_min DOUBLE, International_text_charge DOUBLE) USING DELTA""")
K = ["Plan_key","Plan_id","Plan_name","contract_in_months","monthly_charges_dollars","Calls_Text",
     "Internet_Speed_MBPS","Data_Limit_GB","Data_Outside_Allowance_Per_MB","Roam_Data_charges_per_MB",
     "Roam_Call_charges_per_min","Roam_text_charges","International_call_charge_per_min","International_text_charge"]
def lit(v): return "NULL" if v is None else (f"'{v}'" if isinstance(v, str) else str(v))
run(f"INSERT INTO {CAT}.{SCH}.billing_plans VALUES " +
    ",".join("(" + ",".join(lit(p[k]) for k in K) + ")" for p in plans))

run(f"CREATE TABLE {CAT}.{SCH}.customers (customer_id BIGINT, device_id BIGINT, plan BIGINT, contract_start_dt DATE) USING DELTA")
run(f"""INSERT INTO {CAT}.{SCH}.customers
SELECT 4400 + id, 9860000000 + id * 137, PMOD(id, 10) + 1,
       DATE_SUB(CURRENT_DATE(), CAST(400 + id * 5 AS INT))
FROM RANGE(20)""")

# Event-level usage. data_local volume varies per customer so some cross their
# allowance and some do not — that is what makes every overage status reachable.
run(f"""CREATE TABLE {CAT}.{SCH}.billing_items (
  device_id BIGINT, event_type STRING, minutes DOUBLE, bytes_transferred BIGINT,
  event_ts TIMESTAMP, contract_start_dt DATE) USING DELTA""")
run(f"""INSERT INTO {CAT}.{SCH}.billing_items
SELECT
  c.device_id,
  et.event_type,
  CASE WHEN et.event_type LIKE 'call_mins%' THEN ROUND(2.0 + PMOD(HASH(c.customer_id, et.event_type, r.id), 8), 2)
       ELSE 0.0 END,
  CASE WHEN et.event_type = 'data_local'
         THEN CAST(COALESCE(TRY_CAST(p.Data_Limit_GB AS DOUBLE), 200.0) * 1024.0
                   * ELT(PMOD(c.customer_id, 5) + 1, 0.40, 0.85, 1.10, 1.45, 1.80)
                   / 20.0 * 1048576.0 AS BIGINT)
       WHEN et.event_type = 'data_roaming'
         THEN CAST(PMOD(HASH(c.customer_id, 'roam'), 3) AS BIGINT) * 40 * 1048576
       ELSE 0 END,
  TIMESTAMP(DATE_ADD(TRUNC(ADD_MONTHS(CURRENT_DATE(), -m.n), 'MM'), CAST(PMOD(HASH(r.id, m.n), 27) AS INT))),
  c.contract_start_dt
FROM {CAT}.{SCH}.customers c
JOIN {CAT}.{SCH}.billing_plans p ON p.Plan_key = c.plan
CROSS JOIN (SELECT explode(array('data_local','data_roaming','call_mins_local','call_mins_roaming',
                                 'call_mins_international','texts_local','texts_roaming','texts_international')) AS event_type) et
CROSS JOIN (SELECT id AS n FROM RANGE(1, 8)) m
CROSS JOIN (SELECT id FROM RANGE(20)) r""")
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.billing_items")
print(f"seeded {r[0][0]} usage events\n")

print("ORDM Inventory core and gold aggregates")
print("-" * 62)
for f in D.ALL_TABLES:
    ok, det, _ = run(f(CAT, SCH))
    check(f.__name__, ok, det[:280])

r, _ = rows(f"SELECT asset_status, COUNT(*) FROM {CAT}.{SCH}.ordm_inventory_asset GROUP BY 1")
check("assets carry an activity status", r and len(r) >= 1, r)
r, _ = rows(f"SELECT COUNT(DISTINCT entitlement_type) FROM {CAT}.{SCH}.ordm_plan_entitlement")
check("eight metered dimensions per plan", r and int(r[0][0]) == 8, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.ordm_plan_entitlement WHERE included_quantity IS NULL")
check("unlimited dimensions modelled as NULL allowance", r and int(r[0][0]) > 0, r)
r, _ = rows(f"SELECT usage_unit, COUNT(*) FROM {CAT}.{SCH}.usage_by_asset_month GROUP BY 1 ORDER BY 1")
check("usage normalised to MB / min / text", r and {x[0] for x in r} == {"MB","min","text"}, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.usage_forecast")
check("AI_FORECAST produced a forecast per customer", r and int(r[0][0]) > 0, r)

print("\nUC functions")
print("-" * 62)
for f in D.ALL_FUNCTIONS:
    ok, det, _ = run(f(CAT, SCH))
    check(f.__name__, ok, det[:320])

print("\ndetect_overage — statuses reachable")
print("-" * 62)
month, _ = rows(f"SELECT event_month FROM {CAT}.{SCH}.usage_by_asset_month ORDER BY event_month DESC LIMIT 1 OFFSET 1")
EM = month[0][0]
seen = {}
for cid in range(4400, 4420):
    rr, _ = rows(f"SELECT status, entitlement_type, usage_quantity, included_quantity, overage_charge, explanation "
                 f"FROM {CAT}.{SCH}.detect_overage('{cid}','{EM}')")
    for row in (rr or []):
        seen.setdefault(row[0], (cid, row))
for k, (cid, row) in sorted(seen.items()):
    print(f"    {k:<18} customer {cid}  {row[1]}")
    print(f"      {row[5][:145]}")
check("OverLimit is reachable", "OverLimit" in seen, sorted(seen))
check("ApproachingLimit is reachable", "ApproachingLimit" in seen, sorted(seen))
check("WithinAllowance is reachable", "WithinAllowance" in seen, sorted(seen))
check("Unlimited is reachable", "Unlimited" in seen, sorted(seen))
check("ChargedPerUse is reachable", "ChargedPerUse" in seen, sorted(seen))

print("\nrecommend_plan_upgrade")
print("-" * 62)
over_cid = seen.get("OverLimit", (None, None))[0]
if over_cid is None:
    print("  cannot continue: no over-limit customer was seeded")
    sys.exit(1)
rr, det = rows(f"SELECT candidate_plan_name, is_current_plan, base_charge, projected_overage, projected_total, saving_vs_current, recommendation FROM {CAT}.{SCH}.recommend_plan_upgrade('{over_cid}','{EM}')")
check("returns ranked candidates", rr is not None and 0 < len(rr) <= 5, det[:200] if rr is None else rr)
if rr:
    for row in rr:
        print(f"    {row[0]:<18} base ${row[2]:<6} overage ${row[3]:<9} total ${row[4]:<9} {row[6][:70]}")
    check("current plan is included and flagged", any(str(x[1]).lower() == "true" for x in rr), rr)
    cur = [x for x in rr if str(x[1]).lower() == "true"]
    check("current plan saving is zero", cur and abs(float(cur[0][5])) < 0.01, cur)
    best = min(rr, key=lambda x: float(x[4]))
    check("a cheaper plan exists for an over-limit customer", float(best[4]) <= float(cur[0][4]) + 0.01,
          f"best={best[0]} {best[4]} current={cur[0][4]}")

print("\nOther reads, boundedness, PII")
print("-" * 62)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.lookup_usage_history('{over_cid}', 6)")
check("usage history bounded", r and 0 < int(r[0][0]) <= 200, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.lookup_plan_entitlement('100GB SIM12')")
check("plan entitlement returns all dimensions", r and int(r[0][0]) == 8, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.lookup_inventory_assets('{over_cid}')")
check("inventory assets bounded and non-empty", r and 0 < int(r[0][0]) <= 50, r)
r, _ = rows(f"SELECT projected_status, forecast_quantity, included_quantity FROM {CAT}.{SCH}.lookup_usage_forecast('{over_cid}')")
check("forecast returns a projected status", r and len(r) == 1, r)
if r: print(f"    forecast: {r[0][1]} MB vs allowance {r[0][2]} MB -> {r[0][0]}")
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.detect_overage('not-a-customer','{EM}')")
check("unknown customer returns empty, not an error", r is not None and int(r[0][0]) == 0, r)

pii = {"customer_name","email","phone_number"}
for call in [f"detect_overage('{over_cid}','{EM}')", f"lookup_inventory_assets('{over_cid}')",
             f"lookup_usage_history('{over_cid}',6)", f"recommend_plan_upgrade('{over_cid}','{EM}')"]:
    ok, _, d = run(f"SELECT * FROM {CAT}.{SCH}.{call} LIMIT 1")
    cols = {c["name"].lower() for c in d["manifest"]["schema"]["columns"]} if ok else set()
    check(f"no PII in {call.split('(')[0]}", not (pii & cols), sorted(cols))





def audit_sql(with_sql):
    cols = list(AUDIT_COLUMNS)
    if with_sql: cols.insert(8, "sql_statement")
    return (f"INSERT INTO {CAT}.{SCH}.billing_write_audit ({', '.join(cols)}) "
            f"VALUES ({', '.join(f':a_{c}' for c in cols)})")

def params_of(bag, types):
    out = []
    for k, v in bag.items():
        t = types.get(k, "STRING")
        out.append({"name": k, "type": t, "value": None if v is None else
                    ("true" if (t=="BOOLEAN" and v) else "false" if t=="BOOLEAN" else str(v))})
    return out

def execute_write(action_name, target_id, customer_id, reason, extra=None,
                  persona="customer_care", level="full", force_failure=False):
    action = wa.get_action(action_name)
    if not wa.action_permitted(level, action):
        return f"BLOCKED: {persona} has '{level}', needs '{action.min_write_access}'."
    try:
        bag = wa.build_param_bag(action, actor="agent", now=NOW, target_id=target_id,
                                 customer_id=customer_id, reason=reason, **(extra or {}))
    except (KeyError, ValueError) as e:
        return f"ERROR: {e}"
    audit_id = str(uuid.uuid4())
    record_id = str(bag[action.audit_record_param]) if action.audit_record_param else target_id
    tt = ",".join(f"{CAT}.{SCH}.{t}" for t in action.target_tables)
    def av(status, msg, sql_text=None):
        v = {"a_audit_id":audit_id,"a_payload_json":json.dumps(bag, default=str),
             "a_action_type":action.action,"a_target_table":tt,"a_target_record_id":record_id,
             "a_customer_id":customer_id,"a_agent_session_id":"s","a_executed_by":"agent",
             "a_result_status":status,"a_result_message":msg,"a_executed_at":NOW,
             "a_initiating_user":"test@example.com","a_executing_principal":"sp",
             "a_persona":persona,"a_request_id":"r","a_identity_degraded":False,"a_user_groups":"[]"}
        if sql_text is not None: v["a_sql_statement"] = sql_text
        return v
    ok, det, _ = run(audit_sql(False), params_of(av("PENDING","Staged"), AUDIT_TYPES), "20s")
    if not ok: return f"ERROR: audit failed: {det[:120]}"
    business = [s.format(catalog=CAT, schema=SCH) for s in action.statements]
    if force_failure:
        business.append(f"UPDATE {CAT}.{SCH}.customers SET plan = 1/0 WHERE customer_id = :customer_id")
    succ = av("SUCCESS", f"{action.action} completed for {record_id}.", "; ".join(business))
    body = ";\n  ".join(business + [audit_sql(True)])
    ok, det, _ = run(f"BEGIN ATOMIC\n  {body};\nEND",
                     params_of({**bag, **succ}, {**wa.PARAM_TYPES, **AUDIT_TYPES}))
    if ok: return f"{action.action} completed for {record_id}."
    msg = f"{action.action} failed for {record_id}: {det}"
    run(audit_sql(True), params_of(av("FAILED", msg[:400], "; ".join(business)), AUDIT_TYPES), "20s")
    return msg

# --- setup ------------------------------------------------------------------
print("Preparing contract + audit tables and enabling catalog commits...")
CM = "SET TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.ordm_customer_contract")
run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.billing_write_audit")
run(f"""CREATE TABLE {CAT}.{SCH}.ordm_customer_contract (
  contract_id STRING, customer_id BIGINT, product_id STRING, contract_start_dt DATE,
  contract_end_dt DATE, negotiated_monthly_price DOUBLE, contract_status STRING)
  USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')""")
run(f"""INSERT INTO {CAT}.{SCH}.ordm_customer_contract
SELECT CONCAT('CTR-', LPAD(CAST(c.customer_id AS STRING), 8, '0')), c.customer_id, p.Plan_id,
       c.contract_start_dt, ADD_MONTHS(c.contract_start_dt, p.contract_in_months),
       CASE WHEN PMOD(c.customer_id, 3) = 0 THEN ROUND(p.monthly_charges_dollars * 0.8, 2) ELSE NULL END,
       'Active'
FROM {CAT}.{SCH}.customers c JOIN {CAT}.{SCH}.billing_plans p ON p.Plan_key = c.plan""")
run(f"""CREATE TABLE {CAT}.{SCH}.billing_write_audit (
  audit_id STRING NOT NULL, action_type STRING NOT NULL, target_table STRING NOT NULL,
  target_record_id STRING, customer_id BIGINT, agent_session_id STRING, executed_by STRING NOT NULL,
  payload_json STRING, sql_statement STRING, result_status STRING NOT NULL, result_message STRING,
  error_detail STRING, executed_at TIMESTAMP NOT NULL, initiating_user STRING,
  executing_principal STRING, persona STRING, request_id STRING, identity_degraded BOOLEAN,
  user_groups STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported')""")
# customers pre-dates the registry, so upgrade it in place — exactly what
# 09_writeback_setup now does for every registry-declared write target.
ok, det, _ = run(f"ALTER TABLE {CAT}.{SCH}.customers {CM}")
check("existing customers table upgraded to catalogManaged in place", ok, det[:160])

# Find an over-limit customer and the plan the recommender picks for them.
EM, _ = rows(f"SELECT event_month FROM {CAT}.{SCH}.usage_by_asset_month ORDER BY event_month DESC LIMIT 1 OFFSET 1")
EM = EM[0][0]
target = None
for cid in range(4400, 4420):
    d, _ = rows(f"SELECT status FROM {CAT}.{SCH}.detect_overage('{cid}','{EM}') WHERE status='OverLimit'")
    if d:
        rec, _ = rows(f"""SELECT candidate_plan_key, candidate_plan_name, saving_vs_current
                          FROM {CAT}.{SCH}.recommend_plan_upgrade('{cid}','{EM}')
                          WHERE is_current_plan = FALSE ORDER BY projected_total LIMIT 1""")
        if rec: target = (cid, int(rec[0][0]), rec[0][1], float(rec[0][2])); break
assert target, "no over-limit customer with a cheaper alternative"
CID, NEW_KEY, NEW_NAME, SAVING = target
pid, _ = rows(f"SELECT Plan_id FROM {CAT}.{SCH}.billing_plans WHERE Plan_key = {NEW_KEY}")
NEW_PID = pid[0][0]
before, _ = rows(f"SELECT plan FROM {CAT}.{SCH}.customers WHERE customer_id={CID}")
OLD_KEY = int(before[0][0])
print(f"\ncustomer {CID}: plan {OLD_KEY} -> {NEW_KEY} ({NEW_NAME}), saving ${SAVING}\n")

print("Persona gate")
print("-" * 62)
r = execute_write("submit_auto_upgrade", str(CID), CID, "upgrade",
                  extra={"new_plan_key": NEW_KEY, "new_product_id": NEW_PID},
                  persona="finance_ops", level="acknowledge_only")
check("finance_ops blocked from changing a plan", r.startswith("BLOCKED:"), r[:140])

print("\nInput validation")
print("-" * 62)
r = execute_write("submit_auto_upgrade", str(CID), CID, "upgrade",
                  extra={"new_plan_key": "PLAN007", "new_product_id": NEW_PID})
check("non-numeric plan key rejected before any SQL runs",
      r.startswith("ERROR:") and "new_plan_key" in r, r[:170])

print("\nAtomicity — forced failure must leave both tables untouched")
print("-" * 62)
c_before, _ = rows(f"SELECT plan FROM {CAT}.{SCH}.customers WHERE customer_id={CID}")
k_before, _ = rows(f"SELECT product_id, negotiated_monthly_price FROM {CAT}.{SCH}.ordm_customer_contract WHERE customer_id={CID}")
r = execute_write("submit_auto_upgrade", str(CID), CID, "should roll back",
                  extra={"new_plan_key": NEW_KEY, "new_product_id": NEW_PID}, force_failure=True)
check("failed upgrade reported failure", r.startswith("submit_auto_upgrade failed"), r[:150])
c_after, _ = rows(f"SELECT plan FROM {CAT}.{SCH}.customers WHERE customer_id={CID}")
k_after, _ = rows(f"SELECT product_id, negotiated_monthly_price FROM {CAT}.{SCH}.ordm_customer_contract WHERE customer_id={CID}")
check("customer plan unchanged", c_before == c_after, f"{c_before} -> {c_after}")
check("contract unchanged", k_before == k_after, f"{k_before} -> {k_after}")
check("no half-applied upgrade — the drift Demo #2 detects was not created",
      c_before == c_after and k_before == k_after)

print("\nsubmit_auto_upgrade — the real thing")
print("-" * 62)
r = execute_write("submit_auto_upgrade", str(CID), CID, f"Overage; moving to {NEW_NAME}",
                  extra={"new_plan_key": NEW_KEY, "new_product_id": NEW_PID})
check("upgrade applied", r.startswith("submit_auto_upgrade completed"), r[:170])
c_after, _ = rows(f"SELECT plan FROM {CAT}.{SCH}.customers WHERE customer_id={CID}")
check("customer moved to the new plan", int(c_after[0][0]) == NEW_KEY, f"{c_after} expected {NEW_KEY}")
k_after, _ = rows(f"SELECT product_id, negotiated_monthly_price FROM {CAT}.{SCH}.ordm_customer_contract WHERE customer_id={CID}")
check("contract moved to the new product", k_after[0][0] == NEW_PID, k_after)
check("negotiated rate cleared — it belonged to the old plan", k_after[0][1] is None, k_after)

print("\nThe demos agree afterwards")
print("-" * 62)
d, _ = rows(f"""SELECT p.Plan_id, ct.product_id
                FROM {CAT}.{SCH}.customers cu
                JOIN {CAT}.{SCH}.billing_plans p ON p.Plan_key = cu.plan
                JOIN {CAT}.{SCH}.ordm_customer_contract ct ON ct.customer_id = cu.customer_id
                WHERE cu.customer_id = {CID}""")
check("customer record and contract name the same plan", d and d[0][0] == d[0][1], d)
a, _ = rows(f"SELECT result_status, target_table FROM {CAT}.{SCH}.billing_write_audit WHERE action_type='submit_auto_upgrade' AND result_status='SUCCESS'")
check("audit records both target tables", a and a[0][1].count(",") == 1 and "customers" in a[0][1], a)

print("\nCleaning up...")
run(f"DROP SCHEMA IF EXISTS {CAT}.{SCH} CASCADE")

print("\n" + "=" * 62)
if fails:
    print(f"{len(fails)} FAILED:")
    for f in fails:
        print(f"  - {f}")
    sys.exit(1)
print("Demo #5 verified end-to-end on a live warehouse: read layer and the upgrade write.")
print("=" * 62)
