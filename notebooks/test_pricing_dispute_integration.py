"""Live integration test for Demo #2 — pricing dispute resolution.

Seeds a throwaway schema from the real plan catalogue, builds the ORDM Product
core, creates the four read functions, then exercises both write actions
including the two-table atomic correction and its rollback. Drops everything.

    python3 test_pricing_dispute_integration.py [--warehouse ID] [--catalog CAT]

Needs the Databricks CLI authenticated. Defaults come from config.yaml.
Verified passing on 2026-08-31.
"""
import argparse, json, os, subprocess, sys, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import write_actions as wa

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
SCH, NOW = "cba_demo2_test", "2026-08-31T12:00:00"
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

import demo2_pricing_sql as D



print("Seeding source tables (billing_plans, customers, invoice)...")
run(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
for t in ["ordm_product","ordm_product_pricing","ordm_customer_contract","invoice","customers","billing_plans"]:
    run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.{t}")

plans = [json.loads(l) for l in open(PLANS_JSON) if l.strip()]
run(f"""CREATE TABLE {CAT}.{SCH}.billing_plans (
  Plan_key BIGINT, Plan_id STRING, Plan_name STRING, contract_in_months BIGINT,
  monthly_charges_dollars DOUBLE) USING DELTA""")
vals = ",".join(f"({p['Plan_key']},'{p['Plan_id']}','{p['Plan_name']}',{p['contract_in_months']},{p['monthly_charges_dollars']})" for p in plans)
run(f"INSERT INTO {CAT}.{SCH}.billing_plans VALUES {vals}")

run(f"""CREATE TABLE {CAT}.{SCH}.customers (customer_id BIGINT, plan BIGINT, contract_start_dt DATE) USING DELTA""")
run(f"""INSERT INTO {CAT}.{SCH}.customers
SELECT 4400 + id AS customer_id, PMOD(id, 10) + 1 AS plan,
       DATE_SUB(CURRENT_DATE(), CAST(400 + id * 7 AS INT)) AS contract_start_dt
FROM RANGE(30)""")

# Invoices billed at the plan's *current* list price for the last 12 months. That
# is exactly how drift arises in the real world: the biller applies today's list
# price, ignoring both the price effective that month and any negotiated rate.
run(f"""CREATE TABLE {CAT}.{SCH}.invoice (
  customer_id BIGINT, event_month STRING, plan_name STRING,
  monthly_charges DOUBLE, total_charges DOUBLE) USING DELTA
  TBLPROPERTIES ('delta.feature.catalogManaged'='supported')""")
run(f"""INSERT INTO {CAT}.{SCH}.invoice
SELECT c.customer_id,
       DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -m.n), 'yyyy-MM') AS event_month,
       p.Plan_name,
       p.monthly_charges_dollars AS monthly_charges,
       p.monthly_charges_dollars + 5.0 AS total_charges
FROM {CAT}.{SCH}.customers c
JOIN {CAT}.{SCH}.billing_plans p ON p.Plan_key = c.plan
CROSS JOIN (SELECT id AS n FROM RANGE(1, 13)) m""")
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.invoice")
print(f"seeded {r[0][0]} invoice rows\n")

print("ORDM Product canonical core")
print("-" * 62)
for f in D.ALL_TABLES:
    ok, det, _ = run(f(CAT, SCH))
    check(f.__name__, ok, det[:220])

r, _ = rows(f"SELECT lifecycle_status, COUNT(*) FROM {CAT}.{SCH}.ordm_product GROUP BY 1 ORDER BY 1")
check("catalogue has both active and deprecated plans", r and len(r) == 2, r)
r, _ = rows(f"SELECT COUNT(*), COUNT(DISTINCT product_id) FROM {CAT}.{SCH}.ordm_product_pricing")
check("two effective-dated price versions per product", r and int(r[0][0]) == int(r[0][1]) * 2, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.ordm_product_pricing WHERE effective_to IS NULL")
check("exactly one current version per product", r and int(r[0][0]) == 10, r)
r, _ = rows(f"""SELECT COUNT(*) FROM {CAT}.{SCH}.ordm_product_pricing a
  JOIN {CAT}.{SCH}.ordm_product_pricing b ON a.product_id=b.product_id AND a.pricing_rule_id<>b.pricing_rule_id
  WHERE a.effective_to >= b.effective_from AND a.effective_from <= b.effective_from""")
check("no overlapping effective periods", r and int(r[0][0]) == 0, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.ordm_customer_contract WHERE negotiated_monthly_price IS NOT NULL")
check("some customers have negotiated rates", r and 0 < int(r[0][0]) < 30, r)

print("\nUC functions")
print("-" * 62)
for f in D.ALL_FUNCTIONS:
    ok, det, _ = run(f(CAT, SCH))
    check(f.__name__, ok, det[:300])

print("\ndetect_pricing_drift — every drift type reachable")
print("-" * 62)
r, _ = rows(f"""
SELECT d.drift_type, COUNT(*) AS n
FROM {CAT}.{SCH}.invoice i
LATERAL VIEW OUTER explode(array(1)) t AS x
JOIN LATERAL (SELECT * FROM {CAT}.{SCH}.detect_pricing_drift(CAST(i.customer_id AS STRING), i.event_month)) d
GROUP BY d.drift_type ORDER BY n DESC""")
if r is None:
    # LATERAL over a table function may not be supported; sample directly instead.
    r2, _ = rows(f"SELECT customer_id, event_month FROM {CAT}.{SCH}.invoice ORDER BY customer_id, event_month LIMIT 60")
    seen = {}
    for cid, em in r2:
        rr, _ = rows(f"SELECT drift_type, drift_detected, variance, explanation FROM {CAT}.{SCH}.detect_pricing_drift('{cid}','{em}')")
        if rr: seen.setdefault(rr[0][0], (cid, em, rr[0][2], rr[0][3]))
    print("  drift types seen across 60 sampled invoices:")
    for k, v in sorted(seen.items()):
        print(f"    {k:<20} e.g. customer {v[0]} {v[1]}  variance={v[2]}")
        print(f"      {v[3][:150]}")
    check("ContractMismatch is reachable", "ContractMismatch" in seen, sorted(seen))
    check("PriceBookMismatch is reachable", "PriceBookMismatch" in seen, sorted(seen))
    check("NoDrift is reachable", "NoDrift" in seen, sorted(seen))
else:
    print("  ", r)

print("\nBoundedness and unknown inputs")
print("-" * 62)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.detect_pricing_drift('4401','2026-07')")
check("drift returns at most one row", r and int(r[0][0]) <= 1, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.detect_pricing_drift('not-a-customer','2026-07')")
check("unknown customer returns empty, not an error", r is not None and int(r[0][0]) == 0, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.lookup_pricing_history('100GB SIM12')")
check("pricing history returns the full series", r and int(r[0][0]) == 2, r)
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.lookup_product_pricing('100GB SIM12','')")
check("product pricing as-of today returns one row", r and int(r[0][0]) == 1, r)
legacy, _ = rows(f"""SELECT DATE_FORMAT(DATE_ADD(effective_from, 30), 'yyyy-MM-dd'), list_price
  FROM {CAT}.{SCH}.ordm_product_pricing pr JOIN {CAT}.{SCH}.ordm_product p USING (product_id)
  WHERE p.product_name = '100GB SIM12' AND pr.is_current = FALSE""")
r, _ = rows(f"SELECT list_price FROM {CAT}.{SCH}.lookup_product_pricing('100GB SIM12','{legacy[0][0]}')")
r2, _ = rows(f"SELECT list_price FROM {CAT}.{SCH}.lookup_product_pricing('100GB SIM12','')")
check("as-of inside the legacy window returns the legacy price",
      r and r2 and r[0][0] != r2[0][0] and float(r[0][0]) == float(legacy[0][1]),
      f"legacy={legacy} as_of={r} today={r2}")
r, _ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.lookup_customer_contract('4401')")
check("contract lookup returns one row", r and int(r[0][0]) == 1, r)

pii = {"customer_name", "email", "phone_number"}
for fn in ["detect_pricing_drift('4401','2026-07')", "lookup_customer_contract('4401')"]:
    r, det = rows(f"SELECT * FROM {CAT}.{SCH}.{fn} LIMIT 0")
    ok, _, d = run(f"SELECT * FROM {CAT}.{SCH}.{fn} LIMIT 1")
    cols = {c["name"].lower() for c in d["manifest"]["schema"]["columns"]} if ok else set()
    check(f"no PII in {fn.split('(')[0]}", not (pii & cols), sorted(cols))




# Audit row shape, mirroring agent.py. Prefixed a_ so no audit column can
# collide with a write action's own marker inside the shared atomic block.
AUDIT_COLUMNS = ["audit_id", "action_type", "target_table", "target_record_id",
    "customer_id", "agent_session_id", "executed_by", "payload_json", "result_status",
    "result_message", "executed_at", "initiating_user", "executing_principal",
    "persona", "request_id", "identity_degraded", "user_groups"]
AUDIT_TYPES = {f"a_{c}": ("BIGINT" if c == "customer_id"
                          else "TIMESTAMP" if c == "executed_at"
                          else "BOOLEAN" if c == "identity_degraded"
                          else "STRING") for c in AUDIT_COLUMNS}
AUDIT_TYPES["a_sql_statement"] = "STRING"


def audit_sql(with_sql):
    cols = list(AUDIT_COLUMNS)
    if with_sql: cols.insert(8, "sql_statement")
    return (f"INSERT INTO {CAT}.{SCH}.billing_write_audit ({', '.join(cols)}) "
            f"VALUES ({', '.join(f':a_{c}' for c in cols)})")

def params_of(bag, types):
    out = []
    for k, v in bag.items():
        t = types.get(k, "STRING")
        out.append({"name": k, "type": t,
                    "value": None if v is None else ("true" if (t=="BOOLEAN" and v) else "false" if t=="BOOLEAN" else str(v))})
    return out

def execute_write(action_name, target_id, customer_id, reason, extra=None,
                  persona="customer_care", level="full", force_failure=False):
    action = wa.get_action(action_name)
    if action is None: return "ERROR: unknown action"
    if not wa.action_permitted(level, action):
        return f"BLOCKED: {persona} has '{level}', needs '{action.min_write_access}'."
    try:
        bag = wa.build_param_bag(action, actor="agent", now=NOW, target_id=target_id,
                                 customer_id=customer_id, reason=reason,
                                 **{k: v for k, v in (extra or {}).items()})
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
    if not ok: return f"ERROR: audit failed: {det[:100]}"

    business = [s.format(catalog=CAT, schema=SCH) for s in action.statements]
    if force_failure:
        business.append(f"UPDATE {CAT}.{SCH}.invoice SET total_charges = 1/0 WHERE customer_id = :customer_id")
    succ = av("SUCCESS", f"{action.action} completed for {record_id}.", "; ".join(business))
    body = ";\n  ".join(business + [audit_sql(True)])
    ok, det, _ = run(f"BEGIN ATOMIC\n  {body};\nEND",
                     params_of({**bag, **succ}, {**wa.PARAM_TYPES, **AUDIT_TYPES}))
    if ok: return f"{action.action} completed for {record_id}."
    msg = f"{action.action} failed for {record_id}: {det}"
    run(audit_sql(True), params_of(av("FAILED", msg[:400], "; ".join(business)), AUDIT_TYPES), "20s")
    return msg

# --- setup ------------------------------------------------------------------
print("Preparing dispute + audit tables...")
CM = "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')"
run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.billing_disputes")
run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.billing_write_audit")
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

# Find a real ContractMismatch to dispute.
drift = None
for cid in range(4400, 4415):
    r, _ = rows(f"SELECT event_month FROM {CAT}.{SCH}.invoice WHERE customer_id={cid} ORDER BY event_month DESC LIMIT 6")
    for (em,) in (r or []):
        d, _ = rows(f"SELECT drift_type, billed_monthly_charge, expected_charge, variance FROM {CAT}.{SCH}.detect_pricing_drift('{cid}','{em}')")
        if d and d[0][0] == "ContractMismatch":
            drift = (cid, em, float(d[0][1]), float(d[0][2]), float(d[0][3])); break
    if drift: break
assert drift, "no ContractMismatch found to drive the test"
CID, EM, BILLED, EXPECTED, VAR = drift
print(f"driving with customer {CID} month {EM}: billed {BILLED}, expected {EXPECTED}, variance {VAR}\n")

print("submit_pricing_dispute")
print("-" * 62)
r = execute_write("submit_pricing_dispute", EM, CID, f"Billed {BILLED}, contract rate {EXPECTED}",
                  extra={"event_month": EM, "disputed_amount": VAR})
check("dispute submitted", r.startswith("submit_pricing_dispute completed"), r[:180])
d, _ = rows(f"SELECT dispute_id, dispute_type, status, disputed_amount_usd, event_month FROM {CAT}.{SCH}.billing_disputes WHERE customer_id={CID}")
check("PRICING_DRIFT dispute row created", d and d[0][1] == "PRICING_DRIFT" and d[0][2] == "OPEN", d)
check("disputed_amount carried through `extra`", d and abs(float(d[0][3]) - VAR) < 0.01, d)
DISPUTE_ID = d[0][0]
a, _ = rows(f"SELECT target_record_id FROM {CAT}.{SCH}.billing_write_audit WHERE action_type='submit_pricing_dispute' LIMIT 1")
check("audited under the new dispute's own id", a and a[0][0] == DISPUTE_ID, f"{a} vs {DISPUTE_ID}")

print("\nPersona gate")
print("-" * 62)
r = execute_write("apply_pricing_correction", DISPUTE_ID, CID, "x",
                  extra={"event_month": EM, "corrected_amount": EXPECTED},
                  persona="finance_ops", level="acknowledge_only")
check("finance_ops blocked from applying a correction", r.startswith("BLOCKED:"), r[:140])

print("\napply_pricing_correction — two tables, one transaction")
print("-" * 62)
before, _ = rows(f"SELECT monthly_charges, total_charges FROM {CAT}.{SCH}.invoice WHERE customer_id={CID} AND event_month='{EM}'")
tot_before = float(before[0][1])
r = execute_write("apply_pricing_correction", DISPUTE_ID, CID, "Contract rate applied",
                  extra={"event_month": EM, "corrected_amount": EXPECTED})
check("correction applied", r.startswith("apply_pricing_correction completed"), r[:180])
d, _ = rows(f"SELECT status, resolved_amount_usd, resolution_notes FROM {CAT}.{SCH}.billing_disputes WHERE dispute_id='{DISPUTE_ID}'")
check("dispute moved to CORRECTED", d and d[0][0] == "CORRECTED", d)
check("resolved_amount is the contract rate", d and abs(float(d[0][1]) - EXPECTED) < 0.01, d)
after, _ = rows(f"SELECT monthly_charges, total_charges FROM {CAT}.{SCH}.invoice WHERE customer_id={CID} AND event_month='{EM}'")
check("invoice monthly_charges corrected", abs(float(after[0][0]) - EXPECTED) < 0.01, after)
check("total_charges adjusted by the delta, not overwritten",
      abs(float(after[0][1]) - (tot_before - BILLED + EXPECTED)) < 0.01,
      f"{tot_before} - {BILLED} + {EXPECTED} = {tot_before-BILLED+EXPECTED}, got {after[0][1]}")
d, _ = rows(f"SELECT target_table FROM {CAT}.{SCH}.billing_write_audit WHERE action_type='apply_pricing_correction' LIMIT 1")
check("audit records both target tables", d and d[0][0].count(",") == 1 and "invoice" in d[0][0], d)

print("\nAtomicity across two tables")
print("-" * 62)
r2, _ = rows(f"SELECT event_month FROM {CAT}.{SCH}.invoice WHERE customer_id={CID} AND event_month<>'{EM}' LIMIT 1")
EM2 = r2[0][0]
run(f"""INSERT INTO {CAT}.{SCH}.billing_disputes VALUES ('DSP-rollback', {CID}, NULL, '{EM2}',
  'PRICING_DRIFT','OPEN','rollback probe',NULL,9.99,NULL,'agent',TIMESTAMP '{NOW}',TIMESTAMP '{NOW}',NULL,NULL)""")
inv_before, _ = rows(f"SELECT monthly_charges, total_charges FROM {CAT}.{SCH}.invoice WHERE customer_id={CID} AND event_month='{EM2}'")
r = execute_write("apply_pricing_correction", "DSP-rollback", CID, "should roll back",
                  extra={"event_month": EM2, "corrected_amount": 1.0}, force_failure=True)
check("failed correction reported failure", r.startswith("apply_pricing_correction failed"), r[:160])
d, _ = rows(f"SELECT status FROM {CAT}.{SCH}.billing_disputes WHERE dispute_id='DSP-rollback'")
check("dispute NOT moved to CORRECTED", d and d[0][0] == "OPEN", d)
inv_after, _ = rows(f"SELECT monthly_charges, total_charges FROM {CAT}.{SCH}.invoice WHERE customer_id={CID} AND event_month='{EM2}'")
check("invoice unchanged — both tables rolled back together", inv_before == inv_after, f"{inv_before} -> {inv_after}")
a, _ = rows(f"SELECT result_status FROM {CAT}.{SCH}.billing_write_audit WHERE target_record_id='DSP-rollback' ORDER BY result_status")
check("PENDING resolved to FAILED", sorted(x[0] for x in a) == ["FAILED","PENDING"], a)

print("\nCleaning up...")
run(f"DROP SCHEMA IF EXISTS {CAT}.{SCH} CASCADE")

print("\n" + "=" * 62)
if fails:
    print(f"{len(fails)} FAILED:")
    for f in fails:
        print(f"  - {f}")
    sys.exit(1)
print("Demo #2 verified end-to-end on a live warehouse: read layer and both writes.")
print("=" * 62)
