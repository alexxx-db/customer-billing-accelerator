"""Live integration test for Demos #3 and #4 — order-to-cash and store intelligence.

Seeds a throwaway schema, derives the ORDM Order, Fulfilment and Payment cores
from it, then builds the Store core on top and checks the POS reconciliation.
Drops everything.

    python3 test_order_store_integration.py [--warehouse ID] [--catalog CAT]

Needs the Databricks CLI authenticated. Defaults come from config.yaml.
Verified passing on 2026-09-01.
"""
import argparse, json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import demo3_order_sql as D3
import demo4_store_sql as D4

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
SCH = "cba_demo34_test"
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



print("Seeding sources...")
run(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{SCH}")
for t in ["store_billing_intelligence","pos_transaction","ordm_store_hierarchy","ordm_store",
          "ordm_payment","ordm_fulfillment","ordm_order_line_item","ordm_order_header",
          "ordm_customer","billing_disputes","invoice","billing_items","customers","billing_plans"]:
    run(f"DROP TABLE IF EXISTS {CAT}.{SCH}.{t}")

plans=[json.loads(l) for l in open(PLANS_JSON) if l.strip()]
run(f"""CREATE TABLE {CAT}.{SCH}.billing_plans (Plan_key BIGINT, Plan_id STRING, Plan_name STRING,
  contract_in_months BIGINT, monthly_charges_dollars DOUBLE) USING DELTA""")
run(f"INSERT INTO {CAT}.{SCH}.billing_plans VALUES " + ",".join(
  f"({p['Plan_key']},'{p['Plan_id']}','{p['Plan_name']}',{p['contract_in_months']},{p['monthly_charges_dollars']})" for p in plans))

run(f"CREATE TABLE {CAT}.{SCH}.customers (customer_id BIGINT, device_id BIGINT, plan BIGINT, contract_start_dt DATE) USING DELTA")
run(f"""INSERT INTO {CAT}.{SCH}.customers
SELECT 4400+id, 9860000000+id*137, PMOD(id,10)+1, DATE_SUB(CURRENT_DATE(), CAST(200+id*23 AS INT)) FROM RANGE(24)""")

# Demo #1's ordm_customer, reduced to the region column Demos #3/#4 depend on.
run(f"""CREATE TABLE {CAT}.{SCH}.ordm_customer USING DELTA AS
SELECT customer_id,
  CASE PMOD(HASH(customer_id,'region'),4) WHEN 0 THEN 'West' WHEN 1 THEN 'East'
       WHEN 2 THEN 'Central' ELSE 'International' END AS region
FROM {CAT}.{SCH}.customers""")

# Most customers activate; a few never do, so NotFulfilled is reachable.
run(f"""CREATE TABLE {CAT}.{SCH}.billing_items (device_id BIGINT, event_type STRING,
  minutes DOUBLE, bytes_transferred BIGINT, event_ts TIMESTAMP, contract_start_dt DATE) USING DELTA""")
run(f"""INSERT INTO {CAT}.{SCH}.billing_items
SELECT c.device_id, 'data_local', 0.0, 104857600,
       CAST(DATE_ADD(c.contract_start_dt, CAST(2 + PMOD(HASH(c.customer_id,'act'),9) AS INT)) AS TIMESTAMP),
       c.contract_start_dt
FROM {CAT}.{SCH}.customers c WHERE PMOD(c.customer_id, 8) <> 0""")

# Most activated customers get invoiced; a couple never do -> revenue leakage.
run(f"""CREATE TABLE {CAT}.{SCH}.invoice (customer_id BIGINT, event_month STRING,
  plan_name STRING, monthly_charges DOUBLE, total_charges DOUBLE) USING DELTA""")
run(f"""INSERT INTO {CAT}.{SCH}.invoice
SELECT c.customer_id, DATE_FORMAT(ADD_MONTHS(CURRENT_DATE(), -m.n), 'yyyy-MM'),
       p.Plan_name, p.monthly_charges_dollars, p.monthly_charges_dollars + 4.0
FROM {CAT}.{SCH}.customers c
JOIN {CAT}.{SCH}.billing_plans p ON p.Plan_key = c.plan
CROSS JOIN (SELECT id AS n FROM RANGE(1,7)) m
WHERE PMOD(c.customer_id, 8) <> 0 AND PMOD(c.customer_id, 11) <> 3""")

run(f"""CREATE TABLE {CAT}.{SCH}.billing_disputes (dispute_id STRING, customer_id BIGINT,
  dispute_type STRING, status STRING, created_at TIMESTAMP) USING DELTA""")
run(f"""INSERT INTO {CAT}.{SCH}.billing_disputes
SELECT CONCAT('DSP-',CAST(customer_id AS STRING)), customer_id, 'AGENT_CREATED','OPEN', CURRENT_TIMESTAMP()
FROM {CAT}.{SCH}.customers WHERE PMOD(customer_id, 5) = 0""")
r,_ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.invoice"); print(f"seeded {r[0][0]} invoices\n")

print("Demo #3 — order-to-cash tables")
print("-" * 66)
for f in D3.ALL_TABLES:
    ok, det, _ = run(f(CAT, SCH)); check(f.__name__, ok, det[:260])
r,_ = rows(f"SELECT fulfillment_status, COUNT(*) FROM {CAT}.{SCH}.ordm_fulfillment GROUP BY 1 ORDER BY 1")
check("both fulfilled and never-fulfilled orders exist", r and len(r)==2, r)
r,_ = rows(f"SELECT payment_status, COUNT(*) FROM {CAT}.{SCH}.ordm_payment GROUP BY 1 ORDER BY 1")
check("both settled and unpaid invoices exist", r and len(r)==2, r)
r,_ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.ordm_order_line_item")
check("order lines generated", r and int(r[0][0]) > 0, r)

print("\nDemo #3 — functions")
print("-" * 66)
for f in D3.ALL_FUNCTIONS:
    ok, det, _ = run(f(CAT, SCH)); check(f.__name__, ok, det[:300])

seen = {}
for cid in range(4400, 4424):
    rr,_ = rows(f"SELECT stage, order_id, unbilled_amount, unpaid_amount, explanation FROM {CAT}.{SCH}.reconcile_order_to_cash('{cid}')")
    if rr: seen.setdefault(rr[0][0], rr[0])
for k,v in sorted(seen.items()):
    print(f"    {k:<20} {v[1]}")
    print(f"      {v[4][:130]}")
check("AwaitingFulfilment reachable", "AwaitingFulfilment" in seen, sorted(seen))
check("FulfilledNotBilled reachable", "FulfilledNotBilled" in seen, sorted(seen))
check("at least one collected-or-outstanding stage reachable",
      {"BilledNotCollected","Collected"} & set(seen), sorted(seen))

r,_ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.reconcile_order_to_cash('4401')")
check("reconcile returns at most one row", r and int(r[0][0]) <= 1, r)
r,_ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.reconcile_order_to_cash('not-an-order')")
check("unknown order returns empty, not an error", r is not None and int(r[0][0]) == 0, r)
r,_ = rows(f"SELECT COUNT(*), SUM(leakage_amount) FROM {CAT}.{SCH}.detect_revenue_leakage(0)")
check("revenue leakage detected", r and int(r[0][0]) > 0, r)
if r: print(f"    leakage: {r[0][0]} orders, ${r[0][1]}")
r,_ = rows(f"SELECT region, dso_days, collection_rate_pct, unpaid_amount FROM {CAT}.{SCH}.lookup_dso_by_region()")
check("DSO reported per region", r and len(r) > 0 and all(x[1] is not None for x in r), r)
for x in (r or []): print(f"    {x[0]:<15} DSO {x[1]:>5} days   collection {x[2]}%   unpaid ${x[3]}")

print("\nDemo #4 — store intelligence tables")
print("-" * 66)
for f in D4.ALL_TABLES:
    ok, det, _ = run(f(CAT, SCH)); check(f.__name__, ok, det[:280])
r,_ = rows(f"SELECT COUNT(*), COUNT(DISTINCT region) FROM {CAT}.{SCH}.ordm_store")
check("stores created across regions", r and int(r[0][0]) == 12 and int(r[0][1]) > 1, r)
r,_ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.pos_transaction")
check("POS transactions generated", r and int(r[0][0]) > 0, r)

print("\nDemo #4 — functions")
print("-" * 66)
for f in D4.ALL_FUNCTIONS:
    ok, det, _ = run(f(CAT, SCH)); check(f.__name__, ok, det[:300])

r,_ = rows(f"SELECT region, store_id, billed_total, rank_in_region FROM {CAT}.{SCH}.compare_stores_by_region('')")
check("stores ranked within region", r and len(r) > 0 and any(int(x[3])==1 for x in r), r)
r,_ = rows(f"SELECT gap_type, COUNT(*) FROM {CAT}.{SCH}.detect_pos_reconciliation_gap('') GROUP BY 1 ORDER BY 2 DESC")
check("POS reconciliation gaps detected", r and len(r) > 0, r)
for x in (r or []): print(f"    {x[0]:<18} {x[1]}")
rr,_ = rows(f"SELECT explanation FROM {CAT}.{SCH}.detect_pos_reconciliation_gap('') LIMIT 1")
if rr: print(f"      {rr[0][0][:140]}")
a_store,_ = rows(f"SELECT store_id FROM {CAT}.{SCH}.ordm_store LIMIT 1")
if not a_store:
    print("  cannot continue: no stores were created"); sys.exit(1)
SID = a_store[0][0]
r,_ = rows(f"SELECT COUNT(*) FROM {CAT}.{SCH}.lookup_store_profile('{SID}')")
check("store profile returns one row", r and int(r[0][0]) == 1, r)
r,_ = rows(f"SELECT COUNT(*), SUM(CASE WHEN is_requested_store THEN 1 ELSE 0 END) FROM {CAT}.{SCH}.lookup_store_hierarchy('{SID}')")
check("district returns members with the requested store flagged once",
      r and int(r[0][0]) > 0 and int(r[0][1]) == 1, r)

print("\nBoundedness and PII")
print("-" * 66)
pii = {"customer_name","email","phone_number"}
for call in ["reconcile_order_to_cash('4401')", "detect_revenue_leakage(0)", "lookup_dso_by_region()",
             "lookup_order_line_items('ORD-0000004401')", f"lookup_store_profile('{SID}')",
             "compare_stores_by_region('')", "detect_pos_reconciliation_gap('')",
             f"lookup_store_hierarchy('{SID}')"]:
    ok,_,d = run(f"SELECT * FROM {CAT}.{SCH}.{call} LIMIT 1")
    cols = {c["name"].lower() for c in d["manifest"]["schema"]["columns"]} if ok else set()
    check(f"no PII in {call.split('(')[0]}", ok and not (pii & cols), sorted(cols))

print("\nCleaning up...")
run(f"DROP SCHEMA IF EXISTS {CAT}.{SCH} CASCADE")

print("\n" + "=" * 66)
if fails:
    print(f"{len(fails)} FAILED:")
    for f in fails:
        print(f"  - {f}")
    sys.exit(1)
print("Demos #3 and #4 verified end-to-end on a live warehouse.")
print("=" * 66)
