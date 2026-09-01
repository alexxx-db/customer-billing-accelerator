# Databricks notebook source
# MAGIC %md
# MAGIC # Spike 0 — `BEGIN ATOMIC` preflight for the write path
# MAGIC
# MAGIC `agent.py` runs a write action's statements and its audit resolution row together
# MAGIC inside one `BEGIN ATOMIC` block, sent through the Statement Execution API. This
# MAGIC notebook proves that works on a given workspace before you deploy to it.
# MAGIC
# MAGIC ## Result on the Entrada workspace, 31 Aug 2026
# MAGIC
# MAGIC | Question | Answer |
# MAGIC |---|---|
# MAGIC | Do typed parameter markers bind inside `BEGIN ATOMIC`? | **Yes** — use `writeback_param_mode: parameters` |
# MAGIC | Do escaped literals work as a fallback? | Yes |
# MAGIC | Does the block roll back on a runtime failure? | Yes — verified with a genuine cast overflow, not an analysis error |
# MAGIC | Any prerequisite? | **Yes.** Every target table needs `delta.feature.catalogManaged`. Without it: `TRANSACTION_NOT_SUPPORTED.WRITE_NON_CATALOG_MANAGED_TABLE` |
# MAGIC | Can existing tables be upgraded? | Yes, in place, with data preserved |
# MAGIC
# MAGIC `09_writeback_setup` enables the feature on every write target. Re-run this
# MAGIC notebook against any new workspace (staging, prod) before deploying there.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Setup
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

w = WorkspaceClient()

CATALOG = config['catalog']
SCHEMA = config['database']
WAREHOUSE_ID = config['warehouse_id']
SCRATCH = f"{CATALOG}.{SCHEMA}.spike0_atomic_scratch"

assert WAREHOUSE_ID, "warehouse_id must be set in config.yaml"

# Preflight: config.yaml has previously carried a warehouse id that did not exist
# in the target workspace, which fails only at the moment a user confirms a write.
try:
    wh = w.warehouses.get(WAREHOUSE_ID)
    print(f"Warehouse: {wh.name} ({wh.state})")
except Exception as e:
    raise AssertionError(
        f"warehouse_id '{WAREHOUSE_ID}' is not usable in this workspace: {e}\n"
        f"Available: " + ", ".join(f"{x.id} {x.name}" for x in w.warehouses.list())
    )

print(f"Scratch table: {SCRATCH}")


def run(statement, parameters=None, timeout="50s"):
    """Send a statement; return (succeeded, detail)."""
    try:
        resp = w.statement_execution.execute_statement(
            statement=statement, parameters=parameters,
            warehouse_id=WAREHOUSE_ID, wait_timeout=timeout,
        )
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
    if resp.status and resp.status.state == StatementState.SUCCEEDED:
        return True, "SUCCEEDED"
    detail = (resp.status.error.message if resp.status and resp.status.error
              else str(resp.status.state if resp.status else "?"))
    return False, detail


def count(table=None):
    resp = w.statement_execution.execute_statement(
        statement=f"SELECT COUNT(*) FROM {table or SCRATCH}",
        warehouse_id=WAREHOUSE_ID, wait_timeout="50s")
    return int(resp.result.data_array[0][0])

# COMMAND ----------

# DBTITLE 1,Prerequisite — BEGIN ATOMIC requires catalog commits
# Demonstrates the requirement rather than assuming it, so the failure mode is
# recognisable if it ever reappears on another workspace.
PLAIN = f"{SCRATCH}_plain"
run(f"DROP TABLE IF EXISTS {PLAIN}")
run(f"CREATE TABLE {PLAIN} (id STRING, amount BIGINT) USING DELTA")
run(f"INSERT INTO {PLAIN} VALUES ('pre-existing', 1)")

ok, detail = run(f"BEGIN ATOMIC INSERT INTO {PLAIN} VALUES ('a', 1); END")
print(f"without catalogManaged : {'unexpectedly allowed' if ok else 'refused as expected'}")
print(f"  {detail[:150]}")

ok_alter, detail_alter = run(
    f"ALTER TABLE {PLAIN} SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')")
ok_after, detail_after = run(f"BEGIN ATOMIC INSERT INTO {PLAIN} VALUES ('b', 2); END")
upgrade_works = ok_alter and ok_after and count(PLAIN) == 2

print(f"in-place upgrade       : {'PASS — data preserved' if upgrade_works else 'FAIL'}")
if not upgrade_works:
    print(f"  alter: {detail_alter[:120]}\n  after: {detail_after[:120]}")
run(f"DROP TABLE IF EXISTS {PLAIN}")

# COMMAND ----------

# DBTITLE 1,Create the scratch table with catalog commits enabled
run(f"DROP TABLE IF EXISTS {SCRATCH}")
ok, detail = run(f"""
CREATE TABLE {SCRATCH} (id STRING, amount BIGINT, note STRING, at TIMESTAMP, flag BOOLEAN)
USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')
""")
assert ok, f"Could not create scratch table: {detail}"
print("scratch table created with catalogManaged")

# COMMAND ----------

# DBTITLE 1,Test A — typed parameter markers inside BEGIN ATOMIC
params = [
    StatementParameterListItem(name="p_id", value="A-1", type="STRING"),
    StatementParameterListItem(name="p_amount", value="42", type="BIGINT"),
    StatementParameterListItem(name="p_note", value="it's parameterised", type="STRING"),
    StatementParameterListItem(name="p_at", value="2026-08-31T12:00:00", type="TIMESTAMP"),
    StatementParameterListItem(name="p_flag", value="true", type="BOOLEAN"),
]
before = count()
ok_a, detail_a = run(f"""BEGIN ATOMIC
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  VALUES (:p_id, :p_amount, :p_note, :p_at, :p_flag);
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  VALUES (:p_id, :p_amount, 'second statement', :p_at, :p_flag);
END""", parameters=params)
test_a = ok_a and (count() - before) == 2
print(f"Test A : {'PASS' if test_a else 'FAIL'} — {detail_a[:180]}")

# COMMAND ----------

# DBTITLE 1,Test B — escaped literals inside BEGIN ATOMIC (the fallback)
note_b = "it's a literal".replace("'", "''")
before = count()
ok_b, detail_b = run(f"""BEGIN ATOMIC
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  VALUES ('B-1', 7, '{note_b}', TIMESTAMP '2026-08-31T12:00:00', true);
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  VALUES ('B-2', 8, 'second statement', TIMESTAMP '2026-08-31T12:00:00', false);
END""")
test_b = ok_b and (count() - before) == 2
print(f"Test B : {'PASS' if test_b else 'FAIL'} — {detail_b[:180]}")

# COMMAND ----------

# DBTITLE 1,Test C — genuine runtime failure must roll back
# A statement that fails at *analysis* time proves nothing: the block never runs.
# This one type-checks and fails during execution, which is the case that matters.
before = count()
ok_c, detail_c = run(f"""BEGIN ATOMIC
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  VALUES ('C-1', 1, 'should roll back', TIMESTAMP '2026-08-31T12:00:00', true);
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  SELECT 'C-2', 1/0, 'forces a runtime failure', TIMESTAMP '2026-08-31T12:00:00', true;
END""")
leaked = count() - before
test_c = (not ok_c) and leaked == 0
print(f"Test C : block {'failed as intended' if not ok_c else 'UNEXPECTEDLY SUCCEEDED'}")
print(f"         {detail_c[:150]}")
print(f"         rollback {'PASS — nothing persisted' if leaked == 0 else f'FAIL — {leaked} row(s) leaked'}")

# COMMAND ----------

# DBTITLE 1,Test D — positive control: a clean block commits
before = count()
ok_d, detail_d = run(f"""BEGIN ATOMIC
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  VALUES ('D-1', 1, 'clean', TIMESTAMP '2026-08-31T12:00:00', true);
  INSERT INTO {SCRATCH} (id, amount, note, at, flag)
  VALUES ('D-2', 2, 'clean', TIMESTAMP '2026-08-31T12:00:00', true);
END""")
test_d = ok_d and (count() - before) == 2
print(f"Test D : {'PASS — commit confirmed' if test_d else f'FAIL — {detail_d[:150]}'}")

# COMMAND ----------

# DBTITLE 1,Verdict
print("=" * 66)
mode = "parameters" if test_a else ("literals" if test_b else None)
if test_a:
    print("Parameter markers DO bind inside BEGIN ATOMIC.")
elif test_b:
    print("Markers do NOT bind inside the block; escaped literals do.")
else:
    print("BEGIN ATOMIC is unusable here. Check the warehouse and DBR version.")

if not test_c:
    print("\nSTOP: the block did not roll back on a runtime failure.")
    print("Do not deploy the write path — durability is the whole point.")
if not test_d:
    print("\nSTOP: a clean block did not commit.")
if not upgrade_works:
    print("\nWARNING: existing tables could not be upgraded to catalogManaged.")
    print("09_writeback_setup will fail to enable catalog commits.")

if mode and test_c and test_d:
    print(f"\n  config.yaml -> writeback_param_mode: {mode}")
    print("  Then run 09_writeback_setup to enable catalog commits on write targets.")
print("=" * 66)

# COMMAND ----------

# DBTITLE 1,Clean up
run(f"DROP TABLE IF EXISTS {SCRATCH}")
print("scratch table dropped")
