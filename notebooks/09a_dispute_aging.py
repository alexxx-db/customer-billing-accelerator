# Databricks notebook source
# MAGIC %md
# MAGIC # Dispute Aging: Auto-Escalation
# MAGIC
# MAGIC Checks for disputes that have been OPEN or UNDER_REVIEW for more than 5 days
# MAGIC and auto-escalates them. Writes audit records for each escalation.
# MAGIC
# MAGIC Runs as Task 5 in the daily monitoring workflow.
# MAGIC
# MAGIC NOTE: This notebook does NOT import from agent.py. Write logic is self-contained
# MAGIC using w.statement_execution.execute_statement() directly (Databricks notebook pattern).

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

import uuid
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

catalog = config['catalog']
schema = config['database']
warehouse_id = config.get('warehouse_id', '')

if not warehouse_id:
    print("ERROR: warehouse_id not configured. Cannot execute writes.")
    dbutils.notebook.exit("no_warehouse_id")

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Find Overdue Disputes
overdue_resp = w.statement_execution.execute_statement(
    statement=f"""
        SELECT dispute_id, customer_id, dispute_type, status, updated_at
        FROM {catalog}.{schema}.billing_disputes
        WHERE status IN ('OPEN', 'UNDER_REVIEW')
          AND updated_at < CURRENT_TIMESTAMP - INTERVAL 5 DAYS
        ORDER BY updated_at ASC
    """,
    warehouse_id=warehouse_id,
    wait_timeout="30s",
)

if overdue_resp.status.state != StatementState.SUCCEEDED:
    print(f"ERROR querying overdue disputes: {overdue_resp.status.error}")
    dbutils.notebook.exit("query_failed")

rows = overdue_resp.result.data_array if overdue_resp.result and overdue_resp.result.data_array else []
print(f"Overdue disputes found: {len(rows)}")

if not rows:
    print("No overdue disputes. Aging check complete.")
    dbutils.notebook.exit("no_overdue")

# COMMAND ----------

# DBTITLE 1,Auto-Escalate Overdue Disputes
escalated = 0

for row in rows:
    dispute_id = row[0]
    customer_id = row[1]
    now_ts = datetime.now(timezone.utc).isoformat()
    audit_id = str(uuid.uuid4())

    # Audit record: PENDING
    w.statement_execution.execute_statement(
        statement=f"""
            INSERT INTO {catalog}.{schema}.billing_write_audit
            (audit_id, action_type, target_table, target_record_id, customer_id,
             agent_session_id, executed_by, payload_json, sql_statement,
             result_status, result_message, error_detail, executed_at)
            VALUES ('{audit_id}', 'AUTO_ESCALATE',
            '{catalog}.{schema}.billing_disputes', '{dispute_id}',
            {customer_id if customer_id else 'NULL'}, NULL,
            'dispute_aging_job', NULL, NULL, 'PENDING',
            'Auto-escalation for exceeding 5-day SLA.', NULL,
            TIMESTAMP '{now_ts}')
        """,
        warehouse_id=warehouse_id, wait_timeout="10s")

    # Business write: escalate
    try:
        resp = w.statement_execution.execute_statement(
            statement=f"""
                UPDATE {catalog}.{schema}.billing_disputes
                SET status = 'ESCALATED',
                    resolution_notes = 'Auto-escalated: exceeded 5-day SLA without resolution.',
                    updated_at = TIMESTAMP '{now_ts}'
                WHERE dispute_id = '{dispute_id}'
                  AND status IN ('OPEN', 'UNDER_REVIEW')
            """,
            warehouse_id=warehouse_id, wait_timeout="15s")

        status = "SUCCESS" if resp.status.state == StatementState.SUCCEEDED else "FAILED"
    except Exception as e:
        status = "FAILED"

    # Audit record: result
    w.statement_execution.execute_statement(
        statement=f"""
            INSERT INTO {catalog}.{schema}.billing_write_audit
            (audit_id, action_type, target_table, target_record_id, customer_id,
             agent_session_id, executed_by, payload_json, sql_statement,
             result_status, result_message, error_detail, executed_at)
            VALUES ('{str(uuid.uuid4())}', 'AUTO_ESCALATE',
            '{catalog}.{schema}.billing_disputes', '{dispute_id}',
            {customer_id if customer_id else 'NULL'}, NULL,
            'dispute_aging_job', NULL, NULL, '{status}',
            'Escalation {"completed" if status == "SUCCESS" else "failed"}.', NULL,
            TIMESTAMP '{datetime.now(timezone.utc).isoformat()}')
        """,
        warehouse_id=warehouse_id, wait_timeout="10s")

    if status == "SUCCESS":
        escalated += 1

print(f"\nEscalated {escalated}/{len(rows)} overdue disputes.")
