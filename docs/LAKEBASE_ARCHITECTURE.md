# Lakebase Architecture: Billing Accelerator

## Executive Summary

- The current repo implements Lakehouse Federation (Track A) and local simulation (Track B) for ERP data. **It does not implement Lakebase.** These are different products.
- Lakebase is Databricks-managed PostgreSQL for low-latency transactional CRUD. Federation is query pushdown to external databases. They serve different purposes.
- **Recommended role**: Lakebase as the **operational write-back store** for disputes, agent actions, workflow state, and customer service transactions. Delta remains the analytical surface. Synced tables bridge the two.
- This gives the accelerator a clean architectural split: **Lakebase = transactional OLTP**, **Delta = analytical OLAP**, **Genie = conversational analytics over Delta**, **Agent = orchestration layer**.
- The current write-back architecture (Statement Execution API writing to Delta tables) works but is architecturally wrong: it uses an analytical engine (SQL warehouse) for transactional single-row writes. Lakebase is the correct backend for this workload.
- Lakebase is added as **Track C** — a parallel operational layer, not a replacement for Track A or B. The ext_* view abstraction is preserved for ERP data.
- The Dash app should expose Lakebase-backed dispute workflows directly, showing real-time transactional state alongside analytical summaries from Delta/Genie.
- Synced tables (Delta-to-Lakebase reverse ETL) make billing reference data available to the Lakebase layer for foreign-key joins without duplicating the medallion pipeline.
- Identity propagation extends to Lakebase: every write records `initiating_user` and `executing_principal`, matching the existing audit pattern.
- **Minimum viable POC**: Provision Lakebase instance, create disputes + audit tables in PostgreSQL, modify `_execute_write` in agent.py to write to Lakebase via psycopg2, sync dispute data back to Delta for Genie analytics.

---

## 1. Current-State Assessment

### What exists today

| Component | What it does | Backend |
|---|---|---|
| Track A (`08_federation_setup.py`) | UC connection + foreign catalog to external PostgreSQL ERP | Lakehouse Federation |
| Track B (`08a_erp_data_simulation.py`) | Synthetic ERP data in Delta + `ext_*` view abstraction | Delta tables |
| `08b_external_data_ingestion.py` | Silver/Gold medallion transforms from `ext_*` views | Delta tables |
| `09_writeback_setup.py` | Creates `billing_disputes` and `billing_write_audit` Delta tables | Delta tables |
| `agent.py _execute_write()` | Writes disputes/audit via Statement Execution API | SQL warehouse -> Delta |
| `09a_dispute_aging.py` | Nightly SLA enforcement via Statement Execution API | SQL warehouse -> Delta |

### Why this is architecturally suboptimal

The current write-back path uses a **SQL warehouse** (an analytical engine optimized for large scans) to execute **single-row INSERT/UPDATE** operations (a transactional workload). This means:

1. **Latency**: Each write takes 1-3s because the SQL warehouse has query planning overhead designed for analytical workloads, not point writes.
2. **Cost**: The SQL warehouse must be running to accept writes. It's shared with Genie analytics queries, causing contention (PM-010).
3. **Concurrency**: Delta tables handle concurrent single-row writes via optimistic concurrency control, which can produce conflicts under load.
4. **No real transactions**: Delta doesn't support multi-statement ACID transactions across tables. A dispute + audit write is two separate operations that can partially fail.

Lakebase (managed PostgreSQL) is purpose-built for exactly this workload: low-latency, single-row, transactional CRUD with ACID guarantees.

### Implementation Status (Updated April 2026)

The following items have been implemented:
- `08c_lakebase_setup.py` (378 lines) — provisions instance, bootstraps PostgreSQL schema
- `08d_lakebase_sync.py` (180 lines) — synced tables from Lakebase to Delta
- `08e_validate_lakebase.py` (258 lines) — 7-check validation suite
- `config.yaml` — 10 Lakebase configuration keys
- Gradio app — Data Integration tab with live Lakebase connectivity check
- `requirements.txt` — `psycopg2-binary` added to both apps

**Remaining**: Agent write-path migration (`_execute_write` in agent.py to use
psycopg2 when Lakebase is available) is designed but not yet wired — writes
currently fall back to Statement Execution API -> Delta.

---

## 2. Lakebase Role Recommendation

### Candidates evaluated

| Role | Pros | Cons | Fit | Recommendation |
|---|---|---|---|---|
| **A. Operational write-back store** (disputes, audit, approvals) | Correct engine for transactional writes; ACID; low latency; eliminates SQL warehouse dependency for writes | Requires sync to Delta for analytics; new dependency | Excellent — directly addresses PM-010 (warehouse contention) and the architectural mismatch | **RECOMMENDED** |
| B. Application backend for all CRUD | Full transactional app layer; clean separation | Over-scoped — most data is analytical, not transactional | Moderate | Use selectively |
| C. ERP-style operational entities | Could replace Track B simulation with real PostgreSQL data | Confuses Lakebase with Federation; Track B already works | Low | Don't use for this |
| D. Sync source into Delta | Reverse ETL from Lakebase to Delta for analytics | Useful as a bridge pattern, not a primary role | Complementary | Use alongside A |
| E. Governed transactional complement | Adds OLTP to the OLAP stack | Correct framing | High | This IS role A stated architecturally |

### Preferred role: **Operational write-back store with sync to Delta**

Lakebase owns the transactional lifecycle of:
- Billing disputes (create, update status, resolve)
- Write audit trail (immutable append)
- Agent action log (what the agent did, when, for whom)
- Workflow approvals (pending -> approved -> executed)

Delta owns the analytical lifecycle:
- Billing data (invoices, plans, items)
- Anomaly detection results
- Revenue attribution and finance summaries
- Genie Space analytics surface

**Synced tables** bridge the two: dispute data in Lakebase is synced to Delta so Genie can answer "how many disputes are open?" without querying Lakebase directly.

---

## 3. Target Architecture

```
                    Databricks App (Dash)
                    ┌─────────────────────────────┐
                    │  Chat UI    Dispute Tracker  │
                    │     │            │           │
                    │     v            v           │
                    │  Model       Lakebase        │
                    │  Serving     (psycopg2)      │
                    └─────┬───────────┬───────────┘
                          │           │
            ┌─────────────┤           │
            v             v           v
    ┌──────────────┐  ┌────────┐  ┌──────────────────┐
    │ LangGraph    │  │ Genie  │  │ Lakebase (PgSQL)  │
    │ Agent        │  │ Space  │  │                    │
    │              │  │        │  │ billing_disputes   │
    │ _execute_    │  │ reads  │  │ billing_audit      │
    │  write() ────┼──┼────────┼─>│ agent_actions      │
    │              │  │ Delta  │  │ workflow_approvals  │
    └──────┬───────┘  │ tables │  └────────┬───────────┘
           │          └────────┘           │
           v                    Synced Tables (reverse ETL)
    ┌──────────────────────────────────────┼───────┐
    │         Unity Catalog (Delta)        v       │
    │                                              │
    │  billing_items, customers, invoice           │
    │  billing_anomalies                           │
    │  gold_revenue_attribution                    │
    │  billing_disputes_synced  <── from Lakebase  │
    │  billing_audit_synced     <── from Lakebase  │
    └──────────────────────────────────────────────┘
```

### What lives where

| Entity | Backend | Why |
|---|---|---|
| Billing disputes | **Lakebase** | Transactional lifecycle (create -> update -> resolve). Needs ACID, low latency, multi-statement transactions. |
| Write audit trail | **Lakebase** | Immutable append-only log. Benefits from PostgreSQL's WAL guarantees. Currently 2-INSERT pattern would become a single transaction. |
| Agent actions/tickets | **Lakebase** | Workflow state with status transitions. |
| Invoices, billing items | **Delta** | Analytical data. Scanned by Genie, aggregated in Gold tables. |
| Customers, plans | **Delta** | Reference data. Synced TO Lakebase for FK joins if needed. |
| Anomalies | **Delta** | Batch-produced by PySpark pipeline. Read-heavy, write-infrequent. |
| Revenue attribution | **Delta** | Analytical Gold table. |
| Disputes (for Genie) | **Delta (synced)** | Lakebase disputes synced to Delta so Genie can include them in analytics. |

### Track C alongside Track A/B

Track C (Lakebase) is NOT a replacement for Track A (Federation) or Track B (Simulation). Those tracks provide ERP read data. Track C provides operational write infrastructure.

```
Track A: External ERP → Federation → ext_* views → Silver/Gold (Delta)    [ERP reads]
Track B: Simulated ERP → Delta tables → ext_* views → Silver/Gold (Delta) [ERP reads]
Track C: Lakebase → disputes, audit, actions → synced to Delta            [Operational writes]
```

---

## 4. Data Model

### Lakebase Schema: `billing_ops`

#### `disputes`
| Column | Type | Purpose |
|---|---|---|
| `dispute_id` | `VARCHAR(36) PRIMARY KEY` | UUID, generated by agent |
| `customer_id` | `BIGINT NOT NULL` | FK to customer (validated, not enforced cross-system) |
| `anomaly_id` | `VARCHAR(64)` | Optional link to billing anomaly |
| `dispute_type` | `VARCHAR(32) NOT NULL` | AGENT_CREATED, CUSTOMER_INITIATED, SYSTEM_DETECTED |
| `status` | `VARCHAR(32) NOT NULL` | OPEN, UNDER_REVIEW, ESCALATED, RESOLVED_CREDIT, RESOLVED_NO_ACTION, CLOSED |
| `description` | `TEXT NOT NULL` | Reason for dispute |
| `resolution_notes` | `TEXT` | How it was resolved |
| `disputed_amount_usd` | `NUMERIC(12,2)` | Amount in question |
| `resolved_amount_usd` | `NUMERIC(12,2)` | Credit issued |
| `created_by` | `VARCHAR(128) NOT NULL` | Initiating user email or 'agent' |
| `assigned_to` | `VARCHAR(128)` | Current owner |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Creation time |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Last update |
| `resolved_at` | `TIMESTAMPTZ` | Resolution time |
| `session_id` | `VARCHAR(64)` | Agent conversation session |
| `request_id` | `VARCHAR(64)` | Identity propagation request ID |

**Write pattern**: INSERT on create, UPDATE on status change. ~10-50 writes/day.
**Read pattern**: Point reads by dispute_id or customer_id. Low volume.
**Sync**: CONTINUOUS to Delta `billing_disputes_synced` for Genie analytics.

#### `audit_log`
| Column | Type | Purpose |
|---|---|---|
| `audit_id` | `VARCHAR(36) PRIMARY KEY` | UUID |
| `action_type` | `VARCHAR(64) NOT NULL` | acknowledge_anomaly, create_dispute, update_dispute_status |
| `target_table` | `VARCHAR(128) NOT NULL` | Which table was affected |
| `target_record_id` | `VARCHAR(64)` | Which record |
| `customer_id` | `BIGINT` | Affected customer |
| `initiating_user` | `VARCHAR(128) NOT NULL` | Human who triggered |
| `executing_principal` | `VARCHAR(128) NOT NULL` | SP that executed |
| `persona` | `VARCHAR(32)` | Active persona |
| `request_id` | `VARCHAR(64)` | From RequestContext |
| `session_id` | `VARCHAR(64)` | Agent session |
| `sql_statement` | `TEXT` | What was executed |
| `result_status` | `VARCHAR(16) NOT NULL` | PENDING, SUCCESS, FAILED |
| `result_message` | `TEXT` | Result detail |
| `identity_degraded` | `BOOLEAN DEFAULT FALSE` | True if no user context |
| `user_groups` | `TEXT` | JSON array of groups |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Log time |

**Write pattern**: Append-only INSERT. ~20-100 writes/day.
**Read pattern**: Range scans by time, customer_id, or action_type for audit review.
**Sync**: TRIGGERED to Delta `billing_audit_synced` for analytics/compliance.

#### `agent_actions`
| Column | Type | Purpose |
|---|---|---|
| `action_id` | `VARCHAR(36) PRIMARY KEY` | UUID |
| `action_type` | `VARCHAR(64) NOT NULL` | query, write_staged, write_confirmed, write_cancelled |
| `tool_name` | `VARCHAR(128)` | Which agent tool was called |
| `customer_id` | `BIGINT` | If customer-specific |
| `initiating_user` | `VARCHAR(128)` | Human user |
| `persona` | `VARCHAR(32)` | Active persona |
| `session_id` | `VARCHAR(64)` | Conversation session |
| `request_id` | `VARCHAR(64)` | From RequestContext |
| `input_summary` | `TEXT` | Truncated tool input |
| `output_summary` | `TEXT` | Truncated tool output |
| `duration_ms` | `INTEGER` | Tool call duration |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Action time |

**Write pattern**: High-volume append during active conversations. ~100-500 writes/day.
**Read pattern**: Filtered by session_id for conversation replay, by user for audit.
**Sync**: TRIGGERED to Delta on demand.

---

## 5. Integration Design

### Component changes

| Component | Status | Details |
|---|---|---|
| `notebooks/08c_lakebase_setup.py` | **Done** (378 lines) | Provisions instance, bootstraps `billing_ops` schema, verifies read/write |
| `notebooks/08d_lakebase_sync.py` | **Done** (180 lines) | Synced tables, unified views (`v_all_disputes`, `v_all_audit`) |
| `notebooks/08e_validate_lakebase.py` | **Done** (258 lines) | 7-check validation: connectivity, schema, tables, round-trip, atomicity, columns |
| `notebooks/config.yaml` | **Done** | 10 Lakebase keys: `lakebase_enabled`, `lakebase_instance`, `lakebase_schema`, etc. |
| `notebooks/000-config.py` | **Done** | Lakebase config dict entries with defaults |
| `apps/dash-chatbot-app/app.yaml` | **Done** | Commented Lakebase resource binding ready to activate |
| `apps/dash-chatbot-app/requirements.txt` | **Done** | `psycopg2-binary>=2.9.9` added |
| `apps/gradio-databricks-app/app.py` | **Done** | Data Integration tab with live Lakebase connectivity check |
| `README.md` | **Done** | Track C section with three-track table |
| `ARCHITECTURE.md` | **Done** | Lakebase in write-back flow, platform features, file inventory |
| `notebooks/agent.py` | **Not yet** | Lakebase write path designed but not wired; falls back to Delta |

### Preserved patterns

- **ext_* view abstraction** stays for Track A/B ERP data. Lakebase does not replace it.
- **Delta medallion pipeline** stays for analytical data. Lakebase syncs TO Delta, not the reverse.
- **Write-back audit pattern** (two records: PENDING then SUCCESS/FAILED) is preserved in Lakebase, but as a single PostgreSQL transaction instead of two separate SQL warehouse statements.

---

## 6. Governance and Identity

### How identity flows through Lakebase

1. **App layer**: Extracts `x-forwarded-access-token`, resolves user via SCIM, builds `RequestContext` with HMAC signature (unchanged from current).

2. **Agent layer**: Validates `RequestContext`, stores in `contextvars.ContextVar` (unchanged).

3. **Lakebase writes**: `_execute_write_lakebase()` receives identity from `RequestContext` and records:
   - `initiating_user` = human email from `RequestContext`
   - `executing_principal` = the Lakebase PostgreSQL role (app-specific, auto-provisioned by Databricks)
   - `session_id`, `request_id`, `persona` = from `RequestContext`

4. **Lakebase reads**: Direct psycopg2 queries from the Dash app for real-time dispute status. No UC governance on the PostgreSQL side — governance is enforced at the app layer (which users can see which disputes).

5. **Delta sync**: Synced tables appear in Unity Catalog with full UC governance. Genie queries the synced Delta tables, not Lakebase directly. This means UC tags, row filters, and column masks apply to the analytical surface.

### What UC governs vs what the app governs

| Surface | Governed by | Mechanism |
|---|---|---|
| Delta tables (analytical) | Unity Catalog | Tags, row filters, column masks, grants |
| Lakebase tables (transactional) | Application layer | `RequestContext` validation, psycopg2 parameterized queries |
| Synced tables in UC | Unity Catalog | Same as Delta — synced tables are UC-managed |
| Genie analytics | Unity Catalog + Genie instructions | Genie only sees synced Delta tables |

### Audit dual-identity

Every Lakebase write records both identities:
```sql
INSERT INTO audit_log (initiating_user, executing_principal, ...)
VALUES ($1, $2, ...)  -- $1=human email, $2=PG role
```
This matches the existing Delta audit pattern, making the audit trail consistent regardless of which backend handles the write.

---

## 7. Rollout Plan

### Phase 0: Design Validation (this document)
- Scope: Architecture decision, schema design, integration plan
- Deliverables: This document, config additions, notebook skeletons
- Risks: Lakebase API availability in target workspace
- Exit criteria: Architecture approved, Lakebase instance provisionable

### Phase 1: Sandbox POC
- Scope: Provision instance, bootstrap schema, write/read test, basic sync
- Deliverables: `08c_lakebase_setup.py`, `08e_validate_lakebase.py`, config changes
- Risks: psycopg2 availability in Model Serving container
- Exit criteria: Can create a dispute in Lakebase from a notebook, read it back, see it in Delta via sync

### Phase 2: Integrated Pilot
- Scope: Agent write-path migration, Dash app dispute tracker, Genie over synced tables
- Deliverables: Modified `agent.py`, `08d_lakebase_sync.py`, app changes
- Risks: Latency regression if Lakebase is slower than expected; sync lag affecting Genie freshness
- Exit criteria: End-to-end demo: user creates dispute via chat -> Lakebase stores it -> synced to Delta -> Genie reports it

### Phase 3: Production Hardening
- Scope: Connection pooling, error handling, failover to Delta, monitoring
- Deliverables: Production-grade connection management, health checks, alerting
- Risks: Lakebase availability SLA, connection limits
- Exit criteria: Runs reliably under concurrent load with proper error recovery

---

## 8. Open Questions

1. **Lakebase provisioning**: Is Lakebase available in the target workspace/account? (Public Preview)
2. **psycopg2 in Model Serving**: Can the Model Serving container install psycopg2? If not, the agent write path stays on Statement Execution API and only the Dash app uses Lakebase directly.
3. **Sync latency**: What is the lag for CONTINUOUS synced tables? If >1 minute, Genie analytics on disputes will be slightly stale.
4. **Connection limits**: How many concurrent psycopg2 connections can the Lakebase instance handle? The app needs connection pooling.
5. **PostgreSQL role permissions**: The auto-provisioned app role has `Can connect and create`. Does it support DDL for schema bootstrap?
6. **Cross-system transactions**: Can a single agent action write to both Lakebase (dispute) and Delta (anomaly acknowledgement) atomically? Answer: No — these are separate systems. The audit log in Lakebase provides the correlation.
