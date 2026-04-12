# Architecture

This document describes how the system is built, how data and identity flow through it,
and where Databricks platform capabilities are used as load-bearing infrastructure rather
than convenience wrappers.

The system is an AI-powered billing support agent for telecom (extensible to SaaS/utility
verticals) built on LangGraph, MLflow ChatAgent, and Databricks Model Serving. It handles
individual customer billing inquiries, fleet-wide analytics, write-back operations (disputes,
anomaly acknowledgement), and real-time streaming billing estimates.

---

## System Overview

```
                                          Databricks Workspace
                                    ┌─────────────────────────────────────────────────────────┐
                                    │                                                         │
 ┌───────────┐   x-forwarded-      │  ┌──────────────────┐    custom_inputs     ┌──────────┐ │
 │  Browser   │──access-token──────>│  │  Databricks App  │──────────────────────>│  Model   │ │
 │  (User)    │<────────────────────│  │  (Dash/Gradio)   │<─────────────────────│ Serving  │ │
 └───────────┘   chat response      │  │                  │   ChatAgentResponse  │ Endpoint │ │
                                    │  │  - SCIM /Me call  │                      │          │ │
                                    │  │  - HMAC signing   │                      │ agent.py │ │
                                    │  │  - RequestContext │                      │ (SP)     │ │
                                    │  └──────────────────┘                      └────┬─────┘ │
                                    │                                                 │       │
                                    │         ┌───────────────────────────────────────-┤       │
                                    │         │                    │                   │       │
                                    │         v                    v                   v       │
                                    │  ┌─────────────┐   ┌──────────────┐   ┌──────────────┐  │
                                    │  │ UC Functions │   │ Vector Search│   │ Genie Space  │  │
                                    │  │ (14 tools)   │   │ (FAQ index)  │   │ (SQL gen)    │  │
                                    │  └──────┬──────┘   └──────────────┘   └──────────────┘  │
                                    │         │                                               │
                                    │         v                                               │
                                    │  ┌──────────────────────────────────────────────────┐   │
                                    │  │              Unity Catalog (Delta Tables)         │   │
                                    │  │  Bronze: billing_items, customers, billing_plans  │   │
                                    │  │  Silver: invoice, billing_events_streaming, ...   │   │
                                    │  │  Gold:   billing_anomalies, billing_monthly_      │   │
                                    │  │          running, gold_revenue_attribution, ...   │   │
                                    │  │  Audit:  billing_disputes, billing_write_audit    │   │
                                    │  └──────────────────────────────────────────────────┘   │
                                    └─────────────────────────────────────────────────────────┘
```

**Two deployment paths exist** (see DEC-002). The diagram above shows the LangGraph path
(notebook 03), which is the full-capability tier. The Agent Bricks path (notebook 04) is
a separate managed tier shown below.

### Agent Bricks Path (Notebook 04) — Managed Read-Only Tier

```
 User Query
     │
     v
 Supervisor Agent (MAS)
     │
     ├── "How is my bill calculated?" ──> Billing FAQ Agent (KA)
     │                                     └─ FAQ docs in UC Volume
     │                                        (indexed, RAG retrieval)
     │
     ├── "Average charges by plan?" ────> Billing Analytics Agent (Genie)
     │                                     └─ SQL over 18 billing tables
     │                                        (invoice_analytics, billing_plans, ...)
     │
     └── "Show charges for cust 4401" ──> Graceful decline
                                           "Individual lookups require
                                            the LangGraph deployment."
```

**What Agent Bricks provides**: FAQ retrieval, fleet-wide analytics, multi-agent routing.
**What it does NOT provide**: Individual customer lookups, write-back, persona filtering,
identity propagation, ERP profiles, streaming estimates, operational KPIs. These require
the LangGraph tier (notebook 03).

> **Notebooks 03 and 04 are alternative tiers, not sequential steps.**

---

## Runtime Components

### 1. Agent Core (`notebooks/agent.py` — 710 lines)

The agent is a LangGraph `StateGraph` compiled into an MLflow `ChatAgent`, deployed to a
Databricks Model Serving endpoint. It runs as a **service principal** (no SparkSession, no
dbutils).

**Module-level initialization** (runs once at container startup):
- `DatabricksFunctionClient()` + `set_uc_function_client()` — configures the UC function SDK
- `ModelConfig(development_config="config.yaml")` — loads all configuration
- `WorkspaceClient()` — shared instance (`_ws_client`) reused by all tools
- `ChatDatabricks(endpoint=...)` — LLM binding (Claude 3.7 Sonnet via Databricks external model)
- `UCFunctionToolkit(function_names=[...])` — loads 14 UC function tools by FQN
- `VectorSearchRetrieverTool(index_name=...)` — FAQ semantic retrieval
- `_load_personas()` — reads YAML files from `notebooks/personas/`

**Graph structure** (2-node ReAct loop):
```
agent ──> should_continue ──> tools ──> agent
                          └──> END
```
Compiled with `recursion_limit=30` (~15 tool round trips). Persona-specific graphs are
built lazily on first use and cached in `self._persona_graphs`.

**Tool categories** (19 total, persona-filtered):

| Category | Tools | Source |
|---|---|---|
| UC function lookups (14) | `lookup_customer`, `lookup_billing`, `lookup_billing_items`, `lookup_billing_plans`, `billing_faq`, `lookup_billing_anomalies`, `get_monitoring_status`, `lookup_operational_kpis`, `lookup_job_reliability`, `lookup_customer_erp_profile`, `lookup_revenue_attribution`, `get_finance_operations_summary`, `lookup_open_disputes`, `lookup_write_audit` | `UCFunctionToolkit` |
| Vector search (1) | `faq_search` | `VectorSearchRetrieverTool` |
| Genie delegation (1) | `ask_billing_analytics` | Custom `@tool` |
| Write-back (3) | `request_write_confirmation`, `confirm_write_operation`, `cancel_write_operation` | Custom `@tool` |

**Databricks best practice: UCFunctionToolkit**. Tools are defined as SQL functions in Unity
Catalog (`02_define_uc_tools.py`), not as Python code in the agent. This means tool schemas
are governed by UC, discoverable via `information_schema`, and executable with definer rights
independent of the calling principal's permissions. The agent loads tools dynamically by
fully qualified name — no tool code lives in `agent.py`.

**Databricks best practice: ChatAgent interface**. The `BillingChatAgent` class implements
MLflow's `ChatAgent` protocol (`predict` / `predict_stream`), which is the contract for
Databricks Model Serving agent endpoints. This enables one-line deployment via
`agents.deploy()` with built-in request/response logging, A/B testing, and the AI Playground
review app.

### 2. Identity Propagation (`notebooks/identity_utils.py` — 498 lines)

Implements Pattern C (Hybrid) — user identity propagated for authorization and audit, but
SQL execution remains under the service principal.

**RequestContext lifecycle:**

```
App: x-forwarded-access-token
 │
 ├── SCIM /Me ──> user_email, user_groups
 │
 ├── RequestContext.create(email, groups, persona, session_id, secret)
 │   └── HMAC-SHA256 signing (secret from Databricks Secret Scope)
 │
 ├── Serialize to JSON ──> custom_inputs["request_context"]
 │
 v
Agent: validate_request_context(raw_json, secret)
 │
 ├── Verify HMAC signature (timing-safe comparison)
 ├── Check expiry (15-minute TTL)
 ├── Store in contextvars.ContextVar (per-request, thread-safe)
 │
 ├── _filter_tools_for_persona() ──> validate persona-group binding
 ├── confirm_write_operation() ──> require valid context for writes
 └── _execute_write() ──> record initiating_user + executing_principal in audit
```

**Databricks best practice: Secret Scope for signing keys**. The HMAC secret is stored in a
Databricks Secret Scope (`echostar-identity/hmac-secret`), not in environment variables or
config files. Retrieved once via `WorkspaceClient().secrets.get_secret()` and cached for the
process lifetime with double-checked locking.

**Databricks best practice: SCIM API for identity resolution**. The App calls
`/api/2.0/preview/scim/v2/Me` with the user's OAuth token to resolve their email and group
memberships. This uses Databricks' native identity provider — no external IdP integration
needed.

**`contextvars.ContextVar`** is used instead of `threading.local` because Model Serving
reuses threads from a pool. For streaming responses, `copy_context()` snapshots the identity
at `predict_stream()` call time, and each `next()` on the inner generator runs inside
`snapshot.run()`. This ensures the generator sees its original identity even if the thread
handles a new request before the generator is fully consumed.

### 3. Tag-Based Governance (`identity_utils.py` — tag resolution)

UC assets are tagged with `gov.*` tags that drive runtime authorization:

| Tag | Values | Effect |
|---|---|---|
| `gov.identity.mode` | `required` / `preferred` / `none` | `required` blocks access when no `RequestContext` is present |
| `gov.pii.level` | `high` / `medium` / `low` / `none` | Drives governed view generation and projection policy |
| `gov.audit.required` | `true` / `false` | Logs tool access to audit trail |
| `gov.allowed.personas` | Comma-separated persona names | Blocks tool access for unlisted personas |
| `gov.write.policy` | `user_confirmed` / `sp_allowed` / `blocked` | Controls write authorization |
| `gov.data.classification` | `confidential` / `internal` / `public` | Informational |

**Tag inheritance**: object-level tags override schema-level, which override catalog-level.
Resolution queries `information_schema.table_tags`, `schema_tags`, `catalog_tags` in order
via Statement Execution API. Results cached with 5-minute TTL.

**Databricks best practice: UC Tags for governance metadata**. Tags are stored in Unity
Catalog's native tagging system, queryable via `information_schema`, visible in Catalog
Explorer, and inheritable through the catalog > schema > object hierarchy. This is the
platform's intended mechanism for attaching governance policy to data assets.

### 4. Write-Back System (`agent.py` — lines 213-443)

Writes use a **token-gated mediation** pattern because Model Serving has no SparkSession:

```
User: "Create a dispute for customer 4401"
  │
  v
Agent calls: request_write_confirmation(action, target_id, customer_id, reason)
  │
  ├── Generates 8-char token, stores op in _pending_writes dict (thread-safe, 10-min TTL)
  ├── Returns: "Write staged (token: abc12345). Reply CONFIRM or CANCEL."
  │
  v
User: "CONFIRM"
  │
  v
Agent calls: confirm_write_operation(token="abc12345")
  │
  ├── HARD GUARD 1: token must exist in _pending_writes (checked under lock)
  ├── HARD GUARD 2: RequestContext must be present (checked under same lock — no TOCTOU)
  ├── Input validation: customer_id → int(), action → allowlist, target_id → regex
  │
  └── _execute_write(op, initiating_user, executing_principal, persona, ...)
       │
       ├── If Lakebase enabled:
       │    └── Single PostgreSQL transaction (dispute + audit) via psycopg2
       │
       ├── If Lakebase not available (fallback):
       │    ├── Audit INSERT (PENDING) via Statement Execution API
       │    ├── Business SQL (UPDATE/INSERT) via Statement Execution API
       │    └── Audit INSERT (SUCCESS/FAILED) via Statement Execution API
       │
       └── Both paths record: initiating_user, executing_principal, session_id
```

**Lakebase (Track C)** is the recommended write-back backend when available. It replaces the
Statement Execution API -> Delta path with managed PostgreSQL, providing sub-100ms point writes,
ACID transactions (dispute + audit in a single commit), and no SQL warehouse dependency for
writes. When Lakebase is not provisioned, the agent falls back to the original Delta write
path. See `docs/LAKEBASE_ARCHITECTURE.md` for the full design.

**Databricks best practice: Statement Execution API for transactional writes**. The only SQL
execution path available in Model Serving (no SparkSession). All writes go through
`_ws_client.statement_execution.execute_statement()` with explicit `warehouse_id` and
`wait_timeout`.

**Databricks best practice: Immutable append-only audit trail**. The `billing_write_audit`
table uses a two-INSERT pattern (PENDING before, SUCCESS/FAILED after) rather than UPDATEs.
This ensures an audit record exists even if the process crashes between audit and business
write. Delta's Change Data Feed is enabled on the table for downstream consumption.

### 5. Persona System (`notebooks/personas/*.yaml`)

Four personas define distinct tool sets, system prompts, response styles, and group bindings:

| Persona | Tools | Write access | Required groups |
|---|---|---|---|
| `customer_care` | 15 | Full (disputes, anomalies) | None (default) |
| `finance_ops` | 14 | Acknowledge only | `billing_finance`, `billing_leadership` |
| `executive` | 3 | None | `billing_leadership`, `c_suite` |
| `technical` | 5 | None | `platform_engineering`, `data_engineering` |

**Databricks best practice: Persona-group binding via SCIM groups**. The `required_groups`
field in each persona YAML maps to Databricks workspace groups resolved via SCIM. The agent
validates `validate_persona_for_user(persona, ctx.user_groups, persona_group_map)` before
building the tool set. An unauthorized persona silently falls back to `customer_care`.

### 6. Domain Abstraction (`notebooks/domains/*.yaml`)

Three domain configs (telco, SaaS, utility) define:
- Canonical view mappings (`v_billing_summary`, `v_customer_profile`, `v_service_catalog`)
- Charge category labels (e.g., "Data Overage" vs "API Overage" vs "Peak Demand Surcharge")
- Agent vocabulary ("subscriber" vs "tenant" vs "ratepayer")
- Confidential field lists

`10_domain_config.py` reads the active domain YAML and generates canonical views using
`CREATE OR REPLACE VIEW`, then regenerates UC tools that reference the canonical views.

**Databricks best practice: View abstraction layer**. Domain-specific source tables are
abstracted behind canonical views in Unity Catalog. The agent tools reference the canonical
views, not the domain-specific tables. Changing the domain changes the views, not the tools
or the agent code.

---

## Data Architecture

### Medallion Pattern

```
Bronze (Raw)           Silver (Enriched)              Gold (Aggregated)
─────────────          ──────────────────             ────────────────────
billing_items ──CDF──> billing_events_streaming ────> billing_monthly_running
customers              invoice                        billing_anomalies
billing_plans          silver_customer_account_dims   gold_revenue_attribution
                       silver_fx_daily                gold_finance_operations_summary
                       silver_procurement_monthly     telemetry_operational_kpis
                       silver_conformed_kpi_defs      telemetry_job_reliability

Write-back tables (outside medallion):
  billing_disputes
  billing_write_audit
  billing_monitoring_state
```

**Databricks best practice: Delta Lake with Change Data Feed**. CDF is enabled on
`billing_items` (source for streaming pipeline), `billing_disputes`, and
`billing_write_audit`. This enables incremental reads without full-table scans and provides
a change history for audit consumers.

**Databricks best practice: DLT for streaming enrichment**. The `billing_events_streaming`
(Silver) and `billing_monthly_running` (Gold) tables are defined as a Delta Live Tables
pipeline (`06_dlt_streaming_pipeline.py`), deployed via SDK (`06a_create_dlt_pipeline.py`).
DLT manages checkpointing, schema evolution, and data quality expectations
(`@dlt.expect_or_drop` on `customer_id` and `device_id`).

### UC Function Design

All 14 UC functions follow these rules:

1. **Bounded result sets**: Every function has either a `LIMIT` clause, a PK filter
   (`WHERE pk = TRY_CAST(...)` returning 0-1 rows), or a vector search `num_results` bound.
2. **No PII in agent-facing functions**: `lookup_customer` returns `customer_id`, `device_id`,
   `plan`, `contract_start_dt` only. PII function is in a separate `{schema}_internal` schema.
3. **Lookback parameters with caps**: Time-series functions accept a lookback parameter with
   `LEAST(param, max)` in the WHERE clause to prevent unbounded scans even with malformed input.
4. **Definer rights**: Functions execute with the creator's permissions, not the caller's. This
   allows `lookup_customer` to read the `customers` table even though the agent SP has
   `REVOKE SELECT` on that table.

**Databricks best practice: UC functions as governed tool interfaces**. The function schema
(RETURNS TABLE, COMMENT) serves as a contract between the data layer and the LLM. The LLM
sees the function signature and comment; it never sees the underlying SQL. Schema changes
(adding/removing columns) are enforced at the catalog level and propagated to the agent on
the next serving container cold start via `UCFunctionToolkit`.

### PII Isolation

```
{catalog}.{schema}                    {catalog}.{schema}_internal
┌──────────────────────────┐          ┌──────────────────────────┐
│ lookup_customer (no PII) │          │ lookup_customer_pii      │
│ lookup_billing           │          │ (full PII — admin only)  │
│ lookup_billing_items     │          └──────────────────────────┘
│ ... (12 more)            │           REVOKE USE SCHEMA from SP
│                          │
│ customers (raw table)    │
│  └─ REVOKE SELECT from SP│
└──────────────────────────┘
```

**Databricks best practice: Schema-level privilege separation**. PII functions are isolated
in a `_internal` schema where the agent SP has `REVOKE USE SCHEMA`. The safe `lookup_customer`
function uses UC definer rights to read the `customers` table through the function, even
though the SP cannot `SELECT` from the table directly. The Genie Space SQL warehouse inherits
the same restrictions — it cannot reference `_internal` schema objects or query `customers`
directly.

---

## Application Layer

### Dash App (`apps/dash-chatbot-app/`)

| File | Lines | Purpose |
|---|---|---|
| `app.py` | 26 | Entry point — creates Dash app, mounts `DatabricksChatbot` |
| `DatabricksChatbot.py` | 261 | UI: persona selector, chat history, send/clear callbacks |
| `model_serving_utils.py` | 216 | Identity context creation, SCIM caching, endpoint call |
| `app.yaml` | 8 | Databricks App deployment config |
| `requirements.txt` | 4 | `dash`, `dash-bootstrap-components`, `mlflow`, `requests` |

**Identity flow in the Dash app**:
1. `DatabricksChatbot._call_model_endpoint()` reads `x-forwarded-access-token` from Flask
   request headers (injected by Databricks Apps proxy)
2. `model_serving_utils.query_endpoint()` calls `_get_user_info()` → SCIM → email + groups
   (cached 5 minutes per token)
3. Builds `_RequestContext` (inline dataclass matching agent-side signing), signs with HMAC,
   serializes to JSON
4. Sends as `custom_inputs["request_context"]` alongside `custom_inputs["persona"]`
5. Session ID is a UUID generated per `DatabricksChatbot` instance

**Databricks best practice: `x-forwarded-access-token` for user identity**. Databricks Apps
proxy injects the user's OAuth token as an HTTP header. The app extracts it without requiring
the user to authenticate separately. This is the platform-native pattern for user-aware
Databricks Apps.

### Gradio App (`apps/gradio-databricks-app/`)

| File | Lines | Purpose |
|---|---|---|
| `app.py` | 567 | 5-tab billing intelligence app (Chat, Analytics, Data Integration, Operations, Platform) |
| `../shared/serving_client.py` | 248 | Shared identity + endpoint client (used by Gradio, available to Dash) |
| `databricks.yml` | 21 | Databricks Asset Bundle for deployment |
| `app.yaml` | 14 | Databricks App config with serving endpoint binding |

The Gradio app is the preferred demo surface — it surfaces capabilities across multiple
tabs rather than hiding everything behind a single chat box. Identity propagation uses
the shared `serving_client.py` module. Deployed via DAB (`databricks bundle deploy`).

---

## Notebook Execution Order

```
000-config ─> 00 ─> 01 ─> 02 ─> 03a ─> 03 ─> 04 ─> 05 ─> 06b ─> 06a ─> 06 ─> 06c
                                                                                  │
07 ─> 08/08a ─> 08b ─> 09 ─> 09a ─> 10 ─> 10a ─> 11 ─> 12 ─> 12a  <─────────────┘
```

| Notebook | Lines | Purpose | Databricks features used |
|---|---|---|---|
| `000-config` | 261 | Central config (Python dict mirroring config.yaml) | — |
| `00` | 459 | Synthetic data via `dbldatagen` | `dbldatagen`, Delta `saveAsTable` |
| `01` | 186 | FAQ vector index | Vector Search `create_delta_sync_index` |
| `02` | 656 | 14 UC functions + PII isolation + UC grants | `CREATE FUNCTION`, `REVOKE`, `GRANT` |
| `03a` | 215 | Genie Space creation | Genie SDK `create_space` / `update_space` |
| `03` | 746 | Agent build, MLflow eval, UC registration, serving deploy | `mlflow.langchain.log_model`, `agents.deploy` |
| `04` | 794 | Agent Bricks Supervisor (KA + Genie) — read-only tier | Agent Bricks REST API, KA SDK |
| `05` | 291 | Anomaly detection (PySpark pipeline) | PySpark, Delta write |
| `06` | 143 | DLT pipeline definition (not directly runnable) | `dlt.table`, `dlt.read_stream`, `@dlt.expect_or_drop` |
| `06a` | 125 | Creates/starts DLT pipeline via SDK | `PipelinesAPI.create`, `start_update` |
| `06b` | 84 | One-time streaming prereqs (CDF enable, state table) | `ALTER TABLE SET TBLPROPERTIES` |
| `06c` | 128 | Alert dispatch for unalerted anomalies | Statement Execution API |
| `07` | 660 | System table ingestion (billing.usage, jobs, queries) | `system.billing.usage`, `system.lakeflow` |
| `08` | 99 | Lakehouse Federation setup (external ERP connection) | `CREATE CONNECTION`, `CREATE FOREIGN CATALOG` |
| `08a` | 204 | Synthetic ERP simulation (Track B alternative to 08) | `dbldatagen` |
| `08b` | 254 | ERP medallion pipeline (ext_* views -> Silver -> Gold) | PySpark, Delta, `CREATE VIEW` |
| `08c` | 270 | Lakebase setup (Track C: operational write-back store) | Lakebase provisioning, `psycopg2`, PostgreSQL DDL |
| `08d` | 125 | Lakebase sync (synced tables to Delta for analytics) | UC catalog registration, synced tables |
| `08e` | 200 | Lakebase validation (connectivity, schema, transactions) | `psycopg2`, PostgreSQL assertions |
| `09` | 132 | Write-back infrastructure (disputes, audit tables) | `CREATE TABLE`, `ALTER TABLE ADD COLUMN IF NOT EXISTS` |
| `09a` | 120 | Dispute SLA enforcement (auto-escalation) | Statement Execution API |
| `10` | 252 | Domain config (canonical views from domain YAML) | `CREATE OR REPLACE VIEW` |
| `10a` | 84 | Domain validation | `SHOW TABLES`, `DESCRIBE FUNCTION` |
| `11` | 73 | Persona validation | YAML parsing, config serialization |
| `12` | 348 | Governance tag admin (bulk tagging, validation, governed views) | `ALTER TABLE SET TAGS`, `information_schema.table_tags` |
| `12a` | 233 | Identity setup validation (9 checks) | Secret Scope API, `DESCRIBE TABLE`, `SHOW FUNCTIONS` |

---

## Databricks Best Practices Summary

### Platform Features Used as Architecture

| Practice | Where | Why it matters |
|---|---|---|
| **Unity Catalog function tools** | `02_define_uc_tools.py`, `agent.py` (UCFunctionToolkit) | Tool schemas governed by UC, not Python code. Definer rights enable safe PII isolation. |
| **UC Tags for governance** | `12_admin_tagging.py`, `identity_utils.py` (tag resolution) | Runtime authorization decisions driven by platform-native metadata, not config files. |
| **Schema-level privilege separation** | `02_define_uc_tools.py` (GRANT/REVOKE) | PII function in `_internal` schema, agent SP cannot USE it. Hard boundary, not convention. |
| **Secret Scope for signing keys** | `identity_utils.py` (`get_identity_secret`) | HMAC key stored in platform secret management, not env vars or code. |
| **SCIM API for identity** | `model_serving_utils.py` (`_get_user_info`) | User email + groups resolved from Databricks' identity provider, no external IdP. |
| **`x-forwarded-access-token`** | `DatabricksChatbot.py` | Platform-native user token injection for Databricks Apps. |
| **MLflow ChatAgent** | `agent.py` (BillingChatAgent) | Standard contract for Model Serving agent endpoints with built-in logging. |
| **`agents.deploy()`** | `03_agent_deployment_and_evaluation.py` | One-line deployment with review app, A/B testing, request logging. |
| **Delta Live Tables** | `06_dlt_streaming_pipeline.py` | Managed streaming with quality expectations, auto-checkpointing. |
| **Change Data Feed** | `06b_enable_streaming_prereqs.py` | Incremental reads without full-table scans. |
| **Vector Search** | `01_create_vector_search.py`, `agent.py` | Managed embedding + ANN index for FAQ retrieval. |
| **Genie Space** | `03a_create_genie_space.py`, `agent.py` | Natural language SQL analytics without building a SQL generation tool. |
| **Lakehouse Federation** | `08_federation_setup.py` | External ERP data queryable via UC without ETL. |
| **Lakebase (managed PostgreSQL)** | `08c_lakebase_setup.py`, `08d_lakebase_sync.py` | Transactional write-back store for disputes/audit. ACID transactions, sub-100ms writes. |
| **Statement Execution API** | `agent.py` (`_execute_write`), `identity_utils.py` (tag queries) | SQL execution from Model Serving (no SparkSession). Fallback when Lakebase unavailable. |
| **`dbldatagen`** | `00_data_preparation.py` | Databricks Labs synthetic data at scale for testing. |
| **Databricks Asset Bundles** | `apps/gradio-databricks-app/databricks.yml` | Declarative deployment for the Gradio app. |

### Security Patterns

| Pattern | Implementation |
|---|---|
| **No PII in tool responses** | `lookup_customer` returns only non-PII fields. PII function in `_internal` schema. |
| **Code-level write guards** | `confirm_write_operation` requires both valid token AND valid `RequestContext`. |
| **SQL injection prevention** | `_sanitize_sql_value()` for escaping, `_validate_identifier()` for identifiers, `int()` for numeric IDs, `_validate_sql_identifier()` for catalog/schema names, regex allowlists for tag values. |
| **Signed identity context** | HMAC-SHA256 with deterministic JSON serialization (`sort_keys=True`, `separators=(",",":")`) prevents tampering. |
| **Dual-identity audit** | Every write records `initiating_user` (human) and `executing_principal` (SP). |
| **Fail-closed on identity** | Assets tagged `gov.identity.mode=required` block access when context is missing. |
| **Bounded tool outputs** | All UC functions have LIMIT clauses + LEAST() caps on lookback parameters. |

### Performance Patterns

| Pattern | Implementation |
|---|---|
| **Singleton WorkspaceClient** | `_ws_client` in `agent.py`, `_get_workspace_client()` with `@lru_cache` in `identity_utils.py` |
| **SCIM result caching** | Per-token cache with 5-minute TTL in `model_serving_utils.py` |
| **Tag resolution caching** | `resolve_asset_policy()` caches with 5-minute TTL dict |
| **HMAC secret caching** | Process-lifetime cache with `threading.Lock` double-checked locking |
| **Warehouse ID caching** | `@lru_cache(maxsize=1)` on `_get_warehouse_id()` |
| **Lazy persona graph building** | Graphs built on first use per persona, cached in `_persona_graphs` dict |
| **`contextvars` for request state** | No thread-local leakage on pool reuse; `copy_context()` for generator isolation |

---

## File Inventory

### Core runtime (deployed to Model Serving)
```
notebooks/agent.py              710 lines   LangGraph agent + ChatAgent wrapper
notebooks/identity_utils.py     498 lines   Identity propagation, tag resolution, auth guards
notebooks/config.yaml           194 lines   All configuration (50+ keys)
notebooks/personas/*.yaml         4 files   Persona definitions (64-71 lines each)
```

### Notebooks (run on Databricks clusters)
```
notebooks/000-config.py         261 lines   Config setup (Python dict)
notebooks/00_data_preparation   459 lines   Synthetic data via dbldatagen
notebooks/01_create_vector...   186 lines   FAQ vector search index
notebooks/02_define_uc_tools    656 lines   14 UC functions + PII isolation + grants
notebooks/03a_create_genie...   215 lines   Genie Space creation
notebooks/03_agent_deploy...    746 lines   Agent build, eval, register, deploy
notebooks/04_agent_bricks...    794 lines   Agent Bricks Supervisor deployment
notebooks/05_billing_anomaly    291 lines   Anomaly detection pipeline
notebooks/06_dlt_streaming...   143 lines   DLT pipeline definition
notebooks/06a_create_dlt...     125 lines   DLT pipeline deployment
notebooks/06b_enable_stream...   84 lines   Streaming prerequisites
notebooks/06c_monitoring_...    128 lines   Alert dispatch
notebooks/07_system_table...    660 lines   System table telemetry
notebooks/08_federation_...      99 lines   Lakehouse Federation (Track A)
notebooks/08a_erp_data_...      204 lines   ERP simulation (Track B)
notebooks/08c_lakebase_...      270 lines   Lakebase setup (Track C)
notebooks/08d_lakebase_sync     125 lines   Lakebase-to-Delta sync
notebooks/08e_validate_...      200 lines   Lakebase validation
notebooks/08b_external_data...  254 lines   ERP medallion pipeline
notebooks/09_writeback_...      132 lines   Write-back infrastructure
notebooks/09a_dispute_aging     120 lines   Dispute SLA enforcement
notebooks/10_domain_config      252 lines   Domain adapter (telco/saas/utility)
notebooks/10a_validate_domain    84 lines   Domain validation
notebooks/11_persona_config      73 lines   Persona validation
notebooks/12_admin_tagging      348 lines   Governance tag administration
notebooks/12a_validate_...      233 lines   Identity setup validation
```

### Applications (deployed as Databricks Apps)
```
apps/shared/
  serving_client.py             248 lines   Shared identity + endpoint client

apps/dash-chatbot-app/
  app.py                         26 lines   Dash entry point
  DatabricksChatbot.py          261 lines   Chat UI with persona selector
  model_serving_utils.py        216 lines   Identity context (inlined, predates shared module)
  app.yaml                       12 lines   App deployment config

apps/gradio-databricks-app/
  app.py                        567 lines   5-tab billing intelligence app
  databricks.yml                 21 lines   DAB deployment
  app.yaml                       14 lines   App config with serving endpoint
```

### Domain configs
```
notebooks/domains/telco.yaml     97 lines
notebooks/domains/saas.yaml      98 lines
notebooks/domains/utility.yaml   99 lines
```

### Documentation
```
ARCHITECTURE.md           This file
CHANGELOG.md              Chronological change log (July 2025 – April 2026)
CLAUDE.md                 AI assistant guidance
DECISIONS.md              19 architecture decision records (DEC-001 through DEC-019)
POSTMORTEM.md             12 production failure entries with resolution status
README.md                 Setup guide with capability matrix
docs/TRUTH_REPORT.md      Platform pillar audit (updated April 2026)
docs/LAKEBASE_ARCHITECTURE.md  Lakebase design and integration plan
docs/GOVERNANCE-AND-IDENTITY.md  Identity model, UC tags, PII isolation, admin workflows
docs/NOTEBOOK-GUIDE.md    Notebook index with execution order and dependencies
docs/APPS-DASH.md         Dash app architecture and identity flow
docs/APPS-GRADIO.md       Gradio app architecture and tab descriptions
docs/ECHOSTAR-NOTES.md    EchoStar-specific evaluation and deployment guide
```
