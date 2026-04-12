# Changelog

All notable changes to `customer-billing-accelerator` are recorded here.

Branches covered: `main`, `feature/agent-bricks`, `feature/upgrades`, `feature/best-practices`, plus 12 merged feature branches (see Branch Notes).
Commits: 2025-07-21 – 2026-03-29
Contributors: Alex Barreto, Kyra Wulffert, a0x8o, service-jira-pub-repo-auto

**Organization:** Option A (chronological by month). No version tags exist in the git history.

---

## April 2026

### Identity Propagation (EchoStar)

- **Added** Governed identity propagation system (Pattern C — Hybrid). Signed `RequestContext` flows from Databricks App through Model Serving to agent tools. HMAC-SHA256 signing with secret from Databricks Secret Scope. SCIM `/Me` integration for user email + group resolution. (`2026-04-10`)
  - `notebooks/identity_utils.py`: new file (392 lines) — `RequestContext` dataclass, HMAC signing/verification, SCIM client, UC tag resolution with inheritance (object > schema > catalog), authorization guards, persona-group binding validation
  - `notebooks/agent.py`: identity validation in `predict()`/`predict_stream()`, per-request `contextvars.ContextVar` propagation with `copy_context()` snapshot for generator isolation, `_IDENTITY_AVAILABLE` graceful degradation
  - `notebooks/config.yaml`: `identity_secret_scope`, `identity_secret_key`, `identity_context_ttl_minutes`, `identity_default_mode`, `persona_group_map` ⚠️ Config change
  - `notebooks/personas/*.yaml`: `required_groups` field added to all four personas

### Security & PII

- **Fixed** PII removed from `lookup_customer` UC function response schema — `customer_name`, `email`, `phone_number` no longer returned. Prompt-only PII guard replaced with schema-level enforcement. (PM-001 fix) (`2026-04-10`)
  - `notebooks/02_define_uc_tools.py`: RETURNS TABLE reduced to `customer_id`, `device_id`, `plan`, `contract_start_dt`

- **Added** PII function isolated in `{schema}_internal` schema with UC grant restrictions. `REVOKE USE SCHEMA` on internal schema from agent SP, `REVOKE SELECT` on raw `customers` table from agent SP, `GRANT EXECUTE` on safe `lookup_customer` function (definer rights). (PM-001 + H7 fix) (`2026-04-12`)
  - `notebooks/02_define_uc_tools.py`: `lookup_customer_pii_internal` moved to `{schema}_internal.lookup_customer_pii`, old function dropped from main schema, UC grant/revoke statements added

- **Fixed** SQL injection vulnerabilities in `_execute_write` — `customer_id` validated as `int()`, `action` validated against allowlist, `target_id` validated with identifier regex, all string values passed through `_sanitize_sql_value()`. (`2026-04-11`)
  - `notebooks/agent.py`: `_sanitize_sql_value()`, `_validate_identifier()` functions added, `_execute_write` rewritten

- **Fixed** SQL injection in `identity_utils.py` tag queries — `catalog`, `schema`, `asset_name` validated with `_validate_sql_identifier()` before SQL interpolation. (`2026-04-11`)
  - `notebooks/identity_utils.py`: `_validate_sql_identifier()` added to `_fetch_tags()`

- **Fixed** SQL injection in `12_admin_tagging.py` — tag keys/values validated against regex allowlists before interpolation into `ALTER TABLE SET TAGS`. (`2026-04-11`)
  - `notebooks/12_admin_tagging.py`: input validation added to `bulk_apply_tags()`

### Write-Back & Authorization

- **Fixed** Write confirmation bypass eliminated — `confirm_write_operation` now has hard guards: (1) valid pending-write token required, (2) valid `RequestContext` with user identity required. TOCTOU race fixed by checking identity before popping token from store. (PM-002 fix) (`2026-04-10`)
  - `notebooks/agent.py`: `confirm_write_operation` rewritten with atomic identity+token check under lock

- **Changed** Audit trail enhanced with dual-identity columns: `initiating_user` (human), `executing_principal` (SP), `persona`, `request_id`, `identity_degraded`, `user_groups`. Both PENDING and SUCCESS/FAILED audit INSERTs include identity fields. Audit PENDING insert now verified before business write proceeds. (PM-010 fix) (`2026-04-10`) ⚠️ Schema change
  - `notebooks/09_writeback_setup.py`: 6 new columns in `billing_write_audit` CREATE TABLE + ALTER TABLE fallback
  - `notebooks/agent.py`: `_execute_write` signature extended, audit INSERT verified

### Tools & UC Functions

- **Fixed** `lookup_billing` unbounded result set — added `lookback_months INT DEFAULT 12` parameter with `LEAST(lookback_months, 36)` cap and `LIMIT 100`. Backwards compatible via DEFAULT. (M14 fix) (`2026-04-12`)
  - `notebooks/02_define_uc_tools.py`: function signature and query rewritten

- **Fixed** `lookup_billing_items` unbounded result set — added `LIMIT 100`. (PM-011 fix) (`2026-04-10`)

- **Fixed** Remaining unbounded UC functions — LIMIT added to all 14 functions. (`2026-04-12`)
  - `get_monitoring_status` (LIMIT 200), `lookup_operational_kpis` (LIMIT 90 + `LEAST(lookback_days, 90)`), `lookup_job_reliability` (LIMIT 100), `lookup_revenue_attribution` (LIMIT 36), `get_finance_operations_summary` (LIMIT 200 + `LEAST(lookback_months, 24)`), `lookup_billing_plans` (LIMIT 100)

### Performance & Scale

- **Changed** Request-scoped identity storage migrated from `threading.local()` to `contextvars.ContextVar`. Generator streaming uses `copy_context()` snapshot with per-step `snapshot.run(next, inner)` to maintain identity isolation when thread is reused before generator is fully consumed. (`2026-04-11`)
  - `notebooks/agent.py`: `_request_context_var` replaces `_request_context_local`, `predict_stream` split into outer wrapper + `_do_stream` inner generator

- **Changed** `WorkspaceClient` reused via module-level singleton (`_ws_client`) in `agent.py` and `@lru_cache(maxsize=1)` in `identity_utils.py`. Eliminates per-call client instantiation in analytics tool, write tools, and tag resolution. (`2026-04-11`)
  - `notebooks/agent.py`: `_ws_client = WorkspaceClient()` at module level
  - `notebooks/identity_utils.py`: `_get_workspace_client()` cached

- **Changed** Tag resolution results cached with 5-minute TTL in `resolve_asset_policy()`. `_get_warehouse_id()` cached with `@lru_cache`. HMAC secret cache protected with `threading.Lock` (double-checked locking). (`2026-04-11`)

- **Changed** SCIM `/Me` results cached per token with 5-minute TTL in `model_serving_utils.py`. (`2026-04-11`)

- **Changed** LangGraph `recursion_limit` now explicitly passed to `g.compile()` from config (default 30). (`2026-04-11`)

### Genie Space

- **Changed** Genie Space instructions added to prevent PII access — explicit rules against querying `customers` table directly, calling `*pii*` or `*_internal*` functions, or including PII columns in output. (`2026-04-12`)
  - `notebooks/03a_create_genie_space.py`: `genie_instructions` added to `serialized_space` config

### App Layer

- **Changed** Dash app identity integration — `x-forwarded-access-token` extracted from Flask headers, `RequestContext` built and signed per request, passed to serving endpoint via `custom_inputs.request_context`. (`2026-04-10`)
  - `apps/dash-chatbot-app/model_serving_utils.py`: rewritten with inline `_RequestContext` (matching agent-side signing), SCIM call with caching, identity-aware `query_endpoint()`
  - `apps/dash-chatbot-app/DatabricksChatbot.py`: session UUID per chatbot instance, lazy Flask import

- **Changed** Base agent no longer pre-builds all-tools graph at import time. Persona-specific graphs built lazily on first use. (PM-012 fix) (`2026-04-10`)
  - `notebooks/agent.py`: `BillingChatAgent.__init__` no longer calls `_build_graph(llm, tools, system_prompt)`

### Admin & Validation

- **Added** Governance tag administration notebook. (`2026-04-10`)
  - `notebooks/12_admin_tagging.py`: `bulk_apply_tags()`, `validate_tags()`, `generate_governed_views()`, `export_tags_to_delta()` — all working PySpark/SQL

- **Added** Identity setup validation notebook with 9 checks. (`2026-04-10`, extended `2026-04-12`)
  - `notebooks/12a_validate_identity_setup.py`: secret scope access, RequestContext round-trip, gov.* tag presence, audit column check, PII removal verification, LIMIT check, PII function isolation checks

### Documentation

- **Added** Deployment path capability matrix in README with 10-row comparison table. (`2026-04-10`)
- **Added** EchoStar Identity Propagation section in README: identity flow, setup steps, backwards compatibility. (`2026-04-10`)
- **Changed** CLAUDE.md updated with new file inventory and architecture pattern documentation. (`2026-04-10`)
- **Added** `DECISIONS.md`: DEC-017 (Identity Propagation), DEC-018 (PII Schema Isolation), DEC-019 (UC Function Result Bounding). (`2026-04-12`)
- **Updated** `POSTMORTEM.md`: PM-001, PM-002, PM-010, PM-011 status updated to fixed. PM-012 status updated to fixed. (`2026-04-12`)

---

## March 2026

### Agent & Prompt

- **Added** Multi-persona agent architecture with dynamic persona loading from YAML files. Four personas defined: customer_care, finance_ops, executive, technical. Each persona has distinct system prompt, tool policy (allowed/blocked tools), and response style. Persona agents cached in `_PERSONA_AGENTS` dict at module level. (`3e58106`, 2026-03-24) ⚠️ Prompt change
  - `notebooks/agent.py`: `_load_personas()`, `_get_persona_agent()`, persona routing in `predict()`
  - New files: `notebooks/personas/customer_care.yaml`, `executive.yaml`, `finance_ops.yaml`, `technical.yaml`
  - `notebooks/11_persona_config.py`: persona validation and serialization notebook

- **Changed** Agent code refactored for best practices: dynamic persona discovery via `glob("*.yaml")` instead of hardcoded names, `logging` module replacing print statements, `max_tokens` and `max_history_turns` extracted to config, `_trim_history` added for context window management. (`1a952c6`, 2026-03-27) ⚠️ Prompt change
  - `notebooks/agent.py`: 107 lines changed across 9 files

- **Added** Domain-aware prompt injection: `domain_agent_prompt_section` from config appended to base system prompt if not already present. (`3b8ef8e`, 2026-03-24)

- **Changed** Write-back tools validated — `_execute_sql` audit-first pattern refined, `_extract_pending_write` window set to `messages[-4:]`, `route_after_tools` searches `messages[-3:]`. (`8ede9f4`, 2026-03-24)
  - `notebooks/agent.py`, `notebooks/03_agent_deployment_and_evaluation.py`: 97 insertions, 52 deletions

### Write-Back & Disputes

- **Added** Governed agentic write-back system: `request_write_confirmation`, `acknowledge_anomaly`, `create_billing_dispute`, `update_dispute_status`, `lookup_dispute_history` tools. Two-INSERT audit pattern (`PENDING` before, `SUCCESS`/`FAILED` after) via Statement Execution API. 3-node LangGraph graph: agent → tools → confirm_or_cancel. `WRITE_PENDING_PREFIX` sentinel for confirmation routing. (`cf2e2c9`, 2026-03-24) ⚠️ Write behavior change ⚠️ Schema change
  - `notebooks/agent.py`: +443/-137 lines — complete rewrite from simple agent to write-back architecture
  - `notebooks/09_writeback_setup.py`: creates `billing_disputes` and `billing_write_audit` Delta tables, adds acknowledgement columns to `billing_anomalies`
  - `notebooks/09a_dispute_aging.py`: nightly SLA enforcement — auto-escalates disputes open > 5 days
  - `notebooks/config.yaml`: +19 new config keys for disputes, audit, warehouse_id ⚠️ Config change

- **Added** `lookup_open_disputes` and `lookup_write_audit` UC functions for read-only dispute and audit trail access. (`cf2e2c9`, 2026-03-24)
  - `notebooks/02_define_uc_tools.py`: +61 lines

### Tools & UC Functions

- **Added** `get_monitoring_status` UC function — returns anomaly monitoring summary by time window. (`1cb98fb`, 2026-03-24)

- **Added** `lookup_operational_kpis` and `lookup_job_reliability` UC functions for platform telemetry. (`290da3e`, 2026-03-24)

- **Added** `lookup_customer_erp_profile`, `lookup_revenue_attribution`, `get_finance_operations_summary` UC functions for ERP and finance data access. (`cad9ece`, 2026-03-24)

- **Fixed** UC function type mismatch and minor validation fixes in `lookup_billing_items` and `lookup_customer`. (`4192f51`, 2026-03-24)

### Anomaly Detection

- **Added** Billing anomaly detection pipeline (`notebooks/05_billing_anomaly_detection.py`): PySpark pipeline detecting `total_charge_spike` (z-score > 2.0), `roaming_spike` (3x mean), `international_spike` (3x mean), `data_overage_spike` (3x mean). Writes to `billing_anomalies` Delta table. Creates `lookup_billing_anomalies` UC function. (`6f38135`, 2026-03-24)
  - `notebooks/000-config.py`: anomaly threshold config keys added
  - `notebooks/config.yaml`: threshold values added ⚠️ Config change

- **Changed** Anomaly detection pipeline simplified and validated — reduced from 310 to ~250 effective lines, removed redundant cell separators, tightened column selection. (`8dd4ed5`, 2026-03-24)

### Streaming Pipeline

- **Added** DLT continuous streaming pipeline: `billing_events_streaming` (Silver — enriched billing events joined with customer and plan metadata) and `billing_monthly_running` (Gold — per-customer monthly charge accumulators). Data quality expectations via `@dlt.expect_or_drop`. (`1cb98fb`, 2026-03-24)
  - `notebooks/06_dlt_streaming_pipeline.py`: 143 lines — pipeline definition (not directly runnable)
  - `notebooks/06a_create_dlt_pipeline.py`: creates and starts DLT pipeline via Databricks SDK
  - `notebooks/06b_enable_streaming_prereqs.py`: one-time setup — enables CDF on billing_items, creates monitoring state table and summary view
  - `notebooks/06c_monitoring_alerter.py`: alert dispatch — finds unalerted anomalies and writes to monitoring state

- **Changed** DLT pipeline code quality pass — removed unused imports, tightened `06a` error handling, fixed `06b` column references, added summary output to `06c`. (`15514d2`, 2026-03-24)

- **Changed** DLT pipeline table properties refined for best practices. (`1a952c6`, 2026-03-27)

### ERP & External Data

- **Added** Lakehouse Federation and external data integration: Track A (UC connection to external ERP via `08_federation_setup.py`) and Track B (synthetic ERP simulation via `08a_erp_data_simulation.py`). Medallion pipeline in `08b_external_data_ingestion.py`: ext_* views → Silver (`silver_customer_account_dims`, `silver_fx_daily`, `silver_procurement_monthly`) → Gold (`gold_revenue_attribution`, `gold_finance_operations_summary`). (`cad9ece`, 2026-03-24)
  - 9 files changed, 745 insertions
  - `notebooks/config.yaml`: 20 new ERP config keys (connection host, port, user, tables) ⚠️ Config change

### Evaluation & Deployment

- **Changed** Agent deployment notebook (`03`) extended with write-back tool registration, synthetic evaluation for dispute and anomaly scenarios, and persona artifact packaging for MLflow logging. (`cf2e2c9`, 2026-03-24)
  - `notebooks/03_agent_deployment_and_evaluation.py`: +462/-137 lines

- **Changed** Deployment notebook updated to include all 19 tools (14 UC read + 5 write-back) in config.yaml generation and MLflow model logging. (`3e58106`, 2026-03-24)

- **Changed** Best practices applied to evaluation notebook: vector search index validation, improved error messages, evaluation dataset generation refinements. (`1a952c6`, 2026-03-27)

### Domain & Persona Config

- **Added** Cross-industry schema abstraction system: domain YAML configs for telco, SaaS, and utility verticals. Canonical view layer (`v_billing_summary`, `v_customer_profile`, `v_service_catalog`, `v_billing_events`) generated from domain YAML. (`3b8ef8e`, 2026-03-24)
  - `notebooks/10_domain_config.py`: 246 lines — reads domain YAML, creates canonical views, regenerates UC tools
  - `notebooks/10a_validate_domain.py`: 85 lines — validates deployed domain (canonical views, UC tools, charge column alignment)
  - New files: `notebooks/domains/telco.yaml`, `saas.yaml`, `utility.yaml`
  - `notebooks/config.yaml`: 9 new domain config keys ⚠️ Config change

- **Added** Persona validation notebook (`notebooks/11_persona_config.py`): validates all persona YAML configs, serializes persona metadata to config.yaml. (`3e58106`, 2026-03-24)

- **Changed** Persona config notebook expanded with additional validation checks and best practices. (`1a952c6`, 2026-03-27)

### Genie Space

- **Added** Genie Space integration: `ask_billing_analytics` tool with async polling (exponential backoff, 30 max attempts), `start_conversation` / `get_message` pattern. (`0b35d44`, 2026-03-24)
  - `notebooks/03a_create_genie_space.py`: 135 lines — creates Genie Space via Databricks SDK with PII-safe `invoice_analytics` view
  - `notebooks/agent.py`: +69 lines for Genie tool definition
  - `notebooks/config.yaml`: `genie_space_id` added ⚠️ Config change

- **Changed** Genie Space notebook and agent tool refined — improved error handling, config key alignment, validation of space creation response. (`38a6c4c`, 2026-03-24)

- **Changed** Genie Space creation refactored to use SDK methods consistently, table list aligned with analytics use case. (`6a05513`, 2026-03-24)

### Agent Bricks

- **Added** Agent Bricks deployment path (`notebooks/04_agent_bricks_deployment.py`): Supervisor Agent (MAS) orchestrating FAQ Knowledge Assistant and Billing Analytics Genie Space. REST API helpers for tile CRUD and endpoint provisioning. Routing instructions in `agent_bricks/telco-billing-mas.md`. (`6363a04`, 2026-03-24)
  - 368 lines — complete Agent Bricks deployment notebook
  - Note: this path does NOT include write-back, anomaly acknowledgement, ERP, streaming, or telemetry tools

- **Changed** Agent Bricks deployment refactored — Genie Space and KA creation consolidated, validation step added, endpoint polling improved. (`6a05513`, 2026-03-24)

- **Added** Agent Bricks routing spec: FAQ for policy questions, Analytics for aggregations, hybrid for complex queries. Individual customer lookups redirected to "dedicated customer care tools." (`1a952c6`, 2026-03-27)
  - `agent_bricks/telco-billing-mas.md`: 35 lines

### Ops & Telemetry

- **Added** System table ingestion pipeline (`notebooks/07_system_table_ingestion.py`): materializes Databricks system tables (`billing.usage`, `lakeflow.job_run_timeline`, `query.history`) into Bronze/Silver/Gold telemetry tables. Gold layer: `telemetry_dbu_daily`, `telemetry_job_reliability`, `telemetry_warehouse_utilization`, `telemetry_operational_kpis`. (`290da3e`, 2026-03-24)
  - 605 lines — complete telemetry pipeline
  - `notebooks/config.yaml`: 7 new telemetry table config keys ⚠️ Config change

- **Changed** System table ingestion validation — improved error handling for missing system table access, added fallback paths. (`e0388ed`, 2026-03-24)
  - 45 insertions, 17 deletions

### UI (Dash App)

- **Changed** Dash chatbot app updated with persona support and improved layout. (`3e58106`, 2026-03-24)
  - `apps/dash-chatbot-app/DatabricksChatbot.py`: significant refactor
  - `apps/dash-chatbot-app/model_serving_utils.py`: persona parameter pass-through

- **Changed** Dash app best practices: Bootstrap styling refined, error handling improved. (`1a952c6`, 2026-03-27)

### Configuration

- **Changed** `000-config.py` incrementally extended across 10 commits throughout March 2026. Accumulated additions: anomaly thresholds, streaming table names, telemetry keys, ERP connection settings, domain config keys, persona config path, write-back table names. (`multiple`, 2026-03-24)

- **Changed** `config.yaml` grew from 10 keys (January 2026) to 55 keys (March 2026) across the feature integration sequence. ⚠️ Config change

### Data & Bug Fixes

- **Fixed** Bug fixes across 7 files: removed stale `config.yml` file, fixed DatabricksChatbot layout issues, corrected data preparation column references, aligned UC function schema names, fixed config key references in agent.py. (`6ea4a26`, 2026-03-24)

- **Added** Production postmortem analysis document (`POSTMORTEM.md`): 12 failure entries covering PII exposure, write confirmation bypass, streaming re-aggregation, deployment path divergence, and 5 architectural redesign recommendations. (`d780fdc`, 2026-03-29)

---

## January 2026

### Evaluation & Deployment

- **Changed** Major best practices overhaul across all notebooks and the Dash app: improved error handling, config validation, logging, documentation cells, UC function schema refinements. 12 files, +1616/-414 lines. (`508f93d`, 2026-01-25)
  - `notebooks/agent.py`: +165/-67 — added structured error handling, improved tool binding
  - `notebooks/03_agent_deployment_and_evaluation.py`: +489/-234 — evaluation pipeline restructured
  - `notebooks/000-config.py`: +208/-58 — config validation and defaults added
  - `notebooks/config.yaml`: +89/-10 — expanded config schema ⚠️ Config change ⚠️ Prompt change

### Agent Bricks (feature/agent-bricks — not merged to main)

- **Added** Original Agent Bricks demo notebook (`notebooks/04_agent_bricks_demo.py`): 921-line prototype with inline agent definition, Knowledge Assistant creation, Genie Space integration, and Supervisor Agent setup. (`c7a8bdd`, 2026-01-25)
  - `notebooks/config.yaml`: 53 new config keys for Agent Bricks
  - `README.md`: +97 lines documenting Agent Bricks architecture

- **Fixed** Variable shadowing (`config` → `runnable_config`), logger undefined after Python restart, simplified agent overwriting, missing schema for nested arrays, redundant imports, UC function type mismatch. (`59b7957`, 2026-01-25)
  - `notebooks/01_create_vector_search.py`, `notebooks/02_define_uc_tools.py`

- **Fixed** Removed duplicate `%run` and `%pip install` commands in data preparation notebook. (`6fa2fb3`, 2026-01-25)

- **Docs** Removed license section from README. (`2eed308`, 2026-01-23)

- **Docs** Revised authors, removed logo from README. (`5f14990`, 2026-01-23)

### Data Preparation

- **Added** Agent notebook (`notebooks/agent.py`), config.yaml, and demo assets for the upgrade from placeholder notebooks to functional agent pipeline. (`e5ee862`, 2026-01-20)
  - `notebooks/agent.py`: 161 lines — initial LangGraph agent with UC function tools
  - `notebooks/config.yaml`: 27 lines — initial config ⚠️ Config change

- **Changed** Pipeline updated for demo catalog/schema/deployment. Added `config.yml` (later replaced by `config.yaml`). (`88938ea`, 2026-01-20)

- **Docs** README corrections. (`0615f1e`, 2026-01-21)

---

## July 2025

### Data Preparation

- **Added** Base accelerator code: complete notebook set for billing agent — `000-config.py` (configuration), `00_data_preparation.py` (synthetic data via dbldatagen), `01_create_vector_search.py` (FAQ vector index), `02_define_uc_tools.py` (5 UC functions: lookup_customer, lookup_billing, lookup_billing_items, lookup_billing_plans, billing_faq), `03_agent_deployment_and_evaluation.py` (agent logging + MLflow deployment). (`fece09a`, 2025-07-21)
  - 27 files changed, +1924/-596 lines
  - `data/billing_plans.json`: 10 telco billing plans (SIM50GB through UNLIMITED WORLD)
  - `apps/dash-chatbot-app/`: Dash UI with DatabricksChatbot component
  - Removed: placeholder `notebook1.ipynb`, `notebook2.ipynb`, `demo_app/`, `databricks.yml`, `scripts/`

### Configuration

- **Added** Initial project scaffolding: CI/CD workflows (`databricks-ci.yml`, `publish.yaml`), `.gitignore`, license files, placeholder notebooks and deployment scripts. (`91c7916`, 2025-07-21)

### Docs

- **Changed** README image format: moved images from nested directory, switched to JPG with light background. (3 commits: `8327657`, `f6f7e54`, `10d5a39`, all 2025-07-21)

- **Changed** README formatting and centered image. (`ea2ebe4`, `b03bfd5`, 2025-07-21)

---

## Branch Notes

### `feature/agent-bricks`

7 commits not on main. Contains the original Agent Bricks prototype (`04_agent_bricks_demo.py`, 921 lines), the best practices overhaul (`508f93d`), bug fixes (`59b7957`), and README edits. This branch was superseded by the main-line Agent Bricks implementation in `6363a04` and `6a05513` (March 2026), which rewrote the deployment as `04_agent_bricks_deployment.py`. The branch also contains a self-merge commit (`5cdff29`).

Commits not on main:
```
5cdff29 Merge branch 'feature/agent-bricks' into feature/agent-bricks
6fa2fb3 Remove duplicate %run and %pip install
59b7957 Fixed: Variable shadowing, logger, imports, type mismatch
c7a8bdd Agent Bricks demo
508f93d Implement Databricks Best Practices
2eed308 Remove license section from README
5f14990 Revise authors and remove logo from README
```

### `feature/upgrades`

1 commit not on main (`0615f1e` — README corrections). The rest of this branch was merged to main via PR #1 (`0964c76`).

### Feature branches merged to main (linear)

The following feature branches are fully merged into main and have no divergent commits. They served as development branches for specific gap-fill features and were merged sequentially:

| Branch | Tip commit on main | Feature |
|---|---|---|
| `feature/idreamofgenie` | `38a6c4c` | Genie Space integration |
| `feature/agent-bricks-for-realz` | `4192f51` | Agent Bricks rewrite |
| `feature/automated-billing-anomaly-detection` | `8dd4ed5` | Anomaly detection pipeline |
| `feature/continuous-monitoring` | `15514d2` | DLT streaming + monitoring |
| `feature/telemetry` | `e0388ed` | System table telemetry |
| `feature/data-integration` | `cad9ece` | ERP / federation |
| `feature/write-back` | `8ede9f4` | Write-back + disputes |
| `feature/schema-abstraction` | `3b8ef8e` | Domain YAML + canonical views |
| `feature/multi-persona` | `3e58106` | Persona system |
| `feature/best-practices` | `508f93d` | (on feature/agent-bricks, not main) |

### Merge history

- `549324f` (2025-07-21): Merged `accelerator_code` → main (PR #1 from databricks-industry-solutions)
- `c1e16d5` (2025-07-21): Merged `images_fix` → main (PR #2)
- `8e140a4` (2025-07-21): Merged `images_fix_light` → main (PR #3)
- `5fc9d82` (2025-07-21): Merged `images_fix_light_format` → main (PR #4)
- `0964c76` (2026-01-20): Merged `feature/upgrades` → main (PR #1 from alexxx-db)
- March 2026: Feature branches `idreamofgenie` through `multi-persona` were merged linearly to main (fast-forward, no merge commits). The `feature/agent-bricks` branch was NOT merged — it was superseded by `feature/agent-bricks-for-realz`.

---

## Stats

| Metric | Value |
|---|---|
| Total commits (all branches) | 41 |
| Commits on main | 32 |
| Commits only on feature branches (not on main) | 9 |
| Date range | 2025-07-21 – 2026-03-29 |
| Contributors | 4 unique authors (Alex Barreto, Kyra Wulffert, a0x8o, service-jira-pub-repo-auto) |
| Files changed (unique across all commits) | 66 |
| Most-changed file | `README.md` (19 commits) |
| Second most-changed | `notebooks/000-config.py` (17 commits) |
| Third most-changed | `notebooks/agent.py` (16 commits), `notebooks/03_agent_deployment_and_evaluation.py` (16 commits) |
| Largest single commit (lines) | `fece09a`: +1924/-596 (base code for the ai agent solution accelerator) |
| Second largest | `508f93d`: +1616/-414 (Implement Databricks Best Practices) |
