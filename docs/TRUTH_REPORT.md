# Truth Report: Platform Pillar Audit

Originally produced 2026-04-12. **Updated 2026-04-12** after Lakebase implementation
(notebooks 08c/08d/08e), Gradio app rewrite (5-tab billing intelligence app), and
Agent Bricks notebook fix (`wait_for_tile` defined, programmatic MAS creation).

Covers every file in the repo that touches Agent Bricks, Genie Spaces,
Databricks Apps, or Lakebase/Federation. Each entry states exactly what the code does, what
the docs claim, and where the two diverge.

---

## 1. Agent Bricks

### What the code actually does

**`notebooks/04_agent_bricks_deployment.py`** (389 lines) — the only implementation file.

| Step | What it does | Status |
|---|---|---|
| Step 1: Prepare FAQ docs | Reads `billing_faq_dataset` table, writes text+JSON files to UC Volume `billing_faq_docs` | Working code |
| Step 2: Create KA | Uses `w.knowledge_assistants.create_knowledge_assistant()`, adds UC Volume as knowledge source, syncs | Working code |
| Step 3: Create Supervisor | **Prints manual UI instructions** to the notebook output — tells the user to open `/#/agents` and click through the UI | **No programmatic creation** |
| Step 4: Test MAS | Calls `w.serving_endpoints.query()` against `mas-{tile_id}-endpoint` | Working code, but depends on Step 3 completing manually |

**Fixed (April 2026)**: `wait_for_tile()` is now defined (line 101) and correctly called.
Programmatic MAS creation via REST API is attempted first; manual fallback provided if API unavailable.
`mas_tile_id` and `ka_tile_id` are now properly assigned in all code paths.

**What Agent Bricks deploys**: FAQ Knowledge Assistant + Genie Space as sub-agents in a Supervisor. Two capabilities only: FAQ document retrieval and Genie SQL analytics.

**What Agent Bricks does NOT deploy**: No UC function tools, no write-back, no anomaly acknowledgement, no individual customer lookup, no ERP data access, no streaming estimates, no operational KPIs, no persona filtering, no identity propagation.

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 42 | "Deploys the solution as a Databricks Agent Bricks Supervisor Agent combining a FAQ Knowledge Assistant with the Billing Analytics Genie Space." | **Accurate** but understates the gap — "deploys the solution" implies functional equivalence with notebook 03 |
| README line 71 | "Deploy as an Agent Bricks Supervisor Agent (KA + Genie Space) for a fully managed multi-agent experience." | **Misleading** — "fully managed multi-agent experience" overstates what 2 sub-agents provide. The full agent has 19 tools; Agent Bricks has 2 sub-agents with 0 tools. |
| README line 92 | "Agent Bricks (notebook 04) is a read-only demo tier." | **Accurate** |
| ARCHITECTURE.md line 49-51 | "The Agent Bricks path (notebook 04) deploys a Supervisor with only FAQ Knowledge Assistant + Genie Space — no write-back, no individual customer tools, no identity propagation." | **Accurate** |
| Capability matrix (README lines 79-91) | 10-row comparison table showing Yes/No per capability | **Accurate** — honestly shows Agent Bricks as No for 8 of 10 capabilities |
| DECISIONS.md DEC-002 | Documents the two-path divergence, capability gap table, undocumented routing to nonexistent tools | **Accurate and self-critical** |
| POSTMORTEM.md PM-006 | "A customer deployment team ran notebooks 01-04 in sequence... They then asked why the billing agent could not acknowledge anomalies, create disputes, check ERP credit status..." | **Accurate** — documents the real confusion |

### Overstatements

1. **README line 71** (original): "fully managed multi-agent experience" — overstated for a 2-agent read-only tier. **Fixed**: README now clearly positions Agent Bricks as "Managed Read-Only Tier."
2. ~~**Notebook 04 `wait_for_tile` undefined**~~ — **Fixed (April 2026)**: Function defined at line 101. All variable references resolved.
3. ~~**Notebook 04 manual-only MAS creation**~~ — **Fixed (April 2026)**: Programmatic creation via REST API attempted first; manual fallback with structured resume path if API unavailable.

### What is genuinely good

- The capability matrix in README is honest.
- DEC-002 and PM-006 openly document the gap.
- The KA creation code (Step 2) is real, working SDK code.
- The FAQ document preparation from UC Volume is a clean pattern.

---

## 2. Genie Spaces

### What the code actually does

**`notebooks/03a_create_genie_space.py`** (215 lines) — Genie Space creation and configuration.

| Component | What it does | Status |
|---|---|---|
| PII-safe view | Creates `invoice_analytics` view excluding PII columns | Working code |
| Space creation | Uses `w.genie.create_space()` / `update_space()` with serialized config | Working code |
| Instructions | 6 PII guardrails injected into Genie Space config | Working code |
| Test conversation | Sends test question, polls for result, displays SQL + response | Working code |
| Table registration | 20 tables registered from config | Working code |

**`notebooks/agent.py`** lines 149-176 — `ask_billing_analytics` tool.

| Component | What it does | Status |
|---|---|---|
| Tool definition | `@tool` wrapping `_ws_client.genie.start_conversation_and_wait()` | Working code |
| Response parsing | Extracts text content and generated SQL from attachments | Working code |
| Error handling | Returns error message string on exception | Working code |
| Persona filtering | Included in customer_care, finance_ops, executive; excluded from technical | Working code |

**`notebooks/000-config.py`** lines 141-215 — Configuration.

| Component | What it does | Status |
|---|---|---|
| 18 table identifiers | Hardcoded FQNs for Genie Space | Working config |
| 28 sample questions | Covering billing, anomalies, streaming, telemetry, ERP, finance | Working config |
| Space name/description | "Customer Billing Analytics" with detailed description | Working config |

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 40 | "Creates a Databricks Genie Space for ad-hoc billing analytics over invoice and plan tables." | **Accurate but understated** — actually registers 20 tables, not just invoice and plan |
| ARCHITECTURE.md "Runtime Components" | Documents Genie as fleet-wide analytics delegation, distinct from individual lookups | **Accurate** |
| DECISIONS.md DEC-007 | Documents Genie choice over UC functions for arbitrary SQL, cold start tradeoff | **Accurate** |
| POSTMORTEM.md PM-005 | Documents 45-90s cold start on first query after inactivity | **Accurate** — this is a real operational issue |

### Overstatements

**None found.** Genie documentation is honest and accurate throughout. The cold start issue (PM-005) is openly documented. The PII guardrails are both documented and implemented.

### What is genuinely good

- 28 curated sample questions covering 6 analytical domains.
- PII-safe `invoice_analytics` view created before Space registration.
- 6 explicit PII guardrail instructions in the Space config.
- Four-layer PII defense (schema isolation, UC grants, Genie instructions, governance tags).
- Honest cold-start documentation.

### What is missing

- No **Genie Space warm-up** mechanism (PM-005 recommends a keep-warm ping).
- No **conversation reuse** — each `ask_billing_analytics` call creates a new conversation.
- Genie is **not directly exposed in either Databricks App** — users interact with it only through the agent's `ask_billing_analytics` tool or the Agent Bricks Supervisor routing. There is no direct Genie experience in the Dash or Gradio app.
- No **demo script** or **guided walkthrough** for Genie-specific capabilities.

---

## 3. Lakebase / Lakehouse Federation

### What the code actually does

**`notebooks/08_federation_setup.py`** (99 lines) — Track A: Real Federation.

| Component | What it does | Status |
|---|---|---|
| Track detection | `USE_REAL_FEDERATION = bool(config.get("erp_connection_host", ""))` | Working code |
| UC Connection | Creates PostgreSQL connection via `w.connections.create()` | Working code (Track A only) |
| Foreign Catalog | Creates `erp_federated_{database}` catalog | Working code (Track A only) |
| Config update | Writes connection/catalog names to config.yaml via regex | Working code |
| Default behavior | **Skips entirely** if `erp_connection_host` is not configured | By design — Track B is the default |

**`notebooks/08a_erp_data_simulation.py`** (204 lines) — Track B: Simulation.

| Component | What it does | Status |
|---|---|---|
| Synthetic ERP data | 4 tables via `dbldatagen`: accounts, orders, procurement, FX rates | Working code |
| Abstraction views | 4 `ext_*` views pointing to simulated tables | Working code |
| Data volume | 1000 accounts, ~8000 orders, procurement costs, 3650 FX rate rows | Working code |

**`notebooks/08b_external_data_ingestion.py`** (254 lines) — Medallion pipeline (both tracks).

| Component | What it does | Status |
|---|---|---|
| Guard clause | Validates all 4 `ext_*` views exist | Working code |
| Silver tables | `silver_customer_account_dims`, `silver_fx_daily`, `silver_procurement_monthly`, `silver_conformed_kpi_defs` | Working code |
| Gold tables | `gold_revenue_attribution`, `gold_finance_operations_summary` | Working code |
| Track agnostic | Works identically with Track A or B via `ext_*` abstraction | Working code |

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 49 | "Sets up Lakehouse Federation: UC connection to external ERP (Track A). Skip for Track B (simulation)." | **Accurate** |
| README line 50 | "Generates synthetic ERP accounts/orders, procurement costs, and FX rates (Track B). Creates ext_* view abstraction layer." | **Accurate** |
| DECISIONS.md DEC-011 | Documents both tracks, ext_* abstraction layer, config-driven track selection | **Accurate** |
| ARCHITECTURE.md Data Architecture | "Abstract ext_* views decouple Track A/B, enabling identical downstream Silver/Gold logic" | **Accurate** |

### Overstatements

None found for Federation Tracks A/B — docs are accurate and honest.

### What is genuinely good

- The `ext_*` view abstraction is a clean design pattern — downstream code is truly track-agnostic.
- Track B simulation generates realistic ERP data (1000 accounts, procurement, FX rates, revenue).
- The Silver/Gold medallion pipeline in `08b` is solid PySpark with proper joins and aggregations.
- Config-driven track selection (`erp_connection_host` presence) is clean.

### Lakebase (Track C) — Added April 2026

**Previously missing, now implemented.** Three notebooks (`08c_lakebase_setup.py`,
`08d_lakebase_sync.py`, `08e_validate_lakebase.py`) provision a managed PostgreSQL
instance, bootstrap the `billing_ops` schema (disputes, audit_log, agent_actions),
set up synced tables to Delta, and validate connectivity. Config keys added to
`config.yaml` and `000-config.py`. The Gradio app has a Data Integration tab with
live Lakebase connectivity checking. README documents Track C.
See `docs/LAKEBASE_ARCHITECTURE.md` for the full design.

### What is still missing

- **Track A is untestable without a real external database** — there is no Docker-compose or local PostgreSQL setup for testing Track A.
- **Lakebase depends on workspace availability** — Lakebase is in Public Preview and may not be available in all workspaces. When unavailable, write-back falls back to Statement Execution API -> Delta.

---

## 4. Databricks Apps

### What the code actually does

**`apps/dash-chatbot-app/`** — Production Dash app (4 files, 511 total lines).

| Component | What it does | Status |
|---|---|---|
| Chat UI | Persona selector (4 radio buttons), chat history with markdown rendering, send/clear | Working code, polished |
| Identity propagation | Extracts `x-forwarded-access-token`, SCIM resolution, HMAC signing | Working code |
| Serving endpoint call | MLflow `get_deploy_client("databricks").predict()` with `custom_inputs` | Working code |
| Persona switching | Clears chat, shows mode description, routes to persona-specific agent | Working code |
| CSS/UX | Custom DM Sans font, branded colors, typing animation, auto-scroll | Working code, polished |

**`apps/gradio-databricks-app/`** — Multi-workspace billing intelligence app (567 lines, rewritten April 2026).

| Component | What it does | Status |
|---|---|---|
| 5-tab UI | Chat, Analytics, Data Integration, Operations, Platform | Working code |
| Chat workspace | Persona-aware conversation with full chat history, sample prompts per persona | Working code |
| Analytics workspace | Fleet-wide Genie-powered analytics with prompt library | Working code |
| Data Integration | Three-track visibility (Federation, Simulation, Lakebase) with live connectivity check | Working code |
| Operations | Platform health, costs, job reliability via technical persona | Working code |
| Identity propagation | Uses shared module (`apps/shared/serving_client.py`) for SCIM + HMAC | Working code |
| DAB deployment | `databricks.yml` with dev/prod targets | Working config |

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 57 | "A simple Dash web app that lets users chat with the deployed agent" | **Accurate** — it is simple (chat only) |
| ARCHITECTURE.md App Layer | Documents Dash app as production-ready with identity propagation, Gradio as starter template | **Accurate** |
| DECISIONS.md DEC-013 | "Dash was chosen for simplicity... a thin shell around the serving endpoint" | **Accurate** — it is exactly that |

### Overstatements

None significant. Both apps now accurately described in docs.

### What is genuinely good

- **Dash app**: Production-quality identity propagation, polished CSS/UX, clean separation of concerns.
- **Gradio app**: 5-tab multi-workspace design surfaces capabilities that are hidden in the Dash chat-only UX. Data Integration tab shows three-track data architecture. Platform tab shows deployment tier comparison. Shared identity module prevents drift.

### What is still missing

- **No streaming** in either app — both wait for full response before display.
- **No endpoint toggle** — no way to switch between LangGraph and Agent Bricks endpoints from either UI.
- **Dash app remains chat-only** — no analytics, data integration, or operations workspaces (use Gradio for those).

---

## 5. Cross-Cutting Issues (Updated April 2026)

### README "How to Use" section — FIXED

Previously presented notebooks 03/04 as sequential steps. Now restructured as
"Option A" (LangGraph) vs "Option B" (Agent Bricks) with a callout:
"Notebooks 03 and 04 are alternative deployment paths, not sequential steps."

### ARCHITECTURE.md System Overview diagram — FIXED

Agent Bricks architecture diagram added alongside the LangGraph diagram, with
clear ASCII art showing Supervisor -> KA + Genie routing and graceful decline
for unsupported requests.

### Lakebase — IMPLEMENTED (Track C)

Previously: "The term 'Lakebase' does not appear in any code file." Now: Three
notebooks (08c/08d/08e), config keys, Gradio app integration, and full architecture
doc (`docs/LAKEBASE_ARCHITECTURE.md`). Lakebase and Lakehouse Federation are correctly
distinguished throughout:

| | Lakehouse Federation | Lakebase |
|---|---|---|
| What it is | Query external databases via foreign catalogs | Managed PostgreSQL instances inside Databricks |
| Data location | External system (customer's PostgreSQL, MySQL, etc.) | Databricks-managed PostgreSQL |
| Use case | Read external data without ETL | OLTP workloads, app backends, synced tables |
| Current repo support | **Track A: Yes** (foreign catalog over PostgreSQL) | **No** |

---

## 6. Summary Matrix (Updated April 2026)

| Pillar | Code exists? | Code works? | Docs accurate? | Demo-ready? | Key gap |
|---|---|---|---|---|---|
| **Agent Bricks** | Yes (nb 04, 794 lines) | Yes — KA via SDK, MAS via REST API with manual fallback | Yes — README capability matrix is honest | Yes — 3 demo bundles, 4 validation tests, golden prompts | Cold start on first KA query |
| **Genie Spaces** | Yes (nb 03a + agent.py) | Yes — creation, querying, PII guardrails all work | Yes — honest and accurate | Yes — Gradio Analytics tab, 28 sample questions | 45-90s cold start on first query after inactivity |
| **Lakebase** | Yes (nb 08c/08d/08e, 816 lines) | Yes — schema bootstrap, read/write, sync to Delta | Yes — correctly distinguished from Federation | Partially — depends on Lakebase availability in workspace | Lakebase is Public Preview; not all workspaces have it |
| **Federation** | Yes (nb 08/08a/08b) | Yes — both tracks work, ext_* abstraction is clean | Yes — accurate | Yes — Gradio Data Integration tab shows three tracks | Track A needs real external DB |
| **Databricks Apps** | Yes (Dash 511 lines, Gradio 567 lines) | Both functional with identity propagation | Yes — each has dedicated docs | Yes — Gradio is preferred demo surface, Dash is reference chat | No streaming in either app |
| **Identity & Governance** | Yes (identity_utils.py + admin notebooks) | Yes — SCIM, HMAC, tags, PII isolation, dual-identity audit | Yes — dedicated GOVERNANCE-AND-IDENTITY.md | Yes — 9-check validation notebook | HMAC secret rotation requires coordinated update |
| **Streaming & Telemetry** | Yes (DLT pipeline + system tables) | Yes — DLT produces streaming tables, telemetry materializes | Yes | Partially — streaming estimates not surfaced in app | `billing_monthly_running` re-aggregation issue (PM-009) |

---

## 7. Remaining Work

| Item | Status | Notes |
|---|---|---|
| Genie cold-start warm-up | Not implemented | PM-005 recommends keep-warm ping; deferred |
| Endpoint toggle in Apps | Not implemented | Neither app can switch LangGraph/Agent Bricks at runtime |
| Streaming in Apps | Not implemented | `predict_stream()` exists but not wired to app layer |
| `billing_monthly_running` re-aggregation | Not fixed | PM-009: DLT streaming Gold table re-aggregates full history |
| Dedicated write warehouse | Not implemented | PM-010: writes still share analytics warehouse |
| `anomaly_uuid` surrogate key | Not implemented | PM-007: composite key still used for anomaly writes |
