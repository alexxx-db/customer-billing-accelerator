# Notebook Guide

Execution order, dependencies, and guidance for every notebook in the accelerator.

---

## Execution Flow

```
                    SHARED SETUP (always run)
                    ┌──────────────────────────────┐
                    │ 000-config → 00 → 01 → 02    │
                    │                 └→ 03a (Genie)│
                    └──────────────┬───────────────┘
                                   │
                    CHOOSE ONE DEPLOYMENT TIER
                    ┌──────────────┴───────────────┐
                    │                              │
             OPTION A: LangGraph          OPTION B: Agent Bricks
             (Full capability)            (Read-only, managed)
                    │                              │
                    03 (deploy agent)              04 (deploy supervisor)
                    │                              │
             OPTIONAL FEATURES                     └→ Done (Dash app)
             05 (anomalies)
             06b→06a→06→06c (streaming)
             07 (telemetry)
             08/08a→08b (ERP)
             08c→08d→08e (Lakebase)
             09→09a (write-back)
             10→10a (domain config)
             11 (persona config)
             12→12a (governance)
                    │
                    └→ Dash/Gradio app
```

---

## Notebook Index

### Shared Setup (run first, regardless of tier)

| Notebook | Lines | Purpose | Outputs |
|---|---|---|---|
| `000-config` | 261 | Central configuration dict | `config` variable for all notebooks |
| `00_data_preparation` | 459 | Synthetic billing data via `dbldatagen` | `customers`, `billing_items`, `billing_plans`, `invoice` Delta tables |
| `01_create_vector_search` | 186 | FAQ vector index | `billing_faq_dataset` table, Vector Search index |
| `02_define_uc_tools` | 656 | 14 UC functions + PII isolation + grants | UC functions, `{schema}_internal` schema, GRANT/REVOKE |
| `03a_create_genie_space` | 215 | Genie Space with 20 tables and PII guardrails | Genie Space ID, `invoice_analytics` PII-safe view |

### Deployment Tier A: LangGraph Agent (Full Capability)

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `03_agent_deployment_and_evaluation` | 746 | Build, evaluate, register, deploy agent to Model Serving | Shared setup complete |

### Deployment Tier B: Agent Bricks (Read-Only, Managed)

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `04_agent_bricks_deployment` | 794 | KA + Genie Supervisor Agent | Shared setup complete; 03a (Genie Space) |

> **Notebooks 03 and 04 are alternative tiers, not sequential steps.**

### Optional: Anomaly Detection

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `05_billing_anomaly_detection` | 291 | Detect charge spikes, roaming/data anomalies | 00 (billing data) |

### Optional: DLT Streaming Pipeline

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `06b_enable_streaming_prereqs` | 84 | Enable CDF, create monitoring tables | 00 (billing_items) |
| `06a_create_dlt_pipeline` | 125 | Deploy DLT pipeline via SDK | 06b |
| `06_dlt_streaming_pipeline` | 143 | DLT definition (NOT directly runnable) | Deployed by 06a |
| `06c_monitoring_alerter` | 128 | Alert dispatch for unalerted anomalies | 06b |

### Optional: System Table Telemetry

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `07_system_table_ingestion` | 660 | Materialize system tables to telemetry Gold tables | System catalog access |

### Optional: ERP / External Data

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `08_federation_setup` | 99 | Track A: UC connection + foreign catalog to external ERP | External PG credentials |
| `08a_erp_data_simulation` | 204 | Track B: Synthetic ERP data + `ext_*` view abstraction | 00 (customer data) |
| `08b_external_data_ingestion` | 254 | Silver/Gold transforms from `ext_*` views | 08 or 08a |

### Optional: Lakebase (Track C)

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `08c_lakebase_setup` | 378 | Provision Lakebase instance, bootstrap PG schema | Lakebase available in workspace |
| `08d_lakebase_sync` | 180 | Synced tables from Lakebase to Delta for Genie | 08c |
| `08e_validate_lakebase` | 258 | 7-check Lakebase validation | 08c |

### Optional: Write-Back Infrastructure

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `09_writeback_setup` | 132 | Create `billing_disputes` and `billing_write_audit` tables | 05 (anomalies table) |
| `09a_dispute_aging` | 120 | Nightly dispute SLA enforcement (auto-escalation) | 09 |

### Optional: Domain and Persona Configuration

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `10_domain_config` | 252 | Domain adapter (telco/saas/utility canonical views) | 00 |
| `10a_validate_domain` | 84 | Validate deployed domain | 10 |
| `11_persona_config` | 73 | Validate persona YAML configs | Persona YAML files |

### Optional: Governance Administration

| Notebook | Lines | Purpose | Prerequisites |
|---|---|---|---|
| `12_admin_tagging` | 348 | Bulk-apply `gov.*` tags, validate, generate governed views | 02 (UC functions) |
| `12a_validate_identity_setup` | 233 | 9-check identity propagation validation | 09 (audit table), 02 (UC functions) |

---

## Warnings

- `06_dlt_streaming_pipeline.py` is a DLT definition — run it via `06a`, not directly.
- `08_federation_setup` requires real external PostgreSQL credentials. Use `08a` for demos.
- `08c_lakebase_setup` requires Lakebase to be available in your workspace (Public Preview).
- `12_admin_tagging` GRANT/REVOKE statements require catalog owner or account admin privileges.
- All notebooks assume `000-config` has been run in the same session.
- `_helper_seed_upstream_tables.py` is a helper invoked by `02_define_uc_tools` via `%run`. Do not run it directly.
