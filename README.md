<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">


## Business Problem

Telecoms are leveraging AI to achieve first-point resolution on customer issues and unify fragmented customer data to proactively enhance customer engagement and retention. This solution leverages AI to analyze incoming customer communications, understanding context and sentiment to prepare tailored responses for agents. We have picked one of the common billing issues for telco customers. 

This industry solution accelerator enables the automation and personalization of telecom billing customer care by leveraging customer-specific data available within the data ecosystem.

The aim is to help telco providers scale customer service operations with an AI-powered billing agent that leverages:
- Billing history, customer profiles, and device data
- Unstructured FAQs embedded in a vector search index
- Human-in-the-loop evaluation and observability tools
- A deployable web interface for interactive usage

Designed as a human-in-the-loop solution, it empowers customer service agents to resolve billing queries faster and more accurately, improving CX and reducing call centre load.

<p align="center">
  <img src="./images/billing_assistant.jpg" alt="Billing Assistant Diagram" width="600"/>
</p>


---


## Authors
Alex Barreto <alex.barreto@entrada.ai>

---

## Repository Structure

| Notebook | Description |
|----------|-------------|
| `000-config` | Central config for the accelerator  |
| `00_data_preparation` | Synthetic data generation using [Databricks Labs Data Generator](https://github.com/databrickslabs/dbldatagen). Simulates billing, device, and customer datasets. |
| `01_create_vector_search` | Builds the FAQ dataset and creates a vector search index using Databricks Vector Search. |
| `02_define_uc_tools` | Defines functions as tools in Unity Catalog. These are callable by the agent to query customer, billing, and device information and retrieve relevant data from the vector search with FAQ. |
| `03a_create_genie_space` | Creates a Databricks Genie Space for ad-hoc billing analytics over invoice and plan tables. |
| `03_agent_deployment_and_evaluation` | Builds, logs, evaluates, registers, and deploys the LangGraph agent to a model serving endpoint. |
| `04_agent_bricks_deployment` | Deploys the solution as a Databricks Agent Bricks Supervisor Agent combining a FAQ Knowledge Assistant with the Billing Analytics Genie Space. |
| `05_billing_anomaly_detection` | PySpark pipeline that detects billing anomalies (charge spikes, roaming spikes, data overages) and writes results to a Delta table with a UC function tool. |
| `06b_enable_streaming_prereqs` | One-time setup: enables CDF on billing_items, creates billing_monitoring_state table and billing_monitoring_summary view. |
| `06_dlt_streaming_pipeline` | DLT pipeline definition. Produces billing_events_streaming (enriched events) and billing_monthly_running (real-time charge accumulators). Not directly runnable — deployed by 06a. |
| `06a_create_dlt_pipeline` | Creates and starts the DLT continuous streaming pipeline via Databricks SDK. |
| `06c_monitoring_alerter` | Alert dispatch: finds unalerted anomalies and writes to billing_monitoring_state. Runs as Task 2 in the daily workflow. |
| `07_system_table_ingestion` | Materializes Databricks system tables (billing.usage, lakeflow.job_run_timeline, query.history) into Bronze/Silver/Gold telemetry tables. Requires system catalog access. |
| `08_federation_setup` | Sets up Lakehouse Federation: UC connection to external ERP (Track A). Skip for Track B (simulation). |
| `08a_erp_data_simulation` | Generates synthetic ERP accounts/orders, procurement costs, and FX rates (Track B). Creates ext_* view abstraction layer. |
| `08b_external_data_ingestion` | Medallion pipeline: ext_* views -> Silver (customer_account_dims, fx_daily, procurement_monthly) -> Gold (revenue_attribution, finance_operations_summary). |
| `09_writeback_setup` | Creates billing_disputes and billing_write_audit tables. Adds acknowledgement columns to billing_anomalies. Run before agent re-deployment. |
| `09a_dispute_aging` | Nightly dispute SLA enforcement: auto-escalates disputes open > 5 days. Runs as Task 5 in daily workflow. |
| `10_domain_config` | Domain adapter: reads domain YAML from `notebooks/domains/`, creates canonical views, regenerates UC tools. Run with `domain` = `telco` / `saas` / `utility`. |
| `10a_validate_domain` | Validates deployed domain: checks canonical views, UC tools, charge column alignment. |
| `11_persona_config` | Validates all persona YAML configs from `notebooks/personas/`, serializes persona metadata to config.yaml. |
| `dash-chatbot-app/` | A simple Dash web app that lets users chat with the deployed agent using the Databricks Apps framework. |

---

## How to Use

Follow the notebooks in **numerical order** for a smooth end-to-end experience:

1. **[000-config]** – Set up your catalog, schema, endpoint names, and runtime parameters.
2. **[00_data_preparation]** – Generate synthetic datasets for billing, customers, and devices.
3. **[01_create_vector_search]** – Build the FAQ dataset, create a Delta table, and generate a vector search index.
4. **[02_define_uc_tools]** – Define tools that expose customer data to the agent.
5. **[03a_create_genie_space]** – Create a Genie Space for ad-hoc billing analytics.
6. **[03_agent_deployment_and_evaluation]** – Build and log the LangGraph agent to MLflow, run agent evaluation, register to Unity Catalog, and deploy to a serving endpoint.
7. **[04_agent_bricks_deployment]** – Deploy as an Agent Bricks Supervisor Agent (KA + Genie Space) for a fully managed multi-agent experience.
8. **[05_billing_anomaly_detection]** – Run anomaly detection pipeline, create UC function tool and Genie table, then redeploy agent.
7. **[`dash-chatbot-app`]** – Launch the chatbot UI to interact with your agent.

---

## Deployment Path Capability Matrix

| Capability | LangGraph (nb 03) | Agent Bricks (nb 04) |
|---|---|---|
| FAQ retrieval | Yes | Yes |
| Fleet-wide analytics (Genie) | Yes | Yes |
| Individual customer lookup | Yes | No |
| Write-back (disputes, anomalies) | Yes | No |
| ERP/finance data | Yes | No |
| Streaming estimates | Yes | No |
| Operational KPIs | Yes | No |
| Persona filtering | Yes | No |
| Identity propagation | Yes | No |
| User-context-aware writes | Yes | No |

**If you need write-back, individual customer tools, identity propagation, or persona filtering, you MUST use the LangGraph path (notebook 03). Agent Bricks (notebook 04) is a read-only demo tier.**

---

## EchoStar Identity Propagation

The accelerator implements governed identity propagation using Pattern C (Hybrid):
- **Reads**: User identity passed as parameter through governed views that filter by effective_user_id.
- **Writes**: Token-gated mediation requiring both a valid pending-write token AND a valid RequestContext with user identity.
- **Audit**: Every operation records initiating_user (human), executing_principal (SP), session_id, request_id, and persona.

### Identity Flow

1. Databricks App extracts `x-forwarded-access-token` from HTTP header
2. App calls SCIM `/api/2.0/preview/scim/v2/Me` to get user email + groups
3. App builds `RequestContext` (user_email, user_groups, persona, session_id, request_id, issued_at, expires_at)
4. App signs RequestContext with HMAC-SHA256 using secret from Databricks Secret Scope
5. App sends `custom_inputs.request_context` to Model Serving
6. `agent.py predict()` validates signature, checks expiry, stores in thread-local state
7. Write tools require valid token + valid RequestContext
8. Audit records dual identity (human + SP)

### Setup

1. **Create the secret scope and HMAC key:**
   ```bash
   databricks secrets create-scope echostar-identity
   databricks secrets put-secret echostar-identity hmac-secret --string-value "$(openssl rand -base64 32)"
   ```

2. **Apply governance tags** by running notebook `12_admin_tagging.py`

3. **Validate the setup** by running notebook `12a_validate_identity_setup.py`

4. **Redeploy the agent** via notebook `03_agent_deployment_and_evaluation.py`

### Backwards Compatibility

If `request_context` is absent from `custom_inputs`, the agent still works but with degraded identity — logged as `identity_degraded=true` in audit. Only assets tagged `gov.identity.mode=required` will block access when user context is missing.

---

## Highlights

- **End-to-end LLM agent lifecycle**: From data to deployment.
- **Evaluation-first approach**: Includes synthetic question generation and MLflow integration for benchmarking agent performance.
- **Built-in vector search**: FAQ retrieval using vector search index and semantic similarity.
- **Fully governed**: Unity Catalog integration for tool and model registration.
- **Deployable UI**: Lightweight Dash app included for real-world usage and demoing.

<p align="center">
  <img src="./images/chatbot.jpg" alt="Billing Assistant Diagram" width="600"/>
</p>

---

## Requirements

- Databricks workspace with Unity Catalog enabled
- Access to Databricks Vector Search & Serving Endpoints
- Installed: `databricks-sdk`, `databricks-vectorsearch`, `mlflow`, `dash`, `langchain`, etc.
- Cluster or SQL Warehouse to execute notebooks
- Recommended Databricks Runtime: 15.4 ML

---

## Get Started

Start with `000-config` and move through each notebook step-by-step.  
This project is designed to be modular—feel free to extend tools, customize prompts, or connect new data sources.

---

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

---

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below. 

##This list needs to be updated

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
|  | |  |


