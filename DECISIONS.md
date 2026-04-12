# Architecture Decision Records

This document records WHY the system was built the way it was: what options were
considered, what tradeoffs were accepted, what constraints drove the choices, and
what tensions remain for a future maintainer.

---

## DEC-001: LangGraph as the Agent Orchestration Framework

**Status:** Decided
**Date:** 2026-01-20 (first agent.py: `e5ee862`)
**Commits:** `e5ee862`, `508f93d`, `cf2e2c9`, `1a952c6`

### Context

The accelerator needed an agent framework that (1) runs inside a Databricks Model Serving endpoint via MLflow `ChatAgent`, (2) supports tool calling with UC functions, and (3) could later be extended with human-in-the-loop write confirmation.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| LangGraph StateGraph + MLflow ChatAgent | Chosen. Present from first `agent.py` commit (`e5ee862`). | — |
| Mosaic AI Supervisor (Agent Bricks) | `04_agent_bricks_deployment.py` implements this as a separate deployment path (`6363a04`). | Agent Bricks supports only KA and Genie sub-agents — no custom tools, no write-back, no UC function binding. Chosen for a reduced-capability deployment tier, not as the primary agent. |
| Raw MLflow pyfunc (no graph) | The initial Databricks scaffold (`91c7916`) used placeholder notebooks (`notebook1.ipynb`, `notebook2.ipynb`), deleted in `fece09a`. No agent framework was present. | Replaced by LangGraph in the first real agent commit. No evidence this was evaluated as a long-term option. |

No evidence of LangChain AgentExecutor ever being in the codebase.

### Decision

LangGraph was chosen because its `StateGraph` compiles to a `CompiledGraph` that fits directly inside MLflow's `ChatAgent` interface (`predict`/`predict_stream`), and because its conditional edge model supports the 3-node graph pattern needed for write confirmation. The original graph (`e5ee862`) was 2-node (agent → tools → agent) with no recursion limit. The write-back commit (`cf2e2c9`) extended it to 3 nodes (agent → tools → confirm_or_cancel → agent) and added `recursion_limit=30`.

The `recursion_limit=30` signals that the agent is expected to make up to 15 tool-call round trips per invocation. In practice, a billing inquiry involving lookup + anomaly check + dispute staging + confirmation uses 4-6 round trips. The limit is set conservatively to avoid infinite loops without prematurely terminating complex multi-tool conversations.

### Consequences

**Accepted tradeoffs:**
- LangGraph graph definitions are imperative Python, not declarative config. The graph structure (3 nodes, conditional edges) is embedded in `create_tool_calling_agent()` (`agent.py:550-636`) and cannot be modified without code changes.
- The `ChatAgentToolNode` from `mlflow.langchain` couples the tool execution layer to MLflow's serialization format. Upgrading MLflow or LangGraph versions requires verifying this interface.

**What this locks in:**
- The agent runs in a Model Serving endpoint, not a Databricks notebook context. This means no `SparkSession` is available at inference time — which is why write-back uses Statement Execution API (see DEC-003).
- All tools must be LangChain `BaseTool` instances or UC functions wrapped via `UCFunctionToolkit`. Native Spark operations are not possible at inference time.

### Open questions / deferred work

None identified.

---

## DEC-002: Two Deployment Paths (LangGraph vs Agent Bricks)

**Status:** Decided
**Date:** 2026-01-25 (first Agent Bricks: `c7a8bdd`), 2026-03-24 (rewritten: `6363a04`)
**Commits:** `c7a8bdd`, `6363a04`, `6a05513`, `4192f51`

### Context

The accelerator needed to support two audiences: (1) teams that want full control over the agent with write-back, anomaly management, and persona-based tool filtering; (2) teams that want a quick, managed deployment using Databricks Agent Bricks with minimal configuration.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| LangGraph agent only (notebook 03) | The primary development path from `e5ee862` onward. | Does not provide a managed, no-code deployment option. |
| Agent Bricks Supervisor only (notebook 04) | First attempted as `04_agent_bricks_demo.py` (921 lines, `c7a8bdd`), deleted and rewritten as `04_agent_bricks_deployment.py` (`6363a04`). | Agent Bricks only supports KA and Genie Space sub-agents. No UC function tools, no write-back tools, no persona filtering. Insufficient for the full accelerator capability set. |
| Both paths | Chosen. | — |

### Decision

Both paths coexist because they serve different deployment tiers. The LangGraph path (notebook 03) deploys the full 19-tool agent with write-back, anomaly management, ERP integration, streaming estimates, and persona-based access control. The Agent Bricks path (notebook 04) deploys a Supervisor with only two sub-agents: a FAQ Knowledge Assistant and a Billing Analytics Genie Space. The Agent Bricks path was first prototyped in `c7a8bdd` on `feature/agent-bricks` (January 2026), then completely rewritten and merged to main in `6363a04` (March 2026).

The capability gap is significant. See the **Deployment Path Capability Matrix** in
README.md for the canonical 10-row comparison table (updated April 2026). In summary:
Agent Bricks supports FAQ retrieval and fleet-wide Genie analytics. It does NOT support
individual customer lookup, write-back, ERP data, streaming, KPIs, persona filtering,
or identity propagation.

The Agent Bricks MAS routing instructions (`agent_bricks/telco-billing-mas.md`) now
explicitly handle unsupported requests by directing users to the LangGraph deployment
instead of routing to nonexistent tools (fixed April 2026).

### Consequences

**Accepted tradeoffs:**
- Maintaining two deployment notebooks that share no code. Changes to the agent prompt, tool set, or evaluation approach in notebook 03 do not propagate to notebook 04.
- The README presents the notebooks as a sequence, creating the false impression that notebook 04 extends notebook 03.

**What this locks in:**
- Any new tool added to the LangGraph agent must be separately assessed for Agent Bricks feasibility. Most tools cannot be added to Agent Bricks because it does not support custom tool binding.

### Open questions / deferred work

- No capability comparison table in README or docs.
- The `feature/agent-bricks` branch has 7 unmerged commits (including `508f93d` "Implement Databricks Best Practices") that were superseded by the `feature/agent-bricks-for-realz` rewrite. The old branch is stale.

---

## DEC-003: Write-Back via Statement Execution API With Two-INSERT Audit Pattern

**Status:** Decided
**Date:** 2026-03-24 (`cf2e2c9`)
**Commits:** `cf2e2c9`, `8ede9f4`

### Context

The agent needs to write to Delta tables (`billing_anomalies`, `billing_disputes`, `billing_write_audit`) at inference time. The agent runs inside a Model Serving endpoint where there is no SparkSession.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Statement Execution API (SQL over HTTP) | Chosen. `_execute_sql()` in `agent.py:150-229` uses `w.statement_execution.execute_statement()`. | — |
| DLT pipeline writes | The streaming pipeline (`06_dlt_streaming_pipeline.py`) exists but handles only read-path enrichment. No evidence it was evaluated for write-back. | DLT pipelines are declarative and batch/streaming — not suitable for single-row transactional writes triggered by user actions. |
| Spark DataFrame writes | Not possible. `agent.py` runs in Model Serving, not a notebook. No SparkSession. | The `09a_dispute_aging.py` notebook uses `w.statement_execution` directly (line 37-47), confirming the Statement Execution API is the pattern for writes outside notebook context. Notably, `09a` explicitly states "does NOT import from agent.py" (line 11). |
| Parameterized queries via SDK | The `_sanitize_sql_str()` function (`agent.py:142-147`) is a custom string sanitizer rather than SDK-level parameterized queries. The Databricks SDK's `execute_statement` supports `parameters` in newer versions. | No evidence this was evaluated. The commit `8ede9f4` ("Validation") added `_sanitize_sql_str` as the SQL injection mitigation rather than migrating to parameterized queries. |

### Decision

Statement Execution API was chosen because it is the only SQL execution path available in a Model Serving endpoint context (no SparkSession). The two-INSERT audit pattern (`_execute_sql`, `agent.py:150-229`) writes a `PENDING` record before the business SQL and a `SUCCESS`/`FAILED` record after. This is a pure-append pattern — no UPDATEs on the audit table (`09_writeback_setup.py:73` comment: "Uses two-INSERT pattern (PENDING then SUCCESS/FAILED) — no UPDATEs needed"). The two-INSERT design ensures an audit record exists even if the business write fails or the process crashes between audit and business SQL.

### Consequences

**Accepted tradeoffs:**
- Every write operation requires a SQL warehouse to be running. The warehouse_id (`148ccb90800933a1`) is hardcoded in `config.yaml:5` and `000-config.py:71`. All write operations — agent writes, dispute aging, monitoring alerts — share this single warehouse.
- `_sanitize_sql_str` is a hand-rolled sanitizer that handles single quotes, backslashes, and null bytes. It does not handle all SQL injection vectors (e.g., Unicode normalization attacks). Parameterized queries would be structurally safer.
- `session_id=None` is hardcoded in all three write tool calls (`agent.py:413`, `:457`, `:497`). The `billing_write_audit` table has `agent_session_id` column, but it is always NULL.

**What this locks in:**
- Write latency is bounded by warehouse query latency (~1-3s per statement). Each write operation executes 2-3 SQL statements (PENDING audit + business SQL + SUCCESS/FAILED audit), so a single write takes 3-9s.
- The audit trail cannot be correlated to conversations without session_id.

### Open questions / deferred work

- `session_id=None` in all `_execute_sql` calls — `ChatContext.conversation_id` is available in `predict()` (`agent.py:669`) but never passed down. No TODO or comment marks this as deferred.
- `config.yaml:5`: `warehouse_id: 148ccb90800933a1` — hardcoded to a specific warehouse. `000-config.py:71` has the comment "This is the warehouse id and need to be updated for your environment" but no dynamic resolution mechanism exists.

---

## DEC-004: Write Confirmation as a Message-Content Sentinel

**Status:** Decided
**Date:** 2026-03-24 (`cf2e2c9`)
**Commits:** `cf2e2c9`, `8ede9f4`, `3e58106`

### Context

Write operations need a human confirmation step before execution. The confirmation state must survive the LangGraph message-passing cycle between the `tools` node (where `request_write_confirmation` returns) and the `confirm_or_cancel` node (where user intent is evaluated).

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| WRITE_PENDING_PREFIX sentinel in message content | Chosen. `agent.py:127`: `WRITE_PENDING_PREFIX = "WRITE_PENDING"`. The `request_write_confirmation` tool returns `f"{WRITE_PENDING_PREFIX}\|{json.dumps(pending)}"`. | — |
| Dedicated LangGraph state field | No evidence this was considered. The `ChatAgentState` type from `mlflow.langchain.chat_agent_langgraph` is used unmodified — no custom fields are added. | Extending `ChatAgentState` would require changes to the MLflow integration layer. The message-content approach works within the existing state schema. |

### Decision

The pending write is encoded as a `WRITE_PENDING|{json}` string prefix in a tool response message. `route_after_tools` (`agent.py:569-574`) detects this prefix in `messages[-3:]` and routes to `confirm_or_cancel`. The `_extract_pending_write` function (`agent.py:232-240`) searches `messages[-4:]` to parse the pending write JSON.

The window sizes differ: `route_after_tools` checks `[-3:]` and `_extract_pending_write` checks `[-4:]`. The routing window is smaller because it only needs to see the most recent tool output. The extraction window is one larger to account for a possible intervening tool_call message. Both windows were set in commit `cf2e2c9` and have not been changed since. There is no git history of a different window size.

`CONFIRM_PHRASES` and `CANCEL_PHRASES` (`agent.py:129-130`) are hardcoded string sets. The commit message for `3e58106` ("Multi-Persona Agent Architecture") notes: "Cancel safety: 'no' removed from CANCEL_PHRASES, 'ok' removed from CONFIRM_PHRASES (ambiguity fix from Gap #6 review)." This was a deliberate narrowing — LLM-classified intent was not considered; the approach is substring matching on the user's most recent message.

The "routing inconsistency" path (`agent.py:583-586`) returns `{"messages": []}` when `_extract_pending_write` returns `None` from `confirm_or_cancel`. The commit `8ede9f4` added a comment: "Routing inconsistency — WRITE_PENDING was detected by route_after_tools but _extract_pending_write couldn't parse it. Route back to agent cleanly." This is a known edge case, not an error handler.

### Consequences

**Accepted tradeoffs:**
- The pending write can be lost if more than 4 messages are inserted between staging and confirmation (the `messages[-4:]` window). Multi-tool conversations can exceed this window.
- The sentinel is a string convention, not a type-safe state field. Parsing errors in `_extract_pending_write` (e.g., malformed JSON after the prefix) silently drop the pending write.

**What this locks in:**
- The confirmation mechanism is coupled to message ordering. Any change to message insertion behavior (e.g., adding intermediate assistant messages) can break the window alignment.

### Open questions / deferred work

- No mechanism to extend the search window or move to a state-based approach. No TODO in the code.

---

## DEC-005: PII Fields Included in lookup_customer Response Schema

**Status:** Decided (no mitigation implemented)
**Date:** 2025-07-21 (`fece09a`)
**Commits:** `fece09a`, `508f93d`

### Context

The `lookup_customer` UC function needs to return enough data for the agent to identify a customer and retrieve related records (device_id for billing_items, plan for plan details). The function was part of the original accelerator code from Databricks Industry Solutions.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Return all customer fields including PII | Chosen. `02_define_uc_tools.py:51-77` returns `customer_id`, `customer_name`, `device_id`, `phone_number`, `email`, `plan`, `contract_start_dt`. | — |
| Return only non-PII fields | No evidence this was evaluated. The original `fece09a` commit from Databricks included PII in the schema, and no subsequent commit modified the RETURNS TABLE clause. | — |

### Decision

The UC function returns all customer fields. PII protection is delegated entirely to the agent prompt: `config.yaml:64` — "NEVER disclose confidential information in your responses, including: customer_name, email, device_id, or phone_number." The `customer_care.yaml` persona restates this (line 16). The domain YAML system (`telco.yaml:81-84`) lists `confidential_fields: [customer_name, email, account_identifier, product_identifier]`, but this list is informational — it is injected into the agent prompt by `10_domain_config.py`, not enforced by the function schema.

No git history shows a version of `lookup_customer` that omitted PII. The function has been unchanged in its RETURNS TABLE clause since `fece09a`.

The `device_id` field is functionally required: `lookup_billing_items` (`02_define_uc_tools.py:96-121`) takes `device_id` as input, and the agent must chain `lookup_customer` → `lookup_billing_items`. Removing `device_id` from the response would break this tool chain. However, `customer_name`, `email`, and `phone_number` have no downstream tool dependency — they are returned for display purposes only.

### Consequences

**Accepted tradeoffs:**
- PII enters the LLM context window on every `lookup_customer` call. Any prompt regression, persona misconfiguration, or model version change can result in disclosure.
- The `technical` persona system prompt (`technical.yaml:22`) says "Do NOT answer individual customer billing questions" but does not restate the PII confidentiality instruction. The `executive` persona blocks `lookup_customer` from its tool set entirely (`executive.yaml:39`).

**What this locks in:**
- PII disclosure risk is a prompt-reliability problem, not a schema-enforcement problem. This cannot be mitigated without changing the UC function definition.

### Open questions / deferred work

- No TODO or comment in the codebase acknowledges PII-in-schema as a risk.

---

## DEC-006: 19 Tools Bound Simultaneously + Persona System as Mitigation

**Status:** Partially implemented
**Date:** 2026-03-24 (tools: `cf2e2c9`; personas: `3e58106`)
**Commits:** `0b35d44`, `6f38135`, `1cb98fb`, `290da3e`, `cad9ece`, `cf2e2c9`, `3e58106`, `1a952c6`

### Context

The tool count grew incrementally across 8 feature branches merged sequentially in March 2026: 5 original UC tools → +Genie → +anomalies → +monitoring → +telemetry (2 tools) → +ERP (3 tools) → +write-back (5 tools) → +disputes (2 UC read tools) = 19 total. The persona system was the last feature added (`3e58106`), after all 19 tools were already registered.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| All 19 tools bound to every agent | The base agent at `agent.py:711` does this: `agent = create_tool_calling_agent(llm, tools, system_prompt)`. | Still the default when no persona is specified. |
| Persona-based tool filtering | Added in `3e58106`. `_get_persona_agent()` (`agent.py:643-658`) filters tools via `[t for t in tools if _tool_name(t) in allowed]`. | — |

### Decision

The persona system was introduced as a mitigation for tool routing confusion after the full tool set was in place. The commit message for `3e58106` calls this "Gap #8" and describes it as an architecture addition, not a retrofit. However, the git history shows all 19 tools were already bound before the persona system was added.

The `None` vs `[]` distinction for `tool_policy` (`agent.py:96-98`): `None` means the persona YAML has no `tool_policy` section at all — all tools are allowed (backwards compatibility with pre-persona deployments). `[]` (empty list) means the persona explicitly has zero tools. The commit `1a952c6` body notes this was a bug fix: "Changed if allowed else tools to explicit None vs [] semantics — empty list now correctly means zero tools."

The `_PERSONA_AGENTS` cache (`agent.py:69`) stores compiled graphs per persona, built on first use. This implies personas are expected to be stable within a serving endpoint lifecycle — not switched per-request at high frequency. The persona is selected per request via `custom_inputs.get("persona")` (`agent.py:673`), but the graph compilation happens only once per persona.

`DEFAULT_PERSONA` is `customer_care` (`config.yaml:23`, `agent.py:110`). Unknown persona names fall back silently with a `logger.warning()` (`agent.py:675`).

### Consequences

**Accepted tradeoffs:**
- The base agent (`agent.py:711`) still binds all 19 tools. This agent is constructed at module import time (even in serving) but is not used for serving requests — `predict()` always goes through `_get_persona_agent()` with the default `customer_care` persona. The base agent construction is waste.
- Tool counts per persona: customer_care (15), finance_ops (14), technical (5), executive (3). Only executive and technical achieve meaningful tool reduction.

**What this locks in:**
- Adding a new tool requires updating every persona's `allowed_tools` or `blocked_tools` list in the YAML files. There is no "inherit from base" mechanism — each persona explicitly lists its full tool set.

### Open questions / deferred work

- `personas: {}` in `config.yaml:25` — an empty dict placeholder. The persona system loads from YAML files, not from this config key. Purpose unclear.
- `persona_config_path: ''` in `config.yaml:24` — empty string. Persona files are resolved via `_Path(__file__).parent / "personas"` first (`agent.py:74`), then this config path, then `MLFLOW_MODEL_URI`. The config path is a fallback that has never been populated.

---

## DEC-007: Genie Space for Fleet Analytics Instead of Direct SQL Tool

**Status:** Decided
**Date:** 2026-03-24 (`0b35d44`)
**Commits:** `0b35d44`, `38a6c4c`, `6a05513`

### Context

The agent needs to answer fleet-wide analytical questions (trends, aggregations, comparisons) that span the entire billing dataset. These queries require SQL generation, not just parameterized lookups.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Genie Space (hosted SQL analytics with natural language) | Chosen. `ask_billing_analytics` tool in `agent.py:295-359`. Genie Space created by `03a_create_genie_space.py`. | — |
| UC function with predefined SQL | All 14 UC functions (`02_define_uc_tools.py`) are parameterized lookups — they accept specific inputs and return specific schemas. The `get_finance_operations_summary(lookback_months)` tool is the closest to an analytics query but has a fixed schema. | UC functions cannot generate arbitrary SQL. Each new analytical question would require a new function. |

### Decision

Genie Space was chosen because it can generate and execute arbitrary SQL against the billing dataset, handling question types that cannot be anticipated with predefined UC functions. The `ask_billing_analytics` tool docstring explicitly delineates: "Do NOT use this for individual customer lookups — use the dedicated lookup_customer, lookup_billing, or lookup_billing_items tools instead." This boundary is the intended routing contract.

The Genie Space is configured with 20 tables and 28 sample questions (`000-config.py:167-224`). The sample questions serve as few-shot examples for Genie's SQL generation.

The polling loop (`agent.py:319-335`) uses `max_attempts=30` with exponential backoff (base 1.0s, factor 1.5x, capped at 5.0s). This yields a maximum wait of approximately 2.5 minutes. The backoff parameters were added in commit `1a952c6` — the original implementation (`0b35d44`) used the same structure.

`start_conversation` is called per `ask_billing_analytics` invocation (`agent.py:310`). There is no conversation reuse. The `genie_space_id` is loaded from `config.yaml:56` and the `_genie_client` is a module-level `WorkspaceClient` (`agent.py:293`), but each call creates a new Genie conversation.

### Consequences

**Accepted tradeoffs:**
- Genie cold start: first call after inactivity takes 45-90s (est). No keep-warm mechanism exists.
- No session reuse: each analytical question starts a fresh Genie conversation, losing any context from prior questions.
- The agent has no visibility into Genie's SQL generation. If Genie generates incorrect SQL, the agent passes through the error message without correction.

**What this locks in:**
- Genie Space must be created before deploying either the LangGraph agent (notebook 03) or the Agent Bricks Supervisor (notebook 04). Both paths depend on `genie_space_id`.

### Open questions / deferred work

- `genie_space_id: ''` is the initial value in `config.yaml:56`. It is set by `03a_create_genie_space.py` at runtime. `000-config.py:225`: `config['genie_space_id'] = None`.

---

## DEC-008: DLT for Streaming Billing Events

**Status:** Decided
**Date:** 2026-03-24 (`1cb98fb`)
**Commits:** `1cb98fb`, `15514d2`, `1a952c6`

### Context

The accelerator needed near-real-time billing event enrichment and running charge estimates. The source is the `billing_items` Delta table with CDF enabled.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| DLT (Delta Live Tables) | Chosen. `06_dlt_streaming_pipeline.py` defines two DLT tables. `06a_create_dlt_pipeline.py` deploys via SDK. | — |
| Spark Structured Streaming with manual checkpointing | No evidence this was evaluated. The pipeline was introduced as DLT from the first commit (`1cb98fb`). | — |

### Decision

DLT was chosen for its managed infrastructure (automatic retries, checkpointing, expectations). The pipeline is set to `continuous=True` in `06a_create_dlt_pipeline.py:59` with the comment: "continuous=True keeps the cluster alive for near-real-time processing. For cost-effective demo, set continuous=False and use a trigger interval instead."

The pipeline definition is separated from deployment: `06_dlt_streaming_pipeline.py` contains only DLT table definitions (cannot be run directly — first cell warns of this), and `06a_create_dlt_pipeline.py` creates the pipeline via `w.pipelines.create()`. This separation exists because DLT pipeline definitions are executed by the DLT runtime, not by a notebook runner. The SDK-based deployment allows programmatic pipeline management with config-driven parameters.

CDF is enabled on `billing_items` (`06b_enable_streaming_prereqs.py:17-19`) as the streaming source. CDF was chosen over Auto Loader because the data originates from a Delta table (written by `00_data_preparation.py`), not from external files. CDF provides row-level change tracking on existing Delta tables. No evidence of Kafka or other message bus consideration.

The `billing_monthly_running` Gold table (`06_dlt_streaming_pipeline.py:90-148`) uses `dlt.read_stream("billing_events_streaming").groupBy(...).agg(...)` without a watermark. There is no comment in the code acknowledging this as a deliberate choice. The commit `1a952c6` added `@dlt.expect` annotations to the Gold table but did not add a watermark. This appears to be an oversight — the re-aggregation behavior of streaming groupBy in DLT (complete output mode on each micro-batch) was likely not anticipated.

### Consequences

**Accepted tradeoffs:**
- `billing_monthly_running` re-aggregates all history on each micro-batch. Performance degrades linearly with data volume.
- The pipeline requires a dedicated cluster (`num_workers: 1` in `06a_create_dlt_pipeline.py:70`), which incurs cost even when idle if `continuous=True`.

**What this locks in:**
- CDF on `billing_items` is a prerequisite. Disabling CDF breaks the streaming pipeline.

### Open questions / deferred work

- `dlt_pipeline_id: ''` in `config.yaml:50` — empty until `06a_create_dlt_pipeline.py` runs and populates it.
- No watermark on `billing_monthly_running` — no TODO or comment acknowledges the re-aggregation behavior.

---

## DEC-009: Anomaly Detection as a Batch PySpark Job

**Status:** Decided
**Date:** 2026-03-24 (`6f38135`)
**Commits:** `6f38135`, `8dd4ed5`, `1a952c6`

### Context

The accelerator needed to detect billing anomalies (charge spikes, roaming spikes) and make them available to the agent and monitoring systems.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Batch PySpark job against invoice table | Chosen. `05_billing_anomaly_detection.py` runs against the materialized `invoice` table. | — |
| DLT streaming anomaly detection from `billing_events_streaming` | The DLT pipeline (`06_dlt_streaming_pipeline.py`) exists but performs enrichment, not anomaly detection. No evidence streaming anomaly detection was evaluated. | — |

### Decision

Batch detection was chosen because anomaly detection requires per-customer historical baselines (mean, stddev over all billing months). The z-score calculation (`05_billing_anomaly_detection.py:92-102`) uses window functions over the full customer history: `F.avg("total_charges").over(w_customer)`, `F.stddev("total_charges").over(w_customer)`. This requires the complete invoice table, not incremental events.

`MIN_MONTHS=2` (`000-config.py:81`) requires a minimum of 2 months of history before flagging anomalies. This is a statistical floor — a stddev over 1 month is undefined, and a mean over 1 month has no baseline to compare against.

The thresholds (z-score 2.0, roaming multiplier 3.0, international multiplier 3.0) were initially hardcoded in `6f38135` and extracted to config in `1a952c6`. The commit body for `1a952c6` notes: "Thresholds now read from config.get() with defaults, instead of hardcoded constants." No calibration evidence exists in the commit messages — these appear to be initial values selected for reasonable sensitivity.

The `anomaly_id` is computed at query time as `CONCAT(CAST(customer_id AS STRING), '-', event_month, '-', anomaly_type)` — visible in `agent.py:406`, `06b_enable_streaming_prereqs.py:58-60`, and `06c_monitoring_alerter.py:47-49`. This is used consistently across all systems (agent write-back, monitoring alerts, monitoring summary view) but is not a stored column. The `billing_anomalies` table schema (`05_billing_anomaly_detection.py:197-204`) has no `anomaly_id` column. The table is written with `mode("overwrite")` on each pipeline run (`05_billing_anomaly_detection.py:227`), so all rows are replaced — a UUID column would be regenerated on each run anyway.

### Consequences

**Accepted tradeoffs:**
- Anomaly detection runs on a schedule, not in real time. A charge spike is only detected at the next pipeline run, not when the billing event arrives.
- The `overwrite` mode means anomaly acknowledgements (stored in separate columns added by `09_writeback_setup.py:28-41`) are wiped on each pipeline run. The acknowledgement columns (`acknowledged_by`, `acknowledged_at`, `acknowledgement_reason`, `linked_dispute_id`) are ALTERed onto the table after the initial overwrite.

**What this locks in:**
- The composite anomaly_id is not unique if a customer has multiple anomalies of the same type in the same month. Acknowledgement via `acknowledge_anomaly` matches all such rows.

### Open questions / deferred work

- The interaction between `overwrite` mode in `05` and the acknowledgement columns added by `09` is fragile. After an `05` re-run, all acknowledgement data is lost unless the `overwrite` is changed to `merge`.

---

## DEC-010: Domain/Persona YAML System

**Status:** Decided
**Date:** 2026-03-24 (domains: `3b8ef8e`; personas: `3e58106`)
**Commits:** `3b8ef8e`, `3e58106`, `1a952c6`

### Context

The accelerator was built for telco billing but needs to be reusable across industries (SaaS, utilities) and within a single deployment for different user roles (customer care, finance, executive, technical).

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| YAML-based domain/persona configs with canonical views | Chosen. Three domain YAMLs, four persona YAMLs, view generation notebook. | — |
| Separate repos per vertical | No evidence this was considered. The commit message for `3b8ef8e` ("Cross-Industry Schema Abstraction") frames this as a single-repo solution with a "three-layer" architecture. | — |

### Decision

**Domains:** The domain YAML system maps industry-specific column names and business terminology to a canonical view layer. `10_domain_config.py` reads a domain YAML and creates four views (`v_billing_summary`, `v_customer_profile`, `v_service_catalog`, `v_billing_events`) with standardized column names. The commit body for `3b8ef8e` explains the "graceful degradation" design: "When 10_domain_config.py runs with domain=saas and source tables don't exist, it creates stub views (WHERE 1=0) with the correct schema but zero rows." Views were chosen over table renaming because the underlying tables are referenced by other components (Genie Space, UC functions, anomaly detection) that use physical table names. Views provide a translation layer without breaking existing references.

**Personas:** The persona system provides per-role tool filtering, system prompt override, and response style guidance. The deployment model is one endpoint, multiple personas — persona is selected per request via `custom_inputs: {"persona": "finance_ops"}` passed from the Dash UI (`model_serving_utils.py:12`). The four personas are illustrative: the commit message for `3e58106` calls them out by count and purpose but does not reference specific customer requirements.

### Consequences

**Accepted tradeoffs:**
- Domain YAML changes require re-running `10_domain_config.py` to regenerate views. There is no automatic sync.
- Persona changes require restarting the serving endpoint (the `_PERSONA_AGENTS` cache is populated at first use and never invalidated).
- The canonical views introduce an indirection layer. Debugging a query requires tracing from agent tool → UC function → canonical view → physical table.

**What this locks in:**
- The 4 canonical view names and their column schemas are a contract. Changing them requires updating all domain YAMLs and any UC functions that reference them.

### Open questions / deferred work

- `domain_charge_labels: {}` and `domain_agent_prompt_section: ''` in `config.yaml:29-30` — empty until `10_domain_config.py` runs.
- `personas: {}` in `config.yaml:25` — unused. Persona system reads from YAML files.
- Only `telco.yaml` has been tested against real data. `saas.yaml` and `utility.yaml` are templates that create stub views.

---

## DEC-011: ERP Integration via Two Parallel Tracks

**Status:** Decided
**Date:** 2026-03-24 (`cad9ece`)
**Commits:** `cad9ece`

### Context

The accelerator needs ERP data (accounts receivable, credit ratings, procurement costs, FX rates) for dispute investigation and finance reporting. Not all deployment environments have an external ERP database.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Dual-track: real Lakehouse Federation (Track A) + synthetic simulation (Track B) | Chosen. Track determined by `erp_connection_host` being empty or populated. | — |

### Decision

The commit body for `cad9ece` explains: "Track A (real): erp_connection_host set → UC connection + foreign catalog → ext_* views point at federated tables. Track B (simulation): erp_connection_host empty → simulated_* tables via dbldatagen → ext_* views point at simulation. Downstream code only reads ext_* views — never touches the track-specific tables directly. Swapping tracks = redefining 4 views."

The `ext_*` view abstraction layer is the key design: `ext_erp_accounts`, `ext_erp_orders`, `ext_procurement_costs`, `ext_fx_rates`. These views point at either federated foreign tables or local simulation tables. All downstream code (Silver/Gold pipeline in `08b`, agent tools, Genie Space) reads only from `ext_*` views. This guarantees that swapping from simulation to real federation requires only redefining 4 views — no downstream changes.

ERP data is ingested through a medallion pipeline (`08b_external_data_ingestion.py`) rather than queried live via Federation because: (1) Federation queries have latency that would degrade agent response time, (2) the Silver/Gold transformation (customer account dimensions, revenue attribution) requires joins and aggregations that are better materialized, and (3) the Gold tables (`gold_revenue_attribution`, `gold_finance_operations_summary`) are added to the Genie Space for analytics.

### Consequences

**Accepted tradeoffs:**
- ERP data has pipeline lag. The agent prompt acknowledges this: `config.yaml:95` — "ERP data may have a 24-hour lag."
- The simulation data is generated by `dbldatagen` and does not match real ERP schema variations. Track B is suitable for demos, not for schema validation.

**What this locks in:**
- The 4 `ext_*` view names are a contract between the federation/simulation layer and the medallion pipeline.

### Open questions / deferred work

- `erp_connection_host: ''`, `erp_connection_user: ''`, `erp_connection_password: ''` in `config.yaml:35-39` — all empty. `000-config.py:115-116` includes: `# WARNING: Do NOT commit real passwords. Use Databricks Secret Scope: config['erp_connection_password'] = dbutils.secrets.get(scope="billing-erp", key="password")`. The secrets approach is documented as a comment but not implemented — the line is commented out.

---

## DEC-012: History Trimming Strategy (Front-Trim, Not Summarization)

**Status:** Decided
**Date:** 2026-03-27 (`1a952c6`)
**Commits:** `1a952c6`

### Context

LLM context windows are finite. Long billing conversations (especially dispute resolution involving multiple tool calls) can exceed the context budget. The agent needs a strategy to manage message history.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Front-trim: keep system message + last N messages | Chosen. `_trim_history()` at `agent.py:661-667`. | — |
| Summarization checkpoints | No evidence this was evaluated. No TODO or comment references summarization. | — |
| No trimming | The pre-`1a952c6` code (visible in `0b35d44` version of `agent.py`) had no `_trim_history` call. The original `predict()` passed all messages directly. | Led to context overflow on long conversations. |

### Decision

Front-trim was chosen for simplicity. `_trim_history` keeps the system message(s) plus the last `MAX_HISTORY_TURNS * 2` non-system messages. `MAX_HISTORY_TURNS=20` and `MAX_AGENT_TOKENS=4096` are in `000-config.py:74-75`. The commit body for `1a952c6` lists this as a "HIGH severity" fix: "_trim_history() keeps system msg + last N turn pairs, preventing context overflow."

The `* 2` factor accounts for user/assistant pairs. However, tool call messages (assistant tool_call + tool result) also count as non-system messages, so the effective turn budget is lower than 20 conversational turns.

`MAX_AGENT_TOKENS=4096` is the `max_tokens` parameter passed to `ChatDatabricks` (`agent.py:50`). This limits response length, not context length. The actual context window depends on the model (`databricks-claude-3-7-sonnet` per `config.yaml:3`). There is no code that measures or enforces total context size — the token limit is per-response only.

### Consequences

**Accepted tradeoffs:**
- Front-trim discards the oldest messages, which often include the initial `lookup_customer` and `lookup_billing` results that established the conversation context. The agent may re-request information it already retrieved.
- No distinction between conversational messages and tool call/result messages in the trim logic.

**What this locks in:**
- The trim strategy is in `agent.py`, not configurable via YAML. Changing the strategy requires code changes.

### Open questions / deferred work

- No comment or TODO in `_trim_history` acknowledges the initial-context-loss problem.

---

## DEC-013: Dash App for the UI

**Status:** Decided
**Date:** 2025-07-21 (`fece09a`)
**Commits:** `fece09a`, `508f93d`, `3e58106`, `1a952c6`

### Context

The accelerator needs a web interface for interacting with the deployed agent. Databricks Apps supports Dash, Streamlit, Gradio, Flask, and FastAPI.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Dash (Plotly) | Chosen. `apps/dash-chatbot-app/` with `DatabricksChatbot.py`, `app.py`, `model_serving_utils.py`. | — |
| Streamlit | The deleted `apps/demo_app/app.py` (`fece09a` delete, `91c7916` initial) was a 47-line Streamlit app. | Replaced by Dash in `fece09a` (the same commit that added the full accelerator code). No reason stated for the switch. |

### Decision

Dash was chosen. The `DatabricksChatbot.py` component implements a custom chat UI with Bootstrap styling (`dbc.themes.FLATLY`), a persona selector (`RadioItems` added in `3e58106`), rate limiting (`_MIN_REQUEST_INTERVAL=2.0s`, `1a952c6`), and input length limiting (`maxLength=2000`, `1a952c6`). The Dash app references `app.py` line 3: a reference to Databricks docs for "a more comprehensive example, with support for streaming output."

`model_serving_utils.py` wraps the MLflow deployment client (`mlflow.deployments.get_deploy_client('databricks')`). The `persona` parameter is passed via `custom_inputs` (`model_serving_utils.py:12`). This module is separated from the chatbot for testability — it can be imported and called independently of the Dash UI.

The `SERVING_ENDPOINT` is configured via environment variable in `app.yaml`. The Dash app does not implement streaming — it makes a single synchronous `predict` call and waits for the complete response.

### Consequences

**Accepted tradeoffs:**
- No streaming output. The user sees no intermediate results while the agent makes tool calls (which can take 5-30s depending on Genie cold start).
- The Dash UI is tightly coupled to the Databricks Apps deployment model (`app.yaml` with `SERVING_ENDPOINT` env var).

**What this locks in:**
- Persona selection is a UI-level concern. The Dash app hardcodes the four persona options (`DatabricksChatbot.py:34-39`). Adding a new persona requires updating both the YAML file and the Dash UI.

### Open questions / deferred work

None identified.

---

## DEC-014: Dispute SLA Enforcement as a Separate Scheduled Job

**Status:** Decided
**Date:** 2026-03-24 (`cf2e2c9`)
**Commits:** `cf2e2c9`

### Context

Billing disputes have an SLA: disputes that remain OPEN or UNDER_REVIEW for more than 5 days should be auto-escalated.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Separate scheduled notebook (`09a_dispute_aging.py`) | Chosen. The notebook runs as "Task 5 in the daily monitoring workflow" (notebook header comment, line 9). | — |
| DLT pipeline table constraint | No evidence. DLT handles streaming enrichment, not business-logic enforcement. | — |
| Delta table trigger | No evidence. Delta tables do not support UPDATE triggers natively. | — |

### Decision

Dispute aging is a separate notebook because it is a scheduled business rule, not a streaming or event-driven operation. The notebook explicitly does NOT import from `agent.py` (`09a_dispute_aging.py:11`: "NOTE: This notebook does NOT import from agent.py. Write logic is self-contained"). This is a deliberate decoupling — the aging job uses the Statement Execution API directly rather than depending on the agent's write infrastructure.

The 5-day threshold is hardcoded in the SQL WHERE clause (`09a_dispute_aging.py:43`: `AND updated_at < CURRENT_TIMESTAMP - INTERVAL 5 DAYS`). It is not in `config.yaml` or `000-config.py`.

The README mentions "Task 5 in daily workflow" but there is no `databricks.yml` workflow definition in the current codebase — the original `databricks.yml` was deleted in `fece09a`. The workflow exists only as documentation, not as code.

### Consequences

**Accepted tradeoffs:**
- The 5-day SLA is hardcoded. Changing it requires editing the notebook.
- The aging job uses the same `warehouse_id` from config as the agent write-back — same single-warehouse contention issue.
- The aging job follows the two-INSERT audit pattern independently (lines 72-85, 105-118), duplicating the logic from `agent.py:_execute_sql`. Changes to the audit pattern must be made in both places.

**What this locks in:**
- The daily workflow must be configured manually in Databricks Jobs. There is no bundle-defined workflow in the current codebase.

### Open questions / deferred work

- No workflow definition in `databricks.yml` (deleted). The CI workflow (`.github/workflows/databricks-ci.yml`) references `databricks bundle run demo_workflow` but no `resources.jobs` block exists.

---

## DEC-015: System Table Telemetry Materialization

**Status:** Decided
**Date:** 2026-03-24 (`290da3e`)
**Commits:** `290da3e`, `e0388ed`

### Context

The agent needs to answer operational health questions about the billing platform itself — DBU costs, job reliability, warehouse performance. Databricks system tables (`system.*`) are the data source, but they have two limitations: (1) they are read-only and cannot be added to Genie Spaces directly, (2) they require system catalog access which may not be enabled on all workspaces.

### Options Considered

| Option | Evidence it was considered | Why not chosen (if known) |
|---|---|---|
| Materialize system tables into user catalog | Chosen. `07_system_table_ingestion.py` creates Bronze/Silver/Gold telemetry tables. | — |
| Query system tables directly via UC functions | Not possible — system tables cannot be added to Genie Spaces, and UC functions in a user schema cannot reference `system.*` tables reliably across workspaces. | — |

### Decision

`07_system_table_ingestion.py` (605 lines) probes system table availability before ingestion (`probe_system_table` function, line 83-87) and gracefully degrades when tables are unavailable. The notebook header explains: "System tables (system.*) are read-only and cannot be added to Genie Spaces directly — this notebook snapshots them into the user catalog for analytics."

The materialized tables feed two UC tools: `lookup_operational_kpis` and `lookup_job_reliability`. These tools are available to the `technical` persona (5 tools) and `finance_ops` persona (14 tools), but blocked from `customer_care` and `executive` (`personas/*.yaml` tool_policy sections). This is the intended audience: platform engineers and finance ops, not end customers.

### Consequences

**Accepted tradeoffs:**
- Materialization creates stale data. The notebook must be scheduled to keep telemetry current.
- The Gold KPI table writes NULL for missing sources when system tables are unavailable.

**What this locks in:**
- 7 telemetry table names in `config.yaml:43-49` and `000-config.py:100-106` are committed to the schema.

### Open questions / deferred work

None identified.

---

## DEC-016: `feature/dabsdeploy` Branch

**Status:** Deferred (branch does not exist)
**Date:** N/A
**Commits:** None

### Context

The CI workflow (`.github/workflows/databricks-ci.yml:7-11`) references `feature/dabsdeploy` as a trigger branch for both `pull_request` and `push` events, alongside `main`. The workflow validates and runs a Databricks Asset Bundle.

### Decision

The branch does not exist in the repository (confirmed via `git branch -a`). The CI workflow was committed in the initial scaffold (`91c7916`, 2025-07-21) and has not been updated since. The `databricks.yml` bundle definition was deleted in `fece09a` (same date) when the accelerator code replaced the scaffold. The CI workflow references `databricks bundle validate` and `databricks bundle run demo_workflow`, neither of which can succeed without `databricks.yml`.

This is dead configuration — the CI workflow targets a branch that was never created and depends on a bundle definition that was deleted. The workflow runner (`html_publisher`) is also non-standard, suggesting this was copied from a template and not adapted.

### Consequences

The CI workflow does not function. There is no operational CI/CD for this repository.

### Open questions / deferred work

- The entire `.github/workflows/` directory is vestigial. The `publish.yaml` workflow (221 lines, committed in `91c7916`) is a Databricks Industry Solutions publishing template, not adapted for this repo.
- If Databricks Asset Bundles deployment is needed, a new `databricks.yml` must be written and the CI workflow updated.

---

## DEC-017: EchoStar Identity Propagation (Pattern C — Hybrid)

**Status:** Implemented
**Date:** 2026-04-10

### Context

PM-001 (PII disclosure), PM-002 (write confirmation bypass), and PM-010 (no session linkage
in audit) all stem from the same root cause: the agent has no knowledge of who the human
user is. The serving endpoint runs as a service principal. The Dash app has the user's OAuth
token (via `x-forwarded-access-token`) but does not forward it. Write operations execute
under SP identity with no human attribution.

### Options Considered

| Option | Why not chosen (if applicable) |
|---|---|
| Pattern A: Full user-context passthrough (user token forwarded to serving) | Model Serving does not support per-request OAuth delegation. The SP executes all SQL regardless of who triggered it. |
| Pattern B: Pure SP mediation (no user context) | This is what existed — and it caused PM-001, PM-002, PM-010. |
| Pattern C: Hybrid — user context for reads via governed views, token-gated mediation for writes | Chosen. |

### Decision

Pattern C (Hybrid) was chosen because it works within Model Serving constraints (SP executes
all SQL) while propagating human identity for authorization and audit:

1. **App layer** extracts `x-forwarded-access-token`, calls SCIM `/Me` to resolve email + groups,
   builds a `RequestContext` dataclass, signs it with HMAC-SHA256 (secret from Databricks Secret
   Scope), and sends it in `custom_inputs.request_context`.

2. **Agent layer** validates the signature, checks expiry, stores the `RequestContext` in a
   `contextvars.ContextVar` (per-request, thread-safe, generator-safe via `copy_context()`).

3. **Write tools** check for valid `RequestContext` before executing — this is a code-level
   hard guard, not a prompt convention. If no context is present, the write is blocked and the
   token is preserved for retry.

4. **Audit trail** records dual identity: `initiating_user` (human from RequestContext) and
   `executing_principal` (SP). `identity_degraded=true` when user context is unavailable.

5. **Persona-group binding**: `persona_group_map` in config restricts which Databricks groups
   can use which personas. Validated at tool-filtering time, not prompt time.

The `contextvars.ContextVar` was chosen over `threading.local()` after discovering that
`threading.local` leaks identity across requests when Model Serving reuses threads before a
streaming generator is fully consumed. `copy_context()` snapshots the identity at `predict_stream()`
call time and wraps each `next()` on the inner generator in `snapshot.run()`, so the generator
always sees its original identity even if the thread moves on to a new request.

### Consequences

**Accepted tradeoffs:**
- HMAC signing is symmetric — if the secret leaks, contexts can be forged. Rotation requires
  a coordinated secret scope update + serving endpoint restart.
- SCIM is called on every request from the App layer (mitigated with 5-minute TTL cache per token).
- `model_serving_utils.py` inlines a copy of `_RequestContext` to avoid import path issues. The
  App-side and Agent-side signing must produce identical bytes — pinned JSON separators
  `(",",":")` and `sort_keys=True` enforce this, but it is a drift risk.
- Backwards compatible: if `request_context` is absent from `custom_inputs`, the agent works
  with degraded identity. Only assets tagged `gov.identity.mode=required` block access.

**What this locks in:**
- Every Databricks App deployment must have access to the `echostar-identity` secret scope.
- The Dash/Gradio app must run behind Databricks Apps proxy (which provides `x-forwarded-access-token`).

### Open questions / deferred work

- Inline `_RequestContext` in `model_serving_utils.py` should be extracted to a shared package
  to eliminate drift risk (tracked as M12).

---

## DEC-018: PII Function Isolation via Schema Separation and UC Grants

**Status:** Implemented
**Date:** 2026-04-12

### Context

PM-001 established that PII must be removed from `lookup_customer`'s response schema. The fix
(removing PII columns from RETURNS TABLE) was applied in April 2026. However, a `lookup_customer_pii_internal`
function was initially created in the same schema as all other agent tools. The Genie Space
(`ask_billing_analytics`) has write SQL access to the same schema via its warehouse, meaning
it could potentially call the PII function or query the raw `customers` table directly — even
though the function is not registered as an agent tool.

### Options Considered

| Option | Why not chosen (if applicable) |
|---|---|
| Keep PII function in same schema, rely on "not registered" as guard | The Genie Space can generate arbitrary SQL. Discoverability via `SHOW FUNCTIONS` or `information_schema.routines` makes this a soft control. |
| Move PII function to a separate schema with restricted grants | Chosen. |
| Delete PII function entirely | Backend audit processes need full customer data for dispute investigation and regulatory reporting. |

### Decision

Four-layer defense:

1. **Schema isolation**: PII function moved to `{schema}_internal` — a separate UC schema that
   the agent SP and Genie warehouse cannot access.

2. **UC grants**: `REVOKE USE SCHEMA` on `{schema}_internal` from agent SP. `REVOKE SELECT` on
   raw `customers` table from agent SP. `GRANT EXECUTE` on safe `lookup_customer` function
   (definer rights allow the function to read `customers` even though the SP cannot).

3. **Genie Space instructions**: Curated `instructions` field in Genie Space configuration with
   explicit rules: never query `customers` directly, never reference `*pii*` or `*_internal*`
   functions/schemas, never include PII columns in output.

4. **Governance tags**: `{schema}_internal` tagged with `gov.pii.level=high`,
   `gov.identity.mode=required`, `gov.allowed.personas=admin`.

### Consequences

**Accepted tradeoffs:**
- `REVOKE SELECT` on `customers` means the agent cannot query the raw table directly. The
  non-PII `lookup_customer` function uses definer rights (executes as the function creator,
  not the caller) to read the table through the function. This is correct UC behavior.
- Genie instructions are a soft control — they guide SQL generation but are not an
  enforcement boundary. They are layered on top of the hard UC grants.
- `GRANT`/`REVOKE` statements require the notebook runner to be a catalog owner or account
  admin. If they fail, they must be applied manually via Catalog Explorer.

---

## DEC-019: UC Function Result Set Bounding

**Status:** Implemented
**Date:** 2026-04-12

### Context

PM-011 identified that `lookup_billing_items` returned unbounded result sets. Review in April
2026 found the same issue in 5 additional functions: `lookup_billing` (unbounded invoice
history), `get_monitoring_status`, `lookup_operational_kpis`, `lookup_job_reliability`,
`lookup_revenue_attribution`, and `get_finance_operations_summary`.

UC function tools return their full result set as a serialized string in the LLM's context
window. Unbounded queries create three failure modes: (1) context window overflow causing
truncated or degraded responses, (2) Statement Execution API timeouts on the 30s
`wait_timeout`, (3) excessive token consumption on data the LLM cannot usefully process.

### Options Considered

| Option | Why not chosen (if applicable) |
|---|---|
| Add `LIMIT N` to each function | Chosen as the baseline — simple, declarative, no code changes needed in agent.py. |
| Add lookback parameters with `LEAST()` caps | Chosen in combination with LIMIT for time-series functions — gives the agent control over recency while preventing unbounded scans. |
| Implement pagination in agent tool calls | Overly complex for the current use case. The agent has no mechanism to request "page 2" of a tool result. |
| Truncate results in `UCFunctionToolkit` | Would require patching the SDK layer. Fragile and version-dependent. |

### Decision

All 14 UC functions are now bounded via one or more of:

| Bound type | Functions | Rationale |
|---|---|---|
| `LIMIT N` | All 14 functions | Safety net — caps absolute row count regardless of filter effectiveness |
| `LEAST(param, cap)` in WHERE clause | `lookup_billing`, `lookup_operational_kpis`, `get_finance_operations_summary` | Caps the lookback parameter at the SQL level so even a malformed agent call cannot request unbounded history |
| `DEFAULT` parameter value | `lookup_billing` (`lookback_months DEFAULT 12`) | Backwards compatible — existing tool calls without the parameter still work with a reasonable default |
| PK filter (`WHERE pk = TRY_CAST(...)`) | `lookup_customer`, `lookup_customer_erp_profile` | Returns 0-1 rows by design — LIMIT is unnecessary |
| `num_results` parameter | `billing_faq` (vector_search) | Bounded by the vector search API |

Specific LIMIT values were chosen based on the data model:
- Monthly data: LIMIT 100 (8+ years), with `LEAST` cap at 36 months for `lookup_billing`
- Daily data: LIMIT 90 (3 months), with `LEAST` cap at 90 days
- Per-customer monthly revenue: LIMIT 36 (3 years)
- Aggregated summaries: LIMIT 200 (accommodates multi-segment breakdowns)
- Job/anomaly lists: LIMIT 100 or 50

### Consequences

**Accepted tradeoffs:**
- If a customer has more than 100 billing items, the oldest events are not returned.
  The agent can use `ask_billing_analytics` (Genie) for full-history aggregations.
- The `lookback_months DEFAULT 12` parameter change in `lookup_billing` alters the function
  signature. Existing UC function references in the toolkit will pick up the new signature
  on the next serving container cold start.

---

## Decision Dependencies



| Decision | Depends on | Constrains |
|---|---|---|
| DEC-001 (LangGraph) | — | DEC-003 (no SparkSession → Statement Execution API), DEC-004 (state model → message sentinel), DEC-006 (tool binding model) |
| DEC-002 (two deployment paths) | DEC-001 (LangGraph agent exists), DEC-007 (Genie Space exists) | DEC-010 (persona system only works in LangGraph path) |
| DEC-003 (Statement Execution API) | DEC-001 (no SparkSession in serving) | DEC-014 (dispute aging reuses same pattern) |
| DEC-004 (write confirmation sentinel) | DEC-001 (LangGraph message-passing model), DEC-003 (write tools exist) | — |
| DEC-005 (PII in lookup_customer) | — | DEC-006 (persona tool filtering as partial mitigation) |
| DEC-006 (19 tools + personas) | DEC-005 (PII tools included), DEC-007 (Genie tool), DEC-009 (anomaly tool), DEC-011 (ERP tools), DEC-003 (write tools) | DEC-002 (Agent Bricks cannot replicate persona filtering) |
| DEC-007 (Genie Space) | — | DEC-002 (both paths depend on Genie), DEC-008 (streaming tables added to Genie) |
| DEC-008 (DLT streaming) | DEC-007 (streaming tables in Genie) | — |
| DEC-009 (batch anomaly detection) | — | DEC-003 (anomaly acknowledgement via write-back), DEC-006 (anomaly tool in tool set) |
| DEC-010 (domain/persona YAML) | DEC-006 (tool filtering need) | DEC-013 (Dash UI must list personas) |
| DEC-011 (ERP dual-track) | — | DEC-006 (ERP tools in tool set) |
| DEC-012 (history trimming) | DEC-001 (LangGraph message model) | DEC-004 (trimming can lose write sentinel) |
| DEC-013 (Dash app) | DEC-010 (persona selector) | — |
| DEC-014 (dispute aging job) | DEC-003 (Statement Execution API pattern) | — |
| DEC-015 (telemetry materialization) | — | DEC-006 (telemetry tools in tool set) |
| DEC-016 (feature/dabsdeploy) | — | — (vestigial) |
| DEC-017 (identity propagation) | DEC-001 (LangGraph predict()), DEC-003 (write audit), DEC-010 (persona YAML) | DEC-018 (PII isolation assumes identity exists for authorization) |
| DEC-018 (PII schema isolation) | DEC-005 (PII in lookup_customer — the problem), DEC-017 (identity context for grant model) | DEC-007 (Genie Space instructions updated) |
| DEC-019 (result set bounding) | DEC-007 (Genie as fallback for full-history queries) | — |

---

## Deferred Decisions

These are design choices that the code has not yet made — evidenced by empty config
fields, placeholder implementations, or incomplete integration.

| Item | Location | What needs deciding | Evidence |
|---|---|---|---|
| Session ID in write audit | `agent.py:413`, `:457`, `:497` | How to propagate `ChatContext.conversation_id` into `_execute_sql` calls so audit records can be correlated to conversations | `session_id=None` hardcoded in all three write tool calls |
| DLT pipeline ID | `config.yaml:50` | Whether the DLT pipeline should be auto-discovered or always manually set | `dlt_pipeline_id: ''` — empty until `06a` runs |
| Genie Space ID | `config.yaml:56`, `000-config.py:225` | Whether the Genie Space should be auto-discovered or always manually set | `genie_space_id: ''` / `None` — empty until `03a` runs |
| ERP credentials management | `000-config.py:115-117` | Whether to use Databricks Secret Scopes or another secrets manager | Comment says `dbutils.secrets.get(scope="billing-erp", key="password")` but the line is commented out; password field is empty string |
| ERP federation catalog | `config.yaml:41` | Whether Track A (real federation) is ever deployed | `erp_foreign_catalog: ''` — empty |
| Persona config path | `config.yaml:24` | Whether personas should be loaded from an external path or always co-located with agent.py | `persona_config_path: ''` — empty, fallback never used |
| Domain charge labels | `config.yaml:29` | Whether runtime charge label overrides are needed beyond what domain YAML provides | `domain_charge_labels: {}` — empty dict |
| Domain agent prompt section | `config.yaml:30` | Whether runtime prompt injection is needed beyond what `10_domain_config.py` generates | `domain_agent_prompt_section: ''` — empty until `10` runs |
| Personas config dict | `config.yaml:25` | Purpose of this field vs. the YAML file loading system | `personas: {}` — empty, unused by any code |
| Dispute SLA threshold | `09a_dispute_aging.py:43` | Whether 5-day escalation window should be configurable | Hardcoded `INTERVAL 5 DAYS` in SQL, not in config |
| Workflow definition | `.github/workflows/databricks-ci.yml` | Whether DABs CI/CD should be operational | `databricks.yml` deleted; CI references nonexistent `demo_workflow` and `feature/dabsdeploy` branch |
| Agent Bricks tile IDs | `000-config.py:236`, `:251` | Whether tile IDs should persist across deployments | `ka_tile_id: None`, `mas_tile_id: None` — set at runtime only |
| Anomaly table write mode | `05_billing_anomaly_detection.py:227` | Whether `overwrite` should be changed to `merge` to preserve acknowledgement columns | `mode("overwrite")` wipes acknowledgement data added by `09_writeback_setup.py` |
| lookup_billing_items LIMIT | `02_define_uc_tools.py:108-121` | Whether unbounded result sets are acceptable | No LIMIT clause; all other similar functions have LIMIT 50 or 100 |
| Parameterized SQL queries | `agent.py:142-147` | Whether to migrate from `_sanitize_sql_str` to SDK parameterized queries | Custom sanitizer used; SDK `parameters` support not evaluated |
