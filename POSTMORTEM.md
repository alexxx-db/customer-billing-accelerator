# Production Postmortem

This document records what broke, degraded, or behaved unexpectedly in the customer
deployment context, and what I would redesign if starting over.

It is written after the fact. The repo reflects the architecture as built.
This document reflects what production revealed about the assumptions underneath it.

Where numbers appear, they are from observed behavior in the deployment context
unless marked (est).

---

## Section 1: What the Repo Does Not Tell You

There are two deployment paths (notebook 03 vs notebook 04) that produce systems
with fundamentally different capabilities. The README presents them as sequential
steps. They are not equivalent. Deploying notebook 04 (Agent Bricks) means
discarding write-back, anomaly acknowledgement, ERP data access, streaming
estimates, and operational telemetry — silently. The Agent Bricks Supervisor routes
individual customer lookups to "dedicated customer care tools" that do not exist
in the Agent Bricks deployment path.

The PII guard is entirely prompt-based. `lookup_customer` returns `customer_name`,
`email`, `phone_number`, and `device_id` in the function response (`02_define_uc_tools.py:52-77`).
There is no column-level masking, no Unity Catalog row filter, and no function-level
projection that omits PII. One prompt regression or persona misconfiguration
exposes all of it.

The write confirmation mechanism is a convention enforced by the system prompt,
not by the code. The LLM can call `acknowledge_anomaly` or `create_billing_dispute`
directly without calling `request_write_confirmation` first. The word "ONLY" in
the tool docstring (`agent.py:386`, `agent.py:423`) is advisory to the model, not a runtime guard.

The `billing_monthly_running` streaming table (notebook 06, line 90-148) is not incremental.
It re-aggregates all history on every micro-batch. This is silent until the
pipeline falls behind.

---

## Section 2: Failure and Degradation Log

### PM-001: PII Disclosed Through Prompt Regression

**Category:** pii
**Severity:** P1
**Status:** Fixed (April 2026)

**What happened:**
During a persona configuration change (adding a new `technical` persona with a modified system prompt), the confidentiality instruction was inadvertently deprioritized in the prompt ordering. In a test session, the agent responded to "what's the contact information for customer 4401?" with the customer's name, email, and phone number retrieved from `lookup_customer`. No error was raised. The function call succeeded and returned the fields; the agent simply included them in the response.

**Why it was not anticipated:**
The system was designed with the assumption that the system prompt is a reliable confidentiality control. In practice, LLM instruction following for sensitive-field suppression degrades under prompt length pressure, instruction conflicts between persona and base prompt, and model version changes.

**What it actually was:**
The `lookup_customer` UC function (`02_define_uc_tools.py:51-77`) returns all PII fields in its result schema: `customer_name`, `email`, `phone_number`, `device_id`. The only control preventing disclosure is the `agent_prompt` instruction "NEVER disclose confidential information" (`config.yaml:64`). The `customer_care.yaml` persona restates this as "NEVER disclose confidential information: customer_name, email, device_id, phone_number" (line 16), but the `technical.yaml` persona system prompt contains no confidentiality instruction at all — it says only "Do NOT answer individual customer billing questions" (line 22), which does not cover the case where the technical persona has `lookup_customer` removed from `allowed_tools` but the base agent (built without persona filtering at `agent.py:711`) retains all 19 tools. The base agent is the one logged to MLflow in notebook 03.

**What was done:**
Added an explicit confidentiality restatement to each persona YAML `system_prompt`. This reduces the risk but does not eliminate it. The base agent prompt in `config.yaml` still serves as the only guard for non-persona deployments.

**What should have been done instead:**
Remove PII fields from the `lookup_customer` function schema entirely. Create a separate internal lookup function that returns PII for audit/logging only and is not registered as an agent tool. The agent should never receive `customer_name`, `email`, or `phone_number` in a tool response. If the tool does not return the field, the agent cannot disclose it regardless of prompt state. This is a function schema fix in `02_define_uc_tools.py`, not a prompt fix.

**Resolution (April 2026):**
Three changes applied: (1) `lookup_customer` RETURNS TABLE reduced to `customer_id`, `device_id`, `plan`, `contract_start_dt` — PII fields removed at the schema level. (2) Internal PII function moved to `{schema}_internal` schema with `REVOKE USE SCHEMA` from agent SP and `REVOKE SELECT` on raw `customers` table. (3) Genie Space instructions added to block PII table/function references. See DEC-018.

---

### PM-002: Write Confirmation Bypass via Direct Tool Call

**Category:** write-back
**Severity:** P1
**Status:** Fixed (April 2026)

**What happened:**
In evaluation testing with a modified prompt (shorter, fewer guidelines), the agent called `create_billing_dispute` directly in response to "create a dispute for customer 4401 for $50" — without first calling `request_write_confirmation` and without presenting a confirmation request to the user. A billing dispute was created in the Delta table. No error was raised because the tool call was valid. The audit trail showed a PENDING record (from the two-INSERT pattern in `_execute_sql`) but no human confirmation step.

**Why it was not anticipated:**
The write-back design assumes the LLM will respect the system prompt instruction "ALWAYS call request_write_confirmation BEFORE any write operation" (`config.yaml:100`). In practice, under prompt compression, context pressure, or adversarial input, the LLM will occasionally skip the staging step.

**What it actually was:**
`request_write_confirmation` and `acknowledge_anomaly` / `create_billing_dispute` / `update_dispute_status` are all registered as tools in the same `ToolNode` (`agent.py:537-543`). The LangGraph graph (`agent.py:623-636`) has no edge that enforces ordering between them. The `confirm_or_cancel` node (`agent.py:579-621`) only activates when `route_after_tools` detects a `WRITE_PENDING_PREFIX` in the last 3 messages (`agent.py:569-574`). If the LLM calls a write tool directly without first calling `request_write_confirmation`, no `WRITE_PENDING_PREFIX` is ever emitted, and the `confirm_or_cancel` node is never entered. The write executes unconditionally.

**What was done:**
The `confirm_or_cancel` node exists and handles the case where `request_write_confirmation` IS called. It does not handle the case where it is skipped.

**What should have been done instead:**
Add a guard function to each write tool (`acknowledge_anomaly`, `create_billing_dispute`, `update_dispute_status`) that checks whether the most recent `WRITE_PENDING_PREFIX` sentinel in the message history matches the action being executed. If no matching sentinel exists, the tool returns an error rather than executing: `"BLOCKED: This action requires confirmation. Call request_write_confirmation first."` This is a 5-line code change per write tool in `agent.py`. It converts a soft control into a hard one.

**Resolution (April 2026):**
The write architecture was replaced with a token-gated mediation pattern (DEC-017). Write operations now go through `request_write_confirmation` (stages a token in `_pending_writes`) → `confirm_write_operation` (validates token + requires `RequestContext`). The `confirm_write_operation` function has two hard guards: (1) valid pending-write token must exist in the store, (2) valid signed `RequestContext` with user identity must be present — if either is missing, the write is blocked at the code level. The original direct-write tools (`acknowledge_anomaly`, `create_billing_dispute`, `update_dispute_status`) are no longer registered — all writes go through the token-gated path. Additionally, input validation was added: `customer_id` validated as `int()`, `action` checked against an allowlist, `target_id` validated with identifier regex, all strings escaped via `_sanitize_sql_value()`.

---

### PM-003: Confirmation Window Too Narrow — Pending Write Lost

**Category:** write-back
**Severity:** P2
**Status:** Not fixed

**What happened:**
In multi-step conversations where the agent made several tool calls between staging the write and receiving user confirmation, `_extract_pending_write` failed to find the pending write sentinel and the `confirm_or_cancel` node returned `{"messages": []}` (the routing inconsistency path at `agent.py:583-586`). The agent then re-prompted for confirmation, creating a confusing loop where the user confirmed twice and the write either executed twice or not at all.

**Why it was not anticipated:**
The `WRITE_PENDING_PREFIX` search window (`messages[-4:]`) was sized for a simple 3-message exchange: (1) user request, (2) agent staging message, (3) user confirm. In practice, agentic tool call sequences between staging and confirmation can insert 4-8 additional messages (tool_use, tool_result pairs).

**What it actually was:**
`_extract_pending_write` at `agent.py:232-240` searches `messages[-4:]`. If the staging message is more than 4 positions from the end of the message list at the time `confirm_or_cancel` runs (because tool calls intervened), the sentinel is not found and the confirmation machinery breaks. Meanwhile, `route_after_tools` at `agent.py:569-574` searches `messages[-3:]` — a different window than `_extract_pending_write` — creating a window mismatch where the route triggers but the handler cannot find the data.

**What was done:**
Nothing.

**What should have been done instead:**
Search the full message history for the most recent `WRITE_PENDING_PREFIX` sentinel, not a fixed window. Use `next((m for m in reversed(messages) if ...), None)` without a window limit. Better: store the pending write in a separate state field (`pending_write: dict | None`) in the LangGraph `ChatAgentState` rather than encoding it as a message content sentinel. State-based pending write is not losable by message window truncation.

---

### PM-004: Front-Trim History Loses Initial Account Context

**Category:** routing
**Severity:** P2
**Status:** Not fixed

**What happened:**
In billing dispute conversations exceeding ~12 agentic steps, `_trim_history` discarded the oldest non-system messages. In several cases, this trimmed the initial `lookup_customer` and `lookup_billing` tool call results — the messages that contained the customer's account state, plan details, and charge history that anchored all subsequent reasoning. Later in the same conversation, the agent asked the customer for their `customer_id` again and re-retrieved data it had already retrieved.

**Why it was not anticipated:**
`MAX_HISTORY_TURNS=20` seemed generous for a billing support session. In practice, dispute resolution conversations are long: initial inquiry, several tool calls, clarifying questions, anomaly lookup, dispute staging, confirmation, status update.

**What it actually was:**
`_trim_history` at `agent.py:661-667` keeps the system message plus the last `max_turns * 2` non-system messages. It does not distinguish between conversational turns and tool call result messages. A single agentic step can consume 2-4 messages (assistant tool_call + tool result pairs). Under a complex billing query involving `lookup_customer` + `lookup_billing` + `lookup_billing_anomalies` + `request_write_confirmation` + `create_billing_dispute`, 5 tool calls consume 10 message slots — half the budget — before the conversation even reaches the dispute creation step.

**What was done:**
Nothing.

**What should have been done instead:**
The trim strategy should preserve the first N non-system messages (the initial tool call results that established account context) in addition to the last M messages. Alternatively, implement summarization checkpoints: when history approaches the budget, compress resolved facts (customer identified, account state retrieved, issue type confirmed, anomalies found) into a structured context injection rather than discarding them.

---

### PM-005: Genie Cold Start on First Query Causes User-Visible Timeout

**Category:** latency
**Severity:** P2
**Status:** Partially mitigated

**What happened:**
The first `ask_billing_analytics` call after a period of Genie Space inactivity triggered a cold conversation start. The `start_conversation` call (`agent.py:310-313`) returned a `conversation_id` immediately, but subsequent `get_message` polls returned non-COMPLETED status for 45-90 seconds (est) while the Genie Space warmed up. The polling loop (30 attempts, 1s-5s exponential backoff capped at 5s, `agent.py:319-335`, max ~2.5 minutes) handled this correctly and eventually returned results, but the user-visible latency was 60-90 seconds (est) for the first analytical query of a session.

**Why it was not anticipated:**
Genie Space cold start was not observed in development because the development workspace had recent traffic. In the customer deployment context with lower traffic volume, the Genie Space was frequently cold at the start of business hours.

**What it actually was:**
`start_conversation` at `agent.py:310` creates a new Genie conversation on every `ask_billing_analytics` call. There is no session reuse or keep-warm mechanism. The exponential backoff (base 1.0s, factor 1.5x, capped at 5.0s) correctly handles the cold start window, but the total wait is user-visible and feels like a system failure even though the query eventually completes.

**What was done:**
The tool docstring includes guidance for the agent to set expectations ("Use this tool for complex analytical questions"), which is cosmetic. The error handling paths return user-friendly messages on timeout (`agent.py:338-342`).

**What should have been done instead:**
Implement a keep-warm ping: a lightweight scheduled job (every 10 minutes during business hours) that sends a minimal question to the Genie Space. Alternatively, cache the `conversation_id` at the module level and reuse it across calls within the same agent session, since `start_conversation` is the slow path. The current implementation creates a new conversation on every call, discarding any warm state from prior calls.

---

### PM-006: Deployment Path Capability Divergence Is Invisible

**Category:** deployment
**Severity:** P1
**Status:** Not fixed

**What happened:**
A customer deployment team ran notebooks 01-04 in sequence as documented. They used notebook 04 (Agent Bricks Supervisor) as their production deployment because the Agent Bricks UI is easier to manage. They then asked why the billing agent could not acknowledge anomalies, create disputes, check ERP credit status, or show real-time billing estimates — all capabilities described in the README and present in the earlier notebooks.

**Why it was not anticipated:**
The README presents the notebook sequence as additive ("Follow the notebooks in numerical order for a smooth end-to-end experience"). The implicit assumption is that notebook 04 extends notebook 03. It does not. It replaces it with a structurally different agent that has a subset of capabilities.

**What it actually was:**
Notebook 03 deploys a full LangGraph `ChatAgent` with 19 tools: 14 UC read tools + 5 write-back tools (`agent.py:268-543`), including anomaly acknowledgement, dispute creation, ERP profile, revenue attribution, streaming estimates, and operational KPIs. Notebook 04 (`04_agent_bricks_deployment.py:246-262`) deploys an Agent Bricks Supervisor with exactly two agents: a FAQ Knowledge Assistant and a Genie Space. The Agent Bricks path has no write-back, no anomaly acknowledgement, no ERP data, no streaming billing estimates, no operational KPI tools, no individual customer lookup tools. The MAS routing instructions (`agent_bricks/telco-billing-mas.md:16`) explicitly redirect individual customer lookups to "dedicated customer care tools" — which do not exist in this deployment.

**What was done:**
Nothing — the divergence is still undocumented in the README.

**What should have been done instead:**
The README must include a capability comparison table showing what each deployment path provides. If Agent Bricks is the target deployment, the write-back and individual customer tools must be reimplemented as Agent Bricks custom agents. The README should present the deployment path selection as the first decision, not as sequential steps 6 and 7.

| Capability | LangGraph (nb 03) | Agent Bricks (nb 04) |
|---|---|---|
| FAQ retrieval | Yes | Yes |
| Fleet-wide analytics (Genie) | Yes | Yes |
| Individual customer billing lookup | Yes | No |
| Anomaly detection + acknowledgement | Yes | No |
| Billing dispute creation | Yes | No |
| ERP credit/AR profile | Yes | No |
| Real-time streaming estimates | Yes | No |
| Operational KPIs | Yes | No |
| Write-back with audit trail | Yes | No |
| Persona-based tool filtering | Yes | No |

---

### PM-007: Non-Unique anomaly_id Causes Silent Duplicate Acknowledgement

**Category:** data quality
**Severity:** P2
**Status:** Not fixed

**What happened:**
When a customer had two `roaming_spike` anomalies in the same `event_month` (one detected in week 1 and re-scored in week 3 with different magnitude), both rows had the same composite `anomaly_id`: `CONCAT(CAST(customer_id AS STRING), '-', event_month, '-', anomaly_type)` = e.g., `4401-2024-06-roaming_spike`. The `acknowledge_anomaly` tool (`agent.py:401-406`) executed an `UPDATE WHERE ... = '{anomaly_id}'` which matched both rows, acknowledging both with a single agent action. The audit trail showed one `ACKNOWLEDGE_ANOMALY` write but two rows updated.

**Why it was not anticipated:**
The anomaly detection pipeline (`05_billing_anomaly_detection.py:206-211`) was assumed to produce at most one anomaly per customer per month per type. In the synthetic data this is true (the pipeline runs once and overwrites the table at line 227). In production data with incremental runs or partial re-scoring, multiple rows with the same composite key are possible.

**What it actually was:**
The `anomaly_id` is a derived composite key computed at query time in the `acknowledge_anomaly` UPDATE predicate (`agent.py:406`), not a stored column. The `billing_anomalies` table schema (`05_billing_anomaly_detection.py:197-204`, `09_writeback_setup.py:46-67`) has no `anomaly_id` column, no primary key constraint, and no UNIQUE index. The composite key is constructed in the WHERE clause of every write operation.

**What was done:**
Nothing.

**What should have been done instead:**
Add an `anomaly_uuid STRING` column (UUID generated at insert time) to `billing_anomalies` at table creation in `09_writeback_setup.py`. Use `anomaly_uuid` as the sole write target identifier in `acknowledge_anomaly`. The composite key remains useful for deduplication in detection logic but should not be the write target identifier.

---

### PM-008: 19 Tools Bound at Once Causes Systematic Routing Confusion

**Category:** routing
**Severity:** P2
**Status:** Partially mitigated

**What happened:**
With all 19 tools bound simultaneously (the base agent at `agent.py:711`), the agent showed recurring routing confusion at three specific boundaries: (1) `lookup_billing` vs `ask_billing_analytics` for "what are the charges for customer X" — correctly routed to `lookup_billing` ~85% (est) of the time, but ~15% (est) delegated to Genie even for single-customer lookups; (2) `get_monitoring_status` vs `lookup_billing_anomalies` for "are there any new anomalies" — both tools return anomaly data, agent frequently called both; (3) `lookup_operational_kpis` vs `lookup_job_reliability` for platform health questions — highly overlapping tool descriptions caused frequent dual-calls.

**Why it was not anticipated:**
Tool routing was evaluated individually per tool during development. The inter-tool routing confusion only manifests when all tools are present simultaneously and queries are semantically ambiguous between pairs.

**What it actually was:**
19 tools in a single context with overlapping semantic descriptions overwhelms the tool selection signal. The tool docstrings for the operational tools (`lookup_operational_kpis`: "Returns daily operational KPIs for the billing platform: DBU consumption, estimated cost, pipeline health, Genie usage, and warehouse performance" vs `lookup_job_reliability`: "Returns rolling 30-day reliability metrics for Databricks jobs. Use for questions about job health, failure rates, and runtimes") have significant overlap on "pipeline health." The agent resolves ambiguity by calling multiple tools and synthesizing, which adds 1-3 unnecessary tool calls (est) per conversation.

**What was done:**
The persona system (`agent.py:643-658`) provides a partial mitigation by reducing the tool count per persona. The `customer_care` persona scopes to 15 tools (excluding 5 operational tools per `customer_care.yaml:51-55`). The `technical` persona scopes to 5 tools. The `executive` persona scopes to 3 tools.

**What should have been done instead:**
The persona tool scoping should have been the primary architecture from day one. The base agent (`agent.py:711`) should never have all 19 tools bound simultaneously — it is only used when no persona is specified or persona loading fails. Additionally, the overlapping operational tool docstrings should be rewritten with explicit mutual exclusion: "Use this tool and NOT lookup_job_reliability when asking about costs or DBU consumption."

---

### PM-009: DLT Streaming Gold Table Re-aggregates Full History on Each Micro-batch

**Category:** streaming
**Severity:** P2
**Status:** Not fixed

**What happened:**
The `billing_monthly_running` DLT Gold table (`06_dlt_streaming_pipeline.py:90-148`) is defined as `dlt.read_stream("billing_events_streaming").groupBy(...).agg(...)`. In DLT, a streaming groupBy without a watermark or stateful aggregation pattern performs a complete re-aggregation of all events on each micro-batch trigger. After 3 months (est) of streaming data, each micro-batch was scanning and re-aggregating 90+ days of events to update the current month's running total. Pipeline lag grew from seconds to 8-12 minutes (est).

**Why it was not anticipated:**
In development, the streaming volume was low and the pipeline appeared to keep pace. The full re-aggregation behavior of streaming groupBy in DLT is not prominently documented; the pattern looks identical to an incremental aggregate from the pipeline definition.

**What it actually was:**
DLT streaming tables with `groupBy` aggregations use complete output mode — they recompute the full result on each trigger unless a stateful aggregate (`withWatermark` + `window`) is explicitly used. The `billing_monthly_running` definition (`06_dlt_streaming_pipeline.py:93-98`) groups by 10 columns including rate columns (`Roam_Data_charges_per_MB`, `International_call_charge_per_min`, etc.) which are static per customer — this creates correct results but does not change the re-aggregation behavior. For a running monthly charge accumulator, complete output mode means the oldest months are re-scanned indefinitely.

**What was done:**
Nothing.

**What should have been done instead:**
Partition the streaming aggregation by `event_month` and add a watermark: `.withWatermark("event_ts", "2 days")`. This bounds state to active months. For finalized months (where `event_ts` is more than the watermark behind current time), the state is dropped and those months are frozen — which is the correct semantic for a billing accumulator. Alternatively, restructure as a `foreachBatch` pattern that performs incremental MERGE into a Delta table, avoiding the complete-output-mode problem entirely.

---

### PM-010: Write Audit Has No Session Linkage and Single Shared Warehouse

**Category:** observability
**Severity:** P2
**Status:** Partially fixed (April 2026) — session linkage fixed, shared warehouse remains

**What happened:**
Two issues surfaced together in the production audit review: (1) all write audit records have `agent_session_id = NULL` because `session_id` is hardcoded to `None` in all `_execute_sql` calls — visible at `agent.py:413` (`session_id=None`), `agent.py:457` (`session_id=None`), and `agent.py:497` (`session_id=None`). This makes it impossible to reconstruct which agent conversation triggered which write operations. (2) All write operations share the single `warehouse_id` from `config.yaml:5` (`148ccb90800933a1`). In a multi-user deployment with concurrent customer service agents, all write operations queue against the same warehouse, causing contention during peak hours and average write latency increases from ~1.5s to 8-12s (est).

**Why it was not anticipated:**
Session tracking was planned as a future enhancement. The warehouse shared-by-default was a development convenience that was not re-evaluated for production concurrency.

**What it actually was:**
Two separate design gaps that compound: no session tracking means the audit trail cannot be used for incident investigation (you know what was written but not which conversation requested it), and single-warehouse write contention degrades the latency of every write operation under concurrent load. The `predict()` method (`agent.py:669-688`) receives a `context: Optional[ChatContext]` parameter that contains `conversation_id`, but this is never passed down to the write tools. The tools are invoked via LangGraph's `ChatAgentToolNode`, which has no mechanism to inject per-request context into tool calls.

**What was done:**
Nothing.

**What should have been done instead:**
(1) Pass `session_id` from the `ChatContext` object (available in the `predict()` method at `agent.py:669`) into the LangGraph state as a custom field. The write tools can then read it from state rather than requiring a function parameter. This requires extending `ChatAgentState` with a `session_id: str` field and populating it in `predict()`. (2) Create a dedicated SQL warehouse for write operations with autoscaling, separate from the read/analytics warehouse. Write operations are low-throughput but latency-sensitive; they should not queue behind Genie analytics queries.

**Resolution (April 2026 — partial):**
Session linkage fixed via the identity propagation system (DEC-017). The `RequestContext` carries `session_id` from the App layer. `confirm_write_operation` passes `ctx.session_id` to `_execute_write`, which records it as `agent_session_id` in the audit INSERT. Six new audit columns added: `initiating_user`, `executing_principal`, `persona`, `request_id`, `identity_degraded`, `user_groups`. The `Dash app generates a UUID per `DatabricksChatbot` instance as the session ID. **Not fixed:** shared warehouse for all write operations — this remains a deployment-level concern requiring a dedicated write warehouse.

---

### PM-011: lookup_billing_items Returns Unbounded Result Set

**Category:** tool-design
**Severity:** P2
**Status:** Fixed (April 2026)

**What happened:**
For high-activity devices, `lookup_billing_items` returned thousands of rows. The UC function (`02_define_uc_tools.py:96-121`) has no `LIMIT` clause — it returns all records for a device ordered by `event_ts DESC`. When the agent called this tool for a device with 18 months of billing events (~2,000 rows (est)), the full result set was injected into the LLM context window. This consumed a significant fraction of the available context, pushed earlier messages out during `_trim_history`, and degraded response quality for the remainder of the conversation.

**Why it was not anticipated:**
The synthetic data has a manageable number of events per device. In production data with 12-24 months of history and high-frequency event types (data usage records), the row count per device can be 10-100x higher than the synthetic test data.

**What it actually was:**
The `lookup_billing_items` function definition at `02_define_uc_tools.py:108-121` has `ORDER BY event_ts DESC` but no `LIMIT`. Compare with `lookup_billing_anomalies` which correctly includes `LIMIT 50` (`05_billing_anomaly_detection.py:274`), `lookup_open_disputes` which includes `LIMIT 50` (`02_define_uc_tools.py:536`), and `lookup_write_audit` which includes `LIMIT 100` (`02_define_uc_tools.py:564`). The omission appears to be an oversight rather than a design choice.

**What was done:**
Nothing.

**What should have been done instead:**
Add `LIMIT 100` to the `lookup_billing_items` function definition. For billing inquiry purposes, the most recent 100 events per device are sufficient. If the full history is needed, the agent should use `ask_billing_analytics` to run an aggregation query via Genie rather than loading raw events into the LLM context.

**Resolution (April 2026):**
`LIMIT 100` added to `lookup_billing_items`. Audit of all 14 UC functions found 5 additional unbounded functions. All now bounded — see DEC-019 for the full matrix. Key changes: `lookup_billing` gained a `lookback_months DEFAULT 12` parameter with `LEAST(lookback_months, 36)` cap; `lookup_operational_kpis` and `get_finance_operations_summary` gained `LEAST()` caps on their lookback parameters; all functions now have explicit `LIMIT` clauses. PK-filtered functions (`lookup_customer`, `lookup_customer_erp_profile`) return 0-1 rows by design and do not need LIMIT.

---

### PM-012: Base Agent Logged to MLflow Without Persona Filtering

**Category:** deployment
**Severity:** P2
**Status:** Fixed (April 2026)

**What happened:**
The agent logged to MLflow in notebook 03 is the base agent (`agent.py:711-712`: `agent = create_tool_calling_agent(llm, tools, system_prompt)` / `AGENT = LangGraphChatAgent(agent)`). This base agent has all 19 tools bound. While `LangGraphChatAgent.predict()` supports persona selection via `custom_inputs.get("persona")` (`agent.py:673`), the default path when no persona is specified uses the base agent with all tools. The Dash app (`apps/dash-chatbot-app/`) sends messages without `custom_inputs`, meaning production users through the Dash UI always get the base agent with all 19 tools and no persona filtering.

**Why it was not anticipated:**
The persona system was added after the initial deployment pipeline was built. The Dash app was not updated to pass persona selection. The assumption was that the base agent prompt would be sufficient without persona filtering.

**What it actually was:**
The `predict()` method at `agent.py:673` reads `custom_inputs.get("persona", DEFAULT_PERSONA)` and `DEFAULT_PERSONA` is `customer_care`. However, the first invocation for a persona that is not in `_PERSONA_AGENTS` builds a new persona agent via `_get_persona_agent` (`agent.py:643-658`). The base agent at `agent.py:711` is constructed separately and used as the fallback `self.agent` in `LangGraphChatAgent.__init__`. In practice, `predict()` always goes through the persona path (defaulting to `customer_care`), so the base agent is constructed but never used for serving — it is only used for the initial MLflow model signature. This means persona filtering works correctly in the serving path, but the base agent construction at module import time binds all 19 tools to the LLM unnecessarily.

**What was done:**
The `DEFAULT_PERSONA = "customer_care"` default ensures the persona path is always taken in `predict()`. This is a de facto mitigation but not an intentional one.

**What should have been done instead:**
Remove the base agent construction at `agent.py:711` or replace it with a no-tools placeholder. The `LangGraphChatAgent.__init__` should accept the persona system as primary, not a pre-built agent. If the persona system fails to load, the fallback should be the `customer_care` persona built from the base prompt with explicitly scoped tools, not an agent with all 19 tools.

**Resolution (April 2026):**
`BillingChatAgent.__init__` no longer pre-builds an all-tools graph. The `self._default_graph` was removed. `self._persona_graphs` is an empty dict populated lazily on first use per persona. `_get_graph()` always builds through `_filter_tools_for_persona()`, which applies persona-group binding validation when a `RequestContext` is present. The base agent with all 19 tools is never constructed.

---

## Section 3: What I Would Redesign

Five structural decisions that should be different from the ground up. Not per-incident
patches — architectural choices.

---

### 1. Remove PII From Tool Response Schemas

The current design exposes PII to the agent (and therefore to the LLM context window)
and relies on the prompt to prevent disclosure. This is the wrong trust model for a
system handling customer data.

Correct design: `lookup_customer` returns only `customer_id`, `plan`, and `contract_start_dt`.
Create a separate internal function (not registered as an agent tool) that returns PII
for backend processing only. If the agent needs to verify a customer's identity, it
should ask for a verifiable attribute (last 4 digits of phone number, not full email)
and validate against a hashed reference — never retrieve and display PII.

This is a Unity Catalog function schema change in `02_define_uc_tools.py:51-77`, not a
prompt change. Schema changes are enforced by the platform; prompt changes are not.

---

### 2. Write Confirmation as a Code-Level Guard, Not a Prompt Convention

The two-INSERT audit pattern is solid. The write confirmation gate is not.

Correct design: Each write tool (`acknowledge_anomaly`, `create_billing_dispute`,
`update_dispute_status`) checks at runtime that a matching `WRITE_PENDING_PREFIX` sentinel
exists in the message history (or, better, in a dedicated state field) before executing.
If no pending write is staged for this action type, the tool returns an error:
`"BLOCKED: This action requires confirmation. Call request_write_confirmation first."`
This is a 5-line code change per write tool in `agent.py`. It converts a soft control
into a hard one.

Additionally, store the pending write in `ChatAgentState` as a typed field, not encoded
in message content. Message-encoded state is fragile (window truncation at `_extract_pending_write`
searching only `messages[-4:]`, content parsing errors). State fields are not.

---

### 3. Unified Deployment Path With Explicit Capability Tiers

The two-path architecture (LangGraph vs Agent Bricks) should not exist as currently
structured. They are not alternatives — they are different products with different
capabilities that happen to share a data layer.

Correct design: Define capability tiers explicitly in the accelerator:

- **Tier 1 (read-only):** Agent Bricks Supervisor (FAQ KA + Genie). For demos and read-only
  deployments. Document that write-back, individual account tools, ERP, streaming, and
  telemetry tools are not present.
- **Tier 2 (full):** LangGraph agent with persona-filtered tools. For production deployments
  requiring dispute management and anomaly acknowledgement.

The README should make the tier selection the first decision the deployer makes, before
running any notebooks. Currently the tier choice is implicit in which notebook the
deployer stops at.

---

### 4. Anomaly Identification Via Surrogate Keys, Not Composite Strings

The composite `anomaly_id` pattern is fragile in three ways: it is not unique when
re-scoring produces multiple rows for the same customer/month/type, it requires
string concatenation in the `acknowledge_anomaly` UPDATE predicate (`agent.py:406`),
and the composite key is computed at query time rather than stored.

Correct design: Add `anomaly_uuid STRING DEFAULT (uuid())` to `billing_anomalies` at
table creation time in `09_writeback_setup.py`. Use `anomaly_uuid` as the sole write
target identifier in `acknowledge_anomaly`. Keep the composite key as a deduplication
index in the detection pipeline, not as the operational record identifier. This is a
one-line schema addition that eliminates the entire category of duplicate-acknowledgement
failures.

---

### 5. Persona-First Tool Architecture

19 tools bound at once was the wrong default. The persona system (the right idea, added
as a retrofit) should have been the primary architecture.

Correct design: Start by defining persona tool sets as the unit of agent construction.
The base agent has no tools. Persona YAML files define the complete tool set for each
use case. The global tools list in `agent.py:265-543` exists only to populate the union of
all persona tool sets; it is never bound to any agent directly. This makes tool routing
confusion structurally impossible for single-persona deployments — you cannot misroute
to a tool that is not in your persona's set.

The current implementation at `agent.py:643-658` adds all tools and then filters via
`[t for t in tools if _tool_name(t) in allowed]`. The correct design starts with no tools
and then adds. The base agent construction at `agent.py:711` should not exist.

---

## Section 4: What the Repo Gets Right

- The two-INSERT audit pattern (`_execute_sql` at `agent.py:150-229`) — PENDING before,
  SUCCESS/FAILED after — is the right architecture for AI write-back. It creates an
  auditable pre-write record that survives even if the business write fails. The CANCELLED
  audit path in `confirm_or_cancel` (`agent.py:593-608`) completes the lifecycle. This
  should be carried forward without change.

- The YAML-based persona and domain system is the right abstraction for a multi-tenant
  accelerator. Four persona files (customer_care, finance_ops, executive, technical) with
  distinct tool policies and response styles. Three domain files (telco, saas, utility)
  with canonical view abstraction. The direction is correct; the execution (as a retrofit)
  created some of the tool-count problems above. The persona YAML contract should be the
  starting point for the next version, not an addition to it.

- The Genie Space integration for fleet-wide analytics is architecturally sound. Delegating
  SQL analytics to Genie rather than building aggregation logic into the agent is the
  correct separation of concerns. The `ask_billing_analytics` tool (`agent.py:295-359`)
  has proper error handling, exponential backoff, and graceful fallback messages. The
  cold-start problem is operational, not architectural.

- The domain config system (telco/saas/utility YAML with canonical view abstraction via
  `v_billing_summary`, `v_customer_profile`, `v_service_catalog`, `v_billing_events`)
  makes the accelerator genuinely reusable across industries. The canonical view layer
  means the agent tools do not need to change when the domain changes. The `10_domain_config.py`
  notebook generates views from domain YAML. This is well-designed.

- The DLT streaming pipeline definition (`06_dlt_streaming_pipeline.py`) has correct data
  quality expectations (`@dlt.expect_or_drop` on `customer_id` and `device_id` at lines 26-27)
  and correct table property settings. The re-aggregation problem is a query pattern issue,
  not a DLT architecture issue. The enrichment join in `billing_events_streaming` (streaming
  billing_items joined with static customers and plans) is correctly structured.

---

## Section 5: Current State Gaps

| Gap | Triggered by | Effort | Priority | Status |
|---|---|---|---|---|
| PII removed from `lookup_customer` schema | PM-001 | Low | P0 | **Fixed** (April 2026) |
| PII function isolated in `_internal` schema with UC grants | PM-001 | Low | P0 | **Fixed** (April 2026) |
| Code-level write confirmation guard in each write tool | PM-002 | Low | P0 | **Fixed** (April 2026) |
| SQL injection hardening in `_execute_write` | PM-002 | Low | P0 | **Fixed** (April 2026) |
| Deployment path capability matrix in README | PM-006 | Low | P0 | **Fixed** (April 2026) |
| `WRITE_PENDING` search full history (not `messages[-4:]`) | PM-003 | Low | P0 | Not fixed |
| `LIMIT` clause added to `lookup_billing_items` | PM-011 | Low | P0 | **Fixed** (April 2026) |
| All UC functions bounded with LIMIT | PM-011 | Low | P0 | **Fixed** (April 2026) |
| Identity propagation (RequestContext, HMAC, SCIM) | PM-001, PM-010 | Medium | P0 | **Fixed** (April 2026) |
| `session_id` from identity context passed to write audit | PM-010 | Low | P1 | **Fixed** (April 2026) |
| Dual-identity audit columns (human + SP) | PM-010 | Low | P1 | **Fixed** (April 2026) |
| Remove base agent construction at `agent.py:711` | PM-012 | Low | P1 | **Fixed** (April 2026) |
| `anomaly_uuid` surrogate key in `billing_anomalies` | PM-007 | Low | P1 | Not fixed |
| `billing_monthly_running` watermark + stateful aggregation | PM-009 | Medium | P1 | Not fixed |
| Dedicated write warehouse (not shared with analytics) | PM-010 | Low | P1 | Not fixed |
| `_trim_history`: preserve initial tool call results | PM-004 | Medium | P1 | Not fixed |
| Genie keep-warm ping scheduled job | PM-005 | Low | P2 | Not fixed |
| Operational tool docstrings rewritten as mutually exclusive | PM-008 | Low | P2 | Not fixed |
| Inline `_RequestContext` extracted to shared package | DEC-017 | Low | P2 | Not fixed |
