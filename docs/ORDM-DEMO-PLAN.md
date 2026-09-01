# ORDM Demo Series — Corrected Design and Build Plan

**Status:** All five demos shipped and verified on a live warehouse. Phase 0 complete.
**Date:** 2026-08-31

Shareable version: https://claude.ai/code/artifact/10652d74-5811-4b87-96e1-fb5f3e41e52c

---

## 1. The structural error

Demos #2, #3 and #5 each specified their headline capability — an atomic billing
correction — as a Unity Catalog SQL function:

| Demo | Function as originally specified |
|------|----------------------------------|
| #2   | `approve_dispute_correction`     |
| #3   | `submit_revenue_adjustment`      |
| #5   | `submit_auto_upgrade`            |
| #4   | `share_store_performance`        |

None can be built. A UC SQL function body is a **query expression**: it cannot
`INSERT`, cannot open a `BEGIN ATOMIC` block, and cannot create a Delta Share.
The `catalog_commit_id` these functions were specified to return is not a
returnable value — catalog commits are a table capability, not a call result.

The accelerator already does this correctly. All 17 UC functions are reads;
writes go through the staged-token confirmation flow in `notebooks/agent.py`
(`request_write_confirmation` → `confirm_write_operation` → `_execute_write`),
which executes SQL via the Statement Execution API.

All four blocked capabilities have the same shape. They belong in one shared
write path, built once. That is Phase 0.

---

## 2. Defects found in the shipped write path

All four are closed.

Independent of the demo work. Each worsens with every write action added, so
they belong in Phase 0 rather than a later cleanup.

### 2.1 Audit trail misattributes the target table — correctness

`agent.py:376` and `agent.py:421` both hard-code `billing_disputes` as
`target_table`. `acknowledge_anomaly` writes to `billing_anomalies`. Every
anomaly acknowledgement is audited against a table it never touched.

### 2.2 Persona `write_access` is declared but never enforced — security

`finance_ops` declares `write_access: acknowledge_only` and holds
`request_write_confirmation`, whose `action` argument accepts `create_dispute`
and `update_dispute_status`. `_execute_write` (agent.py:290) never checks the
persona level.

The persona files list `create_billing_dispute` and `update_dispute_status` in
`allowed_tools`, but those are **action** names, not tool names.
`_filter_tools_for_persona` (agent.py:516) matches on `t.name`, so those entries
match nothing and gate nothing. The real gate is whether
`request_write_confirmation` is in `allowed_tools`.

`executive` and `technical` are unaffected — they lack the tool. The exposure is
`finance_ops`.

### 2.4 Sibling modules were never packaged with the served model — blocking

`mlflow.pyfunc.log_model` in `03_agent_deployment_and_evaluation.py` passed
`python_model="agent.py"` with no `code_paths`. `identity_utils.py` was therefore
absent in Model Serving, so:

1. `from identity_utils import ...` raised ImportError, `_IDENTITY_AVAILABLE = False`
2. `predict()` never called `_set_request_context(ctx)` (guarded on that flag)
3. `_get_request_context()` returned None
4. `confirm_write_operation` returned `BLOCKED: Authenticated user context required`

**Every write failed in deployed Model Serving** while working in the notebook,
where `identity_utils.py` sits on the path as a sibling. Fixed by adding
`code_paths=["identity_utils.py", "write_actions.py"]`.

### 2.3 Business write and audit resolution are not atomic — durability

Three separate `execute_statement` calls (agent.py:367–430): audit PENDING,
business write, audit result. If the business write succeeds and the result
insert fails, the audit row is stranded at PENDING — a write that happened and
reads as if it did not. It also means no multi-table write is possible, which is
exactly what #2, #3 and #5 need.

---

## 3. Write path v2 — Phase 0 deliverable

### Keep unchanged

- Staged-token confirm/cancel flow, 10-minute TTL, thread-safe store
- Closed action allowlist — the LLM never composes SQL
- Audit PENDING written and verified *before* the business write
- Identity columns: initiating user, executing principal, persona, request id,
  degraded flag, user groups

### Change

- **Action registry** replaces the `if/elif` chain in `_execute_write`. Each
  action declares: `target_table`, `min_write_access`, typed parameters, and its
  statement list.
- **Typed parameter markers** replace `_sanitize_sql_value` string interpolation.
  The Statement Execution API accepts `{name, type, value}` triples with `:name`
  markers.
- **`BEGIN ATOMIC`** wraps the business statements together with the audit
  resolution row.
- **Persona `write_access` check** against the action's declared minimum.
- **`target_table` from the registry**, not hard-coded.

### Execution sequence

```
1. Resolve action in registry            -> unknown action rejected (security boundary, unchanged)
2. Check persona write_access >= minimum -> closes 2.2
3. INSERT audit PENDING, verify SUCCEEDED-> outside the transaction, deliberately:
                                            intent survives a full rollback
4. BEGIN ATOMIC
     <action.statements>                 -> one or many tables
     INSERT audit SUCCESS
   END                                   -> closes 2.3, unblocks #2/#3/#5
5. On exception: INSERT audit FAILED     -> every PENDING resolves exactly once
```

### Spike 0 — RUN, 2026-08-31

Executed against the Entrada workspace (`dbc-3aa503a9-4fa8`, warehouse
`cfa0e10eed4f00a5`).

| Question | Answer |
|----------|--------|
| Do typed parameter markers bind inside `BEGIN ATOMIC`? | **Yes** — `writeback_param_mode: parameters` confirmed |
| Do escaped literals work as a fallback? | Yes |
| Does the block roll back on a genuine runtime failure? | Yes — verified with a cast overflow, not an analysis error |
| Does a clean block commit? | Yes |

**Prerequisite discovered.** `BEGIN ATOMIC` is refused on any table lacking the
`catalogManaged` feature:

```
[TRANSACTION_NOT_SUPPORTED.WRITE_NON_CATALOG_MANAGED_TABLE]
```

Enable with `ALTER TABLE … SET TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')`.
Existing tables upgrade in place with data preserved. `09_writeback_setup` now
runs this for every write target, with the table list derived from the registry
so a new demo action cannot forget it.

Re-run `spike0_atomic_params.py` against staging and prod before deploying there.

---

## 4. Per-demo design changes

Every read stays a UC function — that part of the original design was sound and
matches the existing 17. Only the writes move.

| Demo | Stays a UC function (read) | Moves to the write registry | Removed |
|------|----------------------------|-----------------------------|---------|
| **#2** Pricing dispute | `lookup_product_pricing`, `lookup_pricing_history`, `lookup_customer_contract`, `detect_pricing_drift` | `submit_pricing_dispute`, `apply_pricing_correction` | `catalog_commit_id` as a return value |
| **#3** Order-to-cash | `lookup_order`, `lookup_order_line_items`, `reconcile_order_to_cash`, `detect_revenue_leakage`, `lookup_dso_by_region` | `submit_revenue_adjustment` | — but `update_flow` and `AUTO CDC FROM SNAPSHOT` are unverified |
| **#4** Store intelligence | `lookup_store_profile`, `lookup_store_hierarchy`, `compare_stores_by_region`, `detect_pos_reconciliation_gap`, `lookup_regional_dso` | none | `share_store_performance` (Delta Sharing is provisioning, not an agent tool); `CREATE EXTERNAL LINEAGE …` (invented syntax) |
| **#5** Usage-based billing | `lookup_inventory_assets`, `lookup_usage_history`, `lookup_plan_entitlement`, `detect_overage`, `recommend_plan_upgrade` | `submit_auto_upgrade` | `predict_usage_forecast` as a SQL function doing ML — use `ai_forecast()` or a scheduled forecast table |

---

## 5. Build plan

### Phase 0 — Write path v2 and the four defects — **BUILT**
*Depends on: nothing. Blocks: #2, #3, #5.*

| | Item | State |
|---|------|-------|
| 1 | `write_actions.py` — declarative registry, three actions ported unchanged | done |
| 2 | Persona `write_access` enforced against each action's declared minimum | done |
| 3 | `target_table` from the registry — fixes 2.1 | done |
| 4 | Typed parameter binding, `literals` fallback behind `writeback_param_mode` | done |
| 5 | `BEGIN ATOMIC` around business statements + audit resolution — fixes 2.3 | done |
| 6 | `code_paths` on `log_model` — fixes 2.4 | done |
| 7 | `test_write_actions.py` — 33 offline checks | passing |
| 8 | Spike 0 run on a live warehouse | **passed** |
| 9 | `catalog_managed_ddl()` + enablement in `09_writeback_setup` | done |
| 10 | `payload_json` carries bound values; both audit rows share one `audit_id` | done |
| 11 | `test_write_path_integration.py` — 18 live checks | passing |

The persona YAML files were left unedited. Their `write_access` and
`blocked_tools` already declared the right intent; Phase 0 makes the executor
honour it. The registry's access matrix reproduces those declarations exactly,
and `test_write_actions.py` asserts the full nine-cell matrix.

**Verified end-to-end on a live warehouse**, not just compiled: persona denial,
all three business writes, correct `target_table` in the audit log, forced
rollback leaving no business row, and the PENDING row resolving to FAILED.

Two items surfaced during verification and are **decisions for you**, not
blockers:

1. `config.yaml` `warehouse_id: 148ccb90800933a1` does not exist in the DEFAULT
   (entrada) workspace. Writes would fail only at the moment a user confirms one.
   Left unchanged because it may be valid in `dish-stg` / `dish-prod`;
   `spike0_atomic_params.py` now preflights it and lists valid ids.
2. ~~**Tool count.**~~ **Addressed 2026-09-01** — see section 8.
3. A write blocked by persona policy currently writes **no audit row**. Denied
   attempts are arguably exactly what an audit log should capture. Adding a
   `BLOCKED` result_status is cheap; it was left out because it extends the audit
   table's semantics beyond Phase 0's scope.

### Phase 1 — Demo #2, pricing dispute resolution — **BUILT**
*Depends on: Phase 0.*

| | Item | State |
|---|------|-------|
| 1 | `ordm_product`, `ordm_product_pricing` (effective-dated), `ordm_customer_contract` | done |
| 2 | `detect_pricing_drift` — billed vs price book vs contract in one call | done |
| 3 | `lookup_pricing_history`, `lookup_product_pricing`, `lookup_customer_contract` | done |
| 4 | `submit_pricing_dispute` + `apply_pricing_correction` in the registry | done |
| 5 | `extra` JSON argument on `request_write_confirmation` for action-specific values | done |
| 6 | Wired into config, agent, and the two write-capable personas | done |
| 7 | `test_pricing_dispute_integration.py` — live, reads and both writes | passing |

**Phase 0 proved out.** `apply_pricing_correction` updates `billing_disputes` and
`invoice` in one `BEGIN ATOMIC` block. The rollback case was tested live: when the
transaction fails, the dispute stays `OPEN` **and** the invoice is untouched. A
dispute cannot be marked corrected unless the invoice is corrected with it.

`invoice` was added to the catalog-commits list automatically, because
`write_target_tables()` derives it from the registry rather than a hand-maintained
list.

The six drift types are all reachable against seeded data. Sample output:

> Billed $25.0 for 2025-09 but the negotiated contract rate is $20.0
> (price book: $21.25). Overcharged by $5.0.

### Phase 2 — Demo #5, usage-based billing — **BUILT**
*Depends on: Phase 0, and Demo #2 for `ordm_customer_contract`.*

| | Item | State |
|---|------|-------|
| 1 | `ordm_inventory_asset` — SIMs with activity status derived from real events | done |
| 2 | `ordm_plan_entitlement` — allowances and overage rates in long form, 8 dimensions | done |
| 3 | `usage_by_asset_month` — event rollup normalised to the entitlement unit | done |
| 4 | `usage_forecast` — materialised `AI_FORECAST`, not a model inside a UC function | done |
| 5 | `detect_overage` — used vs included vs charged, per dimension | done |
| 6 | `recommend_plan_upgrade` — reprices real usage against every plan | done |
| 7 | Four more reads: usage history, plan entitlement, inventory assets, forecast | done |
| 8 | `submit_auto_upgrade` — atomic across `customers` and `ordm_customer_contract` | done |
| 9 | `test_usage_billing_integration.py` — live, reads and the upgrade write | passing |

**The normalisation carries it.** Everything is expressed in the entitlement unit
— MB, minutes, texts — so `detect_overage` is one calculation across all eight
metered dimensions, and `recommend_plan_upgrade` reprices a month against every
plan as a single aggregate. An allowance of `NULL` means unlimited; `0` means
charged per use.

**`ai_forecast` is real**, verified on the warehouse, and replaces the fabricated
`predict_usage_forecast`. It is materialised into a table because a UC function
body cannot call a table-valued function of that shape.

**The upgrade closes a loop with Demo #2.** `submit_auto_upgrade` moves the
customer record and the contract together, because applying only one leaves the
customer billed on a plan their contract does not name — exactly the state
`detect_pricing_drift` reports as `ContractMismatch`. Verified live: on failure,
both tables are untouched.

Sample output:

> data_local: used 148,480 MB against an allowance of 102,400 MB.
> The 46,080 MB over cost $460.80.
> UNLIMITED WORLD SIM24 would save $747.20 for this month of usage.

**Open concern:** `customer_care` now carries 27 tools. That is past the point
where tool-selection accuracy usually starts to degrade, and it is worth an
evaluation run before adding Demos #3 or #4. See below.

### Phase 3 — Demo #4, store intelligence
*Depends on: Demo #1 patterns only. Independent of Phase 0.*

- Read-only agent surface, so lowest risk once the ORDM pattern is established
- Delta Sharing and external lineage move into a setup notebook as provisioning
- Caveat: this accelerator is telco. Store and POS data is entirely synthetic, so
  this demo carries the least domain credibility of the four.

### Phase 4 — Demo #3, order-to-cash
*Depends on: Phase 0.*

- Needs four canonical core tables with no analogue here: order header, order
  line item, fulfillment, payment
- Resolve `update_flow` and `AUTO CDC FROM SNAPSHOT` before committing to the
  pipeline design

Most new data to build before the demo says anything.

---

## 6. Verification status

Nothing below has been run against a workspace — the Databricks MCP server is
failing to connect (`ENOENT` on `/Users/axbo/.ai-dev-kit/.venv/bin/python`).

| Claim | Status | Basis |
|-------|--------|-------|
| UC SQL functions cannot perform writes | **Disproven** | Function body is a query expression; all 17 in-repo functions are reads |
| Lakehouse//RT exists, read-only, sub-second | Verified | Databricks docs, Beta. Write/ETL unsupported |
| Catalog commits, `BEGIN ATOMIC` multi-table | Verified | Public preview since March 2026; any SQL warehouse |
| Typed parameter markers in Statement Execution API | Verified | Documented name/type/value triples |
| Predictive optimization is `ALTER TABLE … ENABLE` | Verified | Dedicated clause, not a table property |
| `ai_forecast()` as a SQL AI function | Verified | Databricks AI Functions |
| Parameter binding inside `BEGIN ATOMIC` | **Unverified** | Spike 0 — gates the Phase 0 design |
| Lakeflow `update_flow` | **Unverified** | Asserted by the original plan, not confirmed |
| `AUTO CDC FROM SNAPSHOT` syntax | **Unverified** | Asserted by the original plan, not confirmed |
| External lineage registration mechanism | **Unverified** | DDL form is invented; API form untested |
| "P95 <1.5s", "35% cost reduction", "$2.3M impact" | **Fabricated** | No benchmark was ever run. Do not show these to a customer |

---

## 7. What exists today

Demo #1 (Customer 360) is built and wired:

- `notebooks/13_customer_360.py` — ORDM canonical core, gold profile, two UC functions
- Tool registration in `config.yaml`, `000-config.py`, `agent.py`
- `allowed_tools` for `customer_care` and `finance_ops`

Its SQL has not been executed against a workspace. Section 6 of the notebook
asserts the invariants when it is run.


---

## 8. Per-turn tool scoping — 2026-09-01

`customer_care` reached 27 tools after three demos. Rather than restructure the
agent, tool selection now filters on two dimensions instead of one:

    tools offered = persona allowlist  ∩  (core ∪ matched domains ∪ write tools)

`personas/*.yaml` filters by *who is asking*. `tool_domains.py` filters by *what
they asked about*, across seven domains of 2–6 tools each.

### Measured, not assumed

`test_tool_routing.py` scores the router against 33 questions taken from the demo
scripts in notebooks 13, 14 and 15 and the persona starter prompts:

| Metric | Result |
|--------|--------|
| Recall — the needed tool stays in scope | **100%** (33/33) |
| Toolset offered per turn | **14.0 mean** (min 9, max 31) against a 31-tool baseline |
| Reduction | **55%** |
| Turns the router could not classify | 2/33, which fall back to the full set |

Two of the 33 initially failed — *"What did this plan cost last year?"* and
*"Would a different plan be cheaper?"* both matched only `billing`. Fixed by
extending the `cost` and `cheaper` keyword coverage. Worth noting that those
fixes were derived from the eval set, so the set is now partly fitted to the
router; new demo questions should be added to it as demos are built.

### Why it is safe to enable

- **Permissive fallback.** An unclassifiable turn returns the full persona set —
  exactly today's behaviour. Scoping narrows when confident, never blanks out.
- **Write tools always in scope.** Gated by persona allowlist and `write_access`
  anyway, so keeping them costs three slots and removes a whole bug class: a
  write staged in one turn can always be confirmed in the next.
- **Lookback window.** Follow-ups carry no topic of their own — *"open a dispute
  for it"* — so the last three user turns feed the match. Verified across three
  multi-turn conversations.
- **Never empty.** A narrow persona cannot be scoped into a corner.

### What this does not measure

The router. Not whether the model then picks correctly from the smaller set —
that needs a deployed endpoint and an MLflow evaluation run. Full recall is a
necessary condition for that result, not a substitute for it.

### The supervisor question

Deferred, not rejected. If an MLflow eval later shows selection accuracy is still
short, the options are a LangGraph supervisor with per-domain sub-graphs, or an
Agent Bricks Supervisor over per-domain agent endpoints. `DECISIONS.md` DEC-002
has been amended: its claim that Agent Bricks cannot bind UC functions is out of
date. What has not changed is that the write path is stateful custom Python and
must live inside an agent, not at the routing layer.


---

## 9. Consistency audit — 2026-09-01

A pass over everything built in Phases 0–2, looking for problems introduced or
inherited. Four found.

### 9.1 `09_writeback_setup` ALTERed a table that did not exist yet — FIXED

Introduced in Phase 0. `catalog_managed_ddl()` derives its table list from the
write action registry, which is the right design — but once Demo #5 added
`submit_auto_upgrade`, that list included `ordm_customer_contract`, which
notebook **14** creates. Notebook **09** runs before 14.

The failure was quiet: the `ALTER` raised, the handler logged a warning, setup
continued, and `ordm_customer_contract` never became catalog-managed. The first
symptom would have been a plan upgrade failing at write time with
`TRANSACTION_NOT_SUPPORTED`.

Fixed on both sides: 14 now creates the table catalog-managed from the start, and
09 skips targets that do not exist yet and says so rather than reporting an
error. Verified live — the table comes out `catalogManaged = supported` and
`BEGIN ATOMIC` works on it without 09 having run.

### 9.2 `personas/` was never packaged with the served model — FIXED

The same class of bug as `identity_utils.py` in Phase 0, and it survived that
fix. `_load_personas` reads `system_prompt` **and** `tool_policy.allowed_tools`
from `personas/*.yaml`. In Model Serving the directory is absent, so:

- `allowed_tools` and `write_access` fall back to the mirror in `config.yaml`
  (added in Phase 0), so tool filtering and write-access enforcement work.
- `system_prompt` has **no** mirror in `config.yaml`. Every persona silently fell
  back to the generic prompt, losing its instructions and response style.

Fixed by adding `personas` to `code_paths`. Worth noting the Phase 0 fallback
masked half of this: tools worked, so nothing looked broken.

### 9.3 The vector search tool is unreachable — NOT FIXED, needs a decision

`VectorSearchRetrieverTool` registers as **`faq_search`** (`agent.py:178`). No
persona's `allowed_tools` names it — they all list `billing_faq`, which is the
*UC function* that runs `vector_search()` in SQL. So `_filter_tools_for_persona`
drops `faq_search` for every persona.

Pre-existing, not introduced by this work. FAQ retrieval still functions through
`billing_faq`, so the capability is not lost — but the dedicated retriever tool
is inert, which means MLflow does not trace it as a retrieval span and the
`resources` auth passthrough registered for it is doing nothing.

`faq_search` has been added to `CORE_TOOLS` in `tool_domains.py` so intent
scoping will not compound the problem. Restoring it needs a decision: add
`faq_search` to the persona allowlists (changes what the agent can call), or drop
the retriever tool and rely on `billing_faq` alone.

### 9.4 Inert persona entries — NO CHANGE, documented

`customer_care` lists `acknowledge_anomaly`, `create_billing_dispute` and
`update_dispute_status` in `allowed_tools`; `finance_ops` lists
`acknowledge_anomaly`. These are **action** names, not tool names, so
`_filter_tools_for_persona` matches nothing on them.

They are not a bug any more — Phase 0 made `write_access` the real gate and the
registry's minimums reproduce exactly what these entries were trying to express.
They now read as documentation of intent. Left in place deliberately: removing
them would be churn on config that cannot be tested end-to-end from here.


---

## 10. Demos #3 and #4, and the tool-selection evaluation — 2026-09-01

### 10.1 Demo #3 — order-to-cash

Built as `16_order_to_cash.py`. The accelerator has no order management system,
but it has everything an order *is* in telco, so the ORDM Order, Fulfilment and
Payment cores are derived rather than invented:

| Layer | Derived from |
|-------|--------------|
| Order | `customers.contract_start_dt` and the plan taken |
| Fulfilment | First observed `billing_items` event for the SIM — a SIM that never carried traffic was never fulfilled |
| Billing | `invoice` |
| Payment | **Synthetic.** The accelerator records no payments at all |

`reconcile_order_to_cash` names the stage an order has reached —
`AwaitingFulfilment`, `FulfilledNotBilled`, `BilledNotCollected`, `Collected` —
with the money at each step. All four are reachable on seeded data. Live run
found 21 leaking orders worth $7,590 and DSO of 17–26 days by region.

`submit_revenue_adjustment` restates the order and reissues the affected invoice
in one atomic block; either half alone *is* the leakage the demo reports.

### 10.2 Demo #4 — store intelligence

Built as `17_store_intelligence.py`, and the weakest fit of the series. This is a
telco accelerator: there are no stores, so every store attribute is synthetic.

What is not synthetic is the reconciliation. `detect_pos_reconciliation_gap`
compares what the till recorded against the order behind it and reports
`PlanMiskeyed`, `AmountVariance` or `OrphanSale` — the nightly check a retailer
actually runs, on data the accelerator genuinely has.

**Deliberately omitted:** a regional DSO function. Demo #3 already has
`lookup_dso_by_region`, and the evaluation below showed duplicate capabilities
are exactly what makes the model pick between near-identical tools.

Delta Sharing and external lineage stayed out of the agent surface — they are
provisioning, not tools, as section 4 recorded.

### 10.3 Tool-selection evaluation — the hypothesis was wrong

`eval_tool_selection.py`, `databricks-claude-sonnet-5`, 33 questions x 2 trials,
real `COMMENT` text as the tool descriptions:

| condition | strict | capability | trial spread | tools offered |
|-----------|--------|-----------|--------------|---------------|
| unscoped  | 89%    | 89%       | 3%           | 30.0 |
| scoped    | 82%    | 88%       | 6%           | 13.0 |

**Scoping does not improve tool selection.** The capability-level difference is
2%, inside the 6% run-to-run spread — no measurable change. The concern that
prompted the whole scoping exercise, that ~30 tools was degrading selection, is
**not supported**: unscoped already scores 89%.

Scoping is still worth keeping, but the justification is cost and latency — 17
fewer tool schemas per call — not quality. That is a weaker claim than the one
made when it was built, and the plan should say so.

Strict and capability accuracy are reported separately because `billing_faq` and
`faq_search` are the same capability (finding 9.3); scoring them as distinct
measures the duplication rather than the router.

**The real lever is description overlap, not tool count.** Every residual error
is one tool losing to a near-neighbour:

| Question | Expected | Chosen |
|----------|----------|--------|
| Show me my billing history and my account details | `lookup_customer_360` | `lookup_billing` |
| What's included in my plan? | `lookup_plan_entitlement` | `lookup_billing_plans` |
| Total billed revenue vs ERP recognised revenue | `lookup_revenue_attribution` | `get_finance_operations_summary` |
| Is the anomaly pipeline healthy? | `get_monitoring_status` | `lookup_operational_kpis` |
| How is my bill calculated? | `billing_faq` | `faq_search` |

Sharpening those five descriptions, or merging the genuinely duplicated pairs,
is a cheaper and better-targeted improvement than any orchestration change. That
is the next thing to do, ahead of a supervisor.

### 10.4 Notes on running the evaluation

- `databricks-claude-sonnet-5` rejects `temperature`, so runs are not
  deterministic. `--trials` measures the spread instead; treat any difference
  smaller than the spread as noise.
- The unscoped condition ships ~30 tool schemas per call and will trip the
  workspace input-token rate limit. The script backs off and retries rather than
  scoring a 429 as a wrong answer, which would have made the condition with more
  tools look worse for reasons unrelated to tool selection.
