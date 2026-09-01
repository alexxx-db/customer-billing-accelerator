"""Offline evaluation of per-turn tool scoping.

    python3 test_tool_routing.py

Measures the two things that decide whether scoping is safe to run:

  Recall     Does the tool the question actually needs stay in scope?
             This must be 100%. Narrowing the toolbox is only worth doing if it
             never removes the right answer.

  Reduction  How many tools the model is offered, against the unscoped baseline.
             This is the point of the exercise.

Questions are lifted from the demo scripts in notebooks 13, 14 and 15 and from
the persona starter prompts in config.yaml, so the set reflects what the agent
is actually demoed on rather than cases invented to make the router look good.

NOTE ON SCOPE: this measures the *router*. It does not measure whether the LLM
then picks correctly from the narrowed set — that needs a deployed endpoint and
an MLflow evaluation run. Recall here is a necessary condition for that, not a
substitute for it.
"""

import sys

import tool_domains as td

# The full toolbox customer_care carries today.
BASELINE = (
    td.all_domain_tools()
    | td.CORE_TOOLS
    | td.WRITE_TOOLS
    | {"lookup_dispute_history", "ask_billing_analytics"}
)

# (question, the tool the answer genuinely needs)
LABELLED = [
    # -- Demo #1, notebook 13 -------------------------------------------------
    ("Show me my billing history and my account details.",        "lookup_customer_360"),
    ("What does my company structure look like?",                 "lookup_customer_hierarchy"),
    ("Which subsidiary has the highest dispute rate?",            "lookup_customer_hierarchy"),
    ("Who is the parent company for this account?",               "lookup_customer_hierarchy"),
    # -- Demo #2, notebook 14 -------------------------------------------------
    ("I was charged more than my contract says.",                 "detect_pricing_drift"),
    ("I was charged $299 for Pro but my contract says $199.",     "detect_pricing_drift"),
    ("When did the price for my plan change?",                    "lookup_pricing_history"),
    ("What did this plan cost last year?",                        "lookup_product_pricing"),
    ("What rate did I negotiate?",                                "lookup_customer_contract"),
    ("Is my billed amount right against the price book?",         "detect_pricing_drift"),
    # -- Demo #5, notebook 15 -------------------------------------------------
    ("Why did my bill jump this month?",                          "detect_overage"),
    ("Why is my bill higher than usual?",                         "detect_overage"),
    ("Am I going to go over my limit again?",                     "lookup_usage_forecast"),
    ("Would a different plan be cheaper?",                        "recommend_plan_upgrade"),
    ("What's included in my plan?",                               "lookup_plan_entitlement"),
    ("How has my data usage been trending?",                      "lookup_usage_history"),
    ("What lines are on my account?",                             "lookup_inventory_assets"),
    ("How much roaming data did I use?",                          "lookup_usage_history"),
    ("Show me my overage charges for July.",                      "detect_overage"),
    # -- Anomalies and disputes ----------------------------------------------
    ("Are there any unusual charges on my account?",              "lookup_billing_anomalies"),
    ("Show me the open disputes.",                                "lookup_open_disputes"),
    ("What writes has the agent made recently?",                  "lookup_write_audit"),
    ("Has this customer disputed anything before?",               "lookup_dispute_history"),
    # -- Finance --------------------------------------------------------------
    ("What is total billed revenue versus ERP recognised revenue?", "lookup_revenue_attribution"),
    ("What is the ERP credit profile for this customer?",         "lookup_customer_erp_profile"),
    ("Which segments have the highest overdue AR ratio?",         "get_finance_operations_summary"),
    # -- Platform -------------------------------------------------------------
    ("Is the billing anomaly detection pipeline healthy?",        "get_monitoring_status"),
    ("What are our DBU costs this month?",                        "lookup_operational_kpis"),
    ("How reliable have the jobs been?",                          "lookup_job_reliability"),
    # -- Plain billing --------------------------------------------------------
    ("What are my charges for last month?",                       "lookup_billing"),
    ("How is my bill calculated?",                                "billing_faq"),
    ("Can I change my bill due date?",                            "billing_faq"),
    ("What plans do you offer?",                                  "lookup_billing_plans"),
]

# Follow-ups carry no topic of their own; the lookback window has to keep the
# earlier domain alive or the conversation loses its tools mid-flow.
CONVERSATIONS = [
    (["Why did my bill jump this month?", "Open a dispute for it."],
     {"detect_overage", "request_write_confirmation"}),
    (["I was charged more than my contract says.", "Yes, please raise that."],
     {"detect_pricing_drift", "request_write_confirmation"}),
    (["Would a different plan be cheaper?", "Move me to that one."],
     {"recommend_plan_upgrade", "request_write_confirmation"}),
]

fails: list[str] = []


def check(label, cond, detail=""):
    if not cond:
        print(f"  FAIL  {label}" + (f"\n          {detail}" if detail else ""))
        fails.append(label)


print("Recall — does the needed tool stay in scope?")
print("-" * 74)
sizes, fallbacks, misses = [], 0, []
for question, expected in LABELLED:
    tools, matched = td.scope_tools([{"role": "user", "content": question}], BASELINE)
    sizes.append(len(tools))
    if not matched:
        fallbacks += 1
    if expected not in tools:
        misses.append((question, expected, sorted(matched)))

for q, exp, dom in misses:
    print(f"  MISS  {exp:<32} {q}")
    print(f"        matched {dom}")
check(f"every labelled question keeps its tool ({len(LABELLED) - len(misses)}/{len(LABELLED)})",
      not misses)
print(f"  recall     {(len(LABELLED) - len(misses)) / len(LABELLED):.0%}"
      f"  ({len(LABELLED) - len(misses)}/{len(LABELLED)})")
print(f"  fallbacks  {fallbacks}/{len(LABELLED)} turns the router could not classify")


print("\nReduction — how many tools the model is offered")
print("-" * 74)
mean = sum(sizes) / len(sizes)
print(f"  baseline   {len(BASELINE)} tools every turn")
print(f"  scoped     {mean:.1f} mean   {min(sizes)} min   {max(sizes)} max")
print(f"  cut        {(1 - mean / len(BASELINE)):.0%}")
check("scoping actually reduces the toolset", mean < len(BASELINE) * 0.75,
      f"mean {mean:.1f} vs baseline {len(BASELINE)}")


print("\nMulti-turn — follow-ups keep the earlier domain")
print("-" * 74)
for turns, required in CONVERSATIONS:
    msgs = [{"role": "user", "content": t} for t in turns]
    tools, matched = td.scope_tools(msgs, BASELINE)
    missing = required - tools
    check(f"{turns[-1]!r} after {turns[0][:34]!r}", not missing,
          f"lost {sorted(missing)}; matched {sorted(matched)}")
    if not missing:
        print(f"  PASS  {turns[-1][:40]:<42} keeps {sorted(required)}")


print("\nSafety properties")
print("-" * 74)
tools, matched = td.scope_tools([{"role": "user", "content": "hello there"}], BASELINE)
check("an unclassifiable turn falls back to the full set", tools == BASELINE and not matched,
      f"{len(tools)} tools, matched {matched}")

tools, _ = td.scope_tools([], BASELINE)
check("no messages falls back to the full set", tools == BASELINE)

for question, _ in LABELLED:
    tools, matched = td.scope_tools([{"role": "user", "content": question}], BASELINE)
    if matched and not (td.WRITE_TOOLS <= tools):
        check("write tools are always in scope", False, f"lost on {question!r}")
        break
else:
    check("write tools are always in scope", True)

for question, _ in LABELLED:
    tools, _ = td.scope_tools([{"role": "user", "content": question}], BASELINE)
    if not tools:
        check("scoping never returns an empty toolbox", False, f"empty on {question!r}")
        break
else:
    check("scoping never returns an empty toolbox", True)

# A persona holding only a couple of tools must not be narrowed to nothing.
tiny = {"ask_billing_analytics", "get_monitoring_status", "get_finance_operations_summary"}
tools, matched = td.scope_tools([{"role": "user", "content": "Why did my bill jump?"}], tiny)
check("a narrow persona is not scoped into a corner", tools and tools <= tiny,
      f"{sorted(tools)}")

print()
for label in ["an unclassifiable turn falls back to the full set",
              "no messages falls back to the full set",
              "write tools are always in scope",
              "scoping never returns an empty toolbox",
              "a narrow persona is not scoped into a corner"]:
    if label not in fails:
        print(f"  PASS  {label}")

print("\n" + "=" * 74)
if fails:
    print(f"{len(fails)} FAILED:")
    for f in fails:
        print(f"  - {f}")
    sys.exit(1)
print("Tool scoping is safe to enable: full recall, and the toolset is materially smaller.")
print("Next: an MLflow eval on a deployed endpoint to measure selection accuracy itself.")
print("=" * 74)
