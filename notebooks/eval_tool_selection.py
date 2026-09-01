"""MLflow-tracked evaluation of agent tool selection.

Measures the thing `test_tool_routing.py` deliberately does not: given a
question, does the model pick the right tool — and does narrowing the toolset
with `tool_domains` change that?

    python3 eval_tool_selection.py [--model ENDPOINT] [--limit N]

Two conditions over the same questions:

  unscoped   every tool the persona holds  (today's behaviour)
  scoped     tool_domains narrows by intent first

Tool descriptions are the real `COMMENT` text from the UC function definitions,
so this measures the descriptions the deployed agent actually ships with.

Results are logged to a local MLflow file store under ./mlruns. It calls a
Foundation Model endpoint, so it costs tokens: 2 x len(questions) requests.
"""

import argparse, json, os, re, subprocess, sys, pathlib, time
from concurrent.futures import ThreadPoolExecutor

import mlflow
from openai import OpenAI

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import tool_domains as td
from test_tool_routing import LABELLED, BASELINE

try:
    import yaml
    _cfg = yaml.safe_load(open(os.path.join(HERE, "config.yaml")))
except Exception:
    _cfg = {}

_ap = argparse.ArgumentParser()
_ap.add_argument("--model", default=_cfg.get("llm_endpoint", "databricks-claude-sonnet-5"))
_ap.add_argument("--limit", type=int, default=0, help="evaluate only the first N questions")
_ap.add_argument("--workers", type=int, default=2)
_ap.add_argument("--trials", type=int, default=2,
                 help="repeat each condition to measure stability")
_args = _ap.parse_args()

QUESTIONS = LABELLED[: _args.limit] if _args.limit else LABELLED


# --- tool schemas -----------------------------------------------------------
# Real COMMENT text, pulled from the notebooks that define the functions.
FN_RE = re.compile(
    r"CREATE OR REPLACE FUNCTION\s+(?:"
    r"\{[^}]+\}\.\{[^}]+\}\.(?P<a>\w+)"          # {CATALOG}.{SCHEMA}.name
    r"|\{FQ\(['\"](?P<b>\w+)['\"]\)\}"            # {FQ('name')}
    r")\s*\((?P<args>.*?)\)\s*RETURNS", re.S)
COMMENT_RE = re.compile(r"COMMENT\s+'((?:[^']|'')*)'\s*\nRETURN", re.S)
SOURCES = ["02_define_uc_tools.py", "05_billing_anomaly_detection.py",
           "07_system_table_ingestion.py", "08b_external_data_ingestion.py",
           "09_writeback_setup.py", "09a_dispute_aging.py", "13_customer_360.py",
           "demo2_pricing_sql.py", "demo5_usage_sql.py"]

# Defined in Python rather than SQL, so their descriptions live here.
PYTHON_TOOLS = {
    "ask_billing_analytics": ("Ask a fleet-wide analytical question over billing data via Genie. "
                              "Use for aggregations across many customers, not one account.",
                              [("question", "STRING")]),
    "lookup_dispute_history": ("Past disputes raised for a customer, with status and outcome.",
                               [("customer_id", "STRING")]),
    "request_write_confirmation": ("Stage a write for user confirmation. MUST be called before any "
                                   "write: acknowledge_anomaly, create_dispute, update_dispute_status, "
                                   "submit_pricing_dispute, apply_pricing_correction, submit_auto_upgrade.",
                                   [("action", "STRING"), ("target_id", "STRING"),
                                    ("customer_id", "STRING"), ("reason", "STRING")]),
    "faq_search": ("Semantic search over the billing FAQ knowledge base.", [("query", "STRING")]),
}

# Isolated in a separate schema; the agent service principal cannot see it.
EXCLUDED = {"lookup_customer_pii"}


def build_schemas() -> dict:
    out = {}
    for src in SOURCES:
        p = pathlib.Path(HERE) / src
        if not p.exists():
            continue
        text = p.read_text()
        for m in FN_RE.finditer(text):
            name = m.group("a") or m.group("b")
            arglist = m.group("args")
            if name in EXCLUDED:
                continue
            cm = COMMENT_RE.search(text[m.end(): m.end() + 4000])
            if not cm:
                continue
            params = re.findall(r"(\w+)\s+(STRING|INT|BIGINT|DOUBLE|DATE)\b", arglist)
            out[name] = (" ".join(cm.group(1).replace("''", "'").split()), params)
    for name, spec in PYTHON_TOOLS.items():
        out[name] = spec
    return out


SCHEMAS = build_schemas()


def openai_tools(names) -> list:
    tools = []
    for n in sorted(names):
        if n not in SCHEMAS:
            continue
        desc, params = SCHEMAS[n]
        props = {p: {"type": "integer" if t in ("INT", "BIGINT") else
                             "number" if t == "DOUBLE" else "string"}
                 for p, t in params}
        tools.append({"type": "function", "function": {
            "name": n, "description": desc[:1024],
            "parameters": {"type": "object", "properties": props,
                           "required": [p for p, _ in params]}}})
    return tools


# --- the model under test ----------------------------------------------------
_token = json.loads(subprocess.run(["databricks", "auth", "token"],
                                   capture_output=True, text=True).stdout)["access_token"]
_desc = subprocess.run(["databricks", "auth", "describe", "-o", "json"],
                       capture_output=True, text=True).stdout
HOST = json.loads(_desc)["details"]["host"].rstrip("/")
client = OpenAI(api_key=_token, base_url=f"{HOST}/serving-endpoints")

EQUIVALENT = {
    frozenset({"billing_faq", "faq_search"}),
}


def same_capability(a, b) -> bool:
    return a == b or any({a, b} <= e for e in EQUIVALENT)


SYSTEM = ("You are a billing support agent. Call exactly one tool to answer the user's "
          "question. Use placeholder values for any argument the user did not supply.")


def pick_tool(question: str, tool_names, attempts: int = 6) -> tuple[str | None, int]:
    """Ask the model to choose one tool.

    The unscoped condition ships ~30 tool schemas per call, which is enough
    input volume to trip the workspace token rate limit. Back off and retry
    rather than scoring a 429 as a wrong answer — that would make the condition
    with more tools look worse for a reason that has nothing to do with tool
    selection.
    """
    tools = openai_tools(tool_names)
    for attempt in range(attempts):
        try:
            return _call(tools, question), len(tools)
        except Exception as e:
            if "REQUEST_LIMIT_EXCEEDED" in str(e) or "429" in str(e):
                time.sleep(min(2 ** attempt * 2, 60))
                continue
            print(f"    error on {question[:40]!r}: {str(e)[:110]}")
            return None, len(tools)
    print(f"    gave up after {attempts} rate-limited attempts: {question[:40]!r}")
    return None, len(tools)


def _call(tools, question):
    r = client.chat.completions.create(
        model=_args.model,
        messages=[{"role": "system", "content": SYSTEM},
                  {"role": "user", "content": question}],
        tools=tools, tool_choice="required", max_tokens=600)
    # NOTE: databricks-claude-sonnet-5 rejects `temperature`, so runs are not
    # deterministic. --trials measures the resulting spread instead.
    calls = r.choices[0].message.tool_calls
    return calls[0].function.name if calls else None


def evaluate(condition: str):
    def one(item):
        question, expected = item
        if condition == "scoped":
            names, _ = td.scope_tools([{"role": "user", "content": question}], BASELINE)
        else:
            names = BASELINE
        chosen, n = pick_tool(question, names)
        return {"question": question, "expected": expected, "chosen": chosen,
                "correct": chosen == expected,
                "capability_correct": same_capability(chosen, expected),
                "n_tools": n}

    with ThreadPoolExecutor(max_workers=_args.workers) as ex:
        return list(ex.map(one, QUESTIONS))


mlflow.set_tracking_uri(f"file:{os.path.join(HERE, 'mlruns')}")
mlflow.set_experiment("billing-agent-tool-selection")

summary = {}
for condition in ("unscoped", "scoped"):
    print(f"\nEvaluating: {condition}  ({len(QUESTIONS)} questions x {_args.trials} "
          f"trials, model {_args.model})")
    trials = []
    for t in range(_args.trials):
        results = evaluate(condition)
        strict = sum(r["correct"] for r in results) / len(results)
        cap = sum(r["capability_correct"] for r in results) / len(results)
        dropped = sum(1 for r in results if r["chosen"] is None)
        if dropped:
            print(f"      WARNING: {dropped} call(s) returned nothing — trial is unusable")
        trials.append((strict, cap, results))
        print(f"    trial {t + 1}: strict {strict:.0%}  capability {cap:.0%}")

    strict = sum(t[0] for t in trials) / len(trials)
    cap = sum(t[1] for t in trials) / len(trials)
    spread = max(t[0] for t in trials) - min(t[0] for t in trials)
    results = trials[0][2]
    mean_tools = sum(r["n_tools"] for r in results) / len(results)
    summary[condition] = (strict, cap, mean_tools, spread, results)

    with mlflow.start_run(run_name=f"tool-selection-{condition}"):
        mlflow.log_params({"condition": condition, "model": _args.model,
                           "n_questions": len(QUESTIONS), "trials": _args.trials,
                           "router": "tool_domains" if condition == "scoped" else "none"})
        mlflow.log_metrics({"tool_selection_accuracy": strict,
                            "capability_accuracy": cap,
                            "accuracy_spread_across_trials": spread,
                            "mean_tools_offered": mean_tools})
        mlflow.log_table({k: [r[k] for r in results]
                          for k in ("question", "expected", "chosen", "correct",
                                    "capability_correct", "n_tools")},
                         artifact_file=f"results_{condition}.json")
    print(f"  strict {strict:.0%}  capability {cap:.0%}  "
          f"spread {spread:.0%}  tools {mean_tools:.1f}")

print("\n" + "=" * 78)
print(f"  {'condition':<11}{'strict':>9}{'capability':>13}{'trial spread':>15}{'tools':>9}")
for condition, (st, cap, mt, sp, _) in summary.items():
    print(f"  {condition:<11}{st:>8.0%}{cap:>13.0%}{sp:>15.0%}{mt:>9.1f}")

d_strict = summary["scoped"][0] - summary["unscoped"][0]
d_cap = summary["scoped"][1] - summary["unscoped"][1]
d_tools = summary["unscoped"][2] - summary["scoped"][2]
print(f"\n  scoping: strict {d_strict:+.0%}, capability {d_cap:+.0%}, "
      f"{d_tools:.1f} fewer tools offered")
worst_spread = max(summary[c][3] for c in summary)
if abs(d_cap) <= worst_spread:
    print(f"  That difference ({abs(d_cap):.0%}) is within the run-to-run spread "
          f"({worst_spread:.0%}) — treat it as no measurable change, not a win or a loss.")

print("\nDisagreements")
print("-" * 78)
un = {r["question"]: r for r in summary["unscoped"][4]}
sc = {r["question"]: r for r in summary["scoped"][4]}
none_shown = True
for q in un:
    a, b = un[q], sc[q]
    if a["correct"] != b["correct"] or a["chosen"] != b["chosen"]:
        none_shown = False
        print(f"  {q[:60]}")
        print(f"    expected {a['expected']}")
        print(f"    unscoped {a['chosen']}{'  ✓' if a['correct'] else ''}")
        print(f"    scoped   {b['chosen']}{'  ✓' if b['correct'] else ''}")
if none_shown:
    print("  none — both conditions chose identically on every question")

print("\nRemaining errors (scoped)")
print("-" * 78)
errs = [r for r in summary["scoped"][4] if not r["capability_correct"]]
for r in errs:
    print(f"  {r['question'][:56]:<58} expected {r['expected']}, chose {r['chosen']}")
if not errs:
    print("  none")
print("\nMLflow runs written to ./mlruns  (mlflow ui --backend-store-uri ./mlruns)")
print("=" * 78)
