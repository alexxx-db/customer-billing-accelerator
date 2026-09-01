"""Per-turn tool scoping by intent.

The agent carries 26 UC function tools plus a handful of Python ones. Offering
all of them on every turn is what degrades tool selection. Persona already
filters by *who is asking*; this filters by *what they asked about*.

    tools offered = persona allowlist  ∩  (core ∪ matched domains ∪ write tools)

Two properties keep this safe to turn on:

1. **Permissive fallback.** When nothing matches confidently the router returns
   the full persona set, i.e. exactly today's behaviour. It can only narrow when
   it is confident, never blank out a question it fails to classify.

2. **Write tools are always offered.** They are gated by the persona allowlist
   and by the write-access level in ``write_actions``, so keeping them in scope
   costs three slots and removes a whole class of bug: a write staged in one
   turn can always be confirmed in the next, whatever the next turn is about.

The matcher is deliberately a keyword union rather than a model call — no extra
latency, no extra spend, and a routing decision you can read. Swapping in an
embedding or LLM router means replacing ``match_domains`` and nothing else.
"""

from __future__ import annotations

import re

# Entry-point tools that stay in scope regardless of topic. These are what a
# vague opening question ("something looks wrong with my bill") needs.
CORE_TOOLS: set[str] = {
    "billing_faq",
    # The VectorSearchRetrieverTool registers as `faq_search`, not `billing_faq`.
    # No persona allowlist names it today, so the persona filter drops it before
    # scoping ever sees it — see the note in docs/ORDM-DEMO-PLAN.md. Listed here
    # so that scoping does not compound the problem if it is restored.
    "faq_search",
    "lookup_customer",
    "lookup_billing",
    "ask_billing_analytics",
}

# Always offered; gated elsewhere by persona allowlist and write_access level.
WRITE_TOOLS: set[str] = {
    "request_write_confirmation",
    "confirm_write_operation",
    "cancel_write_operation",
}

# How many recent user turns feed the match. A follow-up ("open a dispute for
# that") carries no topic of its own, so the window keeps the earlier domain live.
LOOKBACK_TURNS = 3


DOMAINS: dict[str, dict] = {
    "billing": {
        "tools": {"lookup_billing", "lookup_billing_items", "lookup_billing_plans",
                  "lookup_customer", "billing_faq"},
        "keywords": [r"\bbill(s|ing|ed)?\b", r"\binvoice", r"\bstatement", r"\bcharge[sd]?\b",
                     r"\bplan\b", r"\bpay(ment|ing)?\b", r"\bdue date", r"\bautopay",
                     r"\blate fee"],
    },
    "customer": {
        "tools": {"lookup_customer_360", "lookup_customer_hierarchy", "lookup_customer"},
        "keywords": [r"\baccount\b", r"\bhierarch", r"\bsubsidiar", r"\bparent (company|account)",
                     r"\borg(ani[sz]ation)?\b", r"\bcompany structure", r"\bgroup\b",
                     r"\bsegment", r"\btenure", r"\bmy (company|business)"],
    },
    "pricing": {
        "tools": {"detect_pricing_drift", "lookup_pricing_history", "lookup_product_pricing",
                  "lookup_customer_contract", "lookup_billing_plans"},
        "keywords": [r"\bcontract", r"\bpric(e|es|ing)\b", r"\brate\b", r"\bovercharg",
                     r"\bwrong (price|amount|rate)", r"\bshould (be|cost|have been)",
                     r"\bprice (book|change|increase|went up)", r"\btariff",
                     r"\bnegotiated", r"\bquoted", r"\bdrift",
                     r"\bcost\b", r"\bhow much (does|did|is|was)\b"],
    },
    "usage": {
        "tools": {"detect_overage", "recommend_plan_upgrade", "lookup_usage_history",
                  "lookup_plan_entitlement", "lookup_inventory_assets", "lookup_usage_forecast",
                  "lookup_billing_items"},
        "keywords": [r"\busage\b", r"\bused\b", r"\bdata\b", r"\bg(i)?ga?b(yte)?s?\b", r"\bmb\b",
                     r"\broaming\b", r"\boverage", r"\ballowance", r"\blimit\b", r"\bexceed",
                     r"\bforecast", r"\bupgrade", r"\bdowngrade", r"\bswitch plan",
                     r"\bcheaper plan", r"\bminutes\b", r"\btexts?\b", r"\bsim\b",
                     r"\bdevice", r"\bline[s]?\b", r"\binternational\b", r"\bincluded\b",
                     r"\bjump(ed)?\b", r"\bhigher than usual", r"\bwent up\b",
                     r"\bcheaper\b", r"\b(different|better|another) plan\b",
                     r"\bsave (money|on)\b", r"\bmove me to\b"],
    },
    "dispute": {
        "tools": {"lookup_billing_anomalies", "lookup_open_disputes", "lookup_write_audit",
                  "lookup_dispute_history", "get_monitoring_status"},
        "keywords": [r"\bdisput", r"\banomal", r"\bunusual", r"\bunexpected", r"\backnowledge",
                     r"\brefund", r"\bcredit\b", r"\bcomplaint", r"\bwrong\b", r"\bincorrect",
                     r"\baudit", r"\bunresolved", r"\bopen (case|ticket|issue)"],
    },
    "finance": {
        "tools": {"lookup_customer_erp_profile", "lookup_revenue_attribution",
                  "get_finance_operations_summary"},
        "keywords": [r"\brevenue", r"\berp\b", r"\breceivable", r"\bar\b", r"\bmargin",
                     r"\bopex\b", r"\breconcil", r"\battribution", r"\bcredit profile",
                     r"\bfinanc", r"\bcollections?\b", r"\boverdue"],
    },
    "order": {
        "tools": {"reconcile_order_to_cash", "detect_revenue_leakage",
                  "lookup_dso_by_region", "lookup_order_line_items"},
        "keywords": [r"\border\b", r"\borders\b", r"\bfulfil", r"\bfulfill",
                     r"\bshipped\b", r"\bactivat", r"\bdso\b", r"\bdays sales",
                     r"\bcollect(ion|ed)?\b", r"\buncollected", r"\bleakage",
                     r"\bunbilled", r"\border to cash", r"\bsettle"],
    },
    "store": {
        "tools": {"detect_pos_reconciliation_gap", "compare_stores_by_region",
                  "lookup_store_profile", "lookup_store_hierarchy"},
        "keywords": [r"\bstore[s]?\b", r"\bpos\b", r"\btill\b", r"\bbranch",
                     r"\bdistrict", r"\boutlet", r"\bfranchise", r"\bfootfall",
                     r"\bper (sq|square)", r"\bshop\b"],
    },
    "platform": {
        "tools": {"lookup_operational_kpis", "lookup_job_reliability", "get_monitoring_status"},
        "keywords": [r"\bpipeline", r"\bjob (run|fail|reliab)", r"\bdbu\b", r"\bwarehouse\b",
                     r"\bcluster", r"\blatenc", r"\breliab", r"\bfailure rate",
                     r"\bmonitoring\b", r"\bplatform (health|cost)", r"\bsla\b"],
    },
}

_COMPILED = {
    name: [re.compile(p, re.IGNORECASE) for p in spec["keywords"]]
    for name, spec in DOMAINS.items()
}


def match_domains(text: str) -> set[str]:
    """Domains whose keywords appear in ``text``. Empty means undecided."""
    return {name for name, patterns in _COMPILED.items()
            if any(p.search(text) for p in patterns)}


def _recent_user_text(messages, lookback: int = LOOKBACK_TURNS) -> str:
    """Concatenate the last few user turns.

    Accepts LangChain message objects or plain dicts, since the agent handles both.
    """
    texts = []
    for msg in reversed(messages or []):
        role = msg.get("role") if isinstance(msg, dict) else getattr(msg, "type", None)
        if role in ("user", "human"):
            content = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
            if content:
                texts.append(str(content))
            if len(texts) >= lookback:
                break
    return "\n".join(reversed(texts))


def scope_tools(messages, available: set[str]) -> tuple[set[str], set[str]]:
    """Narrow ``available`` to what this turn plausibly needs.

    Returns (tool_names, matched_domains). An empty domain set means the router
    was undecided and everything available is returned unchanged.
    """
    text = _recent_user_text(messages)
    if not text:
        return set(available), set()

    matched = match_domains(text)
    if not matched:
        return set(available), set()

    keep = set(CORE_TOOLS) | set(WRITE_TOOLS)
    for name in matched:
        keep |= DOMAINS[name]["tools"]
    scoped = keep & set(available)

    # Never hand back an empty toolbox: if the intersection is degenerate the
    # persona simply does not hold this domain's tools, so fall back.
    return (scoped, matched) if scoped else (set(available), set())


def all_domain_tools() -> set[str]:
    """Every tool any domain claims. Used by the routing tests."""
    return set().union(*(d["tools"] for d in DOMAINS.values()))
