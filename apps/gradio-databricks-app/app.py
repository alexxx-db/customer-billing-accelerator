"""
Gradio Databricks App — production-ready starter.

Runs locally with `python app.py` and deploys to Databricks Apps.
"""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import gradio as gr
import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("gradio_app")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class Config:
    """Reads environment variables with safe defaults so the app always starts."""

    app_name: str = field(default_factory=lambda: os.getenv("APP_NAME", "Gradio Databricks Starter"))
    app_port: int = field(default_factory=lambda: int(os.getenv("DATABRICKS_APP_PORT", os.getenv("APP_PORT", "8000"))))
    app_host: str = field(default_factory=lambda: os.getenv("APP_HOST", "0.0.0.0"))
    debug: bool = field(default_factory=lambda: os.getenv("APP_DEBUG", "false").lower() == "true")

    # Databricks resource placeholders — populated via app.yaml valueFrom or env
    warehouse_id: str = field(default_factory=lambda: os.getenv("DATABRICKS_WAREHOUSE_ID", ""))
    catalog: str = field(default_factory=lambda: os.getenv("DATABRICKS_CATALOG", ""))
    schema: str = field(default_factory=lambda: os.getenv("DATABRICKS_SCHEMA", ""))
    serving_endpoint: str = field(default_factory=lambda: os.getenv("SERVING_ENDPOINT_NAME", ""))

    def is_databricks_configured(self) -> bool:
        return bool(self.warehouse_id)


cfg = Config()

# ---------------------------------------------------------------------------
# Health / self-check
# ---------------------------------------------------------------------------

def health_check() -> str:
    """Return a plaintext health report."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    mode = "Databricks App" if os.getenv("DATABRICKS_APP_PORT") else "Local"
    lines = [
        f"Status:     OK",
        f"Timestamp:  {now}",
        f"Mode:       {mode}",
        f"App name:   {cfg.app_name}",
        f"Port:       {cfg.app_port}",
        f"Debug:      {cfg.debug}",
        "",
        "--- Databricks Resources ---",
        f"Warehouse:  {'configured' if cfg.warehouse_id else 'not set'}",
        f"Catalog:    {cfg.catalog or 'not set'}",
        f"Schema:     {cfg.schema or 'not set'}",
        f"Serving EP: {cfg.serving_endpoint or 'not set'}",
    ]
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Integration stubs — replace these with real calls
# ---------------------------------------------------------------------------

def _query_sql(query: str) -> pd.DataFrame:
    """
    TODO: Integrate Databricks SQL.

    Example implementation:
        from databricks.sdk.core import Config as SdkConfig
        from databricks import sql

        sdk_cfg = SdkConfig()
        conn = sql.connect(
            server_hostname=sdk_cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            credentials_provider=lambda: sdk_cfg.authenticate,
        )
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    """
    log.info("SQL query requested (mock): %s", query[:120])
    return pd.DataFrame({
        "info": [f"SQL execution is not configured. Set DATABRICKS_WAREHOUSE_ID to enable."],
        "query_preview": [query[:200]],
    })


def _call_serving_endpoint(payload: dict) -> dict:
    """
    TODO: Integrate Model Serving.

    Example implementation:
        import requests
        from databricks.sdk.core import Config as SdkConfig

        sdk_cfg = SdkConfig()
        headers = {**sdk_cfg.authenticate(), "Content-Type": "application/json"}
        resp = requests.post(
            f"https://{sdk_cfg.host}/serving-endpoints/{cfg.serving_endpoint}/invocations",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()
    """
    log.info("Serving endpoint call requested (mock)")
    return {"message": "Model serving is not configured. Set SERVING_ENDPOINT_NAME to enable."}


def _read_unity_catalog_table(table_fqn: str) -> pd.DataFrame:
    """
    TODO: Integrate Unity Catalog reads.

    Use _query_sql(f"SELECT * FROM {table_fqn} LIMIT 100") once SQL is configured.
    """
    log.info("UC table read requested (mock): %s", table_fqn)
    return pd.DataFrame({"info": [f"Unity Catalog not configured. Table: {table_fqn}"]})


def _get_secret(scope: str, key: str) -> str:
    """
    TODO: Integrate Databricks Secrets.

    Example implementation:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        return w.secrets.get_secret(scope=scope, key=key).value
    """
    log.warning("Secret requested but not configured: %s/%s", scope, key)
    return ""

# ---------------------------------------------------------------------------
# Feature: Data Explorer (demo panel)
# ---------------------------------------------------------------------------

SAMPLE_DATA = pd.DataFrame({
    "customer_id": [f"CUST-{i:04d}" for i in range(1, 11)],
    "plan": ["Basic", "Pro", "Enterprise", "Basic", "Pro",
             "Enterprise", "Basic", "Pro", "Enterprise", "Basic"],
    "monthly_charge": [29.99, 59.99, 119.99, 29.99, 59.99,
                       119.99, 29.99, 59.99, 119.99, 29.99],
    "data_usage_gb": [2.1, 8.4, 45.2, 1.3, 12.7, 67.8, 3.5, 9.1, 52.0, 0.8],
    "region": ["US-East", "US-West", "EU-West", "US-East", "APAC",
               "EU-West", "US-East", "US-West", "APAC", "US-East"],
})


def explore_data(plan_filter: str, sort_by: str, limit: int) -> pd.DataFrame:
    """Filter, sort, and limit the sample dataset."""
    df = SAMPLE_DATA.copy()
    if plan_filter and plan_filter != "All":
        df = df[df["plan"] == plan_filter]
    if sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=False)
    return df.head(int(limit))


def compute_summary(plan_filter: str) -> str:
    """Compute quick stats for the selected plan."""
    df = SAMPLE_DATA.copy()
    if plan_filter and plan_filter != "All":
        df = df[df["plan"] == plan_filter]
    if df.empty:
        return "No data for selected filter."
    avg_charge = df["monthly_charge"].mean()
    avg_usage = df["data_usage_gb"].mean()
    return (
        f"Records: {len(df)}\n"
        f"Avg monthly charge: ${avg_charge:,.2f}\n"
        f"Avg data usage: {avg_usage:,.1f} GB"
    )

# ---------------------------------------------------------------------------
# Feature: Text Transformer (lightweight AI utility mock)
# ---------------------------------------------------------------------------

def transform_text(text: str, operation: str) -> str:
    """Apply a text transformation. Placeholder for model-serving integration."""
    if not text.strip():
        return ""
    ops = {
        "UPPERCASE": text.upper(),
        "lowercase": text.lower(),
        "Title Case": text.title(),
        "Word Count": f"Word count: {len(text.split())}",
        "Reverse": text[::-1],
        "Summarize (mock)": f"[Summary placeholder] {text[:80]}..."
            if len(text) > 80 else f"[Summary placeholder] {text}",
    }
    result = ops.get(operation, text)
    # TODO: Replace "Summarize (mock)" with _call_serving_endpoint({"inputs": text})
    log.info("Text transform: op=%s, input_len=%d", operation, len(text))
    return result

# ---------------------------------------------------------------------------
# UI: Build Gradio Blocks
# ---------------------------------------------------------------------------

def build_ui() -> gr.Blocks:
    theme = gr.themes.Soft(
        primary_hue="blue",
        secondary_hue="slate",
    )

    with gr.Blocks(theme=theme, title=cfg.app_name) as app:
        # Header
        gr.Markdown(
            f"# {cfg.app_name}\n"
            "A production-ready Gradio starter app for **Databricks Apps**. "
            "Extend the panels below to integrate SQL, Model Serving, Unity Catalog, and more."
        )

        # --- Health / Status ---
        with gr.Accordion("Health & Status", open=False):
            health_output = gr.Textbox(label="Health Report", lines=12, interactive=False)
            health_btn = gr.Button("Run Health Check", variant="secondary", size="sm")
            health_btn.click(fn=health_check, outputs=health_output)

        # --- Data Explorer ---
        with gr.Tab("Data Explorer"):
            gr.Markdown(
                "Browse sample billing data. When connected to Databricks SQL, "
                "this panel can query live Unity Catalog tables."
            )
            with gr.Row():
                plan_dd = gr.Dropdown(
                    choices=["All", "Basic", "Pro", "Enterprise"],
                    value="All",
                    label="Filter by Plan",
                )
                sort_dd = gr.Dropdown(
                    choices=["customer_id", "monthly_charge", "data_usage_gb", "region"],
                    value="monthly_charge",
                    label="Sort by",
                )
                limit_sl = gr.Slider(minimum=1, maximum=10, value=5, step=1, label="Limit")

            explore_btn = gr.Button("Query Data", variant="primary")
            data_table = gr.Dataframe(label="Results")
            summary_box = gr.Textbox(label="Summary", lines=3, interactive=False)

            explore_btn.click(
                fn=explore_data,
                inputs=[plan_dd, sort_dd, limit_sl],
                outputs=data_table,
            )
            explore_btn.click(
                fn=compute_summary,
                inputs=[plan_dd],
                outputs=summary_box,
            )

        # --- Text Transformer ---
        with gr.Tab("Text Utility"):
            gr.Markdown(
                "A lightweight text tool. Replace the mock summarizer with a "
                "Databricks Model Serving endpoint for real AI capabilities."
            )
            with gr.Row():
                txt_input = gr.Textbox(
                    label="Input Text",
                    placeholder="Type or paste text here...",
                    lines=4,
                )
                txt_output = gr.Textbox(label="Output", lines=4, interactive=False)
            op_dd = gr.Dropdown(
                choices=["UPPERCASE", "lowercase", "Title Case", "Word Count", "Reverse", "Summarize (mock)"],
                value="UPPERCASE",
                label="Operation",
            )
            transform_btn = gr.Button("Transform", variant="primary")
            transform_btn.click(fn=transform_text, inputs=[txt_input, op_dd], outputs=txt_output)

        # --- Configuration / Info ---
        with gr.Tab("Configuration"):
            gr.Markdown("### Current Environment")
            gr.Markdown(
                "| Variable | Value |\n"
                "|----------|-------|\n"
                f"| `APP_NAME` | `{cfg.app_name}` |\n"
                f"| `DATABRICKS_APP_PORT` | `{cfg.app_port}` |\n"
                f"| `DATABRICKS_WAREHOUSE_ID` | `{'set' if cfg.warehouse_id else 'not set'}` |\n"
                f"| `DATABRICKS_CATALOG` | `{cfg.catalog or 'not set'}` |\n"
                f"| `DATABRICKS_SCHEMA` | `{cfg.schema or 'not set'}` |\n"
                f"| `SERVING_ENDPOINT_NAME` | `{'set' if cfg.serving_endpoint else 'not set'}` |\n"
            )
            gr.Markdown(
                "### Integration Hooks\n"
                "- **Databricks SQL**: Set `DATABRICKS_WAREHOUSE_ID` and implement `_query_sql()`\n"
                "- **Model Serving**: Set `SERVING_ENDPOINT_NAME` and implement `_call_serving_endpoint()`\n"
                "- **Unity Catalog**: Configure catalog/schema, use `_read_unity_catalog_table()`\n"
                "- **Secrets**: Use `_get_secret()` for API keys and tokens\n"
                "- **Jobs API**: Use `WorkspaceClient().jobs` for pipeline orchestration\n"
            )

        # Footer
        gr.Markdown(
            "---\n"
            "*Gradio Databricks Starter* — "
            "[Databricks Apps Docs](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/) "
            "| [Apps Cookbook](https://apps-cookbook.dev/) "
            "| [Gradio Docs](https://www.gradio.app/docs)*"
        )

    return app

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Starting %s on %s:%d (debug=%s)", cfg.app_name, cfg.app_host, cfg.app_port, cfg.debug)
    ui = build_ui()
    ui.launch(
        server_name=cfg.app_host,
        server_port=cfg.app_port,
        show_api=cfg.debug,
    )
