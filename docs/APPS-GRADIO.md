# Gradio App: Billing Intelligence Workspace

The Gradio app (`apps/gradio-databricks-app/`) is the multi-workspace billing
intelligence interface. It surfaces the accelerator's capabilities across
multiple tabs rather than hiding everything behind a single chat box.

---

## Role

- **Primary use case**: Demo surface and multi-workspace exploration for the billing accelerator
- **Identity**: Same SCIM + HMAC identity propagation as the Dash app (via shared module)
- **UX model**: 5-tab workspace (Chat, Analytics, Data Integration, Operations, Platform)
- **Serving**: Calls the same deployed Model Serving endpoint as the Dash app

## Architecture

```
Browser → Databricks Apps Proxy (injects x-forwarded-access-token)
   │
   v
Gradio App
   ├── app.py — Full app: 5 tabs, persona selector, identity extraction
   ├── ../shared/serving_client.py — Shared identity + endpoint logic
   └── app.yaml / databricks.yml — Deployment configs
   │
   v
Model Serving Endpoint (custom_inputs: persona + request_context)
```

## Tabs

| Tab | Purpose | Backend |
|---|---|---|
| **Chat** | Persona-aware conversation with the LangGraph agent. Full chat history, sample prompts per persona. | Model Serving |
| **Analytics** | Fleet-wide analytics questions routed to Genie Space. Dedicated prompt library. | Model Serving -> Genie |
| **Data Integration** | Three-track data architecture visibility (Federation, Simulation, Lakebase). Live Lakebase connectivity check. | Status display |
| **Operations** | Platform health, DBU costs, job reliability, audit trail. Routes through technical persona. | Model Serving |
| **Platform** | Deployment tier comparison (LangGraph vs Agent Bricks), four-pillar summary, health check with identity resolution. | Config + SCIM |

## Files

| File | Lines | Purpose |
|---|---|---|
| `app.py` | 567 | Complete 5-tab billing intelligence app |
| `../shared/serving_client.py` | 248 | Shared identity + endpoint client |
| `app.yaml` | 12 | Databricks App deployment config |
| `databricks.yml` | 21 | DAB bundle for dev/prod deployment |
| `requirements.txt` | 6 | `mlflow`, `requests`, `psycopg2-binary` |

## Identity Flow

Same as the Dash app, using the shared `serving_client.py` module:
1. Extracts `x-forwarded-access-token` from `gr.Request` headers
2. Resolves user via SCIM (shared, cached)
3. Builds signed `RequestContext` (shared `RequestContext.create()`)
4. Passes through `custom_inputs` to Model Serving

If the shared module is not importable (path issue), the app falls back to
direct MLflow calls without identity propagation and shows a warning in
the Platform health check.

## Persona UX

Global dropdown at the top of the app, shared across all tabs:
- Persona selection updates sample prompts in the Chat tab
- Analytics and Operations tabs route through the selected persona
- The Platform tab shows persona-group binding documentation

## Configuration

`app.yaml`:
```yaml
env:
  - name: SERVING_ENDPOINT_NAME
    valueFrom: "serving-endpoint"
```

Deploy via DAB:
```bash
cd apps/gradio-databricks-app
databricks bundle deploy --target dev
```

## Comparison with Dash App

| Aspect | Dash App | Gradio App |
|---|---|---|
| UX model | Single-pane chat | 5-tab multi-workspace |
| Identity | Full (inlined) | Full (shared module) |
| Analytics tab | No (via chat only) | Yes (dedicated) |
| Data integration visibility | No | Yes |
| Operations workspace | No | Yes |
| Platform/health panel | No | Yes |
| Streaming support | No | No |
| Demo suitability | Reference chat | Preferred demo surface |
| Production maturity | Higher | Newer |

## Limitations

- **No streaming**: Same as Dash — waits for full response
- **Shared module path**: Requires `apps/shared/serving_client.py` to be accessible; deploys must include the shared directory
- **Lakebase connectivity check**: Only works when PGHOST env var is set (Lakebase resource added to app)
