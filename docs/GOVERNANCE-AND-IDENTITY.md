# Governance and Identity

How the Customer Billing Accelerator enforces governed, user-aware execution
across service-principal-mediated workflows.

---

## Identity Model: Pattern C (Hybrid)

The accelerator uses **Pattern C (Hybrid)** identity propagation: human user
identity is propagated for authorization and audit, but all SQL execution
happens under a service principal.

### Why not full passthrough?

Model Serving does not support per-request OAuth delegation. The serving
endpoint runs as a service principal regardless of who triggered the request.
Pattern C solves this by carrying a signed identity envelope alongside the
request so that downstream tools know WHO asked, even though the SP executes.

### Identity Flow

```
1. Browser → Databricks App
   App proxy injects x-forwarded-access-token (user's OAuth token)

2. Databricks App (Dash or Gradio)
   - Extracts token from HTTP headers
   - Calls SCIM /Me endpoint → resolves email + groups
   - Builds RequestContext (user_email, user_groups, persona, session_id, ...)
   - Signs with HMAC-SHA256 (secret from Databricks Secret Scope)
   - Sends as custom_inputs["request_context"] to Model Serving

3. Model Serving (agent.py)
   - Validates HMAC signature (timing-safe comparison)
   - Checks TTL (15-minute expiry)
   - Stores in contextvars.ContextVar (thread-safe, generator-safe)
   - Tools read identity via _get_request_context()

4. Write Tools
   - HARD GUARD: confirm_write_operation requires valid RequestContext
   - Records initiating_user (human) + executing_principal (SP) in audit
   - Blocked if identity is missing — token preserved for retry

5. Audit Trail
   - Every write records dual identity
   - identity_degraded=true when user context is unavailable
   - Correlatable by session_id, request_id, persona
```

### RequestContext Fields

| Field | Source | Purpose |
|---|---|---|
| `user_email` | SCIM `/Me` | Human identity for audit |
| `user_groups` | SCIM `/Me` | Persona-group binding validation |
| `persona` | App UI selection | Tool filtering and response style |
| `session_id` | App-generated UUID | Conversation correlation |
| `request_id` | Per-request UUID | Individual request tracing |
| `issued_at` | UTC timestamp | TTL start |
| `expires_at` | issued_at + 15 min | TTL enforcement |
| `signature` | HMAC-SHA256 | Tamper detection |

### HMAC Signing

The signing payload is the JSON serialization of all fields except `signature`,
with `sort_keys=True` and `separators=(",",":")` for cross-platform determinism.
The secret is stored in Databricks Secret Scope `echostar-identity/hmac-secret`.

Both the App-side and the Agent-side (`notebooks/identity_utils.py`) must
produce identical signing payloads. The pinned separators prevent whitespace
differences from breaking verification.

**App-side implementations**: The Gradio app uses `apps/shared/serving_client.py`
(the canonical shared module). The Dash app inlines a copy of `_RequestContext`
in `apps/dash-chatbot-app/model_serving_utils.py` (predates the shared module).
Both produce identical bytes. Changes to the signing format must be applied to
all three files: `identity_utils.py`, `serving_client.py`, and `model_serving_utils.py`.

### Thread Safety

Model Serving reuses threads from a pool. `contextvars.ContextVar` is used
instead of `threading.local` because:

- Each `.set()` is scoped to the current context
- `predict_stream()` uses `copy_context()` to snapshot identity at call time
- Each `next()` on the inner generator runs inside `snapshot.run()`, so the
  generator sees its original identity even if the thread handles a new request

---

## UC Tagging Scheme

Assets in Unity Catalog are tagged with `gov.*` tags that drive runtime
authorization decisions. Tags are stored in UC's native tagging system and
queryable via `information_schema`.

### Tag Names and Values

| Tag | Values | Effect |
|---|---|---|
| `gov.identity.mode` | `required` / `preferred` / `none` | `required` blocks access when no RequestContext is present |
| `gov.pii.level` | `high` / `medium` / `low` / `none` | Drives governed view generation |
| `gov.audit.required` | `true` / `false` | Logs tool access with user identity |
| `gov.allowed.personas` | Comma-separated names | Blocks tool access for unlisted personas |
| `gov.projection.policy` | `full` / `redacted` / `minimal` | Controls which columns are visible |
| `gov.write.policy` | `user_confirmed` / `sp_allowed` / `blocked` | Controls write authorization |
| `gov.data.classification` | `confidential` / `internal` / `public` | Informational labeling |

### Tag Inheritance

Tags are resolved with **object > schema > catalog** precedence:
1. Check `information_schema.table_tags` for the specific object
2. If no `gov.*` tags, check `information_schema.schema_tags` for the schema
3. If still none, check `information_schema.catalog_tags` for the catalog

Results are cached with a 5-minute TTL in `resolve_asset_policy()`.

---

## PII Isolation

### Schema Separation

| Schema | Contents | Agent SP Access |
|---|---|---|
| `{schema}` (main) | All agent-facing tools, billing tables, views | Full access to tools; `REVOKE SELECT` on raw `customers` table |
| `{schema}_internal` | `lookup_customer_pii` (full PII) | `REVOKE USE SCHEMA` — completely hidden |

### How It Works

1. `lookup_customer` in the main schema returns only non-PII fields:
   `customer_id`, `device_id`, `plan`, `contract_start_dt`
2. The function uses **definer rights** — it can read the `customers` table
   even though the agent SP has `REVOKE SELECT` on that table
3. `lookup_customer_pii` lives in `{schema}_internal` where the agent SP
   has `REVOKE USE SCHEMA` — it cannot even discover the function
4. The Genie Space has instructions blocking PII table/function references

### UC Function Bounding

Every UC function has a bounded result set:

| Bound type | Functions | Mechanism |
|---|---|---|
| `LIMIT N` | All 14 functions | Hard cap on row count |
| `LEAST(param, cap)` | `lookup_billing`, `lookup_operational_kpis`, `get_finance_operations_summary` | Caps lookback parameter in WHERE clause |
| PK filter | `lookup_customer`, `lookup_customer_erp_profile` | Returns 0-1 rows by design |
| `num_results` | `billing_faq` (vector search) | Bounded by search API |

---

## Admin Workflows

### Applying Governance Tags

Run `notebooks/12_admin_tagging.py`:
- `bulk_apply_tags(catalog, schema, tags_dict)` — applies tags to all tables
- `validate_tags(catalog, schema)` — reports which tables are missing required tags
- `generate_governed_views(catalog, schema)` — creates `v_{table}_governed` views for PII tables
- `export_tags_to_delta(catalog, schema)` — dumps tags to Delta for analytics

### Validating Identity Setup

Run `notebooks/12a_validate_identity_setup.py` — 9 checks:
1. Secret scope accessible
2. RequestContext round-trip (sign, serialize, deserialize, verify)
3. `gov.*` tags exist on at least one table
4. `billing_write_audit` has identity columns
5. `lookup_customer` does NOT return PII columns
6. `lookup_billing_items` has LIMIT clause
7. No PII functions in main schema
8. PII function exists in `_internal` schema
9. `customers` table PII column audit

### Setting Up the Secret Scope

```bash
databricks secrets create-scope echostar-identity
databricks secrets put-secret echostar-identity hmac-secret \
  --string-value "$(openssl rand -base64 32)"
```

---

## Persona-Group Binding

Each persona has a `required_groups` list in its YAML config. When a
`RequestContext` is present, the agent validates that the user's SCIM groups
intersect with the persona's required groups:

| Persona | Required Groups | Fallback |
|---|---|---|
| `customer_care` | None (default, accessible to all) | — |
| `finance_ops` | `billing_finance`, `billing_leadership` | Falls back to `customer_care` |
| `executive` | `billing_leadership`, `c_suite` | Falls back to `customer_care` |
| `technical` | `platform_engineering`, `data_engineering` | Falls back to `customer_care` |

If the user's groups do not authorize the requested persona, the agent silently
falls back to `customer_care` and logs a warning. The UI does not prevent
selection — the backend enforces the boundary.
