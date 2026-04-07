# Gradio Databricks Starter App

A production-ready Gradio application scaffold for [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/). Runs locally with `python app.py` and deploys to Databricks with the CLI or Databricks Asset Bundles.

## Project Structure

```
gradio-databricks-app/
├── app.py                      # Main application (UI + logic)
├── app.yaml                    # Databricks Apps manifest
├── databricks.yml              # Bundle config (dev + prod targets)
├── requirements.txt            # Additional Python dependencies
├── .env.example                # Local env var template
├── .gitignore
├── scripts/
│   ├── bootstrap.sh            # Create venv, install deps
│   ├── run_local.sh            # Run app locally
│   ├── validate.sh             # Check prerequisites + bundle
│   ├── deploy_app.sh           # Deploy via Databricks Apps CLI
│   ├── deploy_bundle.sh        # Deploy via bundles (dev)
│   └── deploy_bundle_prod.sh   # Deploy via bundles (prod)
└── README.md
```

## Architecture

The app is a single-file Gradio Blocks application (`app.py`) with:

- A **Config dataclass** that reads all settings from environment variables with safe defaults.
- **Integration stubs** for Databricks SQL, Model Serving, Unity Catalog, and Secrets — each is a function with a TODO and example implementation in the docstring.
- **Two demo panels**: a Data Explorer (filter/sort/aggregate a sample DataFrame) and a Text Utility (string transformations with a mock summarizer slot).
- A **health check** that reports runtime mode, configured resources, and env status.

The app starts with zero external dependencies by default (mock data only). As you connect Databricks resources, uncomment the relevant `valueFrom` entries in `app.yaml` and replace the stub functions with real implementations.

**Port binding**: The app reads `DATABRICKS_APP_PORT` (set automatically by the Databricks Apps runtime) and falls back to `APP_PORT` or `8000`.

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.11+ | Matches Databricks Apps runtime |
| Databricks CLI | 0.250.0+ | `brew install databricks` or [install docs](https://docs.databricks.com/dev-tools/cli/install.html) |
| Databricks workspace | — | With authenticated CLI profile (`databricks configure`) |

## Quickstart

### 1. Local Setup

```bash
# Clone / copy this directory, then:
cd gradio-databricks-app

# Create venv and install deps
chmod +x scripts/*.sh
./scripts/bootstrap.sh

# Copy env template
cp .env.example .env
# Edit .env as needed
```

### 2. Run Locally

```bash
./scripts/run_local.sh
# → opens at http://localhost:8000
```

Or manually:

```bash
source .venv/bin/activate
python app.py
```

### 3. Debug Locally with Databricks CLI

This uses the Databricks Apps local runner, which simulates the deployed environment (including injected env vars and `app.yaml` processing):

```bash
databricks apps run-local --prepare-environment --debug
```

The `--prepare-environment` flag installs `requirements.txt` into an isolated env. The `--debug` flag enables verbose logging.

### 4. Deploy to Databricks (Direct CLI)

This approach uses the Databricks Apps CLI commands directly — no bundles needed.

```bash
# Step 1: Create the app in your workspace (one time)
databricks apps create my-gradio-app

# Step 2: Deploy
APP_NAME=my-gradio-app ./scripts/deploy_app.sh
```

After deployment, add resources (SQL warehouse, serving endpoint, etc.) through the Databricks Apps UI, then redeploy.

### 5. Deploy with Bundles (Dev)

```bash
# First, edit databricks.yml — replace the placeholder workspace hosts.

./scripts/validate.sh         # Check prerequisites
./scripts/deploy_bundle.sh    # Validate + deploy + start
```

### 6. Deploy with Bundles (Prod)

```bash
./scripts/deploy_bundle_prod.sh
```

This prompts for confirmation before deploying. For CI/CD, set `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` as environment variables instead of using profiles.

## Deployment Methods Compared

| Method | When to Use | Pros | Cons |
|--------|-------------|------|------|
| **`python app.py`** | Local development | Instant, no Databricks needed | No platform auth/resources |
| **`databricks apps run-local`** | Local debugging | Simulates deployed env, processes `app.yaml` | Requires CLI + auth |
| **`databricks apps deploy`** | Single-environment | Simple, direct | Manual resource config, no multi-env |
| **Bundles (`databricks bundle deploy`)** | Multi-environment, CI/CD | Version-controlled infra, dev/prod targets | More setup, workspace hosts in YAML |

### Creating vs. Deploying an App

- **Creating**: `databricks apps create <name>` registers an app in the workspace. This must happen before the first deploy. You can also create apps through the Databricks UI.
- **Deploying**: `databricks apps deploy <name> --source-code-path ...` uploads code and starts the app. Requires the app to already exist.
- **Bundles**: `databricks bundle deploy` handles both creation and deployment. The app resource in `databricks.yml` defines the app; the bundle system creates it if it doesn't exist.
- **Generating from existing**: If you already deployed an app via CLI, run `databricks bundle generate app --existing-app-name <name> --key <key>` to generate bundle config from it.

## Portability and Configuration

Resource IDs (SQL warehouse IDs, endpoint names, catalog/schema names, workspace URLs) **must not be hardcoded** in application code. Instead:

1. **In `app.yaml`**: Use `valueFrom` to reference resources added via the Databricks Apps UI. The runtime injects the actual IDs as environment variables.
2. **In `app.py`**: Read values from `os.getenv()` with safe fallbacks.
3. **In `databricks.yml`**: Use `variables` with `lookup` to resolve resources by name, and `targets` to set per-environment values.

This keeps the same code deployable across dev, staging, and prod workspaces.

## Adding Databricks Integrations

### SQL Warehouse

1. Uncomment `DATABRICKS_WAREHOUSE_ID` in `app.yaml` with `valueFrom: sql-warehouse`
2. Add the SQL warehouse resource via the Databricks Apps UI
3. Add `databricks-sql-connector` to `requirements.txt`
4. Replace `_query_sql()` in `app.py` with the real implementation from its docstring

### Model Serving

1. Uncomment `SERVING_ENDPOINT_NAME` in `app.yaml` with `valueFrom: serving-endpoint`
2. Add the serving endpoint resource via the Databricks Apps UI
3. Replace `_call_serving_endpoint()` in `app.py`

### Unity Catalog

1. Set `DATABRICKS_CATALOG` and `DATABRICKS_SCHEMA` in `app.yaml`
2. Use `_query_sql()` to run `SELECT * FROM catalog.schema.table`

### Secrets

1. Add a secret resource via the Databricks Apps UI
2. Use `_get_secret()` or read the injected env var

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError: gradio` locally | Run `./scripts/bootstrap.sh` to install into venv |
| App starts but shows "not configured" | Expected — set env vars or add resources via app.yaml |
| `databricks apps deploy` fails with "app not found" | Create the app first: `databricks apps create <name>` |
| Bundle validate fails | Check workspace host in `databricks.yml`, ensure CLI auth works |
| Port conflict on 8000 | Set `APP_PORT=8001` in `.env` |
| App crashes on Databricks | Check `databricks apps logs <name>` for stack traces |
| `databricks apps run-local` fails | Ensure `app.yaml` exists in the current directory |

## Security Notes

- Never commit `.env` files — they may contain tokens. The `.gitignore` excludes them.
- In production, use Databricks Secrets or `app.yaml` `valueFrom` for sensitive values.
- Use a service principal (not personal tokens) for production deployments.
- The `app.yaml` `valueFrom` pattern ensures resource IDs are resolved by the platform, not stored in code.

## Future Enhancements

- Replace mock data with live Databricks SQL queries
- Add a chat panel backed by a Model Serving endpoint
- Integrate Unity Catalog table browsing
- Add user-level authorization via `x-forwarded-access-token` header (`gr.Request`)
- Add Lakebase (PostgreSQL) for transactional app state
- Add CI/CD pipeline (GitHub Actions) using bundle deploy with service principal auth
