#!/usr/bin/env bash
set -euo pipefail

# Deploy using Databricks Asset Bundles (prod target).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"

echo "============================================"
echo "  PRODUCTION DEPLOYMENT"
echo "============================================"
echo ""
echo "  Target:  prod"
echo "  Profile: ${DATABRICKS_PROFILE}"
echo ""
echo "  This will deploy to the production workspace."
echo ""
read -rp "  Continue? (y/N): " confirm
if [[ "${confirm}" != [yY] ]]; then
    echo "Aborted."
    exit 0
fi

cd "$PROJECT_DIR"

echo ""
echo "==> Validating bundle (prod)..."
databricks bundle validate -t prod

echo ""
echo "==> Deploying bundle (prod)..."
databricks bundle deploy -t prod

echo ""
echo "==> Starting app resource (prod)..."
databricks bundle run gradio_starter -t prod || {
    echo "[WARN] bundle run failed — configure app resources in the workspace UI first."
}

echo ""
echo "Production deployment complete."
echo ""
echo "To use a service principal for CI/CD, set these environment variables:"
echo "  DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET"
echo "and remove the --profile flag (SDK auto-detects from env)."
