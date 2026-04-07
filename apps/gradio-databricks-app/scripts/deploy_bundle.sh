#!/usr/bin/env bash
set -euo pipefail

# Deploy using Databricks Asset Bundles (dev target).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TARGET="${TARGET:-dev}"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
RUN_AFTER_DEPLOY="${RUN_AFTER_DEPLOY:-true}"

echo "==> Bundle deploy (target: ${TARGET})"
echo "    Profile: ${DATABRICKS_PROFILE}"
echo ""

cd "$PROJECT_DIR"

echo "==> Validating bundle..."
databricks bundle validate -t "$TARGET"

echo ""
echo "==> Deploying bundle..."
databricks bundle deploy -t "$TARGET"

if [ "$RUN_AFTER_DEPLOY" = "true" ]; then
    echo ""
    echo "==> Starting app resource..."
    databricks bundle run gradio_starter -t "$TARGET" || {
        echo "[WARN] bundle run failed — the app may need resources configured in the workspace UI first."
    }
fi

echo ""
echo "Done."
