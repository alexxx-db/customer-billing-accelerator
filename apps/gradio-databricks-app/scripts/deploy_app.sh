#!/usr/bin/env bash
set -euo pipefail

# Deploy the app using the Databricks Apps CLI flow (without bundles).
#
# PREREQUISITE: The app must already exist in the workspace.
#   Create it first via UI or:  databricks apps create <APP_NAME>
#
# Usage:
#   APP_NAME=my-gradio-app ./scripts/deploy_app.sh
#   APP_NAME=my-gradio-app WORKSPACE_PATH=/Workspace/Users/me/apps/my-gradio-app ./scripts/deploy_app.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

APP_NAME="${APP_NAME:?ERROR: Set APP_NAME (e.g. APP_NAME=my-gradio-app)}"
DATABRICKS_PROFILE="${DATABRICKS_PROFILE:-DEFAULT}"
WORKSPACE_PATH="${WORKSPACE_PATH:-/Workspace/Users/$(databricks current-user me --profile "$DATABRICKS_PROFILE" 2>/dev/null | grep userName | cut -d'"' -f4)/apps/${APP_NAME}}"

echo "==> Deploying app: ${APP_NAME}"
echo "    Profile:        ${DATABRICKS_PROFILE}"
echo "    Workspace path: ${WORKSPACE_PATH}"
echo "    Source:         ${PROJECT_DIR}"
echo ""

# Upload source code
echo "==> Uploading source code..."
databricks workspace mkdirs "$WORKSPACE_PATH" --profile "$DATABRICKS_PROFILE" 2>/dev/null || true
databricks workspace import-dir "$PROJECT_DIR" "$WORKSPACE_PATH" \
    --profile "$DATABRICKS_PROFILE" \
    --overwrite

# Deploy
echo "==> Deploying..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "$WORKSPACE_PATH" \
    --profile "$DATABRICKS_PROFILE"

echo ""
echo "==> Checking app status..."
databricks apps get "$APP_NAME" --profile "$DATABRICKS_PROFILE"

echo ""
echo "Done. If the app is not running, start it with:"
echo "  databricks apps start ${APP_NAME} --profile ${DATABRICKS_PROFILE}"
