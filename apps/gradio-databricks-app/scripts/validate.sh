#!/usr/bin/env bash
set -euo pipefail

# Validate prerequisites and bundle configuration.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ERRORS=0

echo "==> Validating project prerequisites..."

# Check required files
for f in app.py app.yaml requirements.txt databricks.yml; do
    if [ -f "${PROJECT_DIR}/${f}" ]; then
        echo "    [OK] ${f}"
    else
        echo "    [MISSING] ${f}" >&2
        ERRORS=$((ERRORS + 1))
    fi
done

# Check Databricks CLI
echo ""
echo "==> Checking Databricks CLI..."
if ! command -v databricks &>/dev/null; then
    echo "    [WARN] databricks CLI not found. Install from https://docs.databricks.com/dev-tools/cli/install.html"
    ERRORS=$((ERRORS + 1))
else
    CLI_VERSION=$(databricks --version 2>/dev/null | head -1)
    echo "    [OK] ${CLI_VERSION}"

    # Check minimum version (0.250.0)
    VERSION_NUM=$(echo "$CLI_VERSION" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    MAJOR=$(echo "$VERSION_NUM" | cut -d. -f1)
    MINOR=$(echo "$VERSION_NUM" | cut -d. -f2)
    if [ "${MAJOR:-0}" -eq 0 ] && [ "${MINOR:-0}" -lt 250 ]; then
        echo "    [WARN] Recommended CLI version >= 0.250.0, found ${VERSION_NUM}"
    fi
fi

# Check Python
echo ""
echo "==> Checking Python..."
if command -v python3 &>/dev/null; then
    echo "    [OK] $(python3 --version)"
else
    echo "    [MISSING] python3" >&2
    ERRORS=$((ERRORS + 1))
fi

# Bundle validation (optional — requires workspace auth)
echo ""
if command -v databricks &>/dev/null; then
    echo "==> Running bundle validate..."
    cd "$PROJECT_DIR"
    if databricks bundle validate 2>&1; then
        echo "    [OK] Bundle is valid"
    else
        echo "    [WARN] Bundle validation failed (workspace auth may be needed)"
    fi
else
    echo "==> Skipping bundle validate (CLI not installed)"
fi

echo ""
if [ "$ERRORS" -gt 0 ]; then
    echo "Validation completed with ${ERRORS} issue(s)."
    exit 1
else
    echo "Validation passed."
fi
