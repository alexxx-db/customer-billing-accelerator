#!/usr/bin/env bash
set -euo pipefail

# Create virtualenv, install dependencies.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
PYTHON="${PYTHON:-python3}"

echo "==> Checking Python..."
if ! command -v "$PYTHON" &>/dev/null; then
    echo "ERROR: $PYTHON not found. Install Python 3.11+ and retry." >&2
    exit 1
fi
echo "    Using: $($PYTHON --version)"

if [ ! -d "$VENV_DIR" ]; then
    echo "==> Creating virtualenv at ${VENV_DIR}..."
    "$PYTHON" -m venv "$VENV_DIR"
else
    echo "==> Virtualenv already exists at ${VENV_DIR}"
fi

echo "==> Activating virtualenv..."
# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"

echo "==> Upgrading pip..."
pip install --quiet --upgrade pip

echo "==> Installing local dev dependencies..."
# For local dev we need gradio + pandas (they are pre-installed on Databricks Apps runtime)
pip install --quiet gradio pandas

# Install any additional project requirements
if [ -f "${PROJECT_DIR}/requirements.txt" ]; then
    echo "==> Installing requirements.txt..."
    pip install --quiet -r "${PROJECT_DIR}/requirements.txt"
fi

echo ""
echo "Done. Activate with:  source ${VENV_DIR}/bin/activate"
