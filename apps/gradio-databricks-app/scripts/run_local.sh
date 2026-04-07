#!/usr/bin/env bash
set -euo pipefail

# Run the app locally.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "ERROR: Virtualenv not found. Run scripts/bootstrap.sh first." >&2
    exit 1
fi

# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"

# Load .env if it exists
if [ -f "${PROJECT_DIR}/.env" ]; then
    echo "==> Loading .env"
    set -a
    # shellcheck source=/dev/null
    source "${PROJECT_DIR}/.env"
    set +a
fi

echo "==> Starting app..."
cd "$PROJECT_DIR"
python app.py
