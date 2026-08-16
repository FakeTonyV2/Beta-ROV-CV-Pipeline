#!/usr/bin/env bash
set -euo pipefail

echo "=== Creating Virtual Environment ==="
command -v python3.12 >/dev/null || { echo "error: Python 3.12 is required" >&2; exit 1; }
python3.12 -m venv .venv

echo "=== Upgrading Core Tooling ==="
./.venv/bin/python -m pip install --upgrade pip setuptools wheel
./.venv/bin/python -m pip install --requirement requirements.lock
./.venv/bin/python -m pip install --editable .
./.venv/bin/python scripts/smoke_imports.py

echo ""
echo "Setup complete! To activate your environment, run:"
echo "  source .venv/bin/activate"
