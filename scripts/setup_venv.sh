#!/usr/bin/env bash
set -euo pipefail

echo "=== Creating Virtual Environment ==="
command -v python3.12 >/dev/null || { echo "error: Python 3.12 is required" >&2; exit 1; }
# PyGObject and its GStreamer overrides are intentionally supplied by the
# matching Ubuntu system packages. Expose those ABI-matched packages inside
# the otherwise isolated project environment.
python3.12 -m venv --system-site-packages .venv

echo "=== Upgrading Core Tooling ==="
./.venv/bin/python -m pip install --upgrade pip setuptools wheel
./.venv/bin/python -m pip install --requirement requirements.lock
./.venv/bin/python -m pip install --editable .
./.venv/bin/python scripts/smoke_imports.py

echo ""
echo "Setup complete! To activate your environment, run:"
echo "  source .venv/bin/activate"
