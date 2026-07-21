#!/usr/bin/env bash
set -euo pipefail

echo "=== Creating Virtual Environment ==="
python3.12 -m venv --system-site-packages .venv

echo "=== Upgrading Core Tooling ==="
./.venv/bin/python -m pip install --upgrade pip setuptools wheel

echo ""
echo "Setup complete! To activate your environment, run:"
echo "  source .venv/bin/activate"