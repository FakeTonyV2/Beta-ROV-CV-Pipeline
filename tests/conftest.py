"""Make the checked-in generated Python package importable during tests."""

import sys
from pathlib import Path

GENERATED_PYTHON = Path(__file__).parents[1] / "generated" / "python"
SRC = Path(__file__).parents[1] / "src"
sys.path.insert(0, str(GENERATED_PYTHON))
sys.path.insert(0, str(SRC))
