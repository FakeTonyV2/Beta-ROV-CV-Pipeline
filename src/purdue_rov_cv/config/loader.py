"""Load and validate YAML configuration at startup."""

from pathlib import Path

import yaml

from .models import AppConfig


def load_config(path: str | Path) -> AppConfig:
    """Read a YAML file and fail if it does not match the strict schema."""
    with Path(path).open("r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream)
    return AppConfig.model_validate(data)
