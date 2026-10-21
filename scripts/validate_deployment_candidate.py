#!/usr/bin/env python3
"""Validate a candidate mission and its locally staged model artifacts."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.module_runner.entrypoints import load_module
from purdue_rov_cv.modules.base import ModuleContext


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_candidate(config_path: Path, model_directory: Path, role: str) -> None:
    """Fail before deployment mutation if config, modules, or Pi models are invalid."""
    if role not in {"pi", "surface"}:
        raise ValueError("role must be pi or surface")
    config = load_config(config_path, environ={})
    for task_id, task in sorted(config.tasks.items()):
        if not task.enabled:
            continue
        module = load_module(task.module_class)
        if role == "pi":
            artifact = model_directory / task.artifact.path.name
            if not artifact.is_file():
                raise FileNotFoundError(f"task {task_id} candidate artifact is missing: {artifact}")
            actual = file_sha256(artifact)
            if actual != task.artifact.sha256:
                raise ValueError(
                    f"task {task_id} candidate artifact SHA-256 mismatch: expected {task.artifact.sha256}, got {actual}"
                )
            task = task.model_copy(update={"artifact": task.artifact.model_copy(update={"path": artifact.resolve()})})
            try:
                module.initialize(ModuleContext(task_id, task_id, config.device.device_id, task.input_camera, task))
            finally:
                module.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--model-directory", type=Path, default=Path("models"))
    parser.add_argument("--role", choices=("pi", "surface"), required=True)
    args = parser.parse_args()
    validate_candidate(args.config, args.model_directory, args.role)
    print(f"candidate mission is valid for {args.role}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
