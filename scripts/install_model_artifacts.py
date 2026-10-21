#!/usr/bin/env python3
"""Install local model artifacts only when they match mission SHA-256 pins."""

from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
from pathlib import Path

from purdue_rov_cv.config.loader import load_config


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def install_artifacts(config_path: Path, model_directory: Path) -> tuple[Path, ...]:
    config = load_config(config_path, environ={})
    installed: list[Path] = []
    for task_id, task in sorted(config.tasks.items()):
        if not task.enabled:
            continue
        destination = task.artifact.path
        source = model_directory / destination.name
        if destination.is_file() and sha256_file(destination) == task.artifact.sha256:
            continue
        if not source.is_file():
            raise FileNotFoundError(
                f"task {task_id} artifact is missing at {destination}; provide {source} with the configured SHA-256"
            )
        actual = sha256_file(source)
        if actual != task.artifact.sha256:
            raise ValueError(
                f"task {task_id} source artifact SHA-256 mismatch: expected {task.artifact.sha256}, got {actual}"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            temporary.write_bytes(source.read_bytes())
            temporary.chmod(0o644)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        installed.append(destination)
    return tuple(installed)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--model-directory", type=Path, default=Path("models"))
    args = parser.parse_args()
    for path in install_artifacts(args.config, args.model_directory):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
