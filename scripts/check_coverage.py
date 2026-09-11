#!/usr/bin/env python3
"""Enforce Phase 7.5's separate core and per-task-module coverage gates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

CORE_PARTS = frozenset(
    {
        "camera",
        "config",
        "deployment",
        "frame_buffer",
        "messaging",
        "module_runner",
        "recording",
        "replay",
        "runtime",
        "video",
        "wire",
    }
)


def _percent(covered: int, statements: int) -> float:
    return 100.0 if statements == 0 else covered * 100.0 / statements


def evaluate_coverage(
    payload: dict[str, Any], *, core_minimum: float = 80.0, task_minimum: float = 70.0
) -> tuple[bool, dict[str, Any]]:
    core_covered = 0
    core_statements = 0
    task_files: dict[str, float] = {}
    for filename, details in payload.get("files", {}).items():
        normalized = filename.replace("\\", "/")
        marker = "/purdue_rov_cv/"
        if marker not in f"/{normalized}":
            continue
        relative = f"/{normalized}".split(marker, 1)[1]
        parts = relative.split("/")
        summary = details["summary"]
        covered = int(summary["covered_lines"])
        statements = int(summary["num_statements"])
        if parts[0] in CORE_PARTS:
            core_covered += covered
            core_statements += statements
        elif parts[0] == "modules" and parts[-1] != "__init__.py":
            task_files[relative] = _percent(covered, statements)
    core_percent = _percent(core_covered, core_statements)
    task_failures = {name: value for name, value in task_files.items() if value + 1e-9 < task_minimum}
    details = {
        "core": {
            "covered_lines": core_covered,
            "statements": core_statements,
            "percent": core_percent,
            "minimum": core_minimum,
        },
        "task_modules": task_files,
        "task_minimum": task_minimum,
        "failures": task_failures,
    }
    return bool(core_statements and task_files and core_percent + 1e-9 >= core_minimum and not task_failures), details


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("coverage_json", type=Path)
    parser.add_argument("--core-min", type=float, default=80.0)
    parser.add_argument("--task-min", type=float, default=70.0)
    args = parser.parse_args(argv)
    payload = json.loads(args.coverage_json.read_text(encoding="utf-8"))
    passed, details = evaluate_coverage(payload, core_minimum=args.core_min, task_minimum=args.task_min)
    print(json.dumps(details, indent=2, sort_keys=True))
    if not details["core"]["statements"]:
        print("error: coverage report contained no core infrastructure files", file=sys.stderr)
    if not details["task_modules"]:
        print("error: coverage report contained no individual task module files", file=sys.stderr)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
