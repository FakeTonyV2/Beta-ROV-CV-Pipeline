#!/usr/bin/env python3
"""Build the Phase 7.5 aggregate machine/human acceptance artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from tests.transport.harness import write_report  # noqa: E402

EXPECTED_SCENARIOS = (
    "loss-1pct",
    "loss-5pct",
    "delay-20ms",
    "jitter-20ms",
    "rate-100mbit",
    "link-interruption-2s",
    "slow-consumer-30hz-to-1hz",
)
MATRIX_COLUMNS = (
    "Scenario",
    "Control",
    "Memory",
    "Queue bounds",
    "CV age recovery",
    "Video recovery",
    "Sequence gaps",
    "Broker/router",
    "Overall",
)


def _valid_cell(value: object) -> bool:
    return isinstance(value, str) and (
        value == "PASS"
        or value.startswith("FAIL — ")
        or value.startswith("N/A — ")
        or value.startswith("UNVERIFIED — ")
    )


def _validated_matrix(payload: dict[str, Any], scenario: str, path: Path) -> tuple[dict[str, str], list[str]]:
    failures: list[str] = []
    source = payload.get("acceptance_matrix")
    if not isinstance(source, dict):
        failures.append(f"{path}: missing acceptance_matrix object")
        source = {}
    row: dict[str, str] = {}
    for column in MATRIX_COLUMNS:
        value = source.get(column)
        valid = isinstance(value, str) and value == scenario if column == "Scenario" else _valid_cell(value)
        if not valid:
            failures.append(f"{path}: invalid or missing matrix cell {column!r}")
            row[column] = scenario if column == "Scenario" else "FAIL — invalid or missing scenario evidence"
        else:
            assert isinstance(value, str)
            row[column] = value
    row["artifact"] = str(path)
    return row, failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_directory", type=Path)
    args = parser.parse_args()
    candidates: dict[str, tuple[str, Path, dict[str, Any]]] = {}
    reports: list[tuple[Path, dict[str, Any]]] = []
    for path in args.artifact_directory.glob("transport-*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        scenario = payload.get("scenario")
        if not isinstance(scenario, str) or scenario == "phase75-acceptance-summary":
            continue
        reports.append((path, payload))
        previous = candidates.get(scenario)
        if previous is None or path.name > previous[0]:
            candidates[scenario] = (path.name, path, payload)
    for path, payload in reports:
        write_report(path, payload)
    matrix: list[dict[str, str]] = []
    failures: list[str] = []
    for scenario in EXPECTED_SCENARIOS:
        candidate = candidates.get(scenario)
        if candidate is None:
            reason = "UNVERIFIED — no current-run artifact was generated"
            matrix.append({column: scenario if column == "Scenario" else reason for column in MATRIX_COLUMNS})
            failures.append(f"missing current-run artifact for {scenario}")
            continue
        _filename, path, payload = candidate
        row, row_failures = _validated_matrix(payload, scenario, path)
        matrix.append(row)
        failures.extend(row_failures)
        if payload.get("outcome") != "PASS" or payload.get("passed") is not True:
            failures.append(f"{scenario} outcome is {payload.get('outcome', 'missing')}")
        if row["Overall"] != "PASS":
            failures.append(f"{scenario} matrix overall is not PASS")
    unexpected = sorted(set(candidates) - set(EXPECTED_SCENARIOS))
    if unexpected:
        failures.append(f"unexpected scenario artifacts: {unexpected}")
    passed = not failures
    summary = {
        "scenario": "phase75-acceptance-summary",
        "outcome": "PASS" if passed else "FAIL",
        "passed": passed,
        "failures": failures,
        "acceptance_matrix": matrix,
        "scenario_count": len(matrix),
        "artifact_directory": str(args.artifact_directory),
    }
    output = args.artifact_directory / "phase75-acceptance-summary.json"
    write_report(output, summary)
    print(output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
