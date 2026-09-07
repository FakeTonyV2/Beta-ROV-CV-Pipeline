"""Deterministic Phase 7.5 netem, report, statistics, and memory tests."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from scripts.check_coverage import evaluate_coverage
from scripts.summarize_transport_reports import MATRIX_COLUMNS, _validated_matrix
from tests.transport.harness import (
    CommandFailure,
    MemorySample,
    NetemNamespaceHarness,
    evaluate_memory_boundedness,
    first_fresh_recovery,
    has_required_netem_capabilities,
    latency_statistics,
    write_report,
)


class _FakeRunner:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []
        self.netem = False

    def run(self, argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        del check
        self.commands.append(argv)
        if "replace" in argv:
            self.netem = True
        if "del" in argv:
            self.netem = False
        stdout = "qdisc netem 8001: root" if "show" in argv and self.netem else "qdisc noqueue 0: root"
        return subprocess.CompletedProcess(argv, 0, stdout, "")


class _FailedQdiscCleanupRunner(_FakeRunner):
    def run(self, argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        if "qdisc" in argv and "del" in argv:
            self.commands.append(argv)
            return subprocess.CompletedProcess(argv, 1, "", "injected qdisc delete failure")
        return super().run(argv, check=check)


def test_netem_capability_check_requires_net_admin_and_sys_admin_even_for_root_container() -> None:
    assert has_required_netem_capabilities(f"Name:\ttest\nCapEff:\t{(1 << 12) | (1 << 21):016x}\n")
    assert not has_required_netem_capabilities(f"Name:\ttest\nCapEff:\t{1 << 12:016x}\n")
    assert not has_required_netem_capabilities("Name:\ttest\nCapEff:\t0000000000000000\n")


@pytest.mark.parametrize(
    ("method", "arguments"),
    [
        ("apply_loss", ["loss", "5%"]),
        ("apply_delay", ["delay", "20ms"]),
        ("apply_jitter", ["delay", "20ms", "20ms", "distribution", "normal"]),
        ("apply_rate_limit", ["rate", "100mbit"]),
        ("interrupt_link", ["loss", "100%"]),
    ],
)
def test_netem_commands_are_symmetric_and_target_only_owned_veth(method: str, arguments: list[str]) -> None:
    runner = _FakeRunner()
    harness = NetemNamespaceHarness(runner)  # type: ignore[arg-type]
    harness._setup = True
    if method == "interrupt_link":
        getattr(harness, method)()
    else:
        getattr(harness, method)(
            5 if method == "apply_loss" else 20 if "delay" in method or "jitter" in method else 100
        )
    replacements = [command for command in runner.commands if "replace" in command]
    assert len(replacements) == 2
    assert all(command[-len(arguments) :] == arguments for command in replacements)
    assert {command[command.index("dev") + 1] for command in replacements} == {
        harness.interface_a,
        harness.interface_b,
    }
    cleared = harness.clear_impairment()
    assert all("netem" not in value for value in cleared.values())


def test_memory_evaluator_accepts_plateau_and_rejects_continuous_growth() -> None:
    mib = 1024 * 1024
    plateau = [MemorySample(float(i), 40 * mib + min(i, 2) * mib, 1.0) for i in range(10)]
    decision = evaluate_memory_boundedness(plateau)
    assert decision.bounded
    assert decision.peak_rss_bytes == 42 * mib

    growth = [MemorySample(float(i), 40 * mib + i * 3 * mib, 1.0) for i in range(10)]
    leaking = evaluate_memory_boundedness(growth)
    assert not leaking.bounded
    assert leaking.late_slope_bytes_per_second > 256 * 1024

    modest_growth = [MemorySample(float(i), 40 * mib + i * 100 * 1024, 1.0) for i in range(10)]
    modest_leak = evaluate_memory_boundedness(modest_growth)
    assert not modest_leak.bounded
    assert modest_leak.final_rss_bytes - modest_leak.baseline_rss_bytes < modest_leak.allowed_growth_bytes

    released_peak = [
        MemorySample(float(i), 40 * mib + (12 * mib if 4 <= i <= 6 else 1 * mib if i >= 7 else 0), 1.0)
        for i in range(12)
    ]
    released = evaluate_memory_boundedness(released_peak)
    assert released.bounded
    assert released.peak_rss_bytes == 52 * mib
    assert released.final_rss_bytes == 41 * mib


def test_memory_evaluator_requires_a_meaningful_window() -> None:
    with pytest.raises(ValueError, match="eight samples"):
        evaluate_memory_boundedness([MemorySample(0, 1, 0), MemorySample(1, 1, 0)])


def test_latency_nearest_rank_and_fresh_recovery_use_monotonic_time() -> None:
    stats = latency_statistics([1.0, 4.0, 2.0, 3.0, 100.0])
    assert stats == {"sample_count": 5, "minimum_ms": 1.0, "maximum_ms": 100.0, "mean_ms": 22.0, "p95_ms": 100.0}
    elapsed, fresh = first_fresh_recovery(
        [
            {"received_monotonic": 10.1, "age_ms": 400.0},
            {"received_monotonic": 10.6, "age_ms": 100.0},
            {"received_monotonic": 10.7, "age_ms": 90.0},
        ],
        impairment_removed_monotonic=10.0,
    )
    assert elapsed == pytest.approx(0.6)
    assert len(fresh) == 2

    elapsed, suffix = first_fresh_recovery(
        [
            {"received_monotonic": 20.1, "age_ms": 100.0},
            {"received_monotonic": 20.2, "age_ms": 400.0},
            {"received_monotonic": 20.3, "age_ms": 90.0},
        ],
        impairment_removed_monotonic=20.0,
    )
    assert elapsed == pytest.approx(0.1)
    assert [sample["age_ms"] for sample in suffix] == [100.0, 400.0, 90.0]


def test_report_serializer_creates_machine_readable_artifact(tmp_path: Path) -> None:
    path = tmp_path / "transport" / "scenario.json"
    write_report(path, {"scenario": "loss-1pct", "passed": False, "failures": ["example"]})
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["scenario"] == "loss-1pct"
    assert payload["passed"] is False
    assert payload["generated_at_utc"].endswith("+00:00")
    summary = path.with_suffix(".md").read_text(encoding="utf-8")
    assert "Result: FAIL" in summary
    assert "example" in summary

    unverified = tmp_path / "transport" / "unverified.json"
    write_report(
        unverified,
        {"scenario": "loss-5pct", "outcome": "UNVERIFIED", "passed": False, "failures": []},
    )
    assert "Result: UNVERIFIED" in unverified.with_suffix(".md").read_text(encoding="utf-8")


def test_cleanup_deletes_only_owned_namespaces_after_partial_setup() -> None:
    runner = _FakeRunner()
    harness = NetemNamespaceHarness(runner)  # type: ignore[arg-type]
    harness._owned_namespaces.add(harness.namespace_a)
    harness.cleanup()
    deleted = [command[-1] for command in runner.commands if command[:3] == ["ip", "netns", "delete"]]
    assert deleted == [harness.namespace_a]
    assert harness.last_cleanup_namespace_state == {harness.namespace_a: False}


def test_cleanup_continues_namespace_deletion_after_qdisc_failure() -> None:
    runner = _FailedQdiscCleanupRunner()
    runner.netem = True
    harness = NetemNamespaceHarness(runner)  # type: ignore[arg-type]
    harness._setup = True
    harness._owned_namespaces.update({harness.namespace_a, harness.namespace_b})
    with pytest.raises(CommandFailure, match="qdisc cleanup"):
        harness.cleanup()
    deleted = {command[-1] for command in runner.commands if command[:3] == ["ip", "netns", "delete"]}
    assert deleted == {harness.namespace_a, harness.namespace_b}
    assert not harness.owned_namespaces


def test_aggregate_matrix_rejects_missing_or_non_explicit_cells(tmp_path: Path) -> None:
    path = tmp_path / "transport-loss-1pct.json"
    complete = {column: "PASS" for column in MATRIX_COLUMNS}
    complete["Scenario"] = "loss-1pct"
    complete["Sequence gaps"] = "N/A — proved by the dedicated slow-consumer scenario"
    row, failures = _validated_matrix({"acceptance_matrix": complete}, "loss-1pct", path)
    assert not failures
    assert row["Overall"] == "PASS"

    del complete["Broker/router"]
    row, failures = _validated_matrix({"acceptance_matrix": complete}, "loss-1pct", path)
    assert failures
    assert row["Broker/router"].startswith("FAIL —")


def test_coverage_gate_is_weighted_for_core_and_individual_for_tasks() -> None:
    payload = {
        "files": {
            "src/purdue_rov_cv/runtime/queues.py": {"summary": {"covered_lines": 80, "num_statements": 100}},
            "src/purdue_rov_cv/messaging/broker.py": {"summary": {"covered_lines": 40, "num_statements": 50}},
            "src/purdue_rov_cv/modules/base.py": {"summary": {"covered_lines": 7, "num_statements": 10}},
            "src/purdue_rov_cv/modules/echo.py": {"summary": {"covered_lines": 9, "num_statements": 10}},
        }
    }
    passed, details = evaluate_coverage(payload)
    assert passed
    assert details["core"]["percent"] == 80.0
    payload["files"]["src/purdue_rov_cv/modules/base.py"]["summary"]["covered_lines"] = 6
    passed, details = evaluate_coverage(payload)
    assert not passed
    assert details["failures"] == {"modules/base.py": 60.0}
