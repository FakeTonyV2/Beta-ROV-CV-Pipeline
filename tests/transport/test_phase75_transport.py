"""Privileged, real-process Phase 7.5 transport acceptance suite."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from purdue_rov.cv.v1 import control_pb2

from purdue_rov_cv.messaging.sockets import BROKER_HWM, CONTROL_ROUTER_HWM
from purdue_rov_cv.module_runner.publisher import RESULT_PUBLISHER_HWM
from tests.transport.harness import (
    MemorySample,
    NetemNamespaceHarness,
    evaluate_memory_boundedness,
    first_fresh_recovery,
    latency_statistics,
    netem_unavailable_reason,
    provenance,
    sample_processes,
    write_report,
)

pytestmark = [pytest.mark.transport, pytest.mark.requires_net_admin]

PROJECT_ROOT = Path(__file__).parents[2]
DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "test-results" / "transport"


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    method: str
    value: float | None
    impairment_seconds: float


SCENARIOS = (
    Scenario("loss-1pct", "apply_loss", 1.0, 8.0),
    Scenario("loss-5pct", "apply_loss", 5.0, 8.0),
    Scenario("delay-20ms", "apply_delay", 20.0, 8.0),
    Scenario("jitter-20ms", "apply_jitter", 20.0, 8.0),
    Scenario("rate-100mbit", "apply_rate_limit", 100.0, 8.0),
    Scenario("link-interruption-2s", "interrupt_link", None, 2.0),
)


def _environment_unavailable_reason(*, require_video: bool) -> str | None:
    reason = netem_unavailable_reason()
    if reason is not None:
        return reason
    if not require_video:
        return None
    try:
        import gi

        gi.require_version("Gst", "1.0")
        from gi.repository import Gst

        Gst.init(None)
        missing = [
            name
            for name in ("x264enc", "rtph264pay", "rtpjitterbuffer", "rtph264depay", "avdec_h264")
            if Gst.ElementFactory.find(name) is None
        ]
        if missing:
            return f"required GStreamer elements are missing: {', '.join(missing)}"
    except (ImportError, ValueError) as error:
        return f"GStreamer Python integration is unavailable: {type(error).__name__}: {error}"
    return None


def _unverified_matrix(scenario: str, reason: str, *, include_video: bool) -> dict[str, str]:
    unverified = f"UNVERIFIED — {reason}"
    return {
        "Scenario": scenario,
        "Control": unverified,
        "Memory": unverified,
        "Queue bounds": unverified,
        "CV age recovery": unverified,
        "Video recovery": unverified if include_video else "N/A — video is unrelated to structured-consumer pressure",
        "Sequence gaps": unverified,
        "Broker/router": unverified,
        "Overall": unverified,
    }


def _skip_with_artifact(
    scenario: str,
    reason: str,
    directory: Path,
    report_path: Path,
    *,
    include_video: bool,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    write_report(
        report_path,
        {
            "scenario": scenario,
            "outcome": "UNVERIFIED",
            "passed": False,
            "unverified_reason": reason,
            "failures": [],
            "acceptance_matrix": _unverified_matrix(scenario, reason, include_video=include_video),
            "provenance": provenance(),
            "artifacts": {"report": str(report_path), "event_directory": str(directory), "logs": {}},
        },
    )
    pytest.skip(f"{reason}; UNVERIFIED artifact={report_path}")


def _read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            # The final line can be observed while another process is appending.
            continue
    return records


class Topology:
    def __init__(
        self,
        network: NetemNamespaceHarness,
        directory: Path,
        *,
        consumer_rate_hz: float,
        payload_bytes: int,
    ) -> None:
        self.network = network
        self.directory = directory
        self.consumer_rate_hz = consumer_rate_hz
        self.payload_bytes = payload_bytes
        self.processes: dict[str, subprocess.Popen[str]] = {}
        self.process_namespaces: dict[str, str] = {}
        self.event_paths: dict[str, Path] = {}
        self.log_paths: dict[str, Path] = {}
        self._streams: list[Any] = []
        self.shutdown: dict[str, dict[str, Any]] = {}
        host = network.address_a
        self.publisher_endpoint = f"tcp://{host}:5555"
        self.subscriber_endpoint = f"tcp://{host}:5556"
        self.client_endpoint = f"tcp://{host}:5557"
        self.module_endpoint = f"tcp://{host}:5558"

    def _launch(self, role: str, namespace: str, *arguments: str) -> None:
        event_path = self.directory / f"{role}.events.jsonl"
        log_path = self.directory / f"{role}.log"
        stream = log_path.open("w", encoding="utf-8")
        self._streams.append(stream)
        command = self.network.exec_prefix(namespace) + [
            sys.executable,
            "-m",
            "tests.transport.roles",
            role,
            "--events",
            str(event_path),
            *arguments,
        ]
        process = subprocess.Popen(
            command,
            cwd=PROJECT_ROOT,
            stdout=stream,
            stderr=subprocess.STDOUT,
            text=True,
            env=os.environ.copy(),
        )
        self.processes[role] = process
        self.process_namespaces[role] = namespace
        self.event_paths[role] = event_path
        self.log_paths[role] = log_path

    def start(self, *, include_video: bool) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        a, b = self.network.namespace_a, self.network.namespace_b
        self._launch(
            "broker",
            a,
            "--publisher-endpoint",
            self.publisher_endpoint,
            "--subscriber-endpoint",
            self.subscriber_endpoint,
        )
        self.wait_for_events("broker", "socket_config", minimum=2)
        self._launch(
            "router",
            a,
            "--client-endpoint",
            self.client_endpoint,
            "--module-endpoint",
            self.module_endpoint,
        )
        self.wait_for_events("router", "socket_config", minimum=2)
        self._launch("module", a, "--module-endpoint", self.module_endpoint)
        self._launch(
            "publisher",
            a,
            "--publisher-endpoint",
            self.publisher_endpoint,
            "--rate-hz",
            "30",
            "--payload-bytes",
            str(self.payload_bytes),
        )
        self._launch(
            "subscriber",
            b,
            "--subscriber-endpoint",
            self.subscriber_endpoint,
            "--consumer-rate-hz",
            f"{self.consumer_rate_hz:g}",
            "--receive-rate-hz",
            "120" if self.consumer_rate_hz == 1.0 else f"{self.consumer_rate_hz:g}",
            "--receive-pause-seconds",
            "0.3" if self.consumer_rate_hz == 1.0 else "0",
        )
        self._launch("control", b, "--client-endpoint", self.client_endpoint)
        if include_video:
            self._launch(
                "video",
                b,
                "--subscriber-endpoint",
                self.subscriber_endpoint,
                "--publisher-endpoint",
                self.publisher_endpoint,
                "--stream-index",
                "27",
            )
            self.wait_for_events("video", "ready")
            self._launch(
                "sender",
                a,
                "--publisher-endpoint",
                self.publisher_endpoint,
                "--video-address",
                self.network.address_b,
                "--stream-index",
                "27",
            )

    def wait_for_events(self, role: str, kind: str, *, minimum: int = 1, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            process = self.processes[role]
            if process.poll() is not None:
                raise AssertionError(f"{role} exited before {kind}: returncode={process.returncode}")
            if len(self.events(role, kind)) >= minimum:
                return
            time.sleep(0.05)
        raise AssertionError(f"{role} did not emit {minimum} {kind} event(s) within {timeout:g}s")

    def events(self, role: str, kind: str | None = None) -> list[dict[str, Any]]:
        events = _read_events(self.event_paths[role])
        return events if kind is None else [event for event in events if event.get("event") == kind]

    def fatal_errors(self) -> list[str]:
        failures = []
        for role, process in self.processes.items():
            fatal = self.events(role, "fatal")
            if fatal:
                failures.append(f"{role} reported fatal error: {fatal[-1].get('message')}")
            if process.poll() is not None:
                failures.append(f"{role} exited unexpectedly with {process.returncode}")
        return failures

    def wait_for_baseline(self, *, include_video: bool, timeout: float = 20.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            fatal = self.fatal_errors()
            if fatal:
                raise AssertionError("; ".join(fatal))
            results = self.events("subscriber", "result")
            controls = self.events("control", "control")
            result_ready = any(event["age_ms"] < 250 for event in results)
            control_ready = any(
                event["structurally_valid"]
                and event["correlated"]
                and event["status"] == control_pb2.COMMAND_STATUS_COMPLETED
                for event in controls
            )
            video_ready = not include_video or (
                any(event.get("state") == ComponentState.RUNNING.value for event in self.events("video", "video_state"))
                and any(event.get("frame_index_hits", 0) > 0 for event in self.events("video", "video_correlation"))
            )
            if result_ready and control_ready and video_ready:
                return
            time.sleep(0.1)
        raise AssertionError("production topology did not establish a complete baseline")

    def stop(self) -> None:
        for role, process in reversed(tuple(self.processes.items())):
            started = time.monotonic()
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(2)
            elapsed = time.monotonic() - started
            self.shutdown[role] = {
                "duration_seconds": elapsed,
                "returncode": process.returncode,
                "within_five_seconds": elapsed < 5,
            }
        for stream in self._streams:
            stream.close()

    def process_evidence(self) -> dict[str, dict[str, Any]]:
        return {
            role: {
                "pid": process.pid,
                "namespace": self.process_namespaces[role],
                "alive": process.poll() is None,
                "event_file": str(self.event_paths[role]),
                "log_file": str(self.log_paths[role]),
            }
            for role, process in self.processes.items()
        }

    def socket_evidence(self) -> dict[str, list[dict[str, Any]]]:
        return {role: self.events(role, "socket_config") for role in self.processes}


# Avoid importing runtime state above solely to build a string comparison.
from purdue_rov_cv.runtime.state import ComponentState  # noqa: E402


def _artifact_path(name: str) -> tuple[Path, Path]:
    root = Path(os.environ.get("TRANSPORT_ARTIFACT_DIR", DEFAULT_ARTIFACT_ROOT))
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    directory = root / f"{name}-{run_id}"
    return directory, root / f"transport-{name}-{run_id}.json"


def _memory_report(samples: dict[str, list[MemorySample]], failures: list[str]) -> dict[str, Any]:
    report: dict[str, Any] = {}
    for role, values in samples.items():
        try:
            decision = evaluate_memory_boundedness(values)
            report[role] = {
                **asdict(decision),
                "sampling_period_seconds": 0.5,
                "cpu_percent": {
                    "mean_after_baseline": (
                        sum(sample.cpu_percent for sample in values[1:]) / (len(values) - 1) if len(values) > 1 else 0.0
                    ),
                    "maximum_after_baseline": max((sample.cpu_percent for sample in values[1:]), default=0.0),
                },
                "samples": [asdict(sample) for sample in values],
            }
            if not decision.bounded:
                failures.append(f"{role} memory did not reach a bounded plateau")
        except ValueError as error:
            report[role] = {"bounded": False, "reason": str(error)}
            failures.append(f"{role} memory was not measured adequately: {error}")
    return report


def _counter_rates(
    before: dict[str, dict[str, int]],
    after: dict[str, dict[str, int]],
    duration_seconds: float,
) -> dict[str, dict[str, float | int]]:
    if duration_seconds <= 0:
        raise ValueError("counter interval must be positive")
    rates: dict[str, dict[str, float | int]] = {}
    for interface, first in before.items():
        last = after[interface]
        tx_delta = last["tx_bytes"] - first["tx_bytes"]
        rx_delta = last["rx_bytes"] - first["rx_bytes"]
        if tx_delta < 0 or rx_delta < 0:
            raise ValueError("interface counters moved backwards")
        rates[interface] = {
            "duration_seconds": duration_seconds,
            "tx_bytes": tx_delta,
            "rx_bytes": rx_delta,
            "tx_mbit_per_second": tx_delta * 8 / duration_seconds / 1_000_000,
            "rx_mbit_per_second": rx_delta * 8 / duration_seconds / 1_000_000,
        }
    return rates


def _hwm_report(topology: Topology, *, include_video: bool, failures: list[str]) -> dict[str, Any]:
    observed = topology.socket_evidence()
    checks: dict[str, bool] = {}
    publisher = observed.get("publisher", [])
    subscriber = observed.get("subscriber", [])
    broker = observed.get("broker", [])
    router = observed.get("router", [])
    checks["publisher_pub_sndhwm_5"] = bool(
        publisher and publisher[-1].get("sndhwm") == RESULT_PUBLISHER_HWM and publisher[-1].get("send_timeout_ms") == 0
    )
    checks["subscriber_sub_rcvhwm_5"] = bool(
        subscriber and subscriber[-1].get("rcvhwm") == 5 and subscriber[-1].get("conflate") == 0
    )
    checks["broker_xsub_xpub_hwm_100"] = len(broker) == 2 and all(
        event.get("rcvhwm") == BROKER_HWM and event.get("sndhwm") == BROKER_HWM for event in broker
    )
    checks["router_hwm_100"] = len(router) == 2 and all(
        event.get("rcvhwm") == CONTROL_ROUTER_HWM and event.get("sndhwm") == CONTROL_ROUTER_HWM for event in router
    )
    if include_video:
        video = observed.get("video", [])
        sender = observed.get("sender", [])
        checks["frame_index_sub_rcvhwm_5"] = bool(
            video and video[-1].get("rcvhwm") == 5 and video[-1].get("conflate") == 0
        )
        checks["frame_index_pub_sndhwm_5"] = bool(
            sender and sender[-1].get("sndhwm") == RESULT_PUBLISHER_HWM and sender[-1].get("send_timeout_ms") == 0
        )
    if not all(checks.values()):
        failures.append(f"effective socket bounds differed from production: {checks}")
    return {"observations": observed, "checks": checks, "passed": all(checks.values())}


def _matrix_cell(passed: bool, failure: str) -> str:
    return "PASS" if passed else f"FAIL — {failure}"


def _sample_until(
    topology: Topology,
    samples: dict[str, list[MemorySample]],
    *,
    started: float,
    deadline: float,
) -> None:
    while time.monotonic() < deadline:
        for role, sample in sample_processes(topology.processes, started).items():
            samples.setdefault(role, []).append(sample)
        time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))


@pytest.mark.timeout(60)
@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda scenario: scenario.name)
def test_required_netem_scenario(scenario: Scenario) -> None:
    directory, report_path = _artifact_path(scenario.name)
    unavailable = _environment_unavailable_reason(require_video=True)
    if unavailable is not None:
        _skip_with_artifact(scenario.name, unavailable, directory, report_path, include_video=True)
    failures: list[str] = []
    report: dict[str, Any] = {
        "scenario": scenario.name,
        "outcome": "FAIL",
        "phases": {},
        "failures": failures,
    }
    network = NetemNamespaceHarness()
    topology: Topology | None = None
    memory_samples: dict[str, list[MemorySample]] = {}
    started = time.monotonic()
    applied = removed = 0.0
    baseline_decoded = 0
    baseline_exact_hits = 0
    try:
        network.setup()
        report["network_architecture"] = {
            "namespace_a": network.namespace_a,
            "namespace_b": network.namespace_b,
            "interface_a": network.interface_a,
            "interface_b": network.interface_b,
            "address_a": network.address_a,
            "address_b": network.address_b,
            "routes": network.route_state(),
            "impairment_direction": "symmetric egress qdisc on both harness-owned veth endpoints",
            "process_placement": {
                "namespace_a": ["publisher", "broker", "router", "module", "sender"],
                "namespace_b": ["subscriber", "control", "video"],
            },
        }
        payload_bytes = 450_000 if scenario.method == "apply_rate_limit" else 16_000
        topology = Topology(network, directory, consumer_rate_hz=60.0, payload_bytes=payload_bytes)
        topology.start(include_video=True)
        topology.wait_for_baseline(include_video=True)
        baseline_decoded = topology.events("video", "video_frame")[-1]["decoded_frames"]
        baseline_exact_hits = topology.events("video", "video_correlation")[-1]["frame_index_hits"]
        report["phases"]["baseline"] = {
            "completed_monotonic": time.monotonic(),
            "result_count": len(topology.events("subscriber", "result")),
            "control_count": len(topology.events("control", "control")),
            "decoded_frames": baseline_decoded,
            "frame_index_hits": baseline_exact_hits,
            "cv_payload_bytes": payload_bytes,
        }
        baseline_counter_started = time.monotonic()
        baseline_counter_first = network.interface_counters()
        _sample_until(topology, memory_samples, started=started, deadline=time.monotonic() + 4.0)
        baseline_counter_last = network.interface_counters()
        baseline_counter_ended = time.monotonic()
        baseline_rates = _counter_rates(
            baseline_counter_first,
            baseline_counter_last,
            baseline_counter_ended - baseline_counter_started,
        )
        if scenario.value is None:
            getattr(network, scenario.method)()
        else:
            getattr(network, scenario.method)(scenario.value)
        applied = time.monotonic()
        impairment_counter_first = network.interface_counters()
        impairment_counter_started = time.monotonic()
        qdisc_active = network.qdisc_state()
        report["phases"]["impairment"] = {
            "applied_monotonic": applied,
            "requested_duration_seconds": scenario.impairment_seconds,
            "qdisc": qdisc_active,
            "qdisc_statistics_at_start": network.qdisc_statistics(),
        }
        _sample_until(
            topology,
            memory_samples,
            started=started,
            deadline=applied + scenario.impairment_seconds,
        )
        impairment_counter_last = network.interface_counters()
        impairment_counter_ended = time.monotonic()
        qdisc_statistics_at_end = network.qdisc_statistics()
        removal_started = time.monotonic()
        network.clear_impairment()
        removed = time.monotonic()
        report["phases"]["recovery"] = {
            "removed_monotonic": removed,
            "removal_started_monotonic": removal_started,
            "actual_impairment_seconds": removal_started - applied,
            "qdisc_after_clear": network.qdisc_state(),
            "qdisc_statistics_before_clear": qdisc_statistics_at_end,
        }
        if removal_started - applied < scenario.impairment_seconds:
            failures.append(
                f"impairment lasted {removal_started - applied:.6f}s, shorter than {scenario.impairment_seconds:.6f}s"
            )
        impairment_rates = _counter_rates(
            impairment_counter_first,
            impairment_counter_last,
            impairment_counter_ended - impairment_counter_started,
        )
        report["network_measurements"] = {
            "baseline": baseline_rates,
            "impairment": impairment_rates,
            "clock_basis": "monotonic interval with per-veth kernel byte counters",
        }
        _sample_until(topology, memory_samples, started=started, deadline=removed + 6.0)

        results = topology.events("subscriber", "result")
        try:
            recovery_elapsed, fresh = first_fresh_recovery(results, impairment_removed_monotonic=removed)
            sustained = fresh[:5]
            freshness_passed = (
                recovery_elapsed <= 2.0 and len(sustained) == 5 and all(event["age_ms"] < 250 for event in sustained)
            )
            report["cv_freshness"] = {
                "clock_basis": "same-kernel time.time_ns publisher/consumer wall clock",
                "impairment_removed_monotonic": removed,
                "recovery_seconds": recovery_elapsed,
                "first_age_ms": fresh[0]["age_ms"],
                "sustained_samples": sustained,
                "passed": freshness_passed,
            }
            if recovery_elapsed > 2.0:
                failures.append(f"CV freshness recovery took {recovery_elapsed:.3f}s (>2s)")
            if len(sustained) < 5 or any(event["age_ms"] >= 250 for event in sustained):
                failures.append("five consecutive near-current CV results did not follow recovery")
        except ValueError as error:
            report["cv_freshness"] = {"recovery_seconds": None, "error": str(error), "passed": False}
            failures.append(str(error))

        controls = topology.events("control", "control")
        during = [
            event for event in controls if event["started_monotonic"] >= applied and event["ended_monotonic"] <= removed
        ]
        after = [event for event in controls if event["started_monotonic"] >= removed]
        successful_during = [
            event
            for event in during
            if event["structurally_valid"]
            and event["correlated"]
            and event["status"] == control_pb2.COMMAND_STATUS_COMPLETED
        ]
        successful_after = [
            event
            for event in after
            if event["structurally_valid"]
            and event["correlated"]
            and event["status"] == control_pb2.COMMAND_STATUS_COMPLETED
        ]
        control_passed = bool(successful_after) and (scenario.method == "interrupt_link" or bool(successful_during))
        report["control"] = {
            "during": None
            if scenario.method == "interrupt_link"
            else latency_statistics([e["latency_ms"] for e in successful_during])
            if successful_during
            else None,
            "outage_expected": scenario.method == "interrupt_link",
            "post_recovery": latency_statistics([e["latency_ms"] for e in successful_after])
            if successful_after
            else None,
            "during_attempt_count": len(during),
            "during_valid_completed_count": len(successful_during),
            "post_recovery_attempt_count": len(after),
            "post_recovery_valid_completed_count": len(successful_after),
            "passed": control_passed,
        }
        if scenario.method != "interrupt_link" and not successful_during:
            failures.append("no valid completed control acknowledgement arrived during impairment")
        if not successful_after:
            failures.append("control did not recover after impairment")

        video_states = topology.events("video", "video_state")
        video_backends = topology.events("video", "video_backend")
        video_frames = topology.events("video", "video_frame")
        video_correlations = topology.events("video", "video_correlation")
        post_frames = [event for event in video_frames if event["monotonic"] >= removed]
        post_exact = [
            event
            for event in video_correlations
            if event["monotonic"] >= removed and event["frame_index_hits"] > baseline_exact_hits
        ]
        video_report: dict[str, Any] = {
            "decoded_before_impairment": baseline_decoded,
            "post_recovery_frames": len(post_frames),
            "process_alive": topology.processes["video"].poll() is None,
            "exact_hits_before_impairment": baseline_exact_hits,
            "first_exact_correlation_recovery_seconds": (post_exact[0]["monotonic"] - removed if post_exact else None),
        }
        if scenario.method == "interrupt_link":
            degraded = [
                event for event in video_states if event["monotonic"] >= applied and event["state"] == "DEGRADED"
            ]
            absent = [event for event in video_backends if event["monotonic"] >= applied and not event["present"]]
            rebuilt = [event for event in video_backends if event["monotonic"] >= removed and event["present"]]
            running = [event for event in video_states if event["monotonic"] >= removed and event["state"] == "RUNNING"]
            video_report.update(
                {
                    "stream_loss_detected_monotonic": degraded[0]["monotonic"] if degraded else None,
                    "pipeline_torn_down": bool(absent),
                    "pipeline_rebuild_seconds": rebuilt[0]["monotonic"] - removed if rebuilt else None,
                    "running_recovery_seconds": running[0]["monotonic"] - removed if running else None,
                }
            )
            if not degraded or not absent:
                failures.append("video receiver did not detect stream loss and tear down its pipeline")
            if not rebuilt or rebuilt[0]["monotonic"] - removed > 3.0:
                failures.append("video receiver pipeline did not rebuild within three seconds")
            frames_after_rebuild = (
                [] if not rebuilt else [e for e in post_frames if e["monotonic"] >= rebuilt[0]["monotonic"]]
            )
            if len(frames_after_rebuild) < 5 or not running:
                failures.append("video receiver did not restore RUNNING after five valid decoded frames")
            video_passed = bool(
                degraded
                and absent
                and rebuilt
                and rebuilt[0]["monotonic"] - removed <= 3.0
                and len(frames_after_rebuild) >= 5
                and running
                and post_exact
            )
        else:
            if not post_frames:
                failures.append("video did not produce a decoded frame after impairment")
            video_passed = bool(post_frames and post_exact)
        if not post_exact:
            failures.append("exact FrameIndex correlation did not recover after impairment")
        video_report["passed"] = video_passed
        report["video"] = video_report

        publisher_samples = topology.events("publisher", "publisher_sample")
        maximum_queue = max(event["queue_maximum"] for event in publisher_samples)
        hwm = _hwm_report(topology, include_video=True, failures=failures)
        queue_passed = maximum_queue <= 4 and hwm["passed"]
        report["queues"] = {
            "publisher_cv_result_queue": {"configured": 4, "observed_maximum": maximum_queue},
            "effective_socket_configuration": hwm,
            "canonical_cross_phase_capacities": {
                "frame_input": 1,
                "cv_result": 4,
                "priority": 32,
                "control_command": 16,
                "control_result": 16,
                "owner_regression": "tests/unit/test_runtime_queues.py",
            },
            "passed": queue_passed,
        }
        if maximum_queue > 4:
            failures.append(f"CvResultQueue exceeded capacity: {maximum_queue}")
        failures.extend(topology.fatal_errors())
        report["memory"] = _memory_report(memory_samples, failures)
        report["process_survival"] = {role: process.poll() is None for role, process in topology.processes.items()}
        if not all(report["process_survival"].values()):
            failures.append("one or more production processes exited during the scenario")
        post_results = [event for event in results if event["received_monotonic"] >= removed]
        broker_router_passed = bool(
            report["process_survival"].get("broker")
            and report["process_survival"].get("router")
            and post_results
            and successful_after
        )
        report["broker_router"] = {
            "broker_pid_alive": report["process_survival"].get("broker", False),
            "broker_functional_post_recovery": bool(post_results),
            "router_pid_alive": report["process_survival"].get("router", False),
            "router_functional_post_recovery": bool(successful_after),
            "passed": broker_router_passed,
        }
        if not broker_router_passed:
            failures.append("broker/router did not remain alive and functional after impairment")
        if scenario.method == "apply_rate_limit":
            key = f"{network.namespace_a}/{network.interface_a}"
            baseline_mbit = float(baseline_rates[key]["tx_mbit_per_second"])
            impaired_mbit = float(impairment_rates[key]["tx_mbit_per_second"])
            approached = baseline_mbit >= 100.0
            constrained = impaired_mbit <= 105.0 and impaired_mbit <= baseline_mbit * 0.98
            report["rate_limit_observation"] = {
                "configured_mbit_per_second": 100.0,
                "baseline_tx_mbit_per_second": baseline_mbit,
                "impaired_tx_mbit_per_second": impaired_mbit,
                "workload_approached_limit": approached,
                "observed_constraint": constrained,
                "qdisc_statistics": qdisc_statistics_at_end,
                "passed": approached and constrained,
            }
            if not approached:
                failures.append(f"baseline traffic did not reach the 100 Mbit/s limiter ({baseline_mbit:.3f} Mbit/s)")
            if not constrained:
                failures.append(
                    f"100 Mbit/s qdisc did not measurably constrain egress ({baseline_mbit:.3f}->{impaired_mbit:.3f})"
                )
        else:
            report["rate_limit_observation"] = {"status": "N/A — scenario is not rate limiting"}
    except BaseException as error:
        failures.append(f"harness failure: {type(error).__name__}: {error}")
    finally:
        if topology is not None:
            report.setdefault("processes", topology.process_evidence())
            topology.stop()
            report["shutdown"] = topology.shutdown
            if any(not item["within_five_seconds"] for item in topology.shutdown.values()):
                failures.append("one or more processes exceeded the five-second shutdown bound")
        try:
            network.cleanup()
        except BaseException as error:
            failures.append(f"network cleanup failed: {error}")
        report["network_cleanup"] = {
            "qdisc_after_cleanup": network.last_cleanup_qdisc_state,
            "owned_namespaces_present_after_cleanup": network.last_cleanup_namespace_state,
            "passed": bool(network.last_cleanup_namespace_state)
            and not any(network.last_cleanup_namespace_state.values())
            and not any("netem" in state for state in network.last_cleanup_qdisc_state.values()),
        }
        if network.owned_namespaces:
            failures.append(f"owned namespaces remain after cleanup: {sorted(network.owned_namespaces)}")
        report["command_log"] = [asdict(record) for record in network.runner.records]
        report["provenance"] = provenance()
        report["duration_seconds"] = time.monotonic() - started
        memory_passed = bool(report.get("memory")) and all(
            item.get("bounded") for item in report.get("memory", {}).values()
        )
        sequence_cell = "N/A — sequence loss is required in the dedicated slow-consumer scenario"
        report["passed"] = not failures
        report["outcome"] = "PASS" if report["passed"] else "FAIL"
        report["acceptance_matrix"] = {
            "Scenario": scenario.name,
            "Control": _matrix_cell(bool(report.get("control", {}).get("passed")), "control health requirement failed"),
            "Memory": _matrix_cell(memory_passed, "one or more process trends were not bounded"),
            "Queue bounds": _matrix_cell(bool(report.get("queues", {}).get("passed")), "queue/HWM proof failed"),
            "CV age recovery": _matrix_cell(
                bool(report.get("cv_freshness", {}).get("passed")), "freshness recovery requirement failed"
            ),
            "Video recovery": _matrix_cell(
                bool(report.get("video", {}).get("passed")), "video/correlation recovery failed"
            ),
            "Sequence gaps": sequence_cell,
            "Broker/router": _matrix_cell(
                bool(report.get("broker_router", {}).get("passed")), "functional survival failed"
            ),
            "Overall": "PASS" if report["passed"] else "FAIL — one or more mandatory assertions failed",
        }
        report["artifacts"] = {
            "report": str(report_path),
            "event_directory": str(directory),
            "logs": {} if topology is None else {role: str(path) for role, path in topology.log_paths.items()},
        }
        write_report(report_path, report)
    assert not failures, f"{'; '.join(failures)}; artifact={report_path}"


@pytest.mark.timeout(60)
def test_slow_consumer_30hz_to_1hz_is_lossy_current_bounded_and_control_responsive() -> None:
    directory, report_path = _artifact_path("slow-consumer-30hz-to-1hz")
    unavailable = _environment_unavailable_reason(require_video=False)
    if unavailable is not None:
        _skip_with_artifact(
            "slow-consumer-30hz-to-1hz",
            unavailable,
            directory,
            report_path,
            include_video=False,
        )
    failures: list[str] = []
    report: dict[str, Any] = {
        "scenario": "slow-consumer-30hz-to-1hz",
        "outcome": "FAIL",
        "failures": failures,
    }
    network = NetemNamespaceHarness()
    topology: Topology | None = None
    memory_samples: dict[str, list[MemorySample]] = {}
    started = time.monotonic()
    try:
        network.setup()
        report["network_architecture"] = {
            "namespace_a": network.namespace_a,
            "namespace_b": network.namespace_b,
            "interface_a": network.interface_a,
            "interface_b": network.interface_b,
            "routes": network.route_state(),
            "structured_path": "production ResultPublisher -> production XSUB/XPUB broker -> real SUB",
            "control_path": "production ControlClient DEALER -> production ROUTER -> real target DEALER",
        }
        topology = Topology(network, directory, consumer_rate_hz=1.0, payload_bytes=100_000)
        topology.start(include_video=False)
        topology.wait_for_baseline(include_video=False)
        baseline_results = topology.events("subscriber", "result")
        baseline_result = baseline_results[-1]
        measurement_start = time.monotonic()
        _sample_until(topology, memory_samples, started=started, deadline=measurement_start + 20.0)
        measurement_end = time.monotonic()
        results = topology.events("subscriber", "result")
        measurement_results = [event for event in results if event["received_monotonic"] >= measurement_start]
        gaps = measurement_results[-1]["observed_sequence_gaps"] if measurement_results else 0
        sequences = [event["sequence_number"] for event in measurement_results]
        actual_jumps = [right - left for left, right in zip(sequences, sequences[1:], strict=False) if right > left + 1]
        last_age = measurement_results[-1]["age_ms"] if measurement_results else None
        current_tail = measurement_results[-3:]
        gap_events = [
            event
            for event in topology.events("subscriber", "sequence_gap")
            if event["monotonic"] > baseline_result["received_monotonic"]
            and measurement_results
            and event["monotonic"] <= measurement_results[-1]["received_monotonic"]
        ]
        observed_gap_delta = gaps - baseline_result["observed_sequence_gaps"]
        evidenced_missing = sum(int(event["missing_sequence_numbers"]) for event in gap_events)
        publisher_samples = [
            event
            for event in topology.events("publisher", "publisher_sample")
            if measurement_start <= event["monotonic"] <= measurement_end
        ]
        actual_publisher_rate = (
            (publisher_samples[-1]["frame_number"] - publisher_samples[0]["frame_number"])
            / (publisher_samples[-1]["monotonic"] - publisher_samples[0]["monotonic"])
            if len(publisher_samples) >= 2
            else 0.0
        )
        actual_consumer_rate = len(measurement_results) / max(measurement_end - measurement_start, 1e-9)
        current_data_passed = len(current_tail) == 3 and all(event["age_ms"] < 250 for event in current_tail)
        report["slow_consumer"] = {
            "configured_publisher_rate_hz": 30,
            "actual_publisher_rate_hz": actual_publisher_rate,
            "configured_consumer_rate_hz": 1,
            "actual_consumer_rate_hz": actual_consumer_rate,
            "transport_receive_rate_hz": 120,
            "transport_pause_seconds_per_half_second": 0.3,
            "payload_bytes": 100_000,
            "duration_seconds": measurement_end - measurement_start,
            "published_message_capacity_multiple": 30 * (measurement_end - measurement_start) / 110,
            "publisher_session_id": measurement_results[-1]["publisher_session_id"] if measurement_results else None,
            "application_processed_sequences": sequences,
            "application_sequence_jumps": actual_jumps,
            "transport_gap_evidence": gap_events,
            "transport_evidenced_missing_sequences": evidenced_missing,
            "observed_sequence_gap_delta": observed_gap_delta,
            "observed_sequence_gaps": gaps,
            "final_result_age_ms": last_age,
            "current_data_tail": current_tail,
            "current_data_passed": current_data_passed,
        }
        if not gap_events or observed_gap_delta <= 0 or evidenced_missing != observed_gap_delta:
            failures.append(
                "validated transport receive gaps did not reconcile with the canonical missing-sequence counter"
            )
        if not current_data_passed:
            failures.append(f"slow consumer did not sustain current data at the tail (last age={last_age})")
        if not 27.0 <= actual_publisher_rate <= 33.0:
            failures.append(f"publisher did not sustain 30 Hz (observed {actual_publisher_rate:.3f} Hz)")
        if not 0.8 <= actual_consumer_rate <= 1.2:
            failures.append(f"consumer did not sustain 1 Hz (observed {actual_consumer_rate:.3f} Hz)")

        control_attempts = [
            event
            for event in topology.events("control", "control")
            if event["started_monotonic"] >= measurement_start and event["ended_monotonic"] <= measurement_end
        ]
        valid_controls = [
            event
            for event in control_attempts
            if event["structurally_valid"]
            and event["correlated"]
            and event["status"] == control_pb2.COMMAND_STATUS_COMPLETED
            and event["latency_ms"] < 500
        ]
        stats = latency_statistics([event["latency_ms"] for event in control_attempts]) if control_attempts else None
        report["control"] = {
            "latency": stats,
            "attempt_count": len(control_attempts),
            "valid_completed_under_500ms_count": len(valid_controls),
            "failed_attempts": [event for event in control_attempts if event not in valid_controls],
            "passed": bool(control_attempts) and len(valid_controls) == len(control_attempts),
        }
        if not control_attempts:
            failures.append("no completed control acknowledgements were observed during backpressure")
        elif len(valid_controls) != len(control_attempts):
            failures.append("at least one control attempt was invalid, uncorrelated, incomplete, or not below 500 ms")

        all_publisher_samples = topology.events("publisher", "publisher_sample")
        maximum_queue = max(event["queue_maximum"] for event in all_publisher_samples)
        hwm = _hwm_report(topology, include_video=False, failures=failures)
        report["queues"] = {
            "publisher_cv_result_queue": {"configured": 4, "observed_maximum": maximum_queue},
            "effective_socket_configuration": hwm,
            "canonical_cross_phase_capacities": {
                "frame_input": 1,
                "cv_result": 4,
                "priority": 32,
                "control_command": 16,
                "control_result": 16,
                "owner_regression": "tests/unit/test_runtime_queues.py",
            },
            "passed": maximum_queue <= 4 and hwm["passed"],
        }
        if maximum_queue > 4:
            failures.append("publisher CvResultQueue exceeded its capacity")
        failures.extend(topology.fatal_errors())
        report["memory"] = _memory_report(memory_samples, failures)
        for required in ("publisher", "broker"):
            if not report["memory"].get(required, {}).get("bounded"):
                failures.append(f"{required} memory boundedness requirement failed")
        report["process_survival"] = {role: process.poll() is None for role, process in topology.processes.items()}
        if not all(report["process_survival"].values()):
            failures.append("one or more production processes exited during slow-consumer pressure")
        broker_router_passed = bool(
            report["process_survival"].get("broker")
            and report["process_survival"].get("router")
            and measurement_results
            and valid_controls
        )
        report["broker_router"] = {
            "broker_pid_alive": report["process_survival"].get("broker", False),
            "broker_functional_structured_delivery": bool(measurement_results),
            "router_pid_alive": report["process_survival"].get("router", False),
            "router_functional_control_delivery": bool(valid_controls),
            "passed": broker_router_passed,
        }
        if not broker_router_passed:
            failures.append("broker/router did not remain alive and functional during slow-consumer pressure")
    except BaseException as error:
        failures.append(f"harness failure: {type(error).__name__}: {error}")
    finally:
        if topology is not None:
            report.setdefault("processes", topology.process_evidence())
            topology.stop()
            report["shutdown"] = topology.shutdown
            if any(not item["within_five_seconds"] for item in topology.shutdown.values()):
                failures.append("one or more processes exceeded the five-second shutdown bound")
        try:
            network.cleanup()
        except BaseException as error:
            failures.append(f"network cleanup failed: {error}")
        report["network_cleanup"] = {
            "qdisc_after_cleanup": network.last_cleanup_qdisc_state,
            "owned_namespaces_present_after_cleanup": network.last_cleanup_namespace_state,
            "passed": bool(network.last_cleanup_namespace_state)
            and not any(network.last_cleanup_namespace_state.values())
            and not any("netem" in state for state in network.last_cleanup_qdisc_state.values()),
        }
        if network.owned_namespaces:
            failures.append(f"owned namespaces remain after cleanup: {sorted(network.owned_namespaces)}")
        report["command_log"] = [asdict(record) for record in network.runner.records]
        report["provenance"] = provenance()
        report["duration_seconds"] = time.monotonic() - started
        memory_passed = bool(report.get("memory")) and all(
            item.get("bounded") for item in report.get("memory", {}).values()
        )
        report["passed"] = not failures
        report["outcome"] = "PASS" if report["passed"] else "FAIL"
        report["acceptance_matrix"] = {
            "Scenario": "slow-consumer-30hz-to-1hz",
            "Control": _matrix_cell(bool(report.get("control", {}).get("passed")), "control acceptance failed"),
            "Memory": _matrix_cell(memory_passed, "one or more process trends were not bounded"),
            "Queue bounds": _matrix_cell(bool(report.get("queues", {}).get("passed")), "queue/HWM proof failed"),
            "CV age recovery": _matrix_cell(
                bool(report.get("slow_consumer", {}).get("current_data_passed")), "current-data evidence failed"
            ),
            "Video recovery": "N/A — video is unrelated to structured-consumer pressure",
            "Sequence gaps": _matrix_cell(
                bool(report.get("slow_consumer", {}).get("transport_gap_evidence")), "no real transport gap evidence"
            ),
            "Broker/router": _matrix_cell(
                bool(report.get("broker_router", {}).get("passed")), "functional survival failed"
            ),
            "Overall": "PASS" if report["passed"] else "FAIL — one or more mandatory assertions failed",
        }
        write_report(report_path, report)
    assert not failures, f"{'; '.join(failures)}; artifact={report_path}"
