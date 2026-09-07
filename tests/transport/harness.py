"""Safe Linux network-namespace, measurement, process, and report helpers.

The privileged harness never accepts an arbitrary host interface.  It creates
two namespaces and a private veth pair, applies qdiscs only to that pair, and
deletes both namespaces during cleanup.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any
from uuid import uuid4

import psutil
import zmq

_PROCESS_CPU_BASELINES: dict[tuple[int, float], tuple[float, float]] = {}


@dataclass(frozen=True, slots=True)
class CommandRecord:
    argv: tuple[str, ...]
    started_monotonic: float
    duration_seconds: float
    returncode: int
    stdout: str
    stderr: str


class CommandFailure(RuntimeError):
    """A namespace or traffic-control command failed."""


class RecordingCommandRunner:
    def __init__(self) -> None:
        self.records: list[CommandRecord] = []

    def run(self, argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        started = time.monotonic()
        completed = subprocess.run(argv, check=False, capture_output=True, text=True)
        self.records.append(
            CommandRecord(
                tuple(argv),
                started,
                time.monotonic() - started,
                completed.returncode,
                completed.stdout,
                completed.stderr,
            )
        )
        if check and completed.returncode:
            raise CommandFailure(
                f"command failed ({completed.returncode}): {' '.join(argv)}\n"
                f"stdout: {completed.stdout}\nstderr: {completed.stderr}"
            )
        return completed


def has_required_netem_capabilities(status: str) -> bool:
    try:
        effective = next(line.split()[1] for line in status.splitlines() if line.startswith("CapEff:"))
        capability_bits = int(effective, 16)
    except (StopIteration, ValueError):
        return False
    # NET_ADMIN owns qdiscs; SYS_ADMIN is required by ip-netns mount/setns
    # operations. Container uid 0 without these capabilities is not enough.
    return bool(capability_bits & (1 << 12) and capability_bits & (1 << 21))


def netem_unavailable_reason() -> str | None:
    if sys.platform != "linux":
        return f"Linux is required (running {sys.platform})"
    for executable in ("ip", "tc"):
        if shutil.which(executable) is None:
            return f"{executable} from iproute2 is missing"
    try:
        status = Path("/proc/self/status").read_text(encoding="utf-8")
        if has_required_netem_capabilities(status):
            return None
    except OSError:
        pass
    return "CAP_NET_ADMIN and CAP_SYS_ADMIN are required for isolated network namespaces and tc netem"


class NetemNamespaceHarness:
    """Two private namespaces connected only by a harness-owned veth pair."""

    def __init__(self, runner: RecordingCommandRunner | None = None) -> None:
        token = uuid4().hex[:6]
        self.namespace_a = f"rov75a-{token}"
        self.namespace_b = f"rov75b-{token}"
        self.interface_a = f"r75a{token}"
        self.interface_b = f"r75b{token}"
        self.address_a = "10.203.75.1"
        self.address_b = "10.203.75.2"
        self.runner = runner or RecordingCommandRunner()
        self._setup = False
        self._owned_namespaces: set[str] = set()
        self.last_cleanup_qdisc_state: dict[str, str] = {}
        self.last_cleanup_namespace_state: dict[str, bool] = {}

    def __enter__(self) -> NetemNamespaceHarness:
        self.setup()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self.cleanup()

    def setup(self) -> None:
        if self._setup:
            raise RuntimeError("network namespace harness is already set up")
        reason = netem_unavailable_reason()
        if reason is not None:
            raise RuntimeError(reason)
        try:
            listed = self.runner.run(["ip", "netns", "list"])
            existing = {line.split()[0] for line in listed.stdout.splitlines() if line.split()}
            collisions = existing.intersection({self.namespace_a, self.namespace_b})
            if collisions:
                raise CommandFailure(f"refusing to reuse pre-existing network namespaces: {sorted(collisions)}")
            for namespace in (self.namespace_a, self.namespace_b):
                self.runner.run(["ip", "netns", "add", namespace])
                self._owned_namespaces.add(namespace)
            self.runner.run(["ip", "link", "add", self.interface_a, "type", "veth", "peer", "name", self.interface_b])
            self.runner.run(["ip", "link", "set", self.interface_a, "netns", self.namespace_a])
            self.runner.run(["ip", "link", "set", self.interface_b, "netns", self.namespace_b])
            for namespace, interface, address in self._ends():
                self.runner.run(["ip", "-n", namespace, "address", "add", f"{address}/30", "dev", interface])
                self.runner.run(["ip", "-n", namespace, "link", "set", "lo", "up"])
                self.runner.run(["ip", "-n", namespace, "link", "set", interface, "up"])
            self._setup = True
            self.runner.run(["ip", "netns", "exec", self.namespace_a, "ping", "-c", "1", "-W", "1", self.address_b])
        except BaseException:
            self.cleanup()
            raise

    def _ends(self) -> tuple[tuple[str, str, str], tuple[str, str, str]]:
        return (
            (self.namespace_a, self.interface_a, self.address_a),
            (self.namespace_b, self.interface_b, self.address_b),
        )

    @property
    def owned_namespaces(self) -> frozenset[str]:
        return frozenset(self._owned_namespaces)

    def exec_prefix(self, namespace: str) -> list[str]:
        if namespace not in {self.namespace_a, self.namespace_b}:
            raise ValueError("only a namespace owned by this harness may be used")
        return ["ip", "netns", "exec", namespace]

    def _replace_netem(self, arguments: list[str]) -> None:
        if not self._setup:
            raise RuntimeError("network namespace harness is not set up")
        for namespace, interface, _address in self._ends():
            self.runner.run(
                ["ip", "netns", "exec", namespace, "tc", "qdisc", "replace", "dev", interface, "root", "netem"]
                + arguments
            )
        states = self.qdisc_state()
        if not all("netem" in state for state in states.values()):
            raise CommandFailure(f"netem qdisc was not active on both private interfaces: {states}")

    def apply_loss(self, percent: float) -> None:
        if not 0 < percent <= 100:
            raise ValueError("loss percent must be in (0, 100]")
        self._replace_netem(["loss", f"{percent:g}%"])

    def apply_delay(self, milliseconds: float) -> None:
        if milliseconds <= 0:
            raise ValueError("delay must be positive")
        self._replace_netem(["delay", f"{milliseconds:g}ms"])

    def apply_jitter(self, milliseconds: float) -> None:
        if milliseconds <= 0:
            raise ValueError("jitter must be positive")
        # A 20 ms base delay plus a 20 ms distribution is actual jitter, not a
        # mislabeled fixed-delay test.
        self._replace_netem(["delay", f"{milliseconds:g}ms", f"{milliseconds:g}ms", "distribution", "normal"])

    def apply_rate_limit(self, mbit: float) -> None:
        if mbit <= 0:
            raise ValueError("rate limit must be positive")
        self._replace_netem(["rate", f"{mbit:g}mbit"])

    def interrupt_link(self) -> None:
        self._replace_netem(["loss", "100%"])

    def qdisc_state(self) -> dict[str, str]:
        states: dict[str, str] = {}
        for namespace, interface, _address in self._ends():
            result = self.runner.run(["ip", "netns", "exec", namespace, "tc", "qdisc", "show", "dev", interface])
            states[f"{namespace}/{interface}"] = result.stdout.strip()
        return states

    def qdisc_statistics(self) -> dict[str, str]:
        statistics_by_interface: dict[str, str] = {}
        for namespace, interface, _address in self._ends():
            result = self.runner.run(["ip", "netns", "exec", namespace, "tc", "-s", "qdisc", "show", "dev", interface])
            statistics_by_interface[f"{namespace}/{interface}"] = result.stdout.strip()
        return statistics_by_interface

    def interface_counters(self) -> dict[str, dict[str, int]]:
        counters: dict[str, dict[str, int]] = {}
        for namespace, interface, _address in self._ends():
            key = f"{namespace}/{interface}"
            counters[key] = {}
            for direction in ("rx", "tx"):
                path = f"/sys/class/net/{interface}/statistics/{direction}_bytes"
                result = self.runner.run(["ip", "netns", "exec", namespace, "cat", path])
                counters[key][f"{direction}_bytes"] = int(result.stdout.strip())
        return counters

    def route_state(self) -> dict[str, str]:
        routes: dict[str, str] = {}
        for namespace, interface, address, peer in (
            (self.namespace_a, self.interface_a, self.address_a, self.address_b),
            (self.namespace_b, self.interface_b, self.address_b, self.address_a),
        ):
            result = self.runner.run(["ip", "-n", namespace, "route", "get", peer])
            route = result.stdout.strip()
            if f"dev {interface}" not in route or f"src {address}" not in route:
                raise CommandFailure(f"peer traffic does not route over the harness veth: {route}")
            routes[f"{namespace}->{peer}"] = route
        return routes

    def clear_impairment(self) -> dict[str, str]:
        if not self._setup:
            return {}
        for namespace, interface, _address in self._ends():
            self.runner.run(
                ["ip", "netns", "exec", namespace, "tc", "qdisc", "del", "dev", interface, "root"],
                check=False,
            )
        states = self.qdisc_state()
        if any("netem" in state for state in states.values()):
            raise CommandFailure(f"netem cleanup failed: {states}")
        return states

    def cleanup(self) -> None:
        failures: list[str] = []
        if self._setup:
            try:
                self.last_cleanup_qdisc_state = self.clear_impairment()
            except BaseException as error:
                failures.append(f"qdisc cleanup: {type(error).__name__}: {error}")
        # Ownership is recorded immediately after each successful creation.
        # Partial setup can therefore never delete a pre-existing namespace.
        owned = tuple(self._owned_namespaces)
        for namespace in owned:
            result = self.runner.run(["ip", "netns", "delete", namespace], check=False)
            if result.returncode:
                failures.append(f"namespace delete failed ({namespace}): {result.stderr.strip()}")
        listed = self.runner.run(["ip", "netns", "list"], check=False)
        if listed.returncode:
            failures.append(f"namespace cleanup verification failed: {listed.stderr.strip()}")
            self.last_cleanup_namespace_state = dict.fromkeys(owned, True)
        else:
            remaining = {line.split()[0] for line in listed.stdout.splitlines() if line.split()}
            self.last_cleanup_namespace_state = {namespace: namespace in remaining for namespace in owned}
        for namespace, present in self.last_cleanup_namespace_state.items():
            if present:
                failures.append(f"owned namespace remained after cleanup: {namespace}")
            else:
                self._owned_namespaces.discard(namespace)
        self._setup = False
        if failures:
            raise CommandFailure("; ".join(failures))


@dataclass(frozen=True, slots=True)
class MemorySample:
    elapsed_seconds: float
    rss_bytes: int
    cpu_percent: float


@dataclass(frozen=True, slots=True)
class MemoryDecision:
    bounded: bool
    baseline_rss_bytes: int
    peak_rss_bytes: int
    final_rss_bytes: int
    late_slope_bytes_per_second: float
    late_span_bytes: int
    allowed_growth_bytes: int
    plateau_tolerance_bytes: int
    reason: str


def evaluate_memory_boundedness(samples: list[MemorySample]) -> MemoryDecision:
    """Reject sustained growth while tolerating allocator warm-up and plateaus."""
    if len(samples) < 8 or samples[-1].elapsed_seconds - samples[0].elapsed_seconds < 5:
        raise ValueError("memory evaluation requires at least eight samples spanning five seconds")
    baseline = samples[0].rss_bytes
    # Use at least the final four samples and normally the final quarter.  This
    # evaluates the post-pressure steady state instead of rejecting a bounded
    # transport-buffer peak that is demonstrably released during recovery.
    late = samples[-max(4, len(samples) // 4) :]
    # Theil-Sen's median pairwise slope is robust to allocator/sample-phase
    # spikes while preserving the exact slope of sustained linear growth.
    pairwise_slopes = [
        (right.rss_bytes - left.rss_bytes) / (right.elapsed_seconds - left.elapsed_seconds)
        for index, left in enumerate(late)
        for right in late[index + 1 :]
        if right.elapsed_seconds > left.elapsed_seconds
    ]
    slope = statistics.median(pairwise_slopes) if pairwise_slopes else 0.0
    late_span = max(sample.rss_bytes for sample in late) - min(sample.rss_bytes for sample in late)
    allowed_growth = max(8 * 1024 * 1024, baseline // 4)
    plateau_tolerance = max(2 * 1024 * 1024, baseline // 20)
    # A temporary transport buffer peak is not a leak when it is released.
    # Bound retained growth and the late trend; retain the peak as evidence.
    growth_ok = samples[-1].rss_bytes - baseline <= allowed_growth
    trend_ok = slope <= 64 * 1024 and late_span <= plateau_tolerance
    bounded = growth_ok and trend_ok
    reason = (
        "growth stayed within the warm-up allowance and the late window plateaued"
        if bounded
        else "RSS exceeded the warm-up allowance or retained a continuous positive late-window trend"
    )
    return MemoryDecision(
        bounded,
        baseline,
        max(sample.rss_bytes for sample in samples),
        samples[-1].rss_bytes,
        slope,
        late_span,
        allowed_growth,
        plateau_tolerance,
        reason,
    )


def latency_statistics(samples_ms: list[float]) -> dict[str, float | int]:
    if not samples_ms:
        raise ValueError("at least one latency sample is required")
    ordered = sorted(samples_ms)
    p95_index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return {
        "sample_count": len(ordered),
        "minimum_ms": ordered[0],
        "maximum_ms": ordered[-1],
        "mean_ms": statistics.fmean(ordered),
        "p95_ms": ordered[p95_index],
    }


def first_fresh_recovery(
    samples: list[dict[str, Any]],
    *,
    impairment_removed_monotonic: float,
    maximum_age_ms: float = 250.0,
) -> tuple[float, list[dict[str, Any]]]:
    post_recovery = [sample for sample in samples if sample["received_monotonic"] >= impairment_removed_monotonic]
    first_index = next(
        (index for index, sample in enumerate(post_recovery) if sample["age_ms"] < maximum_age_ms),
        None,
    )
    if first_index is None:
        raise ValueError("no fresh post-recovery result was observed")
    first = post_recovery[first_index]
    return first["received_monotonic"] - impairment_removed_monotonic, post_recovery[first_index:]


@cache
def provenance() -> dict[str, Any]:
    git_prefix = ["git", "-c", f"safe.directory={Path.cwd().resolve()}"]
    git = subprocess.run(git_prefix + ["rev-parse", "HEAD"], check=False, capture_output=True, text=True)
    dirty = subprocess.run(git_prefix + ["status", "--porcelain"], check=False, capture_output=True, text=True)
    tracked_diff = subprocess.run(git_prefix + ["diff", "--binary", "HEAD"], check=False, capture_output=True)
    untracked = subprocess.run(
        git_prefix + ["ls-files", "--others", "--exclude-standard", "-z"],
        check=False,
        capture_output=True,
    )
    fingerprint = hashlib.sha256()
    fingerprint.update(tracked_diff.stdout)
    untracked_paths: list[str] = []
    for encoded_path in filter(None, untracked.stdout.split(b"\0")):
        path = Path(os.fsdecode(encoded_path))
        untracked_paths.append(path.as_posix())
        fingerprint.update(b"\0untracked\0")
        fingerprint.update(encoded_path)
        try:
            fingerprint.update(path.read_bytes())
        except OSError as error:
            fingerprint.update(f"unreadable:{type(error).__name__}:{error}".encode())
    tc = subprocess.run(["tc", "-V"], check=False, capture_output=True, text=True) if shutil.which("tc") else None
    gst = (
        subprocess.run(["gst-launch-1.0", "--version"], check=False, capture_output=True, text=True)
        if shutil.which("gst-launch-1.0")
        else None
    )
    return {
        "git_commit": git.stdout.strip() or "unknown",
        "git_error": git.stderr.strip() or None,
        "working_tree_dirty": bool(dirty.stdout.strip()),
        "git_status_porcelain": dirty.stdout.splitlines(),
        "untracked_implementation_files": untracked_paths,
        "worktree_fingerprint_sha256": fingerprint.hexdigest(),
        "python": platform.python_version(),
        "pyzmq": zmq.pyzmq_version(),
        "libzmq": zmq.zmq_version(),
        "kernel": platform.release(),
        "distribution": platform.freedesktop_os_release().get("PRETTY_NAME", "unknown"),
        "tc_iproute2": None if tc is None else (tc.stdout or tc.stderr).strip(),
        "gstreamer": (
            gst.stdout.splitlines()[0] if gst is not None and gst.returncode == 0 and gst.stdout else "unavailable"
        ),
    }


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(report)
    payload.setdefault("generated_at_utc", datetime.now(UTC).isoformat())
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=asdict) + "\n", encoding="utf-8")
    failures = payload.get("failures") or []
    outcome = payload.get("outcome")
    if outcome is None:
        outcome = "PASS" if payload.get("passed") else "FAIL"
        payload["outcome"] = outcome
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=asdict) + "\n", encoding="utf-8")
    if outcome not in {"PASS", "FAIL", "UNVERIFIED"}:
        raise ValueError(f"invalid transport outcome: {outcome}")
    lines = [
        f"# Transport scenario: {payload.get('scenario', 'unknown')}",
        "",
        f"- Result: {outcome}",
        f"- Duration: {payload.get('duration_seconds', 'not recorded')} seconds",
        f"- Generated: {payload['generated_at_utc']}",
        f"- Failures: {', '.join(str(item) for item in failures) if failures else 'none'}",
        "",
    ]
    for section in (
        "acceptance_matrix",
        "control",
        "cv_freshness",
        "video",
        "slow_consumer",
        "memory",
        "queues",
        "shutdown",
    ):
        if section not in payload:
            continue
        lines.extend(
            [
                f"## {section.replace('_', ' ').title()}",
                "",
                "```json",
                json.dumps(payload[section], indent=2, sort_keys=True, default=asdict),
                "```",
                "",
            ]
        )
    path.with_suffix(".md").write_text("\n".join(lines), encoding="utf-8")


def sample_processes(processes: dict[str, subprocess.Popen[str]], started: float) -> dict[str, MemorySample]:
    samples: dict[str, MemorySample] = {}
    for name, process in processes.items():
        if process.poll() is not None:
            continue
        try:
            observed = psutil.Process(process.pid)
            sampled_at = time.monotonic()
            cpu_times = observed.cpu_times()
            cpu_total = cpu_times.user + cpu_times.system
            cpu_key = (process.pid, observed.create_time())
            previous = _PROCESS_CPU_BASELINES.get(cpu_key)
            cpu_percent = (
                0.0
                if previous is None or sampled_at <= previous[0]
                else max(0.0, (cpu_total - previous[1]) / (sampled_at - previous[0]) * 100.0)
            )
            _PROCESS_CPU_BASELINES[cpu_key] = (sampled_at, cpu_total)
            samples[name] = MemorySample(
                sampled_at - started,
                observed.memory_info().rss,
                cpu_percent,
            )
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            continue
    return samples


__all__ = [
    "CommandFailure",
    "MemoryDecision",
    "MemorySample",
    "NetemNamespaceHarness",
    "RecordingCommandRunner",
    "evaluate_memory_boundedness",
    "first_fresh_recovery",
    "has_required_netem_capabilities",
    "latency_statistics",
    "netem_unavailable_reason",
    "provenance",
    "sample_processes",
    "write_report",
]
