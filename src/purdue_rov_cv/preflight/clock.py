"""Chrony-compatible clock status and deterministic validity tracking."""

from __future__ import annotations

import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from purdue_rov_cv.runtime.shutdown import ShutdownToken


@dataclass(frozen=True, slots=True)
class ClockSample:
    """One synchronization observation expressed on a monotonic timeline."""

    source_reachable: bool
    leap_normal: bool
    estimated_offset_ms: float
    checked_monotonic: float
    detail: str = ""

    def passes(self, *, maximum_offset_ms: float = 10.0) -> bool:
        return self.source_reachable and self.leap_normal and abs(self.estimated_offset_ms) < maximum_offset_ms


class ClockProbe(Protocol):
    def check(self) -> ClockSample: ...


class ChronyClockProbe:
    """Production probe backed by ``chronyc tracking`` and ``sources``."""

    _OFFSET = re.compile(r"Last offset\s*:\s*([+-]?[0-9.eE+-]+)\s+seconds", re.I)
    _LEAP = re.compile(r"Leap status\s*:\s*(.+)", re.I)

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self._monotonic = monotonic
        self._run = run

    def check(self) -> ClockSample:
        checked = self._monotonic()
        try:
            tracking = self._run(
                ["chronyc", "-n", "tracking"],
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            sources = self._run(
                ["chronyc", "-n", "sources"],
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as error:
            return ClockSample(False, False, float("inf"), checked, f"chrony query failed: {error}")
        offset_match = self._OFFSET.search(tracking.stdout)
        leap_match = self._LEAP.search(tracking.stdout)
        reachable = sources.returncode == 0 and any(
            line.lstrip().startswith(("^*", "^+", "#*", "#+")) for line in sources.stdout.splitlines()
        )
        leap_normal = bool(leap_match and leap_match.group(1).strip().lower() == "normal")
        offset_ms = float(offset_match.group(1)) * 1_000.0 if offset_match else float("inf")
        detail = "" if tracking.returncode == 0 else tracking.stderr.strip() or "chronyc tracking failed"
        return ClockSample(reachable, leap_normal, offset_ms, checked, detail)


class SimulatedClockProbe:
    """Explicit injectable probe used by preflight and fault tests."""

    def __init__(self, samples: ClockSample | list[ClockSample]) -> None:
        self._samples = list(samples) if isinstance(samples, list) else [samples]
        if not self._samples:
            raise ValueError("at least one clock sample is required")
        self._index = 0

    def check(self) -> ClockSample:
        sample = self._samples[min(self._index, len(self._samples) - 1)]
        self._index += 1
        return sample


@dataclass(frozen=True, slots=True)
class ClockStatus:
    synchronized: bool
    cross_device_latency_valid: bool
    consecutive_failures: int
    last_success_monotonic: float | None
    sample: ClockSample | None
    reason: str


class ClockMonitor:
    """Apply §24 thresholds while keeping duration decisions monotonic."""

    def __init__(
        self,
        *,
        maximum_offset_ms: float = 10.0,
        freshness_seconds: float = 15.0,
        invalidate_after_failures: int = 3,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if maximum_offset_ms <= 0 or freshness_seconds <= 0 or invalidate_after_failures <= 0:
            raise ValueError("clock monitor thresholds must be positive")
        self.maximum_offset_ms = maximum_offset_ms
        self.freshness_seconds = freshness_seconds
        self.invalidate_after_failures = invalidate_after_failures
        self._monotonic = monotonic
        self._last_sample: ClockSample | None = None
        self._last_success: float | None = None
        self._failures = 0
        self._recovery_successes = 0
        self._invalidated = False

    def observe(self, sample: ClockSample) -> ClockStatus:
        self._last_sample = sample
        if sample.passes(maximum_offset_ms=self.maximum_offset_ms):
            self._last_success = sample.checked_monotonic
            self._failures = 0
            if self._invalidated:
                self._recovery_successes += 1
                if self._recovery_successes >= self.invalidate_after_failures:
                    self._invalidated = False
                    self._recovery_successes = 0
        else:
            self._failures += 1
            self._recovery_successes = 0
            if self._failures >= self.invalidate_after_failures:
                self._invalidated = True
        return self.status()

    def poll(self, probe: ClockProbe) -> ClockStatus:
        return self.observe(probe.check())

    def status(self, *, now: float | None = None) -> ClockStatus:
        current = self._monotonic() if now is None else now
        sample = self._last_sample
        sample_valid = bool(sample and sample.passes(maximum_offset_ms=self.maximum_offset_ms))
        success_age = None if self._last_success is None else current - self._last_success
        fresh = success_age is not None and 0.0 <= success_age <= self.freshness_seconds
        synchronized = sample_valid and fresh and not self._invalidated
        latency_valid = fresh and self._failures < self.invalidate_after_failures and not self._invalidated
        if sample is None:
            reason = "clock has not been checked"
        elif not sample.source_reachable:
            reason = "chrony has no reachable source"
        elif not sample.leap_normal:
            reason = "chrony leap status is not normal"
        elif abs(sample.estimated_offset_ms) >= self.maximum_offset_ms:
            reason = (
                f"absolute clock offset {abs(sample.estimated_offset_ms):.3f} ms is not below "
                f"{self.maximum_offset_ms:.3f} ms"
            )
        elif not fresh:
            reason = f"last successful clock check is older than {self.freshness_seconds:.0f} seconds"
        elif self._invalidated:
            reason = (
                "clock recovery requires three consecutive valid checks "
                f"({self._recovery_successes}/{self.invalidate_after_failures})"
            )
        else:
            reason = ""
        return ClockStatus(synchronized, latency_valid, self._failures, self._last_success, sample, reason)


class ClockStatusService:
    """Run the injected clock probe on the configured five-second cadence."""

    def __init__(
        self,
        probe: ClockProbe,
        monitor: ClockMonitor,
        *,
        interval_seconds: float = 5.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("clock check interval must be positive")
        self.probe = probe
        self.monitor = monitor
        self.interval_seconds = interval_seconds
        self._monotonic = monotonic
        self._next_check = 0.0

    def step(self) -> ClockStatus:
        now = self._monotonic()
        if now >= self._next_check:
            status = self.monitor.poll(self.probe)
            self._next_check = now + self.interval_seconds
            return status
        return self.monitor.status(now=now)

    def run(self, shutdown: ShutdownToken) -> None:
        while not shutdown.is_requested:
            self.step()
            remaining = max(0.0, self._next_check - self._monotonic())
            shutdown.wait(min(remaining, 0.250))


__all__ = [
    "ChronyClockProbe",
    "ClockMonitor",
    "ClockProbe",
    "ClockSample",
    "ClockStatus",
    "ClockStatusService",
    "SimulatedClockProbe",
]
