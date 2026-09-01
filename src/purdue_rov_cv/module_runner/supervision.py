"""Deterministic processing deadline, exception, and progress supervision."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock

from purdue_rov_cv.runtime.exit_codes import EscalationRequest, ExitCode
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from purdue_rov_cv.wire.errors import ErrorCode


@dataclass(frozen=True)
class ProcessingOutcome:
    deadline_miss_streak: int
    exception_streak: int
    escalation: EscalationRequest | None = None


class ProcessingSupervisor:
    """Own consecutive processing counters while metrics retain lifetime totals."""

    def __init__(
        self,
        deadline_ms: int,
        *,
        metrics: RuntimeMetrics,
        state_machine: ComponentStateMachine,
        escalate: Callable[[EscalationRequest], None],
    ) -> None:
        if deadline_ms <= 0:
            raise ValueError("deadline_ms must be positive")
        self.deadline_ms = deadline_ms
        self.metrics = metrics
        self.state_machine = state_machine
        self._escalate = escalate
        self._deadline_streak = 0
        self._exception_streak = 0
        self._deadline_degraded = False
        self._exception_degraded = False
        self._external_degraded = False
        self._lock = Lock()

    @property
    def deadline_miss_streak(self) -> int:
        with self._lock:
            return self._deadline_streak

    @property
    def exception_streak(self) -> int:
        with self._lock:
            return self._exception_streak

    @property
    def requires_restart(self) -> bool:
        with self._lock:
            return self._external_degraded

    def record_success(self, duration_ns: int) -> ProcessingOutcome:
        if duration_ns < 0:
            raise ValueError("duration_ns must be non-negative")
        duration_ms = duration_ns / 1_000_000.0
        self.metrics.increment("frames_processed")
        self.metrics.observe_processing_ms(duration_ms)
        escalation = None
        with self._lock:
            self._exception_streak = 0
            self._exception_degraded = False
            if duration_ms > self.deadline_ms:
                self._deadline_streak += 1
                self.metrics.increment("processing_deadline_misses")
                if self._deadline_streak == 5:
                    if self.state_machine.state is ComponentState.RUNNING:
                        self.state_machine.transition_to(ComponentState.DEGRADED)
                    if self.state_machine.state is ComponentState.DEGRADED:
                        self._deadline_degraded = True
                if self._deadline_streak == 20:
                    escalation = EscalationRequest(
                        ExitCode.TEMPORARY_FAILURE,
                        "twenty consecutive processing deadlines were missed",
                        ErrorCode.PROCESSING_WATCHDOG_EXCEEDED.value,
                    )
            else:
                self._deadline_streak = 0
                self._deadline_degraded = False
            if (
                self.state_machine.state is ComponentState.DEGRADED
                and not self._deadline_degraded
                and not self._exception_degraded
                and not self._external_degraded
            ):
                self.state_machine.transition_to(ComponentState.RUNNING)
            deadline_streak = self._deadline_streak
            exception_streak = self._exception_streak
        if escalation is not None:
            self._escalate(escalation)
        return ProcessingOutcome(deadline_streak, exception_streak, escalation)

    def record_exception(self) -> ProcessingOutcome:
        self.metrics.increment("processing_exceptions")
        with self._lock:
            self._deadline_streak = 0
            self._deadline_degraded = False
            self._exception_streak += 1
            if self._exception_streak == 1:
                if self.state_machine.state is ComponentState.RUNNING:
                    self.state_machine.transition_to(ComponentState.DEGRADED)
                if self.state_machine.state is ComponentState.DEGRADED:
                    self._exception_degraded = True
            if self._exception_streak == 3 and self.state_machine.state in {
                ComponentState.RUNNING,
                ComponentState.DEGRADED,
            }:
                self.state_machine.transition_to(ComponentState.ERROR)
                self._exception_degraded = False
            return ProcessingOutcome(self._deadline_streak, self._exception_streak)

    def record_external_degradation(self) -> None:
        """Prevent processing success from clearing a degradation owned elsewhere."""

        with self._lock:
            self._external_degraded = True
            if self.state_machine.state is ComponentState.RUNNING:
                self.state_machine.transition_to(ComponentState.DEGRADED)

    def reset(self) -> None:
        with self._lock:
            self._deadline_streak = 0
            self._exception_streak = 0
            self._deadline_degraded = False
            self._exception_degraded = False
            self._external_degraded = False


class WorkerWatchdog:
    """Track progress written only by the worker and detect a blocked cycle."""

    def __init__(
        self,
        processing_deadline_ms: int,
        *,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        minimum_seconds: float = 10.0,
    ) -> None:
        if processing_deadline_ms <= 0 or minimum_seconds <= 0:
            raise ValueError("watchdog settings must be positive")
        self._clock = monotonic_ns
        self.threshold_ns = int(max(minimum_seconds, 5 * processing_deadline_ms / 1_000.0) * 1_000_000_000)
        self._last_progress_ns = monotonic_ns()
        self._lock = Lock()

    @property
    def last_worker_progress_monotonic_ns(self) -> int:
        with self._lock:
            return self._last_progress_ns

    def progress(self) -> int:
        now = self._clock()
        with self._lock:
            self._last_progress_ns = now
        return now

    def exceeded(self) -> bool:
        now = self._clock()
        with self._lock:
            return now - self._last_progress_ns > self.threshold_ns

    def escalation(self) -> EscalationRequest:
        return EscalationRequest(
            ExitCode.TEMPORARY_FAILURE,
            "module worker made no progress before its watchdog threshold",
            ErrorCode.PROCESSING_WATCHDOG_EXCEEDED.value,
        )


__all__ = ["ProcessingOutcome", "ProcessingSupervisor", "WorkerWatchdog"]
