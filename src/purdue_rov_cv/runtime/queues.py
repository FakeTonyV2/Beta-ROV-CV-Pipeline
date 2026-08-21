"""Named bounded queues implementing service-level overflow contracts."""

from __future__ import annotations

import queue
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from typing import Generic, Protocol, TypeVar, cast

import numpy as np

from purdue_rov_cv.wire.errors import ErrorCode

from .exit_codes import EscalationRequest, ExitCode
from .metrics import RuntimeMetrics
from .rate_limit import WarningRateLimiter

T = TypeVar("T")
MAX_RECEIVE_TIMEOUT_SECONDS = 0.250


class ShutdownReadable(Protocol):
    @property
    def is_requested(self) -> bool: ...


class ReceiveStatus(StrEnum):
    ITEM = "item"
    TIMEOUT = "timeout"
    SHUTDOWN = "shutdown"


@dataclass(frozen=True)
class ReceiveResult(Generic[T]):
    status: ReceiveStatus
    item: T | None = None


class QueueOfferStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    DROPPED = "DROPPED"


@dataclass(frozen=True)
class CallbackFailure:
    action: str
    exception_type: str
    message: str


@dataclass(frozen=True)
class QueueOfferResult:
    status: QueueOfferStatus
    error_code: ErrorCode | None = None
    escalation: EscalationRequest | None = None
    callback_failures: tuple[CallbackFailure, ...] = ()

    @property
    def accepted(self) -> bool:
        return self.status is QueueOfferStatus.ACCEPTED


@dataclass(frozen=True)
class QueueEvent:
    level: str
    event_code: str
    message: str
    context: dict[str, object]


EventCallback = Callable[[QueueEvent], None]
DegradeCallback = Callable[[str], None]
EscalationCallback = Callable[[EscalationRequest], None]
CallbackArgument = TypeVar("CallbackArgument")


def _invoke_callback(
    action: str,
    callback: Callable[[CallbackArgument], None],
    argument: CallbackArgument,
) -> CallbackFailure | None:
    try:
        callback(argument)
    except Exception as error:
        return CallbackFailure(action, type(error).__name__, str(error))
    return None


class _RuntimeQueue(Generic[T]):
    def __init__(self, capacity: int, *, metrics: RuntimeMetrics | None = None) -> None:
        self._queue: queue.Queue[T] = queue.Queue(maxsize=capacity)
        self.metrics = metrics or RuntimeMetrics()

    @property
    def capacity(self) -> int:
        return self._queue.maxsize

    def qsize(self) -> int:
        return self._queue.qsize()

    def get_nowait(self) -> T:
        return self._queue.get_nowait()

    def receive(
        self,
        *,
        timeout_seconds: float = MAX_RECEIVE_TIMEOUT_SECONDS,
        shutdown: ShutdownReadable | None = None,
    ) -> ReceiveResult[T]:
        if not 0 <= timeout_seconds <= MAX_RECEIVE_TIMEOUT_SECONDS:
            raise ValueError("receive timeout must be between 0 and 250 ms")
        if shutdown is not None and shutdown.is_requested:
            return ReceiveResult(ReceiveStatus.SHUTDOWN)
        try:
            return ReceiveResult(ReceiveStatus.ITEM, self._queue.get(timeout=timeout_seconds))
        except queue.Empty:
            if shutdown is not None and shutdown.is_requested:
                return ReceiveResult(ReceiveStatus.SHUTDOWN)
            return ReceiveResult(ReceiveStatus.TIMEOUT)


class _DropOldestQueue(_RuntimeQueue[T]):
    def __init__(self, capacity: int, counter_name: str, *, metrics: RuntimeMetrics | None = None) -> None:
        super().__init__(capacity, metrics=metrics)
        self._counter_name = counter_name
        self._producer_lock = Lock()

    def _offer_latest(self, item: T) -> QueueOfferResult:
        with self._producer_lock:
            try:
                self._queue.put_nowait(item)
                return QueueOfferResult(QueueOfferStatus.ACCEPTED)
            except queue.Full:
                removed = False
                try:
                    self._queue.get_nowait()
                    removed = True
                except queue.Empty:
                    # A racing consumer already created capacity.
                    pass
                try:
                    self._queue.put_nowait(item)
                except queue.Full:
                    # A race outside this wrapper must not crash the producer.
                    return QueueOfferResult(QueueOfferStatus.DROPPED)
                if removed:
                    self.metrics.increment(self._counter_name)
                return QueueOfferResult(QueueOfferStatus.ACCEPTED)


def _copy_numpy_frame(frame: np.ndarray) -> np.ndarray:
    return np.array(frame, copy=True)


class FrameInputQueue(_DropOldestQueue[T]):
    """Capacity-one, nonblocking, private-copy, keep-latest frame queue."""

    def __init__(
        self,
        *,
        metrics: RuntimeMetrics | None = None,
        copy_item: Callable[[T], T] | None = None,
    ) -> None:
        super().__init__(1, "frames_dropped_before_processing", metrics=metrics)
        self._copy_item = copy_item

    def offer(self, frame: T) -> QueueOfferResult:
        if self._copy_item is not None:
            copied = self._copy_item(frame)
        elif isinstance(frame, np.ndarray):
            copied = cast(T, _copy_numpy_frame(frame))
        else:
            raise TypeError("non-NumPy frame queues require an explicit copy_item callback")
        return self._offer_latest(copied)

    def offer_owned(self, frame: T) -> QueueOfferResult:
        """Transfer an already-private item without an avoidable second copy.

        The producer must not mutate or reuse the item after this call. Generic
        callers should use :meth:`offer`, which retains copy-on-offer safety.
        """

        return self._offer_latest(frame)


class CvResultQueue(_DropOldestQueue[T]):
    """Capacity-four, nonblocking, drop-oldest completed-result queue."""

    def __init__(self, *, metrics: RuntimeMetrics | None = None) -> None:
        super().__init__(4, "results_dropped_local_queue", metrics=metrics)

    def offer(self, result: T) -> QueueOfferResult:
        return self._offer_latest(result)


class PriorityPublicationQueue(_RuntimeQueue[T]):
    """Capacity-32 queue with a rolling failure-window exit-75 escalation."""

    def __init__(
        self,
        *,
        event: EventCallback,
        degrade: DegradeCallback,
        escalate: EscalationCallback,
        metrics: RuntimeMetrics | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        put_impl: Callable[[T, float], None] | None = None,
    ) -> None:
        super().__init__(32, metrics=metrics)
        self._event = event
        self._degrade = degrade
        self._escalate = escalate
        self._monotonic = monotonic
        self._put_impl = put_impl or (lambda item, timeout: self._queue.put(item, timeout=timeout))
        self._failures: deque[float] = deque()
        self._failure_lock = Lock()

    def offer(self, item: T) -> QueueOfferResult:
        try:
            self._put_impl(item, 0.050)
            return QueueOfferResult(QueueOfferStatus.ACCEPTED)
        except queue.Full:
            now = self._monotonic()
            with self._failure_lock:
                self._failures.append(now)
                while self._failures and now - self._failures[0] > 10.0:
                    self._failures.popleft()
                escalate_now = len(self._failures) == 5
            self.metrics.increment("priority_messages_dropped")
            callback_failures: list[CallbackFailure] = []
            event_failure = _invoke_callback(
                "event",
                self._event,
                QueueEvent("ERROR", "PRIORITY_QUEUE_FULL", "priority publication queue remained full for 50 ms", {}),
            )
            if event_failure is not None:
                callback_failures.append(event_failure)
            degrade_failure = _invoke_callback("degrade", self._degrade, "PRIORITY_QUEUE_FULL")
            if degrade_failure is not None:
                callback_failures.append(degrade_failure)
            escalation = None
            if escalate_now:
                escalation = EscalationRequest(
                    ExitCode.TEMPORARY_FAILURE,
                    "five priority queue insertions failed within ten seconds",
                    "PRIORITY_QUEUE_FULL",
                )
                escalation_failure = _invoke_callback("escalate", self._escalate, escalation)
                if escalation_failure is not None:
                    callback_failures.append(escalation_failure)
            return QueueOfferResult(
                QueueOfferStatus.DROPPED,
                escalation=escalation,
                callback_failures=tuple(callback_failures),
            )


class ControlCommandQueue(_RuntimeQueue[T]):
    """Capacity-16 nonblocking command queue returning REJECTED/MODULE_BUSY."""

    def __init__(self, *, metrics: RuntimeMetrics | None = None) -> None:
        super().__init__(16, metrics=metrics)

    def offer(self, command: T) -> QueueOfferResult:
        try:
            self._queue.put_nowait(command)
            return QueueOfferResult(QueueOfferStatus.ACCEPTED)
        except queue.Full:
            return QueueOfferResult(QueueOfferStatus.REJECTED, ErrorCode.MODULE_BUSY)


class ControlResultQueue(_RuntimeQueue[T]):
    """Capacity-16 queue caching a timed-out result before requesting exit 75."""

    def __init__(
        self,
        *,
        cache_result: Callable[[T], None],
        event: EventCallback,
        escalate: EscalationCallback,
        metrics: RuntimeMetrics | None = None,
        put_impl: Callable[[T, float], None] | None = None,
    ) -> None:
        super().__init__(16, metrics=metrics)
        self._cache_result = cache_result
        self._event = event
        self._escalate = escalate
        self._put_impl = put_impl or (lambda item, timeout: self._queue.put(item, timeout=timeout))

    def offer(self, result: T) -> QueueOfferResult:
        try:
            self._put_impl(result, 0.100)
            return QueueOfferResult(QueueOfferStatus.ACCEPTED)
        except queue.Full:
            escalation = EscalationRequest(
                ExitCode.TEMPORARY_FAILURE,
                "control result could not be delivered to control thread",
                "CONTROL_RESULT_QUEUE_FULL",
            )
            callback_failures: list[CallbackFailure] = []
            event_failure = _invoke_callback(
                "event",
                self._event,
                QueueEvent("ERROR", "CONTROL_RESULT_QUEUE_FULL", "control result queue remained full for 100 ms", {}),
            )
            if event_failure is not None:
                callback_failures.append(event_failure)
            cache_failure = _invoke_callback("cache_result", self._cache_result, result)
            if cache_failure is not None:
                callback_failures.append(cache_failure)
            escalation_failure = _invoke_callback("escalate", self._escalate, escalation)
            if escalation_failure is not None:
                callback_failures.append(escalation_failure)
            return QueueOfferResult(
                QueueOfferStatus.DROPPED,
                escalation=escalation,
                callback_failures=tuple(callback_failures),
            )


class RecorderQueue(_RuntimeQueue[T]):
    """Capacity-4096 queue dropping newest input and rate-limiting critical logs."""

    def __init__(
        self,
        *,
        event: EventCallback,
        degrade: DegradeCallback,
        metrics: RuntimeMetrics | None = None,
        warning_limiter: WarningRateLimiter | None = None,
    ) -> None:
        super().__init__(4096, metrics=metrics)
        self._event = event
        self._degrade = degrade
        self._limiter = warning_limiter or WarningRateLimiter()

    def offer(self, record: T) -> QueueOfferResult:
        try:
            self._queue.put_nowait(record)
            return QueueOfferResult(QueueOfferStatus.ACCEPTED)
        except queue.Full:
            self.metrics.increment("recorder_queue_overflow")
            callback_failures: list[CallbackFailure] = []
            degrade_failure = _invoke_callback("degrade", self._degrade, "RECORDER_QUEUE_FULL")
            if degrade_failure is not None:
                callback_failures.append(degrade_failure)
            decision = self._limiter.check(("RECORDER_QUEUE_FULL", "recorder"))
            if decision.emit:
                event_failure = _invoke_callback(
                    "event",
                    self._event,
                    QueueEvent(
                        "CRITICAL",
                        "RECORDER_QUEUE_FULL",
                        "recorder queue is full; newest record dropped",
                        {"suppressed_count": decision.suppressed_count},
                    ),
                )
                if event_failure is not None:
                    callback_failures.append(event_failure)
            else:
                self.metrics.increment("warnings_suppressed")
            return QueueOfferResult(
                QueueOfferStatus.DROPPED,
                ErrorCode.RECORDER_QUEUE_FULL,
                callback_failures=tuple(callback_failures),
            )


__all__ = [
    "CallbackFailure",
    "ControlCommandQueue",
    "ControlResultQueue",
    "CvResultQueue",
    "FrameInputQueue",
    "MAX_RECEIVE_TIMEOUT_SECONDS",
    "PriorityPublicationQueue",
    "QueueEvent",
    "QueueOfferResult",
    "QueueOfferStatus",
    "ReceiveResult",
    "ReceiveStatus",
    "RecorderQueue",
]
