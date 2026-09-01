"""Every queue capacity, overflow, timeout, metric, state, log, and escalation path."""

from __future__ import annotations

import queue

import numpy as np
import pytest

from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.queues import (
    ControlCommandQueue,
    ControlResultQueue,
    CvResultQueue,
    FrameInputQueue,
    PriorityPublicationQueue,
    QueueOfferStatus,
    ReceiveStatus,
    RecorderQueue,
)
from purdue_rov_cv.runtime.rate_limit import WarningRateLimiter
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from purdue_rov_cv.wire.errors import ErrorCode


class FakeClock:
    def __init__(self, value: float = 0.0):
        self.value = value

    def __call__(self) -> float:
        return self.value


def ignore_event(event):
    del event


def ignore_reason(reason):
    del reason


def ignore_escalation(escalation):
    del escalation


def _priority_queue(**kwargs):
    return PriorityPublicationQueue(
        event=kwargs.pop("event", ignore_event),
        degrade=kwargs.pop("degrade", ignore_reason),
        escalate=kwargs.pop("escalate", ignore_escalation),
        **kwargs,
    )


def test_frame_input_queue_keeps_private_copy_of_newest_and_counts_replacement():
    metrics = RuntimeMetrics()
    frames = FrameInputQueue(metrics=metrics)
    assert frames.capacity == 1
    original = np.array([1, 2, 3])
    assert frames.offer(original).accepted
    assert metrics.snapshot().values["frames_dropped_before_processing"] == 0
    original[0] = 99
    retained_copy = frames.get_nowait()
    assert retained_copy.tolist() == [1, 2, 3]

    assert frames.offer(np.array([1, 2, 3])).accepted
    assert frames.offer(np.array([4, 5, 6])).accepted
    retained = frames.get_nowait()
    assert retained.tolist() == [4, 5, 6]
    assert metrics.snapshot().values["frames_dropped_before_processing"] == 1


def test_frame_input_queue_can_take_ownership_of_an_already_private_frame():
    frames = FrameInputQueue()
    owned = np.array([1, 2, 3])

    assert frames.offer_owned(owned).accepted

    assert frames.get_nowait() is owned


def test_drop_oldest_race_is_safe_when_consumer_removes_during_replacement(monkeypatch):
    frames = FrameInputQueue()
    frames.offer(np.array([1]))
    original_get = frames._queue.get_nowait

    def racing_empty():
        frames._queue.queue.clear()
        raise queue.Empty

    monkeypatch.setattr(frames._queue, "get_nowait", racing_empty)
    assert frames.offer(np.array([2])).accepted
    assert original_get().tolist() == [2]


def test_cv_result_queue_capacity_ordering_and_exact_counter():
    metrics = RuntimeMetrics()
    results = CvResultQueue[int](metrics=metrics)
    assert results.capacity == 4
    for item in range(5):
        assert results.offer(item).accepted
    assert [results.get_nowait() for _ in range(4)] == [1, 2, 3, 4]
    assert metrics.snapshot().values["results_dropped_local_queue"] == 1


def test_priority_queue_timeout_failure_window_state_event_metric_and_exit75():
    clock = FakeClock()
    timeouts: list[float] = []
    events = []
    escalations = []
    metrics = RuntimeMetrics()
    state = ComponentStateMachine(ComponentState.RUNNING)

    def always_full(item, timeout):
        del item
        timeouts.append(timeout)
        raise queue.Full

    priority = _priority_queue(
        metrics=metrics,
        event=events.append,
        degrade=lambda reason: state.transition_to(ComponentState.DEGRADED),
        escalate=escalations.append,
        monotonic=clock,
        put_impl=always_full,
    )
    assert priority.capacity == 32
    for attempt in range(4):
        assert priority.offer(attempt).escalation is None
    fifth = priority.offer(4)
    assert timeouts == [0.050] * 5
    assert fifth.escalation and fifth.escalation.exit_code is ExitCode.TEMPORARY_FAILURE
    assert escalations == [fifth.escalation]
    assert state.state is ComponentState.DEGRADED
    assert len(events) == 5 and {event.event_code for event in events} == {"PRIORITY_QUEUE_FULL"}
    assert metrics.snapshot().values["priority_messages_dropped"] == 5


def test_priority_failures_age_out_and_success_does_not_reset_history():
    clock = FakeClock()
    outcomes = [False, False, False, False, True, False]

    def scripted_put(item, timeout):
        del item, timeout
        if not outcomes.pop(0):
            raise queue.Full

    priority = _priority_queue(monotonic=clock, put_impl=scripted_put)
    for item in range(4):
        assert priority.offer(item).escalation is None
    assert priority.offer(4).accepted
    assert priority.offer(5).escalation is not None

    old_clock = FakeClock()
    old_failures = _priority_queue(
        monotonic=old_clock,
        put_impl=lambda item, timeout: (_ for _ in ()).throw(queue.Full),
    )
    for item in range(4):
        old_failures.offer(item)
    old_clock.value = 10.001
    assert old_failures.offer(5).escalation is None


def test_priority_queue_successful_insertion():
    priority = _priority_queue()
    assert priority.offer(1).accepted
    assert priority.get_nowait() == 1


def test_control_command_queue_rejects_full_queue_without_inserting():
    commands = ControlCommandQueue[int]()
    assert commands.capacity == 16
    for item in range(16):
        assert commands.offer(item).accepted
    rejected = commands.offer(99)
    assert rejected.status is QueueOfferStatus.REJECTED
    assert rejected.error_code is ErrorCode.MODULE_BUSY
    assert commands.qsize() == 16
    assert [commands.get_nowait() for _ in range(16)] == list(range(16))


def test_control_result_timeout_caches_before_exit75_and_logs_100ms():
    order: list[str] = []
    timeouts: list[float] = []

    def full(item, timeout):
        del item
        timeouts.append(timeout)
        raise queue.Full

    results = ControlResultQueue[str](
        cache_result=lambda result: order.append(f"cache:{result}"),
        event=lambda event: order.append(f"event:{event.event_code}"),
        escalate=lambda request: order.append(f"escalate:{request.exit_code}"),
        put_impl=full,
    )
    assert results.capacity == 16
    offered = results.offer("answer")
    assert timeouts == [0.100]
    assert order == ["event:CONTROL_RESULT_QUEUE_FULL", "cache:answer", "escalate:75"]
    assert offered.escalation and offered.escalation.exit_code is ExitCode.TEMPORARY_FAILURE


def test_control_result_normal_insertion():
    results = ControlResultQueue[int](
        cache_result=lambda result: None,
        event=ignore_event,
        escalate=ignore_escalation,
    )
    assert results.offer(1).accepted
    assert results.get_nowait() == 1


def test_recorder_drops_newest_degrades_counts_and_rate_limits_critical_log():
    clock = FakeClock()
    limiter = WarningRateLimiter(monotonic=clock)
    metrics = RuntimeMetrics()
    state = ComponentStateMachine(ComponentState.RUNNING)
    events = []
    records = RecorderQueue[int](
        metrics=metrics,
        event=events.append,
        degrade=lambda reason: state.transition_to(ComponentState.DEGRADED),
        warning_limiter=limiter,
    )
    assert records.capacity == 4096
    for item in range(4096):
        assert records.offer(item).accepted
    assert records.offer(4096).status is QueueOfferStatus.DROPPED
    assert records.offer(4097).status is QueueOfferStatus.DROPPED
    assert records.qsize() == 4096
    assert records._queue.queue[0] == 0
    assert records._queue.queue[-1] == 4095
    assert state.state is ComponentState.DEGRADED
    assert len(events) == 1 and events[0].level == "CRITICAL"
    clock.value = 1.0
    records.offer(4098)
    assert len(events) == 2
    assert events[1].context["suppressed_count"] == 1
    assert records.get_nowait() == 0
    snapshot = metrics.snapshot().values
    assert snapshot["recorder_queue_overflow"] == 3
    assert snapshot["warnings_suppressed"] == 1


def test_receive_timeout_and_shutdown_are_normal_polling_results():
    metrics = RuntimeMetrics()
    results = CvResultQueue[int](metrics=metrics)
    before = metrics.snapshot().values
    assert results.receive(timeout_seconds=0).status is ReceiveStatus.TIMEOUT
    assert metrics.snapshot().values["invalid_messages"] == before["invalid_messages"]
    shutdown = ShutdownToken()
    shutdown.request("test")
    assert results.receive(timeout_seconds=0.250, shutdown=shutdown).status is ReceiveStatus.SHUTDOWN
    with np.testing.assert_raises(ValueError):
        results.receive(timeout_seconds=0.251)


@pytest.mark.parametrize(
    ("boundary", "escalates"),
    [(9.999, True), (10.000, True), (10.001, False)],
)
def test_priority_failure_window_boundaries(boundary, escalates):
    clock = FakeClock()

    def full(item, timeout):
        del item, timeout
        raise queue.Full

    priority = _priority_queue(monotonic=clock, put_impl=full)
    for item in range(4):
        assert priority.offer(item).escalation is None
    clock.value = boundary
    assert (priority.offer(4).escalation is not None) is escalates


def test_priority_escalates_once_at_threshold_and_attempts_all_callbacks_after_failures():
    actions: list[str] = []

    def fail(action):
        def callback(value):
            del value
            actions.append(action)
            raise RuntimeError(f"{action} failed")

        return callback

    def full(item, timeout):
        del item, timeout
        raise queue.Full

    priority = _priority_queue(
        event=fail("event"),
        degrade=fail("degrade"),
        escalate=fail("escalate"),
        put_impl=full,
    )
    for item in range(4):
        result = priority.offer(item)
        assert [failure.action for failure in result.callback_failures] == ["event", "degrade"]
    fifth = priority.offer(4)
    sixth = priority.offer(5)

    assert [failure.action for failure in fifth.callback_failures] == ["event", "degrade", "escalate"]
    assert sixth.escalation is None
    assert actions.count("escalate") == 1


def test_control_result_callback_failures_do_not_skip_cache_or_escalation():
    actions: list[str] = []

    def fail(action):
        def callback(value):
            del value
            actions.append(action)
            raise RuntimeError(f"{action} failed")

        return callback

    def full(item, timeout):
        del item, timeout
        raise queue.Full

    results = ControlResultQueue[str](
        cache_result=fail("cache_result"),
        event=fail("event"),
        escalate=fail("escalate"),
        put_impl=full,
    )
    offered = results.offer("answer")

    assert actions == ["event", "cache_result", "escalate"]
    assert [failure.action for failure in offered.callback_failures] == ["event", "cache_result", "escalate"]
    assert offered.escalation and offered.escalation.exit_code is ExitCode.TEMPORARY_FAILURE


def test_recorder_callback_failures_do_not_skip_critical_warning():
    events = []

    def broken_degrade(reason):
        del reason
        raise RuntimeError("state unavailable")

    records = RecorderQueue[int](event=events.append, degrade=broken_degrade)
    for item in range(records.capacity):
        records.offer(item)

    result = records.offer(records.capacity)

    assert [failure.action for failure in result.callback_failures] == ["degrade"]
    assert len(events) == 1 and events[0].level == "CRITICAL"


def test_receive_passes_default_250ms_timeout_to_queue(monkeypatch):
    results = CvResultQueue[int]()
    observed = []

    def empty(*, timeout):
        observed.append(timeout)
        raise queue.Empty

    monkeypatch.setattr(results._queue, "get", empty)
    assert results.receive().status is ReceiveStatus.TIMEOUT
    assert observed == [0.250]
