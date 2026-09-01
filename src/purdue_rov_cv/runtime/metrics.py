"""Thread-safe typed runtime metrics with canonical names."""

from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import RLock
from typing import Any

from purdue_rov_cv.config.models import (
    DIAGNOSTICS_PUBLISH_INTERVAL_MAX_MS,
    DIAGNOSTICS_PUBLISH_INTERVAL_MIN_MS,
)
from purdue_rov_cv.wire.errors import is_error_code

from .state import ComponentState

HEALTH_INTERVAL_MIN_MS = DIAGNOSTICS_PUBLISH_INTERVAL_MIN_MS
HEALTH_INTERVAL_MAX_MS = DIAGNOSTICS_PUBLISH_INTERVAL_MAX_MS


class MetricKind(StrEnum):
    COUNTER = "counter"
    GAUGE = "gauge"
    METADATA = "metadata"


COUNTER_NAMES = frozenset(
    {
        "frames_received",
        "frame_timeouts",
        "pipeline_restarts",
        "shared_memory_write_count",
        "shared_memory_read_conflicts",
        "shared_memory_disconnects",
        "shared_memory_reattach_count",
        "frames_read",
        "frames_processed",
        "frames_dropped_before_processing",
        "processing_exceptions",
        "processing_deadline_misses",
        "results_published",
        "results_dropped_local_queue",
        "zmq_send_dropped",
        "messages_sent",
        "messages_received",
        "invalid_messages",
        "unknown_payload_types",
        "observed_sequence_gaps",
        "reconnect_count",
        "rtp_packets_received",
        "rtp_packets_lost",
        "decoded_frames",
        "frame_index_hits",
        "frame_index_misses",
        "stream_restarts",
        "priority_messages_dropped",
        "recorder_queue_overflow",
        "invalid_multipart_message",
        "warnings_suppressed",
    }
)
GAUGE_NAMES = frozenset(
    {
        "process_cpu_percent",
        "resident_memory_bytes",
        "thread_count",
        "uptime_seconds",
        "frames_per_second",
        "current_width",
        "current_height",
        "usb_device_present",
        "average_processing_ms",
        "p95_processing_ms",
        "last_frame_age_ms",
        "input_source_present",
        "cpu_temperature_c",
        "memory_available_bytes",
        "disk_free_bytes",
        "clock_synchronized",
        "clock_offset_ms",
        "tether_link_up",
    }
)
METADATA_NAMES = frozenset({"state", "last_error_code", "last_error_message", "current_pixel_format"})


@dataclass(frozen=True)
class MetricsSnapshot:
    values: dict[str, Any]
    captured_monotonic: float


class RuntimeMetrics:
    """Reject counter decreases and keep snapshots coherent across threads."""

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        processing_sample_capacity: int = 1_024,
    ) -> None:
        if (
            isinstance(processing_sample_capacity, bool)
            or not isinstance(processing_sample_capacity, int)
            or processing_sample_capacity <= 0
        ):
            raise ValueError("processing_sample_capacity must be a positive integer")
        self._monotonic = monotonic
        self._started = monotonic()
        self._lock = RLock()
        self._values: dict[str, Any] = {name: 0 for name in sorted(COUNTER_NAMES | GAUGE_NAMES)}
        self._values.update({name: None for name in sorted(METADATA_NAMES)})
        self._processing_samples: deque[float] = deque(maxlen=processing_sample_capacity)
        self._processing_count = 0
        self._processing_total = 0.0

    def kind(self, name: str) -> MetricKind:
        if name in COUNTER_NAMES:
            return MetricKind.COUNTER
        if name in GAUGE_NAMES:
            return MetricKind.GAUGE
        if name in METADATA_NAMES:
            return MetricKind.METADATA
        raise KeyError(f"unknown metric: {name}")

    def increment(self, name: str, amount: int = 1) -> int:
        if name not in COUNTER_NAMES:
            raise KeyError(f"not a counter: {name}")
        if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
            raise ValueError("counter increments must be non-negative integers")
        with self._lock:
            self._values[name] += amount
            return self._values[name]

    def set_gauge(self, name: str, value: int | float | bool) -> None:
        if name not in GAUGE_NAMES:
            raise KeyError(f"not a gauge: {name}")
        if not isinstance(value, (int, float, bool)) or (isinstance(value, float) and not math.isfinite(value)):
            raise ValueError("gauge value must be finite numeric or boolean")
        with self._lock:
            self._values[name] = value

    def set_metadata(self, name: str, value: str | None) -> None:
        if name not in METADATA_NAMES:
            raise KeyError(f"not metadata: {name}")
        if value is not None and not isinstance(value, str):
            raise ValueError("metadata value must be a string or None")
        if name == "state" and value is not None:
            try:
                value = ComponentState(value).value
            except ValueError as error:
                raise ValueError("state metadata must be a canonical component state") from error
        if name == "last_error_code" and value is not None and not is_error_code(value):
            raise ValueError("last_error_code metadata must be a canonical error code")
        with self._lock:
            self._values[name] = value

    def observe_processing_ms(self, duration_ms: float) -> None:
        if not math.isfinite(duration_ms) or duration_ms < 0:
            raise ValueError("processing duration must be finite and non-negative")
        with self._lock:
            self._processing_samples.append(duration_ms)
            self._processing_count += 1
            self._processing_total = math.fsum((self._processing_total, duration_ms))
            ordered = sorted(self._processing_samples)
            self._values["average_processing_ms"] = self._processing_total / self._processing_count
            index = max(0, math.ceil(0.95 * len(ordered)) - 1)
            self._values["p95_processing_ms"] = ordered[index]

    @property
    def processing_sample_count(self) -> int:
        """Return the bounded sample count retained for percentile calculation."""
        with self._lock:
            return len(self._processing_samples)

    def snapshot(self) -> MetricsSnapshot:
        captured = self._monotonic()
        with self._lock:
            values = {name: self._values[name] for name in sorted(self._values)}
        values["uptime_seconds"] = max(0.0, captured - self._started)
        return MetricsSnapshot(values, captured)


__all__ = [
    "COUNTER_NAMES",
    "GAUGE_NAMES",
    "HEALTH_INTERVAL_MAX_MS",
    "HEALTH_INTERVAL_MIN_MS",
    "METADATA_NAMES",
    "MetricKind",
    "MetricsSnapshot",
    "RuntimeMetrics",
]
