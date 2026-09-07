"""Encoded H.264 to segmented Matroska recording without re-encoding."""

from __future__ import annotations

import importlib
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

from purdue_rov_cv.config.models import validate_identifier
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.video.models import EncodedAccessUnit

from .disk import RUNTIME_MINIMUM_BYTES, DiskSpaceGuard

PRODUCTION_SEGMENT_SECONDS = 300
NANOSECONDS_PER_SECOND = 1_000_000_000


class VideoDiskSpaceLow(RuntimeError):
    error_code = "DISK_SPACE_LOW"


class VideoSegmentPaths:
    """Collision-safe `<session>/<camera>/<UTC-start>.mkv` allocator."""

    def __init__(
        self,
        recording_root: Path,
        session_label: str,
        camera_id: str,
        *,
        utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        validate_identifier(session_label)
        validate_identifier(camera_id)
        self.directory = recording_root / session_label / camera_id
        self._utc_now = utc_now
        self._lock = Lock()
        self._last_stem = ""
        self._collision = 0

    def next_path(self) -> Path:
        with self._lock:
            now = self._utc_now().astimezone(UTC)
            stem = now.strftime("%Y%m%dT%H%M%S.%fZ")
            if stem == self._last_stem:
                self._collision += 1
            else:
                self._last_stem = stem
                self._collision = 0
            suffix = "" if self._collision == 0 else f"-{self._collision:03d}"
            candidate = self.directory / f"{stem}{suffix}.mkv"
            while candidate.exists():
                self._collision += 1
                candidate = self.directory / f"{stem}-{self._collision:03d}.mkv"
            return candidate


class EncodedMatroskaRecorder:
    """GStreamer appsrc recorder receiving the Phase 7 encoded AU seam."""

    def __init__(
        self,
        paths: VideoSegmentPaths,
        *,
        segment_seconds: int = PRODUCTION_SEGMENT_SECONDS,
        disk_guard: DiskSpaceGuard | None = None,
        metrics: RuntimeMetrics | None = None,
        disk_check_interval_seconds: float = 1.0,
    ) -> None:
        if segment_seconds <= 0:
            raise ValueError("video segment duration must be positive")
        self.paths = paths
        self.segment_seconds = segment_seconds
        self.disk_guard = disk_guard or DiskSpaceGuard()
        self.metrics = metrics
        self.disk_check_interval_seconds = disk_check_interval_seconds
        self._gst: Any = None
        self._pipeline: Any = None
        self._source: Any = None
        self._sink: Any = None
        self._format_handler: int | None = None
        self._next_disk_check = 0.0
        self._pushed_units = 0

    @property
    def running(self) -> bool:
        return self._pipeline is not None

    def pipeline_description(self) -> str:
        # h264parse is parser/mux preparation, never an encoder. splitmuxsink
        # receives encoded H.264 and writes Matroska fragments directly.
        return (
            "appsrc name=encoded_source is-live=true format=time block=false "
            'caps="video/x-h264,stream-format=(string)byte-stream,alignment=(string)au" '
            "! h264parse config-interval=-1 ! queue max-size-buffers=32 leaky=downstream "
            f"! splitmuxsink name=recording_sink muxer-factory=matroskamux "
            f"max-size-time={self.segment_seconds * NANOSECONDS_PER_SECOND} async-finalize=true"
        )

    def _load_gst(self) -> Any:
        gi = importlib.import_module("gi")
        gi.require_version("Gst", "1.0")
        gst = importlib.import_module("gi.repository.Gst")
        gst.init(None)
        return gst

    def _format_location(self, _sink: Any, _fragment_id: int, *_args: Any) -> str:
        return str(self.paths.next_path())

    def start(self) -> None:
        if self.running:
            return
        snapshot = self.disk_guard.allows_start(self.paths.directory)
        if self.metrics is not None:
            self.metrics.set_gauge("disk_free_bytes", snapshot.free_bytes)
        self.paths.directory.mkdir(parents=True, exist_ok=True)
        gst = self._load_gst()
        pipeline = gst.parse_launch(self.pipeline_description())
        source = pipeline.get_by_name("encoded_source")
        sink = pipeline.get_by_name("recording_sink")
        if source is None or sink is None:
            raise RuntimeError("encoded recorder pipeline lacks required elements")
        self._gst, self._pipeline, self._source, self._sink = gst, pipeline, source, sink
        self._pushed_units = 0
        self._format_handler = int(sink.connect("format-location-full", self._format_location))
        self._next_disk_check = time.monotonic() + self.disk_check_interval_seconds
        result = pipeline.set_state(gst.State.PLAYING)
        if result == gst.StateChangeReturn.FAILURE:
            try:
                self.stop()
            except RuntimeError:
                pass
            raise RuntimeError("encoded Matroska recorder failed to enter PLAYING")
        # A live appsrc cannot complete its asynchronous READY->PLAYING
        # transition until the first access unit arrives. A non-failure state
        # change is therefore the strongest valid readiness signal here.

    def push(self, unit: EncodedAccessUnit) -> bool:
        if self._source is None or self._gst is None:
            return False
        buffer = self._gst.Buffer.new_allocate(None, len(unit.data), None)
        buffer.fill(0, unit.data)
        buffer.pts = unit.presentation_timestamp_ns
        buffer.dts = unit.presentation_timestamp_ns
        if unit.is_delta_unit:
            buffer.set_flags(self._gst.BufferFlags.DELTA_UNIT)
        result = self._source.emit("push-buffer", buffer)
        accepted = result == self._gst.FlowReturn.OK
        if accepted:
            self._pushed_units += 1
        return accepted

    def check_bus(self) -> None:
        if self._pipeline is None:
            return
        now = time.monotonic()
        if now >= self._next_disk_check:
            snapshot = self.disk_guard.runtime_is_low(self.paths.directory)
            if self.metrics is not None:
                self.metrics.set_gauge("disk_free_bytes", snapshot.free_bytes)
            self._next_disk_check = now + self.disk_check_interval_seconds
            if snapshot.free_bytes < RUNTIME_MINIMUM_BYTES:
                message = f"encoded video recording stopped below {RUNTIME_MINIMUM_BYTES} free bytes"
                try:
                    self.stop()
                except RuntimeError as error:
                    message = f"{message}; finalization error: {error}"
                raise VideoDiskSpaceLow(message)
        message = self._pipeline.get_bus().timed_pop_filtered(0, self._gst.MessageType.ERROR)
        if message is not None:
            parsed_error, debug = message.parse_error()
            self._abort()
            raise RuntimeError(f"encoded Matroska recorder error: {parsed_error}; {debug or 'no detail'}")

    def _detach(self) -> tuple[Any, Any, Any, Any, int | None, int]:
        pipeline, source, gst = self._pipeline, self._source, self._gst
        sink, format_handler = self._sink, self._format_handler
        pushed_units = self._pushed_units
        self._pipeline = self._source = None
        self._sink = self._gst = None
        self._format_handler = None
        self._pushed_units = 0
        return pipeline, source, gst, sink, format_handler, pushed_units

    @staticmethod
    def _disconnect(sink: Any, format_handler: int | None) -> None:
        if sink is not None and format_handler is not None:
            try:
                sink.disconnect(format_handler)
            except Exception:
                pass

    def _abort(self) -> None:
        pipeline, _source, gst, sink, format_handler, _pushed_units = self._detach()
        try:
            if pipeline is not None and gst is not None:
                pipeline.set_state(gst.State.NULL)
                pipeline.get_state(NANOSECONDS_PER_SECOND)
        finally:
            self._disconnect(sink, format_handler)

    def stop(self) -> None:
        pipeline, source, gst, sink, format_handler, pushed_units = self._detach()
        failures: list[str] = []
        try:
            if source is not None and gst is not None and pushed_units:
                flow = source.emit("end-of-stream")
                if flow != gst.FlowReturn.OK:
                    failures.append(f"appsrc rejected EOS with {flow}")
            if pipeline is not None and gst is not None:
                if pushed_units:
                    bus = pipeline.get_bus()
                    message = bus.timed_pop_filtered(
                        2 * NANOSECONDS_PER_SECOND,
                        gst.MessageType.EOS | gst.MessageType.ERROR,
                    )
                    if message is None:
                        failures.append("timed out waiting for Matroska EOS/finalization")
                    elif message.type == gst.MessageType.ERROR:
                        error, debug = message.parse_error()
                        failures.append(f"Matroska finalization failed: {error}; {debug or 'no detail'}")
                if pipeline.set_state(gst.State.NULL) == gst.StateChangeReturn.FAILURE:
                    failures.append("Matroska pipeline rejected NULL state")
                _result, current, _pending = pipeline.get_state(NANOSECONDS_PER_SECOND)
                if current != gst.State.NULL:
                    failures.append("Matroska pipeline did not reach NULL state")
        finally:
            self._disconnect(sink, format_handler)
        if failures:
            raise RuntimeError("; ".join(failures))


__all__ = [
    "EncodedMatroskaRecorder",
    "NANOSECONDS_PER_SECOND",
    "PRODUCTION_SEGMENT_SECONDS",
    "VideoDiskSpaceLow",
    "VideoSegmentPaths",
]
