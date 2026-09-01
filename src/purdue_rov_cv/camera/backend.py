"""Capture backend boundary and the real GStreamer simulated source."""

from __future__ import annotations

import importlib
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any, Protocol

from purdue_rov_cv.frame_buffer import PixelFormat


class CaptureBackendError(RuntimeError):
    pass


class CaptureBackendUnavailable(CaptureBackendError):
    pass


@dataclass(frozen=True, slots=True)
class CapturedFrame:
    data: bytes
    width: int
    height: int
    stride_bytes: int
    pixel_format: PixelFormat
    capture_time_unix_ns: int
    capture_monotonic_ns: int


class CaptureBackend(Protocol):
    def start(self) -> None: ...

    def poll(self, timeout_seconds: float) -> CapturedFrame | None: ...

    def stop(self) -> None: ...


class GStreamerCaptureBackend:
    """`videotestsrc` -> raw BGR -> bounded dropping `appsink`.

    PyGObject is deliberately loaded at runtime because it is supplied by the
    Ubuntu platform packages, not PyPI. A source-pad probe records both clocks
    when each source buffer first becomes software-visible, before conversion.
    """

    def __init__(
        self,
        width: int,
        height: int,
        frame_rate: int,
        *,
        pattern: str = "smpte",
        time_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if min(width, height, frame_rate) <= 0:
            raise ValueError("simulated capture dimensions and frame rate must be positive")
        self.width = width
        self.height = height
        self.frame_rate = frame_rate
        self.pattern = pattern
        self._time_ns = time_ns
        self._monotonic_ns = monotonic_ns
        self._gst: Any = None
        self._pipeline: Any = None
        self._sink: Any = None
        self._source_pad: Any = None
        self._probe_id: int | None = None
        self._timestamps: OrderedDict[int, tuple[int, int]] = OrderedDict()
        self._timestamp_lock = Lock()

    def _load_gst(self) -> Any:
        try:
            gi = importlib.import_module("gi")
            gi.require_version("Gst", "1.0")
            gst = importlib.import_module("gi.repository.Gst")
        except (ImportError, AttributeError, ValueError) as error:
            raise CaptureBackendUnavailable(
                "PyGObject GStreamer bindings are unavailable; install python3-gi and python3-gst-1.0 for Python 3.12"
            ) from error
        gst.init(None)
        return gst

    @property
    def running(self) -> bool:
        return self._pipeline is not None

    def pipeline_description(self) -> str:
        return (
            f"videotestsrc name=source is-live=true pattern={self.pattern} "
            f"! video/x-raw,width={self.width},height={self.height},framerate={self.frame_rate}/1 "
            "! videoconvert ! video/x-raw,format=BGR "
            "! appsink name=sink emit-signals=false sync=false max-buffers=1 drop=true"
        )

    def _source_probe(self, _pad: Any, info: Any) -> Any:
        gst = self._gst
        buffer = info.get_buffer()
        if buffer is not None:
            timestamps = (self._time_ns(), self._monotonic_ns())
            with self._timestamp_lock:
                self._timestamps[int(buffer.pts)] = timestamps
                while len(self._timestamps) > 16:
                    self._timestamps.popitem(last=False)
        return gst.PadProbeReturn.OK

    def start(self) -> None:
        if self.running:
            return
        gst = self._load_gst()
        try:
            pipeline = gst.parse_launch(self.pipeline_description())
            sink = pipeline.get_by_name("sink")
            source = pipeline.get_by_name("source")
            source_pad = source.get_static_pad("src") if source is not None else None
            if sink is None or source_pad is None:
                raise CaptureBackendError("simulated GStreamer pipeline lacks source or appsink")
            self._gst = gst
            self._pipeline = pipeline
            self._sink = sink
            self._source_pad = source_pad
            self._probe_id = int(source_pad.add_probe(gst.PadProbeType.BUFFER, self._source_probe))
            result = pipeline.set_state(gst.State.PLAYING)
            if result == gst.StateChangeReturn.FAILURE:
                raise CaptureBackendError("simulated GStreamer pipeline failed to enter PLAYING")
        except Exception:
            self.stop()
            raise

    def _raise_bus_failure(self) -> None:
        pipeline = self._pipeline
        gst = self._gst
        if pipeline is None:
            raise CaptureBackendError("capture backend is not running")
        message = pipeline.get_bus().timed_pop_filtered(0, gst.MessageType.ERROR | gst.MessageType.EOS)
        if message is None:
            return
        if message.type == gst.MessageType.ERROR:
            error, debug = message.parse_error()
            raise CaptureBackendError(f"GStreamer pipeline error: {error}; {debug or 'no debug detail'}")
        raise CaptureBackendError("GStreamer pipeline reached end of stream")

    def poll(self, timeout_seconds: float) -> CapturedFrame | None:
        if not 0 <= timeout_seconds <= 0.250:
            raise ValueError("capture poll timeout must be between zero and 250 ms")
        self._raise_bus_failure()
        sample = self._sink.emit("try-pull-sample", int(timeout_seconds * 1_000_000_000))
        if sample is None:
            self._raise_bus_failure()
            return None
        buffer = sample.get_buffer()
        caps = sample.get_caps()
        structure = caps.get_structure(0)
        width = int(structure.get_value("width"))
        height = int(structure.get_value("height"))
        raw_format = str(structure.get_value("format"))
        if raw_format != "BGR":
            raise CaptureBackendError(f"appsink negotiated unsupported format {raw_format!r}")
        mapped, info = buffer.map(self._gst.MapFlags.READ)
        if not mapped:
            raise CaptureBackendError("could not map GStreamer source buffer")
        try:
            data = bytes(info.data)
        finally:
            buffer.unmap(info)
        if height <= 0 or len(data) % height:
            raise CaptureBackendError("GStreamer frame size cannot be represented by an integral stride")
        stride = len(data) // height
        with self._timestamp_lock:
            timestamps = self._timestamps.pop(int(buffer.pts), None)
        if timestamps is None:
            raise CaptureBackendError("source-boundary timestamp was not retained for an appsink sample")
        return CapturedFrame(data, width, height, stride, PixelFormat.BGR8, timestamps[0], timestamps[1])

    def stop(self) -> None:
        pipeline = self._pipeline
        source_pad = self._source_pad
        probe_id = self._probe_id
        gst = self._gst
        self._pipeline = None
        self._sink = None
        self._source_pad = None
        self._probe_id = None
        self._gst = None
        with self._timestamp_lock:
            self._timestamps.clear()
        failures: list[str] = []
        if source_pad is not None and probe_id is not None:
            try:
                source_pad.remove_probe(probe_id)
            except Exception as error:
                failures.append(f"probe removal failed: {type(error).__name__}: {error}")
        if pipeline is not None and gst is not None:
            try:
                result = pipeline.set_state(gst.State.NULL)
                if result == gst.StateChangeReturn.FAILURE:
                    failures.append("pipeline rejected the NULL state transition")
            except Exception as error:
                failures.append(f"pipeline NULL transition failed: {type(error).__name__}: {error}")
            try:
                _result, current, _pending = pipeline.get_state(1_000_000_000)
                if current != gst.State.NULL:
                    failures.append(f"pipeline remained in {current!s} instead of NULL")
            except Exception as error:
                failures.append(f"pipeline NULL confirmation failed: {type(error).__name__}: {error}")
        if failures:
            raise CaptureBackendError("; ".join(failures))


__all__ = [
    "CaptureBackend",
    "CaptureBackendError",
    "CaptureBackendUnavailable",
    "CapturedFrame",
    "GStreamerCaptureBackend",
]
