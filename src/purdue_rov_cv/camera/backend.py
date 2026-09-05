"""Capture backend boundary and the real GStreamer simulated source."""

from __future__ import annotations

import importlib
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Any, Protocol

from purdue_rov.cv.v1 import frame_index_pb2

from purdue_rov_cv.frame_buffer import PixelFormat
from purdue_rov_cv.video.mapping import RtpFrameIndexMapper


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
    frame_number: int | None = None


@dataclass(frozen=True, slots=True)
class SurfaceRtpStream:
    camera_id: str
    camera_session_id: bytes
    host: str
    port: int
    payload_type: int
    ssrc: int
    on_frame_index: Callable[[frame_index_pb2.FrameIndex], None]
    mtu: int = 1_200
    mapper: RtpFrameIndexMapper | None = None


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
        surface_stream: SurfaceRtpStream | None = None,
        time_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if min(width, height, frame_rate) <= 0:
            raise ValueError("simulated capture dimensions and frame rate must be positive")
        self.width = width
        self.height = height
        self.frame_rate = frame_rate
        self.pattern = pattern
        self.surface_stream = surface_stream
        self._time_ns = time_ns
        self._monotonic_ns = monotonic_ns
        self._gst: Any = None
        self._gst_rtp: Any = None
        self._pipeline: Any = None
        self._sink: Any = None
        self._source_pad: Any = None
        self._probe_id: int | None = None
        self._probe_bindings: list[tuple[Any, int]] = []
        self._timestamps: OrderedDict[int, tuple[int, int, int | None]] = OrderedDict()
        self._timestamp_lock = Lock()
        self._mapper = (
            None
            if surface_stream is None
            else surface_stream.mapper
            or RtpFrameIndexMapper(
                surface_stream.camera_id,
                surface_stream.camera_session_id,
                time_ns=time_ns,
                monotonic_ns=monotonic_ns,
            )
        )

    def _load_gst(self) -> tuple[Any, Any | None]:
        try:
            gi = importlib.import_module("gi")
            gi.require_version("Gst", "1.0")
            if self.surface_stream is not None:
                gi.require_version("GstRtp", "1.0")
            gst = importlib.import_module("gi.repository.Gst")
            gst_rtp = None if self.surface_stream is None else importlib.import_module("gi.repository.GstRtp")
        except (ImportError, AttributeError, ValueError) as error:
            raise CaptureBackendUnavailable(
                "PyGObject GStreamer bindings are unavailable; install python3-gi and python3-gst-1.0 for Python 3.12"
            ) from error
        gst.init(None)
        return gst, gst_rtp

    @property
    def running(self) -> bool:
        return self._pipeline is not None

    def pipeline_description(self) -> str:
        source = (
            f"videotestsrc name=source is-live=true do-timestamp=true pattern={self.pattern} "
            f"! video/x-raw,width={self.width},height={self.height},framerate={self.frame_rate}/1 "
        )
        if self.surface_stream is None:
            return (
                source + "! videoconvert ! video/x-raw,format=BGR "
                "! appsink name=sink emit-signals=false sync=false max-buffers=1 drop=true"
            )
        stream = self.surface_stream
        return (
            source + "! tee name=capture_tee "
            "capture_tee. ! queue max-size-buffers=1 max-size-bytes=0 max-size-time=0 leaky=downstream "
            "! videoconvert ! video/x-raw,format=BGR "
            "! appsink name=sink emit-signals=false sync=false max-buffers=1 drop=true "
            "capture_tee. ! queue max-size-buffers=8 max-size-bytes=0 max-size-time=0 leaky=downstream "
            "! videoconvert ! x264enc name=encoder tune=zerolatency speed-preset=ultrafast "
            "key-int-max=30 bframes=0 byte-stream=true ! h264parse "
            f"! rtph264pay name=pay config-interval=1 pt={stream.payload_type} "
            f"ssrc={stream.ssrc & 0xFFFFFFFF} mtu={stream.mtu} "
            f"! udpsink host={stream.host} port={stream.port} sync=false async=false"
        )

    def _source_probe(self, _pad: Any, info: Any) -> Any:
        gst = self._gst
        buffer = info.get_buffer()
        if buffer is not None:
            pts = int(buffer.pts)
            timestamps: tuple[int, int, int | None]
            if self._mapper is None:
                timestamps = (self._time_ns(), self._monotonic_ns(), None)
            else:
                identity = self._mapper.observe_source(pts)
                timestamps = (
                    identity.capture_time_unix_ns,
                    identity.capture_monotonic_ns,
                    identity.frame_number,
                )
            with self._timestamp_lock:
                self._timestamps[pts] = timestamps
                while len(self._timestamps) > 256:
                    self._timestamps.popitem(last=False)
        return gst.PadProbeReturn.OK

    def _encoder_input_probe(self, _pad: Any, info: Any) -> Any:
        buffer = info.get_buffer()
        if buffer is not None and self._mapper is not None:
            self._mapper.observe_encoder_input(int(buffer.pts))
        return self._gst.PadProbeReturn.OK

    def _encoder_output_probe(self, _pad: Any, info: Any) -> Any:
        buffer = info.get_buffer()
        if buffer is not None and self._mapper is not None:
            self._mapper.observe_encoded_output(int(buffer.pts))
        return self._gst.PadProbeReturn.OK

    def _publish_packet(self, buffer: Any) -> None:
        if self._mapper is None or self._gst_rtp is None or self.surface_stream is None:
            return
        success, packet = self._gst_rtp.RTPBuffer.map(buffer, self._gst.MapFlags.READ)
        if not success:
            return
        try:
            value = self._mapper.frame_index_for_packet(
                int(buffer.pts),
                int(packet.get_ssrc()),
                int(packet.get_timestamp()),
                int(packet.get_payload_type()),
            )
        finally:
            packet.unmap()
        if value is not None:
            self.surface_stream.on_frame_index(value)

    def _pay_probe(self, _pad: Any, info: Any) -> Any:
        if info.type & self._gst.PadProbeType.BUFFER:
            buffer = info.get_buffer()
            if buffer is not None:
                self._publish_packet(buffer)
        if info.type & self._gst.PadProbeType.BUFFER_LIST:
            buffer_list = info.get_buffer_list()
            if buffer_list is not None:
                for index in range(buffer_list.length()):
                    self._publish_packet(buffer_list.get(index))
        return self._gst.PadProbeReturn.OK

    def start(self) -> None:
        if self.running:
            return
        gst, gst_rtp = self._load_gst()
        try:
            pipeline = gst.parse_launch(self.pipeline_description())
            sink = pipeline.get_by_name("sink")
            source = pipeline.get_by_name("source")
            source_pad = source.get_static_pad("src") if source is not None else None
            if sink is None or source_pad is None:
                raise CaptureBackendError("simulated GStreamer pipeline lacks source or appsink")
            self._gst = gst
            self._gst_rtp = gst_rtp
            self._pipeline = pipeline
            self._sink = sink
            self._source_pad = source_pad
            self._probe_id = int(source_pad.add_probe(gst.PadProbeType.BUFFER, self._source_probe))
            if self.surface_stream is not None:
                encoder = pipeline.get_by_name("encoder")
                pay = pipeline.get_by_name("pay")
                if encoder is None or pay is None:
                    raise CaptureBackendError("surface stream lacks encoder or RTP payloader")
                encoder_sink = encoder.get_static_pad("sink")
                encoder_source = encoder.get_static_pad("src")
                pay_source = pay.get_static_pad("src")
                for pad, probe_type, callback in (
                    (encoder_sink, gst.PadProbeType.BUFFER, self._encoder_input_probe),
                    (encoder_source, gst.PadProbeType.BUFFER, self._encoder_output_probe),
                    (
                        pay_source,
                        gst.PadProbeType.BUFFER | gst.PadProbeType.BUFFER_LIST,
                        self._pay_probe,
                    ),
                ):
                    if pad is None:
                        raise CaptureBackendError("surface stream lacks a required probe pad")
                    self._probe_bindings.append((pad, int(pad.add_probe(probe_type, callback))))
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
        return CapturedFrame(
            data,
            width,
            height,
            stride,
            PixelFormat.BGR8,
            timestamps[0],
            timestamps[1],
            timestamps[2],
        )

    def stop(self) -> None:
        pipeline = self._pipeline
        source_pad = self._source_pad
        probe_id = self._probe_id
        gst = self._gst
        self._pipeline = None
        self._sink = None
        self._source_pad = None
        self._probe_id = None
        self._probe_bindings, probe_bindings = [], self._probe_bindings
        self._gst_rtp = None
        self._gst = None
        with self._timestamp_lock:
            self._timestamps.clear()
        failures: list[str] = []
        if source_pad is not None and probe_id is not None:
            try:
                source_pad.remove_probe(probe_id)
            except Exception as error:
                failures.append(f"probe removal failed: {type(error).__name__}: {error}")
        for pad, binding_id in probe_bindings:
            try:
                pad.remove_probe(binding_id)
            except Exception as error:
                failures.append(f"probe removal failed: {type(error).__name__}: {error}")
        if self._mapper is not None:
            self._mapper.clear()
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
    "SurfaceRtpStream",
]
