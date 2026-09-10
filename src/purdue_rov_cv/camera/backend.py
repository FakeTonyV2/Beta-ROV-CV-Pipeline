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

from purdue_rov_cv.config.models import CameraConfig, CameraFormat
from purdue_rov_cv.frame_buffer import PixelFormat
from purdue_rov_cv.video.mapping import RtpFrameIndexMapper

from .v4l2 import (
    H264ProfileStatus,
    ResolvedV4L2Device,
    V4L2DeviceProbe,
    exact_caps,
)

PRODUCTION_RTP_MTU = 1_200


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
    mtu: int = PRODUCTION_RTP_MTU
    mapper: RtpFrameIndexMapper | None = None

    def __post_init__(self) -> None:
        if self.mtu != PRODUCTION_RTP_MTU:
            raise ValueError(f"production RTP MTU must be exactly {PRODUCTION_RTP_MTU}")
        if not 0 <= self.ssrc <= 0xFFFFFFFF:
            raise ValueError("RTP SSRC must fit in an unsigned 32-bit integer")


class CaptureBackend(Protocol):
    def start(self) -> None: ...

    def poll(self, timeout_seconds: float) -> CapturedFrame | None: ...

    def stop(self) -> None: ...


class DisconnectAfterFramesBackend:
    """One-shot fault wrapper that fails through the normal capture boundary."""

    def __init__(self, backend: CaptureBackend, frame_count: int) -> None:
        if frame_count <= 0:
            raise ValueError("disconnect frame count must be positive")
        self.backend = backend
        self.frame_count = frame_count
        self._observed = 0

    def start(self) -> None:
        self.backend.start()

    def poll(self, timeout_seconds: float) -> CapturedFrame | None:
        if self._observed >= self.frame_count:
            raise CaptureBackendError("injected camera disconnect")
        frame = self.backend.poll(timeout_seconds)
        if frame is not None:
            self._observed += 1
        return frame

    def stop(self) -> None:
        self.backend.stop()


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
            try:
                pipeline = gst.parse_launch(self.pipeline_description())
            except Exception as error:
                raise CaptureBackendUnavailable(f"GStreamer pipeline could not be constructed: {error}") from error
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
            _state_result, current, pending = pipeline.get_state(2_000_000_000)
            if current != gst.State.PLAYING:
                raise CaptureBackendError(
                    f"GStreamer pipeline did not reach PLAYING (current={current!s}, pending={pending!s})"
                )
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
        if buffer is None or caps is None or caps.get_size() < 1:
            raise CaptureBackendError("appsink sample lacks a buffer or negotiated caps")
        structure = caps.get_structure(0)
        width = int(structure.get_value("width"))
        height = int(structure.get_value("height"))
        raw_format = str(structure.get_value("format"))
        if width != self.width or height != self.height:
            raise CaptureBackendError(f"appsink negotiated {width}x{height}; expected exact {self.width}x{self.height}")
        if raw_format != "BGR":
            raise CaptureBackendError(f"appsink negotiated unsupported format {raw_format!r}")
        mapped, info = buffer.map(self._gst.MapFlags.READ)
        if not mapped:
            raise CaptureBackendError("could not map GStreamer source buffer")
        try:
            data = bytes(info.data)
        finally:
            buffer.unmap(info)
        if width <= 0 or height <= 0 or not data or len(data) % height:
            raise CaptureBackendError("GStreamer frame size cannot be represented by an integral stride")
        stride = len(data) // height
        if stride < width * 3:
            raise CaptureBackendError(
                f"GStreamer BGR frame stride {stride} is smaller than the required {width * 3} bytes"
            )
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


class V4L2CaptureBackend(GStreamerCaptureBackend):
    """Production UVC/V4L2 capture using the Phase 6 lifecycle.

    Resolution and exact tuple validation happen before every build, including
    reconnects. The stable configured path is never replaced with an enumerated
    device selected by the application.
    """

    def __init__(
        self,
        camera_id: str,
        camera: CameraConfig,
        *,
        surface_stream: SurfaceRtpStream | None = None,
        device_probe: V4L2DeviceProbe | None = None,
        time_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        super().__init__(
            camera.width,
            camera.height,
            camera.frame_rate,
            surface_stream=surface_stream,
            time_ns=time_ns,
            monotonic_ns=monotonic_ns,
        )
        self.camera_id = camera_id
        self.camera = camera
        self.device_probe = device_probe or V4L2DeviceProbe()
        self.resolved_device: ResolvedV4L2Device | None = None
        self.h264_profile_status = H264ProfileStatus.UNVALIDATED
        self.h264_profile_detail = "pipeline has not started"

    def _source(self) -> str:
        if self.resolved_device is None:
            # Useful for deterministic construction tests; start() always
            # replaces this with the verified target.
            assert self.camera.device_path is not None
            device = self.camera.device_path
        else:
            device = self.resolved_device.resolved_path
        return f'v4l2src name=device_source device="{device}" do-timestamp=true ! {exact_caps(self.camera)} '

    @staticmethod
    def _cv_branch(decoder: str) -> str:
        decode = f"! {decoder.strip()} " if decoder.strip() else ""
        return (
            "capture_tee. ! queue name=cv_queue max-size-buffers=1 max-size-bytes=0 "
            "max-size-time=0 leaky=downstream "
            f"{decode}! videoconvert ! video/x-raw,format=BGR "
            "! appsink name=sink emit-signals=false max-buffers=1 drop=true sync=false "
        )

    def pipeline_description(self) -> str:
        source = self._source()
        stream = self.surface_stream
        if self.camera.format is CameraFormat.H264:
            base = source + "! h264parse ! identity name=source ! tee name=capture_tee "
            cv = self._cv_branch("avdec_h264 ")
            if stream is None:
                return base + cv
            rtp = (
                "capture_tee. ! queue name=rtp_queue max-size-buffers=2 max-size-bytes=0 "
                "max-size-time=0 leaky=downstream ! identity name=encoder "
                f"! rtph264pay name=pay pt={stream.payload_type} mtu={stream.mtu} config-interval=1 "
                f"ssrc={stream.ssrc & 0xFFFFFFFF} ! udpsink name=udp_sink host={stream.host} port={stream.port} "
                "sync=false async=false"
            )
            return base + cv + rtp
        if self.camera.format is CameraFormat.MJPEG:
            base = source + "! identity name=source ! tee name=capture_tee "
            cv = self._cv_branch("jpegdec ")
            if stream is None:
                return base + cv
            rtp = (
                "capture_tee. ! queue name=rtp_queue max-size-buffers=2 max-size-bytes=0 "
                "max-size-time=0 leaky=downstream ! identity name=encoder "
                f"! rtpjpegpay name=pay pt={stream.payload_type} mtu={stream.mtu} "
                f"ssrc={stream.ssrc & 0xFFFFFFFF} ! udpsink name=udp_sink host={stream.host} port={stream.port} "
                "sync=false async=false"
            )
            return base + cv + rtp

        base = source + "! identity name=source ! tee name=capture_tee "
        cv = self._cv_branch("")
        if stream is None:
            return base + cv
        if not self.camera.allow_software_encode:
            raise ValueError("raw surface streaming requires allow_software_encode=true")
        rtp = (
            "capture_tee. ! queue name=rtp_queue max-size-buffers=2 max-size-bytes=0 "
            "max-size-time=0 leaky=downstream ! videoconvert "
            f"! x264enc name=encoder tune=zerolatency speed-preset=ultrafast key-int-max={self.camera.frame_rate} "
            "bframes=0 byte-stream=true ! h264parse "
            f"! rtph264pay name=pay pt={stream.payload_type} mtu={stream.mtu} config-interval=1 "
            f"ssrc={stream.ssrc & 0xFFFFFFFF} ! udpsink name=udp_sink host={stream.host} port={stream.port} "
            "sync=false async=false"
        )
        return base + cv + rtp

    def _verify_negotiated_mode(self) -> None:
        source = self._pipeline.get_by_name("source") if self._pipeline is not None else None
        pad = source.get_static_pad("src") if source is not None else None
        caps = pad.get_current_caps() if pad is not None else None
        if caps is None or caps.get_size() < 1:
            raise CaptureBackendError("negotiated source caps are unavailable after the pipeline reached PLAYING")
        structure = caps.get_structure(0)
        expected_media_type = {
            CameraFormat.H264: "video/x-h264",
            CameraFormat.MJPEG: "image/jpeg",
            CameraFormat.YUYV: "video/x-raw",
            CameraFormat.NV12: "video/x-raw",
        }[self.camera.format]
        media_type = str(structure.get_name())
        if media_type != expected_media_type:
            raise CaptureBackendError(
                f"GStreamer negotiated media type {media_type!r}; expected {expected_media_type!r}"
            )
        width = int(structure.get_value("width"))
        height = int(structure.get_value("height"))
        fraction = structure.get_value("framerate")
        numerator = getattr(fraction, "num", getattr(fraction, "numerator", None))
        denominator = getattr(fraction, "denom", getattr(fraction, "denominator", None))
        if numerator is None or denominator is None:
            raise CaptureBackendError("GStreamer negotiated source caps without an exact frame rate")
        numerator = int(numerator)
        denominator = int(denominator)
        if (width, height, numerator, denominator) != (
            self.camera.width,
            self.camera.height,
            self.camera.frame_rate,
            1,
        ):
            raise CaptureBackendError("GStreamer negotiated a mode different from the validated exact V4L2 tuple")
        if self.camera.format in {CameraFormat.YUYV, CameraFormat.NV12}:
            expected_format = "YUY2" if self.camera.format is CameraFormat.YUYV else "NV12"
            negotiated_format = str(structure.get_value("format"))
            if negotiated_format != expected_format:
                raise CaptureBackendError(
                    f"GStreamer negotiated raw format {negotiated_format!r}; expected {expected_format!r}"
                )
        if self.camera.format is not CameraFormat.H264:
            self.h264_profile_detail = "not an H.264 source"
            return
        profile = structure.get_value("profile")
        if profile is None:
            self.h264_profile_status = H264ProfileStatus.UNVALIDATED
            self.h264_profile_detail = "driver/parser did not expose an H.264 profile"
        elif str(profile).lower() in {"baseline", "constrained-baseline", "main", "high"}:
            self.h264_profile_status = H264ProfileStatus.UNVALIDATED
            self.h264_profile_detail = (
                f"negotiated profile={profile}; B-frames, keyframe interval, and bitrate were not verified"
            )
        else:
            self.h264_profile_status = H264ProfileStatus.FAILED
            self.h264_profile_detail = f"unsupported negotiated profile={profile}"
            raise CaptureBackendError(self.h264_profile_detail)

    def _verify_instantiated_properties(self) -> None:
        if self._pipeline is None:
            raise CaptureBackendError("physical pipeline is unavailable for property verification")

        def element(name: str) -> Any:
            value = self._pipeline.get_by_name(name)
            if value is None:
                raise CaptureBackendError(f"physical pipeline lacks required element {name}")
            return value

        cv_queue = element("cv_queue")
        sink = element("sink")
        source = element("device_source")
        if self.resolved_device is None:
            raise CaptureBackendError("physical device identity was lost before property verification")
        expectations = (
            (source, "device", str(self.resolved_device.resolved_path)),
            (source, "do-timestamp", True),
            (cv_queue, "max-size-buffers", 1),
            (cv_queue, "max-size-bytes", 0),
            (cv_queue, "max-size-time", 0),
            (sink, "max-buffers", 1),
            (sink, "drop", True),
            (sink, "sync", False),
        )
        for target, name, expected in expectations:
            actual = target.get_property(name)
            if actual != expected:
                raise CaptureBackendError(f"pipeline property {name}={actual!r}; expected {expected!r}")
        if "downstream" not in str(cv_queue.get_property("leaky")).lower() and int(cv_queue.get_property("leaky")) != 2:
            raise CaptureBackendError("CV queue is not downstream-leaky")
        if self.surface_stream is None:
            return
        rtp_queue = element("rtp_queue")
        pay = element("pay")
        udp_sink = element("udp_sink")
        stream_expectations: tuple[tuple[Any, str, object], ...] = (
            (rtp_queue, "max-size-buffers", 2),
            (rtp_queue, "max-size-bytes", 0),
            (rtp_queue, "max-size-time", 0),
            (pay, "pt", self.surface_stream.payload_type),
            (pay, "mtu", PRODUCTION_RTP_MTU),
            (pay, "ssrc", self.surface_stream.ssrc),
            (udp_sink, "host", self.surface_stream.host),
            (udp_sink, "port", self.surface_stream.port),
            (udp_sink, "sync", False),
            (udp_sink, "async", False),
        )
        for target, name, expected_value in stream_expectations:
            actual = target.get_property(name)
            if actual != expected_value:
                raise CaptureBackendError(f"pipeline property {name}={actual!r}; expected {expected_value!r}")
        if (
            "downstream" not in str(rtp_queue.get_property("leaky")).lower()
            and int(rtp_queue.get_property("leaky")) != 2
        ):
            raise CaptureBackendError("RTP queue is not downstream-leaky")
        if self.camera.format is not CameraFormat.MJPEG and pay.get_property("config-interval") != 1:
            raise CaptureBackendError("H.264 RTP config-interval is not 1")
        if self.camera.format in {CameraFormat.YUYV, CameraFormat.NV12}:
            encoder = element("encoder")
            for name, expected in (
                ("key-int-max", self.camera.frame_rate),
                ("bframes", 0),
                ("byte-stream", True),
            ):
                actual = encoder.get_property(name)
                if actual != expected:
                    raise CaptureBackendError(f"software encoder property {name}={actual!r}; expected {expected!r}")

    def start(self) -> None:
        if self.running:
            return
        report = self.device_probe.validate(self.camera_id, self.camera, open_mode=False)
        self.resolved_device = report.device
        try:
            super().start()
            self._verify_instantiated_properties()
            self._verify_negotiated_mode()
        except BaseException:
            try:
                self.stop()
            except Exception:
                pass
            raise

    def stop(self) -> None:
        try:
            super().stop()
        finally:
            self.resolved_device = None


class SyntheticCaptureBackend:
    """Dependency-free paced source for full-process simulation.

    It implements the normal capture boundary, so camera service recovery,
    shared-memory publication, module readiness, and shutdown remain unchanged.
    ``disconnect_after_frames`` is an explicit simulation hook; a replacement
    backend created by the service resumes with a fresh instance.
    """

    def __init__(
        self,
        width: int,
        height: int,
        frame_rate: int,
        *,
        disconnect_after_frames: int | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        time_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if min(width, height, frame_rate) <= 0:
            raise ValueError("synthetic capture dimensions and frame rate must be positive")
        if disconnect_after_frames is not None and disconnect_after_frames <= 0:
            raise ValueError("disconnect_after_frames must be positive")
        self.width = width
        self.height = height
        self.frame_rate = frame_rate
        self.disconnect_after_frames = disconnect_after_frames
        self._monotonic = monotonic
        self._monotonic_ns = monotonic_ns
        self._time_ns = time_ns
        self._sleep = sleep
        self._running = False
        self._next_frame = 0.0
        self._count = 0
        self._pixels = bytes(width * height * 3)

    @property
    def running(self) -> bool:
        return self._running

    def start(self) -> None:
        self._running = True
        self._next_frame = self._monotonic()

    def poll(self, timeout_seconds: float) -> CapturedFrame | None:
        if not 0 <= timeout_seconds <= 0.250:
            raise ValueError("capture poll timeout must be between zero and 250 ms")
        if not self._running:
            raise CaptureBackendError("synthetic capture backend is not running")
        if self.disconnect_after_frames is not None and self._count >= self.disconnect_after_frames:
            raise CaptureBackendError("injected camera disconnect")
        now = self._monotonic()
        remaining = self._next_frame - now
        if remaining > timeout_seconds:
            self._sleep(timeout_seconds)
            return None
        if remaining > 0:
            self._sleep(remaining)
        captured = CapturedFrame(
            self._pixels,
            self.width,
            self.height,
            self.width * 3,
            PixelFormat.BGR8,
            self._time_ns(),
            self._monotonic_ns(),
            None,
        )
        self._count += 1
        self._next_frame += 1.0 / self.frame_rate
        return captured

    def stop(self) -> None:
        self._running = False


__all__ = [
    "CaptureBackend",
    "CaptureBackendError",
    "CaptureBackendUnavailable",
    "CapturedFrame",
    "DisconnectAfterFramesBackend",
    "GStreamerCaptureBackend",
    "PRODUCTION_RTP_MTU",
    "SyntheticCaptureBackend",
    "SurfaceRtpStream",
    "V4L2CaptureBackend",
]
