"""Real bounded GStreamer H.264/RTP surface receiver backend."""

from __future__ import annotations

import importlib
import time
from collections import OrderedDict
from collections.abc import Callable
from threading import RLock
from typing import Any

from .models import DecodedVideoFrame, EncodedAccessUnit

RTP_RECEIVE_BUFFER_BYTES = 4 * 1024 * 1024
RTP_JITTER_LATENCY_MS = 50
PTS_IDENTITY_CAPACITY = 512


class VideoReceiverBackendError(RuntimeError):
    pass


class VideoReceiverBackendUnavailable(VideoReceiverBackendError):
    pass


class GStreamerRtpReceiver:
    """Receive one RTP/H.264 stream and preserve its RTP key through decode.

    A probe after ``rtpjitterbuffer`` maps the jitterbuffer-assigned PTS to the
    RTP `(ssrc, timestamp)` still present on that packet. Depayloading, parsing,
    and decoding preserve PTS, so the decoded appsink uses that bounded mapping
    to recover the original RTP identity without wall-clock approximation.
    """

    def __init__(
        self,
        port: int,
        payload_type: int,
        *,
        on_packet: Callable[[int, int, int], None],
        on_packet_lost: Callable[[int], None],
        on_decoded: Callable[[DecodedVideoFrame], None],
        on_invalid_decoded: Callable[[str], None],
        on_encoded: Callable[[EncodedAccessUnit], None],
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self.port = port
        self.payload_type = payload_type
        self._on_packet = on_packet
        self._on_packet_lost = on_packet_lost
        self._on_decoded = on_decoded
        self._on_invalid_decoded = on_invalid_decoded
        self._on_encoded = on_encoded
        self._monotonic_ns = monotonic_ns
        self._gst: Any = None
        self._gst_rtp: Any = None
        self._pipeline: Any = None
        self._decoded_sink: Any = None
        self._encoded_sink: Any = None
        self._probe_bindings: list[tuple[Any, int]] = []
        self._signal_bindings: list[tuple[Any, int]] = []
        self._pts_identities: OrderedDict[int, tuple[int, int]] = OrderedDict()
        self._lock = RLock()

    @property
    def running(self) -> bool:
        return self._pipeline is not None

    def pipeline_description(self) -> str:
        caps = (
            f"application/x-rtp,media=video,encoding-name=H264,payload=(int){self.payload_type},clock-rate=(int)90000"
        )
        return (
            f'udpsrc name=rtp_source port={self.port} buffer-size={RTP_RECEIVE_BUFFER_BYTES} caps="{caps}" '
            f"! rtpjitterbuffer name=jitter latency={RTP_JITTER_LATENCY_MS} drop-on-latency=true do-lost=true "
            "! rtph264depay name=depay ! h264parse name=parser ! tee name=encoded_tee "
            "encoded_tee. ! queue name=decode_queue max-size-buffers=1 max-size-bytes=0 max-size-time=0 "
            "leaky=downstream ! avdec_h264 ! videoconvert ! video/x-raw,format=BGR "
            "! appsink name=decoded_sink emit-signals=true max-buffers=1 drop=true sync=false "
            "encoded_tee. ! queue name=encoded_queue max-size-buffers=8 max-size-bytes=0 max-size-time=0 "
            "leaky=downstream ! appsink name=encoded_sink emit-signals=true max-buffers=8 drop=true sync=false"
        )

    def _load_gst(self) -> tuple[Any, Any]:
        try:
            gi = importlib.import_module("gi")
            gi.require_version("Gst", "1.0")
            gi.require_version("GstRtp", "1.0")
            gst = importlib.import_module("gi.repository.Gst")
            gst_rtp = importlib.import_module("gi.repository.GstRtp")
        except (ImportError, AttributeError, ValueError) as error:
            raise VideoReceiverBackendUnavailable(
                "ABI-compatible PyGObject Gst/GstRtp bindings are unavailable"
            ) from error
        gst.init(None)
        return gst, gst_rtp

    def _read_rtp(self, buffer: Any) -> tuple[int, int] | None:
        success, packet = self._gst_rtp.RTPBuffer.map(buffer, self._gst.MapFlags.READ)
        if not success:
            return None
        try:
            return int(packet.get_ssrc()), int(packet.get_timestamp())
        finally:
            packet.unmap()

    def _packet_probe(self, _pad: Any, info: Any) -> Any:
        buffer = info.get_buffer()
        if buffer is not None:
            identity = self._read_rtp(buffer)
            if identity is not None:
                self._on_packet(identity[0], identity[1], self._monotonic_ns())
        return self._gst.PadProbeReturn.OK

    def _jitter_probe(self, _pad: Any, info: Any) -> Any:
        if info.type & self._gst.PadProbeType.BUFFER:
            buffer = info.get_buffer()
            if buffer is not None:
                identity = self._read_rtp(buffer)
                pts = int(buffer.pts)
                if identity is not None and pts != int(self._gst.CLOCK_TIME_NONE):
                    with self._lock:
                        self._pts_identities[pts] = identity
                        self._pts_identities.move_to_end(pts)
                        while len(self._pts_identities) > PTS_IDENTITY_CAPACITY:
                            self._pts_identities.popitem(last=False)
        if info.type & self._gst.PadProbeType.EVENT_DOWNSTREAM:
            event = info.get_event()
            if event is not None and event.type == self._gst.EventType.CUSTOM_DOWNSTREAM:
                structure = event.get_structure()
                if structure is not None and structure.get_name() == "GstRTPPacketLost":
                    self._on_packet_lost(1)
        return self._gst.PadProbeReturn.OK

    def _decoded_sample(self, sink: Any) -> Any:
        sample = sink.emit("pull-sample")
        if sample is None:
            self._on_invalid_decoded("decoded appsink returned no sample")
            return self._gst.FlowReturn.ERROR
        buffer = sample.get_buffer()
        caps = sample.get_caps()
        structure = caps.get_structure(0)
        width = int(structure.get_value("width"))
        height = int(structure.get_value("height"))
        pixel_format = str(structure.get_value("format"))
        pts = int(buffer.pts)
        with self._lock:
            identity = self._pts_identities.pop(pts, None)
        if width <= 0 or height <= 0 or pixel_format != "BGR" or identity is None:
            self._on_invalid_decoded("decoded frame lacks valid caps or preserved RTP identity")
            return self._gst.FlowReturn.OK
        mapped, mapping = buffer.map(self._gst.MapFlags.READ)
        if not mapped:
            self._on_invalid_decoded("decoded frame buffer could not be mapped")
            return self._gst.FlowReturn.OK
        try:
            pixels = bytes(mapping.data)
        finally:
            buffer.unmap(mapping)
        if len(pixels) % height:
            self._on_invalid_decoded("decoded frame has a non-integral row stride")
            return self._gst.FlowReturn.OK
        self._on_decoded(
            DecodedVideoFrame(
                pixels,
                width,
                height,
                len(pixels) // height,
                pixel_format,
                pts,
                identity[0],
                identity[1],
                self._monotonic_ns(),
            )
        )
        return self._gst.FlowReturn.OK

    def _encoded_sample(self, sink: Any) -> Any:
        sample = sink.emit("pull-sample")
        if sample is None:
            return self._gst.FlowReturn.ERROR
        buffer = sample.get_buffer()
        mapped, mapping = buffer.map(self._gst.MapFlags.READ)
        if not mapped:
            return self._gst.FlowReturn.OK
        try:
            data = bytes(mapping.data)
        finally:
            buffer.unmap(mapping)
        self._on_encoded(
            EncodedAccessUnit(
                data,
                int(buffer.pts),
                bool(buffer.has_flags(self._gst.BufferFlags.DELTA_UNIT)),
            )
        )
        return self._gst.FlowReturn.OK

    def _add_probe(self, pad: Any, probe_type: Any, callback: Callable[..., Any]) -> None:
        if pad is None:
            raise VideoReceiverBackendError("receiver pipeline is missing a required probe pad")
        probe_id = int(pad.add_probe(probe_type, callback))
        self._probe_bindings.append((pad, probe_id))

    def start(self) -> None:
        if self.running:
            return
        gst, gst_rtp = self._load_gst()
        try:
            pipeline = gst.parse_launch(self.pipeline_description())
            source = pipeline.get_by_name("rtp_source")
            jitter = pipeline.get_by_name("jitter")
            decoded_sink = pipeline.get_by_name("decoded_sink")
            encoded_sink = pipeline.get_by_name("encoded_sink")
            if None in {source, jitter, decoded_sink, encoded_sink}:
                raise VideoReceiverBackendError("receiver pipeline is missing a required element")
            self._gst = gst
            self._gst_rtp = gst_rtp
            self._pipeline = pipeline
            self._decoded_sink = decoded_sink
            self._encoded_sink = encoded_sink
            self._add_probe(source.get_static_pad("src"), gst.PadProbeType.BUFFER, self._packet_probe)
            self._add_probe(
                jitter.get_static_pad("src"),
                gst.PadProbeType.BUFFER | gst.PadProbeType.EVENT_DOWNSTREAM,
                self._jitter_probe,
            )
            self._signal_bindings.append((decoded_sink, int(decoded_sink.connect("new-sample", self._decoded_sample))))
            self._signal_bindings.append((encoded_sink, int(encoded_sink.connect("new-sample", self._encoded_sample))))
            result = pipeline.set_state(gst.State.PLAYING)
            if result == gst.StateChangeReturn.FAILURE:
                raise VideoReceiverBackendError("receiver pipeline failed to enter PLAYING")
        except Exception:
            self.stop()
            raise

    def check_bus(self) -> None:
        if self._pipeline is None:
            return
        message = self._pipeline.get_bus().timed_pop_filtered(
            0,
            self._gst.MessageType.ERROR | self._gst.MessageType.EOS,
        )
        if message is None:
            return
        if message.type == self._gst.MessageType.ERROR:
            error, debug = message.parse_error()
            raise VideoReceiverBackendError(f"GStreamer receiver error: {error}; {debug or 'no debug detail'}")
        raise VideoReceiverBackendError("GStreamer receiver reached end of stream")

    def stop(self) -> None:
        pipeline = self._pipeline
        gst = self._gst
        self._pipeline = None
        for sink, handler_id in self._signal_bindings:
            try:
                sink.disconnect(handler_id)
            except Exception:
                pass
        self._signal_bindings.clear()
        for pad, probe_id in self._probe_bindings:
            try:
                pad.remove_probe(probe_id)
            except Exception:
                pass
        self._probe_bindings.clear()
        with self._lock:
            self._pts_identities.clear()
        if pipeline is not None and gst is not None:
            result = pipeline.set_state(gst.State.NULL)
            if result == gst.StateChangeReturn.FAILURE:
                raise VideoReceiverBackendError("receiver pipeline rejected NULL state")
            _result, current, _pending = pipeline.get_state(1_000_000_000)
            if current != gst.State.NULL:
                raise VideoReceiverBackendError("receiver pipeline did not reach NULL state")
        self._decoded_sink = None
        self._encoded_sink = None
        self._gst_rtp = None
        self._gst = None


__all__ = [
    "GStreamerRtpReceiver",
    "PTS_IDENTITY_CAPACITY",
    "RTP_JITTER_LATENCY_MS",
    "RTP_RECEIVE_BUFFER_BYTES",
    "VideoReceiverBackendError",
    "VideoReceiverBackendUnavailable",
]
