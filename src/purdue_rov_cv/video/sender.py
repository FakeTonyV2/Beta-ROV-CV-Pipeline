"""Bounded simulated H.264/RTP sender with canonical FrameIndex mapping."""

from __future__ import annotations

import importlib
import queue
import time
from collections.abc import Callable
from threading import Event
from typing import Any

import zmq
from purdue_rov.cv.v1 import frame_index_pb2

from purdue_rov_cv.module_runner.publisher import configure_result_publisher
from purdue_rov_cv.runtime.envelope import EnvelopeBuilder, EnvelopeBuildError
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.shutdown import ShutdownToken

from .mapping import SENDER_MAPPING_CAPACITY, RtpFrameIndexMapper


class GStreamerRtpSender:
    """Real ``videotestsrc`` sender used by deterministic process integration.

    The source-pad PTS identifies a canonical camera frame. The payloader probe
    reads the produced RTP header and publishes only the first packet for each
    `(ssrc, timestamp)`, so fragmented access units still produce one index.
    """

    def __init__(
        self,
        camera_id: str,
        camera_session_id: bytes,
        host: str,
        port: int,
        payload_type: int,
        *,
        width: int = 320,
        height: int = 240,
        frame_rate: int = 30,
        ssrc: int = 0x50555244,
        mtu: int = 500,
        on_frame_index: Callable[[frame_index_pb2.FrameIndex], None],
        time_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if len(camera_session_id) != 16:
            raise ValueError("camera_session_id must be a 16-byte UUID")
        self.camera_id = camera_id
        self.camera_session_id = camera_session_id
        self.host = host
        self.port = port
        self.payload_type = payload_type
        self.width = width
        self.height = height
        self.frame_rate = frame_rate
        self.ssrc = ssrc & 0xFFFFFFFF
        self.mtu = mtu
        self._on_frame_index = on_frame_index
        self._time_ns = time_ns
        self._monotonic_ns = monotonic_ns
        self._gst: Any = None
        self._gst_rtp: Any = None
        self._pipeline: Any = None
        self._probe_bindings: list[tuple[Any, int]] = []
        self._mapper = RtpFrameIndexMapper(
            camera_id,
            camera_session_id,
            time_ns=time_ns,
            monotonic_ns=monotonic_ns,
        )

    @property
    def running(self) -> bool:
        return self._pipeline is not None

    @property
    def mapping_count(self) -> int:
        return self._mapper.encoded_mapping_count

    def pipeline_description(self) -> str:
        return (
            "videotestsrc name=source is-live=true do-timestamp=true pattern=smpte "
            f"! video/x-raw,width={self.width},height={self.height},framerate={self.frame_rate}/1 "
            "! videoconvert ! x264enc name=encoder tune=zerolatency speed-preset=ultrafast "
            "key-int-max=30 bframes=0 byte-stream=true "
            "! h264parse ! rtph264pay name=pay config-interval=1 "
            f"pt={self.payload_type} ssrc={self.ssrc} mtu={self.mtu} "
            f"! udpsink host={self.host} port={self.port} sync=false async=false"
        )

    def _load_gst(self) -> tuple[Any, Any]:
        try:
            gi = importlib.import_module("gi")
            gi.require_version("Gst", "1.0")
            gi.require_version("GstRtp", "1.0")
            gst = importlib.import_module("gi.repository.Gst")
            gst_rtp = importlib.import_module("gi.repository.GstRtp")
        except (ImportError, AttributeError, ValueError) as error:
            raise RuntimeError("ABI-compatible PyGObject Gst/GstRtp bindings are unavailable") from error
        gst.init(None)
        return gst, gst_rtp

    def _source_probe(self, _pad: Any, info: Any) -> Any:
        buffer = info.get_buffer()
        if buffer is not None:
            self._mapper.observe_source(int(buffer.pts))
        return self._gst.PadProbeReturn.OK

    def _encoder_input_probe(self, _pad: Any, info: Any) -> Any:
        buffer = info.get_buffer()
        if buffer is not None:
            self._mapper.observe_encoder_input(int(buffer.pts))
        return self._gst.PadProbeReturn.OK

    def _encoder_output_probe(self, _pad: Any, info: Any) -> Any:
        buffer = info.get_buffer()
        if buffer is not None:
            self._mapper.observe_encoded_output(int(buffer.pts))
        return self._gst.PadProbeReturn.OK

    def _publish_packet(self, buffer: Any) -> None:
        success, packet = self._gst_rtp.RTPBuffer.map(buffer, self._gst.MapFlags.READ)
        if not success:
            return
        try:
            timestamp = int(packet.get_timestamp())
            ssrc = int(packet.get_ssrc())
            payload_type = int(packet.get_payload_type())
        finally:
            packet.unmap()
        value = self._mapper.frame_index_for_packet(
            int(buffer.pts),
            ssrc,
            timestamp,
            payload_type,
        )
        if value is None:
            return
        self._on_frame_index(value)

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
            source = pipeline.get_by_name("source")
            encoder = pipeline.get_by_name("encoder")
            pay = pipeline.get_by_name("pay")
            if source is None or encoder is None or pay is None:
                raise RuntimeError("simulated RTP sender lacks source, encoder, or payloader")
            self._gst = gst
            self._gst_rtp = gst_rtp
            self._pipeline = pipeline
            source_pad = source.get_static_pad("src")
            encoder_sink_pad = encoder.get_static_pad("sink")
            encoder_pad = encoder.get_static_pad("src")
            pay_pad = pay.get_static_pad("src")
            self._probe_bindings.append(
                (source_pad, int(source_pad.add_probe(gst.PadProbeType.BUFFER, self._source_probe)))
            )
            self._probe_bindings.append(
                (
                    encoder_sink_pad,
                    int(encoder_sink_pad.add_probe(gst.PadProbeType.BUFFER, self._encoder_input_probe)),
                )
            )
            self._probe_bindings.append(
                (
                    encoder_pad,
                    int(encoder_pad.add_probe(gst.PadProbeType.BUFFER, self._encoder_output_probe)),
                )
            )
            self._probe_bindings.append(
                (
                    pay_pad,
                    int(
                        pay_pad.add_probe(
                            gst.PadProbeType.BUFFER | gst.PadProbeType.BUFFER_LIST,
                            self._pay_probe,
                        )
                    ),
                )
            )
            if pipeline.set_state(gst.State.PLAYING) == gst.StateChangeReturn.FAILURE:
                raise RuntimeError("simulated RTP sender failed to enter PLAYING")
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
            raise RuntimeError(f"GStreamer sender error: {error}; {debug or 'no debug detail'}")
        raise RuntimeError("GStreamer sender reached end of stream")

    def stop(self) -> None:
        pipeline, gst = self._pipeline, self._gst
        self._pipeline = None
        for pad, probe_id in self._probe_bindings:
            try:
                pad.remove_probe(probe_id)
            except Exception:
                pass
        self._probe_bindings.clear()
        if pipeline is not None and gst is not None:
            pipeline.set_state(gst.State.NULL)
            pipeline.get_state(1_000_000_000)
        self._mapper.clear()
        self._gst_rtp = None
        self._gst = None


class FrameIndexPublisher:
    """Socket-confined nonblocking camera FrameIndex publisher."""

    def __init__(
        self,
        endpoint: str,
        camera_id: str,
        *,
        metrics: RuntimeMetrics,
        shutdown: ShutdownToken,
        sequence: PublisherSequence | None = None,
        ready: Event | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.camera_id = camera_id
        self.metrics = metrics
        self.shutdown = shutdown
        self.sequence = sequence or PublisherSequence()
        self.ready = ready or Event()
        self._queue: queue.Queue[frame_index_pb2.FrameIndex] = queue.Queue(maxsize=5)

    def publish(self, value: frame_index_pb2.FrameIndex) -> None:
        try:
            self._queue.put_nowait(value)
            return
        except queue.Full:
            pass
        try:
            self._queue.get_nowait()
        except queue.Empty:
            pass
        else:
            self.metrics.increment("zmq_send_dropped")
        try:
            self._queue.put_nowait(value)
        except queue.Full:
            self.metrics.increment("zmq_send_dropped")

    def run(self) -> None:
        context = zmq.Context()
        socket: zmq.Socket[bytes] | None = None
        try:
            socket = context.socket(zmq.PUB)
            configure_result_publisher(socket)
            socket.connect(self.endpoint)
            builder = EnvelopeBuilder(self.sequence)
            self.ready.set()
            while not self.shutdown.is_requested:
                try:
                    value = self._queue.get(timeout=0.100)
                except queue.Empty:
                    continue
                try:
                    built = builder.build(
                        topic=f"cv.frame_index.{self.camera_id}",
                        payload_type="frame_index_v1",
                        payload=value,
                        task_id="video_sender",
                        source_id=self.camera_id,
                        camera_id=self.camera_id,
                        camera_session_id=value.camera_session_id,
                        frame_number=value.frame_number,
                        capture_time_unix_ns=value.capture_time_unix_ns,
                    )
                    socket.send_multipart(list(built.frames), flags=zmq.DONTWAIT)
                except (EnvelopeBuildError, zmq.Again):
                    self.metrics.increment("zmq_send_dropped")
                    continue
                self.metrics.increment("messages_sent")
        finally:
            self.ready.set()
            if socket is not None:
                socket.close(linger=0)
            context.term()


__all__ = ["FrameIndexPublisher", "GStreamerRtpSender", "SENDER_MAPPING_CAPACITY"]
