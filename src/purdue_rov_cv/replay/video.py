"""Local Matroska/H.264 replay through the Phase 7 decoded-frame shape."""

from __future__ import annotations

import importlib
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.video.models import DecodedVideoFrame

from .structured import ReplayRate


class MatroskaVideoReplay:
    """Demux and decode recorded H.264; never encode during replay."""

    def __init__(
        self,
        path: Path,
        on_frame: Callable[[DecodedVideoFrame], None],
        *,
        rate: ReplayRate = ReplayRate.NORMAL,
        shutdown: ShutdownToken | None = None,
    ) -> None:
        self.path = path
        self.on_frame = on_frame
        self.rate = rate
        self.shutdown = shutdown or ShutdownToken()
        self._gst: Any = None
        self._pipeline: Any = None
        self._sink: Any = None

    def pipeline_description(self) -> str:
        location = str(self.path).replace("\\", "\\\\").replace('"', '\\"')
        sync = "false" if self.rate is ReplayRate.MAXIMUM else "true"
        return (
            f'filesrc location="{location}" ! matroskademux ! h264parse ! avdec_h264 '
            "! videoconvert ! video/x-raw,format=BGR "
            f"! appsink name=replay_sink emit-signals=true max-buffers=2 drop=false sync={sync}"
        )

    def _load_gst(self) -> Any:
        gi = importlib.import_module("gi")
        gi.require_version("Gst", "1.0")
        gst = importlib.import_module("gi.repository.Gst")
        gst.init(None)
        return gst

    def _sample(self, sink: Any) -> Any:
        sample = sink.emit("pull-sample")
        if sample is None:
            return self._gst.FlowReturn.ERROR
        buffer = sample.get_buffer()
        structure = sample.get_caps().get_structure(0)
        width, height = int(structure.get_value("width")), int(structure.get_value("height"))
        mapped, mapping = buffer.map(self._gst.MapFlags.READ)
        if not mapped:
            return self._gst.FlowReturn.ERROR
        try:
            pixels = bytes(mapping.data)
        finally:
            buffer.unmap(mapping)
        if width <= 0 or height <= 0 or len(pixels) % height:
            return self._gst.FlowReturn.ERROR
        self.on_frame(
            DecodedVideoFrame(
                pixels,
                width,
                height,
                len(pixels) // height,
                "BGR",
                int(buffer.pts),
                0,
                0,
                time.monotonic_ns(),
            )
        )
        return self._gst.FlowReturn.OK

    def run(self) -> int:
        if not self.path.is_file():
            raise FileNotFoundError(f"recorded video does not exist: {self.path}")
        gst = self._load_gst()
        pipeline = gst.parse_launch(self.pipeline_description())
        sink = pipeline.get_by_name("replay_sink")
        if sink is None:
            raise RuntimeError("video replay pipeline lacks appsink")
        self._gst, self._pipeline, self._sink = gst, pipeline, sink
        frames = 0

        def deliver(value: Any) -> Any:
            nonlocal frames
            result = self._sample(value)
            if result == gst.FlowReturn.OK:
                frames += 1
            return result

        handler = int(sink.connect("new-sample", deliver))
        try:
            if pipeline.set_state(gst.State.PAUSED) == gst.StateChangeReturn.FAILURE:
                raise RuntimeError("video replay could not preroll")
            pipeline.get_state(2_000_000_000)
            multiplier = self.rate.multiplier
            if multiplier is not None and multiplier != 1.0:
                ok = pipeline.seek(
                    multiplier,
                    gst.Format.TIME,
                    gst.SeekFlags.FLUSH | gst.SeekFlags.ACCURATE,
                    gst.SeekType.SET,
                    0,
                    gst.SeekType.NONE,
                    -1,
                )
                if not ok:
                    raise RuntimeError(f"video replay does not support {self.rate.value}x seek rate")
            if pipeline.set_state(gst.State.PLAYING) == gst.StateChangeReturn.FAILURE:
                raise RuntimeError("video replay could not enter PLAYING")
            bus = pipeline.get_bus()
            while not self.shutdown.is_requested:
                message = bus.timed_pop_filtered(
                    50_000_000,
                    gst.MessageType.ERROR | gst.MessageType.EOS,
                )
                if message is None:
                    continue
                if message.type == gst.MessageType.ERROR:
                    error, debug = message.parse_error()
                    raise RuntimeError(f"video replay failed: {error}; {debug or 'no detail'}")
                break
            return frames
        finally:
            sink.disconnect(handler)
            pipeline.set_state(gst.State.NULL)
            pipeline.get_state(1_000_000_000)
            self._pipeline = self._sink = self._gst = None

    def request_shutdown(self) -> None:
        self.shutdown.request("video replay interrupted")


__all__ = ["MatroskaVideoReplay"]
