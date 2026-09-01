"""Deterministic reference module exercising the normal runner path."""

from __future__ import annotations

from google.protobuf.message import Message
from purdue_rov.cv.v1 import bounding_box_pb2

from .base import CVModule, DynamicConfig, Frame, ModuleContext


class EchoModule(CVModule):
    """Emit one full-frame detection carrying the configured threshold."""

    requires_artifact = False

    def __init__(self) -> None:
        self._context: ModuleContext | None = None
        self._confidence_threshold = 0.0
        self.started = False
        self.stopped = False
        self.shutdown_called = False

    def initialize(self, context: ModuleContext) -> None:
        if context.task.payload_type != "bounding_boxes_v1":
            raise ValueError("EchoModule requires payload_type=bounding_boxes_v1")
        self._context = context
        self._confidence_threshold = context.task.dynamic.confidence_threshold

    def process(self, frame: Frame) -> list[Message]:
        if self._context is None:
            raise RuntimeError("EchoModule has not been initialized")
        return [
            bounding_box_pb2.BoundingBoxResult(
                camera_id=frame.camera_id,
                camera_session_id=frame.camera_session_id,
                frame_number=frame.frame_number,
                capture_time_unix_ns=frame.capture_time_unix_ns,
                detections=[
                    bounding_box_pb2.Detection(
                        class_id=0,
                        class_name="echo",
                        confidence=self._confidence_threshold,
                        x=0.0,
                        y=0.0,
                        width=1.0,
                        height=1.0,
                    )
                ],
            )
        ]

    def apply_dynamic_config(self, config: DynamicConfig) -> None:
        if "confidence_threshold" not in config:
            return
        value = config.get("confidence_threshold")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("confidence_threshold must be numeric")
        threshold = float(value)
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("confidence_threshold must be between zero and one")
        self._confidence_threshold = threshold

    def on_start(self) -> None:
        self.started = True
        self.stopped = False

    def on_stop(self) -> None:
        self.started = False
        self.stopped = True

    def shutdown(self) -> None:
        self.shutdown_called = True


__all__ = ["EchoModule"]
