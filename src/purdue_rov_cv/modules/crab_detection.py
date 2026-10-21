"""ONNX YOLO detector for the three configured crab classes."""

from __future__ import annotations

import ast
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
from google.protobuf.message import Message
from purdue_rov.cv.v1 import bounding_box_pb2

from .base import CVModule, DynamicConfig, Frame, ModuleContext

CLASS_NAMES = ("European_Green_Crab", "Rock_Crab", "Jonah_Crab")
INPUT_SIZE = 640
OUTPUT_ROWS = 4 + len(CLASS_NAMES)
NMS_IOU_THRESHOLD = 0.45
_PAD_VALUE = 114.0


def _create_session(model_path: Path) -> Any:
    """Import ONNX Runtime lazily so non-inference tools can import the SDK."""
    try:
        import onnxruntime as ort  # type: ignore[import-untyped]
    except ImportError as error:
        raise RuntimeError("CrabDetectionModule requires the onnxruntime package") from error
    return ort.InferenceSession(str(model_path), providers=["CPUExecutionProvider"])


def _metadata_names(value: str | None) -> tuple[str, ...] | None:
    if value is None:
        return None
    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, dict):
            pairs = sorted(parsed.items(), key=lambda item: int(item[0]))
            if [int(key) for key, _ in pairs] != list(range(len(pairs))):
                return None
            return tuple(str(name) for _, name in pairs)
        if isinstance(parsed, (list, tuple)):
            return tuple(str(name) for name in parsed)
    except (ValueError, SyntaxError, TypeError):
        return None
    return None


def _resize_bilinear(image: np.ndarray, height: int, width: int) -> np.ndarray:
    """Small NumPy-only bilinear resize, avoiding an undeclared OpenCV dependency."""
    source_height, source_width, _ = image.shape
    y = np.clip((np.arange(height, dtype=np.float32) + 0.5) * source_height / height - 0.5, 0, source_height - 1)
    x = np.clip((np.arange(width, dtype=np.float32) + 0.5) * source_width / width - 0.5, 0, source_width - 1)
    y0, x0 = y.astype(np.intp), x.astype(np.intp)
    y1, x1 = np.minimum(y0 + 1, source_height - 1), np.minimum(x0 + 1, source_width - 1)
    wy, wx = (y - y0)[..., None, None], (x - x0)[None, ..., None]
    top = image[y0[:, None], x0[None, :]].astype(np.float32)
    bottom = image[y1[:, None], x0[None, :]].astype(np.float32)
    top_right = image[y0[:, None], x1[None, :]].astype(np.float32)
    bottom_right = image[y1[:, None], x1[None, :]].astype(np.float32)
    return (
        top * (1.0 - wy) * (1.0 - wx) + top_right * (1.0 - wy) * wx + bottom * wy * (1.0 - wx) + bottom_right * wy * wx
    )


def _prepare_input(pixels: np.ndarray) -> tuple[np.ndarray, float, float, float]:
    if pixels.dtype != np.uint8:
        raise ValueError(f"frame pixels must be uint8, got {pixels.dtype}")
    if pixels.ndim == 2:
        image = np.repeat(pixels[:, :, None], 3, axis=2)
    elif pixels.ndim == 3 and pixels.shape[2] == 3:
        # The camera shared-memory contract currently publishes BGR8.
        image = pixels[:, :, ::-1]
    else:
        raise ValueError(f"frame pixels must be HxW grayscale or HxWx3 BGR, got {pixels.shape}")
    height, width = image.shape[:2]
    if height <= 0 or width <= 0:
        raise ValueError("frame dimensions must be positive")
    scale = min(INPUT_SIZE / width, INPUT_SIZE / height)
    resized_width = max(1, int(round(width * scale)))
    resized_height = max(1, int(round(height * scale)))
    pad_x = (INPUT_SIZE - resized_width) / 2.0
    pad_y = (INPUT_SIZE - resized_height) / 2.0
    resized = _resize_bilinear(image, resized_height, resized_width)
    canvas = np.full((INPUT_SIZE, INPUT_SIZE, 3), _PAD_VALUE, dtype=np.float32)
    left, top = int(round(pad_x - 0.1)), int(round(pad_y - 0.1))
    canvas[top : top + resized_height, left : left + resized_width] = resized
    tensor = np.ascontiguousarray(canvas.transpose(2, 0, 1)[None] / 255.0, dtype=np.float32)
    return tensor, scale, float(left), float(top)


def _iou(box: np.ndarray, boxes: np.ndarray) -> np.ndarray:
    left_top = np.maximum(box[:2], boxes[:, :2])
    right_bottom = np.minimum(box[2:], boxes[:, 2:])
    size = np.maximum(0.0, right_bottom - left_top)
    intersection = size[:, 0] * size[:, 1]
    area_a = max(0.0, box[2] - box[0]) * max(0.0, box[3] - box[1])
    area_b = np.maximum(0.0, boxes[:, 2] - boxes[:, 0]) * np.maximum(0.0, boxes[:, 3] - boxes[:, 1])
    return intersection / np.maximum(area_a + area_b - intersection, 1e-9)


class CrabDetectionModule(CVModule):
    """Run the exported YOLOv8 crab detector and publish normalized boxes."""

    requires_artifact = True

    def __init__(self, *, session_factory: Callable[[Path], Any] = _create_session) -> None:
        self._session_factory = session_factory
        self._session: Any | None = None
        self._input_name = "images"
        self._output_name = "output0"
        self._confidence_threshold = 0.6

    def initialize(self, context: ModuleContext) -> None:
        if context.task.payload_type != "bounding_boxes_v1":
            raise ValueError("CrabDetectionModule requires payload_type=bounding_boxes_v1")
        artifact = context.task.artifact
        if artifact.format.value != "onnx" or artifact.runtime.value != "onnxruntime":
            raise ValueError("CrabDetectionModule requires an ONNX artifact and onnxruntime")
        session = self._session_factory(artifact.path)
        inputs, outputs = session.get_inputs(), session.get_outputs()
        if len(inputs) != 1 or inputs[0].shape != [1, 3, INPUT_SIZE, INPUT_SIZE] or inputs[0].type != "tensor(float)":
            raise ValueError("model input must be one float32 NCHW tensor with shape [1, 3, 640, 640]")
        if len(outputs) != 1 or outputs[0].shape != [1, OUTPUT_ROWS, 8400] or outputs[0].type != "tensor(float)":
            raise ValueError("model output must be one float32 tensor with shape [1, 7, 8400]")
        metadata = session.get_modelmeta().custom_metadata_map
        names = _metadata_names(metadata.get("names"))
        if names != CLASS_NAMES:
            raise ValueError(f"model class labels must be {CLASS_NAMES!r}, got {names!r}")
        if metadata.get("task") != "detect":
            raise ValueError("model metadata task must be 'detect'")
        self._input_name = inputs[0].name
        self._output_name = outputs[0].name
        self._confidence_threshold = context.task.dynamic.confidence_threshold
        self._session = session
        # A failed warm-up keeps the module out of READY/RUNNING through runner lifecycle handling.
        warmup = np.zeros((1, 3, INPUT_SIZE, INPUT_SIZE), dtype=np.float32)
        result = self._session.run([self._output_name], {self._input_name: warmup})
        if len(result) != 1 or np.asarray(result[0]).shape != (1, OUTPUT_ROWS, 8400):
            self._session = None
            raise ValueError("model warm-up returned an incompatible output tensor")
        warmup_output = np.asarray(result[0])
        if warmup_output.dtype != np.float32 or not np.isfinite(warmup_output).all():
            self._session = None
            raise ValueError("model warm-up must return finite float32 detections")

    def process(self, frame: Frame) -> list[Message]:
        if self._session is None:
            raise RuntimeError("CrabDetectionModule has not been initialized")
        tensor, scale, pad_x, pad_y = _prepare_input(frame.pixels)
        raw = self._session.run([self._output_name], {self._input_name: tensor})[0]
        detections = self._decode(raw, frame.pixels.shape[:2], scale, pad_x, pad_y)
        result = bounding_box_pb2.BoundingBoxResult(
            camera_id=frame.camera_id,
            camera_session_id=frame.camera_session_id,
            frame_number=frame.frame_number,
            capture_time_unix_ns=frame.capture_time_unix_ns,
            detections=detections,
        )
        return [result]

    def _decode(
        self, output: np.ndarray, source_shape: tuple[int, ...], scale: float, pad_x: float, pad_y: float
    ) -> list[bounding_box_pb2.Detection]:
        tensor = np.asarray(output)
        if tensor.shape != (1, OUTPUT_ROWS, 8400) or tensor.dtype != np.float32:
            raise ValueError(f"inference output must be float32 [1, 7, 8400], got {tensor.dtype} {tensor.shape}")
        if not np.isfinite(tensor).all():
            raise ValueError("inference output contains non-finite values")
        if np.any(tensor[0, 4:, :] < 0.0) or np.any(tensor[0, 4:, :] > 1.0):
            raise ValueError("inference class confidence values must be within [0, 1]")
        candidates = tensor[0].T
        class_scores = candidates[:, 4:]
        class_ids = class_scores.argmax(axis=1)
        scores = class_scores[np.arange(class_scores.shape[0]), class_ids]
        selected = np.flatnonzero(scores >= self._confidence_threshold)
        if selected.size == 0:
            return []
        xywh = candidates[selected, :4]
        boxes = np.column_stack(
            (
                xywh[:, 0] - xywh[:, 2] / 2,
                xywh[:, 1] - xywh[:, 3] / 2,
                xywh[:, 0] + xywh[:, 2] / 2,
                xywh[:, 1] + xywh[:, 3] / 2,
            )
        )
        selected_scores = scores[selected]
        selected_classes = class_ids[selected]
        keep: list[int] = []
        for class_id in range(len(CLASS_NAMES)):
            indexes = np.flatnonzero(selected_classes == class_id)
            indexes = indexes[np.argsort(-selected_scores[indexes], kind="stable")]
            while indexes.size:
                chosen = int(indexes[0])
                keep.append(chosen)
                rest = indexes[1:]
                if not rest.size:
                    break
                indexes = rest[_iou(boxes[chosen], boxes[rest]) <= NMS_IOU_THRESHOLD]
        keep.sort(key=lambda index: (-float(selected_scores[index]), int(selected_classes[index]), index))
        height, width = source_shape[:2]
        detections: list[bounding_box_pb2.Detection] = []
        for index in keep:
            x1 = float(np.clip((boxes[index, 0] - pad_x) / scale, 0, width))
            y1 = float(np.clip((boxes[index, 1] - pad_y) / scale, 0, height))
            x2 = float(np.clip((boxes[index, 2] - pad_x) / scale, 0, width))
            y2 = float(np.clip((boxes[index, 3] - pad_y) / scale, 0, height))
            if x2 <= x1 or y2 <= y1:
                continue
            class_id = int(selected_classes[index])
            detections.append(
                bounding_box_pb2.Detection(
                    class_id=class_id,
                    class_name=CLASS_NAMES[class_id],
                    confidence=float(selected_scores[index]),
                    x=x1 / width,
                    y=y1 / height,
                    width=(x2 - x1) / width,
                    height=(y2 - y1) / height,
                )
            )
        return detections

    def apply_dynamic_config(self, config: DynamicConfig) -> None:
        if "confidence_threshold" not in config:
            return
        value = config["confidence_threshold"]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0.0 <= float(value) <= 1.0:
            raise ValueError("confidence_threshold must be numeric and between zero and one")
        self._confidence_threshold = float(value)

    def shutdown(self) -> None:
        self._session = None


__all__ = ["CLASS_NAMES", "CrabDetectionModule"]
