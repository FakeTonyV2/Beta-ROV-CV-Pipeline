from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import numpy as np
import pytest
from purdue_rov.cv.v1 import bounding_box_pb2

from purdue_rov_cv.modules.base import Frame, ModuleContext
from purdue_rov_cv.modules.crab_detection import (
    CLASS_NAMES,
    CrabDetectionModule,
    _prepare_input,
)


def _output() -> np.ndarray:
    return np.zeros((1, 7, 8400), dtype=np.float32)


class _Session:
    def __init__(self, *, names: str | None = None, shape: list[int] | None = None) -> None:
        self.output = _output()
        self.calls: list[tuple[list[str], dict[str, np.ndarray]]] = []
        self._names = names or "{0: 'European_Green_Crab', 1: 'Rock_Crab', 2: 'Jonah_Crab'}"
        self._shape = shape or [1, 7, 8400]

    def get_inputs(self) -> list[object]:
        return [SimpleNamespace(name="images", shape=[1, 3, 640, 640], type="tensor(float)")]

    def get_outputs(self) -> list[object]:
        return [SimpleNamespace(name="output0", shape=self._shape, type="tensor(float)")]

    def get_modelmeta(self) -> object:
        return SimpleNamespace(custom_metadata_map={"names": self._names, "task": "detect"})

    def run(self, names: list[str], feeds: dict[str, np.ndarray]) -> list[np.ndarray]:
        self.calls.append((names, feeds))
        return [self.output]


def _context(path: Path = Path("/models/crab.onnx"), **changes: object) -> ModuleContext:
    task = SimpleNamespace(
        payload_type="bounding_boxes_v1",
        artifact=SimpleNamespace(
            path=path, format=SimpleNamespace(value="onnx"), runtime=SimpleNamespace(value="onnxruntime")
        ),
        dynamic=SimpleNamespace(confidence_threshold=0.6),
    )
    for key, value in changes.items():
        setattr(task, key, value)
    return ModuleContext("crab_detection", "crab_detection", "rov_pi5", "front_camera", task)  # type: ignore[arg-type]


def _frame(pixels: np.ndarray | None = None) -> Frame:
    return Frame(
        pixels if pixels is not None else np.zeros((640, 640, 3), dtype=np.uint8),
        "front_camera",
        UUID(int=3).bytes,
        8,
        10_000,
        20_000,
    )


def test_initialize_validates_model_contract_and_warms_once() -> None:
    session = _Session()
    module = CrabDetectionModule(session_factory=lambda _path: session)

    module.initialize(_context())

    assert module.requires_artifact
    assert len(session.calls) == 1
    assert session.calls[0][1]["images"].shape == (1, 3, 640, 640)
    assert session.calls[0][1]["images"].dtype == np.float32


@pytest.mark.parametrize(
    ("session", "message"),
    [
        (_Session(shape=[1, 6, 8400]), "output must"),
        (_Session(names="{0: 'wrong', 1: 'Rock_Crab', 2: 'Jonah_Crab'}"), "class labels"),
    ],
)
def test_initialize_rejects_incompatible_model(session: _Session, message: str) -> None:
    module = CrabDetectionModule(session_factory=lambda _path: session)
    with pytest.raises(ValueError, match=message):
        module.initialize(_context())


def test_preprocess_bgr_letterboxes_and_normalizes() -> None:
    pixels = np.zeros((320, 640, 3), dtype=np.uint8)
    pixels[:, :, 2] = 255  # BGR red becomes RGB channel zero.

    tensor, scale, pad_x, pad_y = _prepare_input(pixels)

    assert tensor.shape == (1, 3, 640, 640)
    assert tensor.dtype == np.float32
    assert scale == 1.0 and pad_x == 0.0 and pad_y == 160.0
    assert tensor[0, 0, 200, 100] == pytest.approx(1.0)
    assert tensor[0, 0, 10, 100] == pytest.approx(114 / 255)


def test_process_maps_yolo_candidates_to_normalized_crab_box() -> None:
    session = _Session()
    session.output[0, :, 0] = [320, 320, 100, 120, 0.95, 0.02, 0.01]
    session.output[0, :, 1] = [322, 322, 100, 120, 0.80, 0.01, 0.01]
    session.output[0, :, 2] = [100, 100, 40, 60, 0.01, 0.02, 0.9]
    module = CrabDetectionModule(session_factory=lambda _path: session)
    module.initialize(_context())

    result = module.process(_frame())[0]

    assert isinstance(result, bounding_box_pb2.BoundingBoxResult)
    assert (result.camera_id, result.frame_number, result.capture_time_unix_ns) == ("front_camera", 8, 10_000)
    assert [(box.class_id, box.class_name) for box in result.detections] == [
        (0, CLASS_NAMES[0]),
        (2, CLASS_NAMES[2]),
    ]
    assert result.detections[0].x == pytest.approx(270 / 640)
    assert result.detections[0].y == pytest.approx(260 / 640)
    assert result.detections[0].width == pytest.approx(100 / 640)
    assert result.detections[0].height == pytest.approx(120 / 640)
    assert result.detections[0].confidence == pytest.approx(0.95)


def test_process_returns_empty_detection_list_below_threshold() -> None:
    session = _Session()
    session.output[0, 4, 0] = 0.59
    module = CrabDetectionModule(session_factory=lambda _path: session)
    module.initialize(_context())

    assert list(module.process(_frame())[0].detections) == []


def test_process_rejects_invalid_pixels_and_nonfinite_model_output() -> None:
    session = _Session()
    module = CrabDetectionModule(session_factory=lambda _path: session)
    module.initialize(_context())
    with pytest.raises(ValueError, match="uint8"):
        module.process(_frame(np.zeros((640, 640, 3), dtype=np.float32)))
    session.output[0, 4, 0] = np.nan
    with pytest.raises(ValueError, match="non-finite"):
        module.process(_frame())
