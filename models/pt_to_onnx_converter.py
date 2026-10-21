"""Export the local crab checkpoint and verify the ONNX contract used by V1."""

from __future__ import annotations

import argparse
import ast
import hashlib
import shutil
import tempfile
from pathlib import Path

EXPECTED_NAMES = ("European_Green_Crab", "Rock_Crab", "Jonah_Crab")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path(__file__).with_name("best_v2.pt"))
    parser.add_argument("--output", type=Path, default=Path(__file__).with_name("crab_dection.onnx"))
    args = parser.parse_args()
    if not args.source.is_file():
        parser.error(f"checkpoint not found: {args.source}")

    try:
        from ultralytics import YOLO
    except ImportError as error:
        parser.error(f"export requires Ultralytics and its PyTorch dependencies: {error}")
    model = YOLO(str(args.source))
    names = tuple(model.names[index] for index in range(len(model.names)))
    if names != EXPECTED_NAMES:
        raise ValueError(f"checkpoint class order differs from mission contract: {names!r}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="crab-onnx-") as temporary_dir:
        exported = Path(model.export(format="onnx", imgsz=640, batch=1, dynamic=False, nms=False, simplify=True))
        temporary_output = Path(temporary_dir) / "crab.onnx"
        shutil.copy2(exported, temporary_output)
        try:
            import onnxruntime as ort
        except ImportError as error:
            raise RuntimeError("verifying exported model requires onnxruntime") from error
        session = ort.InferenceSession(str(temporary_output), providers=["CPUExecutionProvider"])
        inputs, outputs = session.get_inputs(), session.get_outputs()
        if len(inputs) != 1 or inputs[0].shape != [1, 3, 640, 640] or inputs[0].type != "tensor(float)":
            raise ValueError("exported input does not match float32 [1, 3, 640, 640]")
        if len(outputs) != 1 or outputs[0].shape != [1, 7, 8400] or outputs[0].type != "tensor(float)":
            raise ValueError("exported output does not match float32 [1, 7, 8400]")
        metadata_names = ast.literal_eval(session.get_modelmeta().custom_metadata_map.get("names", "None"))
        normalized_names = tuple(metadata_names[index] for index in range(len(metadata_names)))
        if normalized_names != EXPECTED_NAMES:
            raise ValueError(f"exported class order differs from mission contract: {normalized_names!r}")
        import numpy as np

        warmup = session.run([outputs[0].name], {inputs[0].name: np.zeros((1, 3, 640, 640), np.float32)})[0]
        if warmup.shape != (1, 7, 8400) or warmup.dtype != np.float32:
            raise ValueError("exported model warm-up returned an incompatible output")
        args.output.write_bytes(temporary_output.read_bytes())

    digest = hashlib.sha256(args.output.read_bytes()).hexdigest()
    print(f"verified ONNX model: {args.output}")
    print("input: images [1, 3, 640, 640] float32")
    print("output: output0 [1, 7, 8400] float32")
    print(f"classes: {EXPECTED_NAMES}")
    print(f"sha256: {digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
