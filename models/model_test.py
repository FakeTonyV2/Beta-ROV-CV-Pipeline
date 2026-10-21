"""Run the production crab module against one local image (development helper)."""

from __future__ import annotations

import argparse
from pathlib import Path
from uuid import uuid4

import cv2

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.modules.base import Frame, ModuleContext
from purdue_rov_cv.modules.crab_detection import CrabDetectionModule


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", type=Path, default=Path(__file__).with_name("crab_dection.onnx"))
    parser.add_argument("--image", type=Path, required=True, help="local BGR image to run through the module")
    parser.add_argument("--save", type=Path, help="optional path for an annotated image")
    parser.add_argument("--confidence", type=float, default=0.60)
    args = parser.parse_args()
    image = cv2.imread(str(args.image), cv2.IMREAD_COLOR)
    if image is None:
        parser.error(f"could not read image: {args.image}")

    config = load_config(Path(__file__).parents[1] / "config" / "mission.yaml", environ={})
    task = config.tasks["crab_detection"].model_copy(
        update={"artifact": config.tasks["crab_detection"].artifact.model_copy(update={"path": args.model.resolve()})}
    )
    module = CrabDetectionModule()
    module.initialize(
        ModuleContext("crab_detection", "crab_detection", config.device.device_id, task.input_camera, task)
    )
    module.apply_dynamic_config({"confidence_threshold": args.confidence})
    frame = Frame(image, task.input_camera, uuid4().bytes, 0, 0, 0)
    try:
        result = module.process(frame)[0]
    finally:
        module.shutdown()

    print(f"detections: {len(result.detections)}")
    for detection in result.detections:
        x1 = int(detection.x * image.shape[1])
        y1 = int(detection.y * image.shape[0])
        x2 = int((detection.x + detection.width) * image.shape[1])
        y2 = int((detection.y + detection.height) * image.shape[0])
        print(
            f"class={detection.class_id} {detection.class_name} "
            f"confidence={detection.confidence:.3f} box=({x1},{y1},{x2},{y2})"
        )
        if args.save:
            cv2.rectangle(image, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.putText(
                image,
                f"{detection.class_name} {detection.confidence:.2f}",
                (x1, max(20, y1 - 5)),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                (0, 255, 0),
                1,
                cv2.LINE_AA,
            )
    if args.save and not cv2.imwrite(str(args.save), image):
        raise OSError(f"could not write annotated image: {args.save}")
    if args.save:
        print(f"annotated image: {args.save}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
