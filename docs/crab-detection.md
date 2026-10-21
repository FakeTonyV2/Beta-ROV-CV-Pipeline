# Crab detector development and deployment

The production mission task is `crab_detection`, implemented by
`purdue_rov_cv.modules.crab_detection.CrabDetectionModule`. It consumes the
runner's process-private BGR8 `uint8` frames and publishes `bounding_boxes_v1`
results. Class IDs are fixed by the training YAML and ONNX metadata:

| ID | Label |
|---:|---|
| 0 | `European_Green_Crab` |
| 1 | `Rock_Crab` |
| 2 | `Jonah_Crab` |

The accepted export contract is one float32 NCHW input `[1, 3, 640, 640]` and
one float32 output `[1, 7, 8400]`. The module validates this contract and the
class metadata, then runs a warm-up during initialization. The seven output
channels are four center/size coordinates followed by three per-class scores;
postprocessing applies the configured confidence threshold, class-wise NMS,
letterbox reversal, clamping, and normalized coordinates.

## Convert the local checkpoint

The `.pt` checkpoint is a development/export input. The deployed runtime uses
the exported ONNX file. Model weights and generated ONNX files are ignored by
Git; provision them separately and verify the SHA-256 before deployment.

From the repository root in the Python 3.12 environment that has the training
dependencies installed:

```bash
source .venv/bin/activate
python models/pt_to_onnx_converter.py
```

The converter reads `models/best_v2.pt`, verifies its class order, exports at
640x640, checks ONNX input/output metadata, runs a warm-up, and prints the
resulting digest. Export requires PyTorch/Ultralytics. The pipeline runtime and
the image test below use ONNX Runtime and do not load the `.pt` checkpoint.

The checked-in mission config pins the ONNX file at
`/opt/purdue-rov-cv/models/crab_dection.onnx`. The deployment installer copies
`models/crab_dection.onnx` to the configured location only when its SHA-256
matches the config. Calculate the digest after export and update
`tasks.crab_detection.artifact.sha256` before installing:

```bash
sha256sum models/crab_dection.onnx
```

## Run an image prediction through the production module

Use a local crab image. This executes the same ONNX initialization,
preprocessing, inference, NMS, and protobuf conversion as the module runner:

```bash
python models/model_test.py --image /path/to/crab.jpg --save /tmp/crab-prediction.jpg
```

The command prints each class ID, label, confidence, and pixel bounding box. The
optional `--save` image draws those predictions. Adjust the score threshold
with `--confidence 0.60`; use `--model` to test a different ONNX file. It does
not need camera hardware or a running ZeroMQ broker.

## Automated tests

The unit suite uses synthetic model sessions and tensors, so it does not need
to load the ignored model files:

```bash
python -m pytest tests/unit/test_crab_detection.py -q
```

This covers the model contract, warm-up, BGR conversion, letterboxing,
confidence filtering, NMS, class mapping, coordinate normalization, empty
results, and malformed inference output. A real Pi camera run and deadline
measurement remain hardware acceptance work.
