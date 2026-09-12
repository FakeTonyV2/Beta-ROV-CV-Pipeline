# Purdue ROV CV Runtime — Setup, Development & Operations Guide

This is the practical path from a clean checkout to development, deployment, operation, and verification. Commands are taken from the checked-in entry points and scripts. Run Linux commands from the repository root unless an absolute deployed path is shown.

## 1. What this repository installs

The Python distribution is `purdue-rov-cv` 0.1.0 and requires Python 3.12. It installs these commands:

| Command | Purpose |
| --- | --- |
| `rov-cv` | config validation, preflight, and surface operator commands |
| `purdue-cv-broker` | ZeroMQ XSUB/XPUB data broker |
| `purdue-cv-control-router` | control ROUTER and module registry |
| `purdue-cv-camera` | one camera/SHM/RTP process |
| `purdue-cv-module-runner` | one configured CV task process |
| `purdue-cv-video-receiver` | one surface RTP/correlation process |
| `purdue-cv-recorder` | structured MCAP recorder and session owner |
| `purdue-cv-replay-broker` | isolated replay broker |
| `purdue-cv-replay` | structured MCAP replay publisher |
| `purdue-cv-video-replay` | local H.264 MKV demux/decode |
| `purdue-cv-validate-environment` | role-specific deployment checks |
| `purdue-cv-system-health` / `purdue-cv-surface-health` | local clock/resource JSON health |

The base dependencies include protobuf, ZeroMQ, NumPy, Pydantic, MCAP, YAML, and psutil. Ubuntu packages provide GStreamer and PyGObject. ONNX Runtime and TensorRT are not base dependencies; install the runtime required by a real model separately.

## 2. Supported environment and important paths

Production targets Ubuntu 24.04, Python 3.12.x, systemd, GStreamer 1.22+, and a dedicated Ethernet tether. The onboard reference is Raspberry Pi 5 ARM64; the surface may be x86-64 or ARM64. Native Windows is not a production target. WSL2 can run many development/simulation tests, but it is not physical UVC/systemd acceptance.

- **Supported:** Raspberry Pi 5 ARM64 onboard and Ubuntu 24.04 surface hosts, Python 3.12, systemd, GStreamer 1.22+, chrony, tethered IPv4, and `gstreamer_v4l2` UVC devices with a validated H.264/MJPEG/raw tuple.
- **Tested automatically:** Ubuntu 24.04 CI; simulated camera/process/GStreamer paths; separately scheduled tc/netem and 60-minute jobs when their environments are available.
- **Implemented but hardware-dependent:** real UVC identity/mode, RTP across tether, disconnect/reconnect, two-host clock, camera-hub, and full stability. The checked-in acceptance matrix has no PASS evidence yet.
- **Likely compatible but unsupported:** other Linux distributions/versions, WSL for non-hardware development, and other ARM64/x86-64 machines. `ALLOW_NON_REFERENCE_PLATFORM=1` bypasses the install-script platform guard for development; it is not certification.
- **Not runtime-supported:** native Windows/macOS production, DepthAI, and RealSense. The latter two have configuration identity schemas only.

| Purpose | Development | Deployment |
| --- | --- | --- |
| package/venv | repository and `.venv` | `/opt/purdue-rov-cv`, `/opt/purdue-rov-cv/.venv` |
| config | `config/*.yaml` | `/etc/purdue-rov-cv/mission.yaml` |
| runtime state | normally `/tmp` for local overrides | `/run/purdue-rov-cv` |
| durable state | test temp dirs | `/var/lib/purdue-rov-cv` |
| recordings | configured path | normally `/var/lib/purdue-rov-cv/recordings` |
| model | developer-selected | `/opt/purdue-rov-cv/models/...` |
| logs | terminal stdout | journald |

Config path precedence is CLI path, `PURDUE_ROV_CV_CONFIG`, then `/etc/purdue-rov-cv/mission.yaml`. `PURDUE_ROV_CV_LOG_LEVEL` is the only supported field override.

## 3. Clean development installation

On Ubuntu 24.04:

```bash
git clone https://github.com/FakeTonyV2/Beta-ROV-CV-Pipeline.git
cd Beta-ROV-CV-Pipeline
sudo ./scripts/setup_system_deps.sh
./scripts/setup_venv.sh
source .venv/bin/activate
```

On a non-ARM development host, explicitly allow the non-reference architecture:

```bash
sudo env ALLOW_NON_REFERENCE_PLATFORM=1 ./scripts/setup_system_deps.sh
./scripts/setup_venv.sh
source .venv/bin/activate
```

`setup_system_deps.sh` installs chrony, networking/V4L2 tools, GStreamer plugins, Python 3.12 venv support, system GI/GStreamer bindings, and `protoc`. `setup_venv.sh` creates a `--system-site-packages` venv, installs `requirements.lock`, installs this checkout editable, and runs the import smoke test.

Verify the result:

```bash
.venv/bin/python scripts/smoke_imports.py
.venv/bin/python scripts/verify_dependencies.py
.venv/bin/python -m pip check
.venv/bin/rov-cv --help
```

## 4. First local simulated run

Use an Ubuntu/WSL shell with POSIX shared memory. Make a local-only copy of `config/development.yaml`:

```bash
cp config/development.yaml config/local.yaml
```

Edit only the copy as follows:

- set `network.tether_interface` to `lo` and all three network/clock IP values to `127.0.0.1`;
- set broker endpoints to `tcp://127.0.0.1:5555` and `tcp://127.0.0.1:5556`;
- set the control client endpoint to `tcp://127.0.0.1:5560` and module endpoint to `ipc:///tmp/purdue-rov-cv-local-control.sock`;
- leave `recording.enabled: false`, set the recording directory under `/tmp`, and set `cameras.usb_webcam.stream_to_surface: false` for the smallest run.

Validate it:

```bash
.venv/bin/rov-cv config validate config/local.yaml
```

Run each long-lived command in a separate terminal, in this order:

```bash
.venv/bin/purdue-cv-broker --config config/local.yaml
```

```bash
# Development-only router with deterministic START authorization.
.venv/bin/python -m purdue_rov_cv.preflight.process_role router --config config/local.yaml
```

```bash
.venv/bin/purdue-cv-camera --camera usb_webcam --config config/local.yaml --simulate
```

```bash
.venv/bin/purdue-cv-module-runner --task gate_detection --config config/local.yaml
```

Start a validated development subscriber before `START`:

```bash
.venv/bin/python -m purdue_rov_cv.preflight.process_role subscriber \
  --endpoint tcp://127.0.0.1:5556 \
  --topic cv.result.gate_detection.usb_webcam \
  --marker /tmp/purdue-rov-cv-result.txt
```

After the module has attached and seen a frame:

```bash
.venv/bin/rov-cv operator get_status gate_detection --config config/local.yaml
.venv/bin/rov-cv operator start gate_detection --config config/local.yaml
.venv/bin/rov-cv operator stop gate_detection --config config/local.yaml
cat /tmp/purdue-rov-cv-result.txt
```

The result marker proves broker → validated subscriber flow; camera/module JSON logs and recurring `cv.health.usb_webcam` / `cv.health.gate_detection` messages prove health. The internal `preflight.process_role` commands are test/development seams. Do not deploy them. The production router requires fresh production preflight and health files for every `START`. Stop local processes with Ctrl-C in reverse order.

To exercise the real GStreamer/FrameIndex code without a camera, retain `stream_to_surface: true`, run the surface receiver, and replace `--simulate` with `--simulate-gstreamer`. Pure `--simulate` creates SHM frames but no RTP stream.

## 5. Understanding and editing configuration

All fields are required, unknown fields are rejected, and the schema version must be 1. Begin with `config/mission.yaml` for production structure or `config/development.yaml` for development. Always validate after editing:

```bash
.venv/bin/rov-cv config validate config/mission.yaml
.venv/bin/rov-cv config validate config/mission.yaml --probe-hardware
```

Key rules:

- IDs use lowercase-safe canonical identifiers and task topics must be exactly `cv.result.<task>.<camera>`.
- Each enabled task references a CV-enabled camera and its execution target must match `device.execution_target`.
- Stream indices are unique; RTP is port `5000 + 2 × index`, RTCP reserves the next port, and RTP payload type is `96 + index`.
- V4L2 uses stable `/dev/v4l/by-id/...` or `/dev/purdue-rov-cv/...`, never `/dev/videoN`.
- Artifact SHA-256 is exactly 64 lowercase hexadecimal characters and paths are absolute Linux paths.
- Recording values are fixed at 300-second video segments, 10 GiB start threshold, 1 MiB MCAP chunks, and zstd.
- Only task FPS/threshold, diagnostic interval, and a subset of debug-snapshot fields can be updated inside a running module. YAML reload and cross-process atomic application are not implemented.

The sample `gate_detection` task runs `EchoModule`, not a detector. Its model path/hash are placeholders until replaced with a trusted artifact and real module.

| Section | What it controls |
| --- | --- |
| `schema_version` | accepted configuration contract; currently exactly 1 |
| `device` | local device ID and execution target used in task compatibility/health identity |
| `network` | tether interface, Pi address, and surface address |
| `clock` | surface time server, maximum offset, polling interval, and invalid-failure threshold |
| `messaging.broker` | XSUB publisher ingress and XPUB subscriber egress endpoints |
| `messaging.control` | surface-client and module DEALER/ROUTER endpoints |
| `messaging` limits | 4 MiB maximum and result send/receive HWMs |
| `diagnostics` | health publication interval and JSON log level |
| `debug_snapshots` | bounded snapshot policy; publication/request path is incomplete in production |
| `recording` | enable flag, absolute root, fixed segmentation/disk/chunk/compression values |
| `camera_limits` | configured/active camera ceilings |
| `cameras.<id>` | backend identity, exact mode, stream index, surface/CV branches, SHM slot capacity |
| `tasks.<id>` | module class, enablement, input camera, target, FPS/deadline, exact topic/payload, dynamic threshold, artifact |

Typical failures include `TASK_TOPIC_MISMATCH`, an enabled task pointing at a non-CV camera, duplicate stream/port allocations, `/dev/videoN` instead of stable identity, target mismatch, malformed lowercase SHA-256, missing backend identity fields, or an undersized SHM slot. Validation prints the exact issue path and exits 78 for invalid configuration.

## 6. Provisioning a physical UVC camera

Connect cameras in their intended hub ports, then inspect and generate stable identity rules:

```bash
v4l2-ctl --list-devices
udevadm info --query=property --name=/dev/video0
.venv/bin/python scripts/configure_udev.py config/mission.yaml --json
sudo .venv/bin/python scripts/configure_udev.py config/mission.yaml --install
```

Unplug/replug after rule installation and confirm the configured stable path resolves. Then prove the exact mode:

```bash
CAMERA_PATH=/dev/v4l/by-id/usb-purdue-rov-front-camera
v4l2-ctl --device "$CAMERA_PATH" --list-formats-ext
.venv/bin/rov-cv config validate config/mission.yaml --probe-hardware
```

Run one production camera manually when systemd is not active:

```bash
.venv/bin/purdue-cv-camera --camera front_camera --config config/mission.yaml
```

The service re-resolves identity and retries after disconnect in code. This is not physical proof. For opt-in hardware tests:

```bash
export PURDUE_ROV_CV_HARDWARE_CONFIG="$PWD/config/mission.yaml"
export PURDUE_ROV_CV_HARDWARE_CAMERA=front_camera
.venv/bin/python -m pytest tests/hardware/test_phase10_uvc.py -v -ra

# Disruptive manual unplug/replug test:
export PURDUE_ROV_CV_HARDWARE_DISCONNECT=1
.venv/bin/python -m pytest tests/hardware/test_phase10_uvc.py -v -ra
```

DepthAI and RealSense identities validate in YAML, but their runtime backends are not implemented.

## 7. Complete minimal new-module example

This example adds a deterministic `classification_result_v1` task without opening platform resources. Create `src/purdue_rov_cv/modules/mean_brightness.py`:

```python
from __future__ import annotations

import numpy as np
from google.protobuf.message import Message
from purdue_rov.cv.v1 import classification_pb2

from purdue_rov_cv.modules.base import CVModule, DynamicConfig, Frame, ModuleContext


class MeanBrightnessModule(CVModule):
    requires_artifact = False

    def __init__(self) -> None:
        self._ready = False
        self._threshold = 0.5

    def initialize(self, context: ModuleContext) -> None:
        if context.task.payload_type != "classification_result_v1":
            raise ValueError("MeanBrightnessModule requires classification_result_v1")
        self._threshold = context.task.dynamic.confidence_threshold
        self._ready = True

    def process(self, frame: Frame) -> list[Message]:
        if not self._ready:
            raise RuntimeError("module is not initialized")
        if frame.pixels.dtype != np.uint8 or frame.pixels.ndim != 3 or frame.pixels.shape[2] != 3:
            raise ValueError("MeanBrightnessModule requires uint8 BGR8 frames")
        brightness = float(np.mean(frame.pixels)) / 255.0
        bright = brightness >= self._threshold
        confidence = brightness if bright else 1.0 - brightness
        return [
            classification_pb2.ClassificationResult(
                camera_id=frame.camera_id,
                camera_session_id=frame.camera_session_id,
                frame_number=frame.frame_number,
                capture_time_unix_ns=frame.capture_time_unix_ns,
                classes=[classification_pb2.ClassScore(
                    class_id=1 if bright else 0,
                    class_name="bright" if bright else "dark",
                    confidence=confidence,
                )],
            )
        ]

    def apply_dynamic_config(self, config: DynamicConfig) -> None:
        value = config.get("confidence_threshold")
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError("confidence_threshold must be numeric")
            candidate = float(value)
            if not 0.0 <= candidate <= 1.0:
                raise ValueError("confidence_threshold must be in [0, 1]")
            self._threshold = candidate
```

Add a task to the local config. The artifact block remains required by schema even when the class opts out of runner artifact loading:

```yaml
  mean_brightness:
    module_class: purdue_rov_cv.modules.mean_brightness.MeanBrightnessModule
    enabled: true
    input_camera: usb_webcam
    execution_target: surface_laptop
    max_input_fps: 10
    processing_deadline_ms: 50
    publish_topic: cv.result.mean_brightness.usb_webcam
    payload_type: classification_result_v1
    dynamic: {confidence_threshold: 0.5}
    artifact:
      format: onnx
      path: /tmp/purdue-rov-cv/unused.onnx
      sha256: 0000000000000000000000000000000000000000000000000000000000000000
      runtime: onnxruntime
```

The SDK has no formal `accepted_pixel_formats` declaration. Current V4L2 CV pipelines deliver BGR8, while `Frame` itself can carry any nonempty NumPy array. Validate `frame.pixels.dtype`, rank, channels, and expected geometry inside the module; add a config/startup compatibility check if the assumption is static. Do not rely on an undocumented conversion.

Add `tests/unit/test_mean_brightness.py`:

```python
from pathlib import Path
from uuid import uuid4

import numpy as np

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.modules.base import Frame, ModuleContext
from purdue_rov_cv.modules.mean_brightness import MeanBrightnessModule


def test_white_frame_is_bright() -> None:
    config = load_config(Path("config/local.yaml"), environ={})
    task = config.tasks["mean_brightness"]
    module = MeanBrightnessModule()
    module.initialize(ModuleContext(
        module_id="mean_brightness",
        task_id="mean_brightness",
        host_device_id=config.device.device_id,
        camera_id=task.input_camera,
        task=task,
    ))
    frame = Frame(
        pixels=np.full((2, 3, 3), 255, dtype=np.uint8),
        camera_id=task.input_camera,
        camera_session_id=uuid4().bytes,
        frame_number=7,
        capture_time_unix_ns=1,
        capture_monotonic_ns=2,
    )
    result = module.process(frame)[0]
    assert result.frame_number == 7
    assert result.classes[0].class_name == "bright"
    assert result.classes[0].confidence == 1.0
```

Validate, add a known-answer unit test that builds `Frame` objects with black and white arrays, and run:

```bash
.venv/bin/rov-cv config validate config/local.yaml
.venv/bin/python -m pytest tests/unit -q
.venv/bin/purdue-cv-module-runner --task mean_brightness --config config/local.yaml
```

For a model-backed module, leave the default `requires_artifact = True`; install its declared runtime; load `context.task.artifact.path` in `initialize()`; validate input shape, dtype, output names/shapes, and warm-up; and fail initialization on any mismatch. Production preflight hashes every configured artifact, including artifact-optional modules, so use a real existing artifact/hash before deployment.

If a new payload is required, add a versioned `.proto`, register it in `src/purdue_rov_cv/wire/payloads.py`, run `scripts/generate_proto.sh`, and add protobuf/envelope validator tests. Do not dynamically import payload classes from wire strings.

Complete the path before production: add malformed-output/deadline/dynamic-update tests beside `test_phase5_module_runner.py`; add a real process flow beside `test_phase5_module_runner_processes.py`; use `MatroskaVideoReplay` in a custom test callback to create representative `Frame` inputs; run hardware config validation and production preflight on the Pi; then capture the module publication and timing evidence required by the Phase 11 matrix. The stock replay CLI does not feed the module runner, so that regression adapter must be explicit.

The runtime already supplies the camera loop, SHM attach/copy, latest-frame queue, PUB and DEALER sockets, envelope serialization/validation, lifecycle and watchdog, diagnostic health, signal shutdown, and systemd instance. A module should supply none of those.

## 8. Consuming CV results safely

Subscribe to the broker subscriber endpoint and validate before using payloads:

```python
import zmq
from purdue_rov.cv.v1 import classification_pb2
from purdue_rov_cv.runtime.envelope import ReceivedMultipartValidator
from purdue_rov_cv.runtime.metrics import RuntimeMetrics

endpoint = "tcp://127.0.0.1:5556"
topic = b"cv.result.mean_brightness.usb_webcam"

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.setsockopt(zmq.RCVHWM, 5)
socket.setsockopt(zmq.RCVTIMEO, 250)
socket.setsockopt(zmq.LINGER, 0)
socket.setsockopt(zmq.MAXMSGSIZE, 4 * 1024 * 1024)
socket.setsockopt(zmq.SUBSCRIBE, topic)
socket.connect(endpoint)
validator = ReceivedMultipartValidator(RuntimeMetrics())

try:
    while True:
        try:
            frames = socket.recv_multipart()
        except zmq.Again:
            continue
        result = validator.validate(frames)
        if not result.valid:
            continue
        assert result.envelope is not None and result.payload is not None
        if isinstance(result.payload, classification_pb2.ClassificationResult):
            identity = (
                result.envelope.camera_id,
                bytes(result.envelope.camera_session_id),
                result.envelope.frame_number,
            )
            print(identity, result.payload.classes)
finally:
    socket.close(linger=0)
    context.term()
```

Start subscribers before publishers when testing because ZeroMQ PUB/SUB has a subscription handshake. Never assume every live message arrives. Treat publisher sequence gaps as observability, and use `(camera_id, camera_session_id, frame_number)` as the frame key.

For all task results, subscribe with `socket.setsockopt(zmq.SUBSCRIBE, b"cv.result.")`; prefix filtering is byte-prefix filtering, so keep the trailing period. `ReceivedMultipartValidator` rejects wrong frame counts, malformed envelopes, unknown payload types, topic/payload mismatches, oversized semantic messages, duplicates, and reordered sequences. It increments `invalid_messages`, `invalid_multipart_message`, `unknown_payload_types`, and `observed_sequence_gaps` through the supplied `RuntimeMetrics`. There is no higher-level general consumer SDK in the repository.

## 9. Controlling tasks and dynamic settings

| Command | Current production support | Invocation |
| --- | --- | --- |
| `GET_STATUS` | task runner | `rov-cv operator get_status` or `ControlClient` |
| `START` / `STOP` | task runner | operator CLI or `ControlClient`; START is preflight-gated |
| `SET_DYNAMIC_CONFIG` | task runner | `ControlClient` only |
| `RESET` | task runner in `ERROR` | `ControlClient` only |
| `GET_COMMAND_STATUS` | task runner cache | `ControlClient.get_command_status()` |
| `SET_MODE` | schema only | no production target advertises it |
| `REQUEST_DEBUG_SNAPSHOT` | schema only | no production target advertises it |
| `START_RECORDING` / `STOP_RECORDING` | schema only | use recorder/receiver service lifecycle instead |

The supported operator path is:

```bash
rov-cv operator get_status gate_detection --config /etc/purdue-rov-cv/mission.yaml
rov-cv operator start gate_detection --config /etc/purdue-rov-cv/mission.yaml
rov-cv operator stop gate_detection --config /etc/purdue-rov-cv/mission.yaml
```

Optional `--preflight-report /var/lib/purdue-rov-cv/preflight.json` adds an observation to operator JSON output; it does not bypass or replace the router's production START authorization.

Use `ControlClient` for supported runner commands not exposed by the CLI:

```python
import time
from uuid import uuid4

from google.protobuf.struct_pb2 import Struct
from purdue_rov.cv.v1 import control_pb2
from purdue_rov_cv.messaging.client import ControlClient

fields = Struct()
fields.update({"dynamic.confidence_threshold": 0.7})
request = control_pb2.CommandRequest(
    command_id=uuid4().bytes,
    target_id="gate_detection",
    issued_time_unix_ns=time.time_ns(),
    requested_timeout_ms=3000,
    set_dynamic_config=control_pb2.SetDynamicConfig(fields=fields),
)
with ControlClient("tcp://192.168.50.2:5560") as client:
    response = client.execute_command(request)
    print(response)
```

`RESET` uses the same request shape with a fresh command UUID and `reset=control_pb2.Reset()`. `GET_COMMAND_STATUS` is normally `client.get_command_status("gate_detection", original_command_id)`. It returns the cached final/pending status under a new query UUID. The runner rejects RESET outside `ERROR` and rejects static fields in dynamic updates as `RESTART_REQUIRED`.

After an acknowledgement timeout, the client returns `OUTCOME_UNKNOWN` and does not resend. Preserve the original UUID and use the one allowed status query. Do not blindly create a new state-changing command. `set_mode`, debug-snapshot request, and recording commands exist in protobuf but have no production target owner.

## 10. Installing the actual ROV and surface deployment

Use the same reviewed mission YAML on both hosts. Place the trusted model/runtime on the Pi before installation and set its real hash:

The mission config uses surface `192.168.50.1`, Pi `192.168.50.2`, and tether interface `eth0`. Configure those addresses outside this repository, connect the dedicated tether, and verify `ip -brief link show eth0`, `ip -brief address show eth0`, and mutual reachability. `install_chrony.sh surface` installs a local stratum-8 server bound to the surface address; `install_chrony.sh pi` installs the Pi client. Confirm both with `chronyc -n tracking` and `chronyc -n sources -v`.

```bash
sudo install -d -m 0755 /opt/purdue-rov-cv/models
TRUSTED_MODEL=/path/to/trusted-model.onnx
sudo install -m 0644 "$TRUSTED_MODEL" /opt/purdue-rov-cv/models/gate_detector.onnx
sha256sum /opt/purdue-rov-cv/models/gate_detector.onnx
```

On the Pi:

```bash
sudo ./scripts/setup_system_deps.sh
sudo ./scripts/install_deployment.sh pi config/mission.yaml
```

On the surface:

```bash
sudo env ALLOW_NON_REFERENCE_PLATFORM=1 ./scripts/setup_system_deps.sh
sudo ./scripts/install_deployment.sh surface config/mission.yaml
```

The installer records revision/worktree state, backs up a changed prior config as `mission.yaml.previous`, installs non-editably, reconciles instances, installs chrony configuration, enables one role target, and runs environment validation. It does not start the target.

> **Implementation note:** The configuration describes `recording.enabled: false`, while `systemd/purdue-cv-surface.target` statically wants the recorder and its entrypoint does not reject disabled recording. The current runtime behavior is that the target starts the recorder; do not rely on disabled recording until this discrepancy is fixed.

## 11. Operating the Pi and surface

Start surface first, then Pi:

```bash
# Surface
sudo ./scripts/start_deployment.sh surface

# Pi
sudo ./scripts/start_deployment.sh pi
```

Inspect both topology and application readiness:

```bash
systemctl status purdue-cv-surface.target
systemctl status 'purdue-cv-video-receiver@*' purdue-cv-recorder purdue-cv-surface-health
systemctl status purdue-cv-onboard.target
systemctl status purdue-cv-broker purdue-cv-control-router 'purdue-cv-camera@*' 'purdue-cv-module@*' purdue-cv-system-health
```

A running PID is not readiness. A task needs initialization, registration, SHM attachment, a first frame, and an authorized `START` before it is `RUNNING`.

The surface side owns one receiver for each `stream_to_surface` camera, the recorder/session file, surface resource/clock health, and on-demand operator/control clients. It connects to the Pi broker subscriber endpoint for FrameIndex/results and to Pi control port 5560, while its receivers listen on derived RTP UDP ports. The Pi side owns broker, router, cameras, SHM, modules, and system health. There is no built-in video display; receiver fan-out/correlation is a library seam.

After both targets are ready, run production preflight on the Pi as shown in section 14, then issue `rov-cv operator start` from the surface. Mission enablement is an application command, not a systemd target state.

Stop Pi first, then surface:

```bash
sudo systemctl stop purdue-cv-onboard.target
sudo systemctl stop purdue-cv-surface.target
```

Each service has a five-second stop bound. A normal code-level crash is process-local, but camera/module units `Require=` broker and router, so stopping those dependencies can stop the dependents.

## 12. Recording a mission

With recording enabled, the recorder chooses a safe UTC session ID, writes it to `/run/purdue-rov-cv/recording-session`, stores structured traffic in `structured.mcap`, and lets H.264 receivers write five-minute MKV segments under the same session. Monitor it with:

Set `recording.enabled: true`, keep the fixed recording values valid, install the updated config, and restart the surface target so recorder and receivers agree on one session from startup:

```bash
sudo systemctl restart purdue-cv-surface.target
```

There is no implemented control-plane START/STOP recording command. Recording starts with the recorder and H.264 receiver processes. Stop it cleanly with the surface target during mission shutdown; stopping only the recorder does not retroactively reconfigure already-running receivers.

```bash
systemctl status purdue-cv-recorder
journalctl -fu purdue-cv-recorder
find /var/lib/purdue-rov-cv/recordings -maxdepth 3 -type f -printf '%TY-%Tm-%Td %TH:%TM %s %p\n'
```

For a manual session outside systemd:

```bash
purdue-cv-recorder --config config/mission.yaml --session test-001 --session-file /tmp/recording-session
purdue-cv-video-receiver --camera front_camera --config config/mission.yaml --record-session-file /tmp/recording-session
```

Run those two manual commands in separate terminals, recorder first. Stop the receiver, then recorder with SIGINT/SIGTERM so MCAP indexes and current Matroska segments finalize.

Start requires 10 GiB free in the recording filesystem. Runtime recording stops below 2 GiB. Video recording accepts H.264 only. If the recorder does not publish the session file within five seconds while recording is enabled, the receiver exits 78 rather than running without the configured recording branch.

## 13. Replaying recorded data

Use the isolated replay broker defaults:

```bash
# Terminal 1
purdue-cv-replay-broker

# Terminal 2: point a validated subscriber at tcp://127.0.0.1:5656

# Terminal 3
purdue-cv-replay /path/to/structured.mcap --rate 1
```

Allowed rates are `0.25`, `0.5`, `1`, `2`, and `max`; optional `--start-time-ns` and `--end-time-ns` filter the MCAP log-time range. A nondefault endpoint requires `--config` or `--allow-live-broker`; publishing to the configured live broker requires `--allow-live-broker` explicitly.

Decode an H.264 MKV segment locally with:

```bash
purdue-cv-video-replay /path/to/segment.mkv --rate 1
```

The video replay CLI currently discards decoded callback output. Structured and video replay are separate: there is no built-in command that reconstructs SHM, feeds a module, displays video, or rejoins FrameIndex identities. Build a custom tool around the replay classes when that workflow is required.

Use MCAP replay for consumer/debug regression and post-mission envelope analysis. Use `MatroskaVideoReplay` with a custom callback for algorithm fixtures or visualization, and explicitly construct module `Frame` identities if calling a module in a test. Use preserved MCAP/MKV plus the config/revision evidence to reproduce a mission. None of these custom uses qualifies as testing the production module-runner path unless the test actually exercises that process boundary.

## 14. Running preflight and authorizing a mission

Use simulation only to check report structure and failure handling:

```bash
rov-cv preflight config/mission.yaml --simulate --scenario success --camera-duration 10 --json-report /tmp/preflight.json
rov-cv preflight config/mission.yaml --simulate --scenario invalid_model_hash --camera-duration 10
```

Production preflight must run on the Pi after surface and onboard services are ready:

```bash
sudo -u purdue-cv /opt/purdue-rov-cv/.venv/bin/rov-cv preflight \
  /etc/purdue-rov-cv/mission.yaml \
  --camera-duration 10 \
  --json-report /var/lib/purdue-rov-cv/preflight.json
```

Exit 0 means all required checks passed, 1 means a required check failed, and 2 means execution/evidence failure. The control router accepts `START` only while that canonical report is at most 30 seconds old, matches config, and current health/camera/artifact/module gates also pass. Inspect `/run/purdue-rov-cv/mission-state.json` for the decision.

The report covers strict config and hashes; tether/ping; broker round-trip and control status; chrony; stable camera identity and exact mode; simultaneous run duration, FPS, and maximum gap; RTP and exact FrameIndex correlation; CPU/memory/root/recording disk; temperature/throttling observations; task frame processing; and required component states. Human output names each PFL check; JSON includes version, run identity, evidence kind, measurements, fatality, and result for automation. Correct the specific failing subsystem—do not edit the report or bypass router authorization.

> **Implementation note:** The intended component inventory makes recorder conditional, while `src/purdue_rov_cv/preflight/probes.py::_required_component_states()` always includes `recorder`. The current production behavior is that PFL-020 requires it even when recording is disabled. A simulated pass does not prove physical UVC, RTP, clock, model, or deployed component behavior.

## 15. Monitoring and diagnostics

Useful commands:

```bash
journalctl -fu purdue-cv-camera@front_camera
journalctl -fu purdue-cv-module@gate_detection
journalctl -fu purdue-cv-control-router
journalctl --since '-5 minutes' -u 'purdue-cv-*' --no-pager
cat /run/purdue-rov-cv/system-health.json
cat /run/purdue-rov-cv/surface-health.json
cat /run/purdue-rov-cv/mission-state.json
chronyc -n tracking
chronyc -n sources -v
ip -brief link show eth0
ip -brief address show eth0
```

Camera/module health is also published on `cv.health.<source_id>`. Watch frame timeouts, pipeline restarts, SHM conflicts/reattach, frame/result drops, processing deadline misses, ZeroMQ drops, RTP loss, correlation outcomes, disk/memory, and clock validity. Logs are JSON on stdout; persistent retention depends on host journald configuration.

The standalone system-health files contain local resource/clock observations, not a complete registry of all component states.

## 16. Troubleshooting common failures

| Problem | Check | Action |
| --- | --- | --- |
| `config validate` exits 78 | full issue path/code, stable paths, topics, hashes, endpoints | correct config; never bypass validation |
| camera exits 78 | adapter support, stable path, exact V4L2 mode | use `gstreamer_v4l2`; reprovision path/mode |
| camera repeatedly degrades | USB/hub power, `v4l2-ctl`, GStreamer journal | restore hardware; reconnect loop retries automatically |
| module remains `STARTING` | router registration, camera frame/SHM, initialize log | restore dependency; verify task/class/artifact |
| task `START` rejected | preflight age/result, health age, SHM frame age, model hash, registration | repair failed gate and rerun preflight immediately |
| no subscriber output | endpoint direction, topic prefix, subscriber startup timing | connect to subscriber endpoint and start SUB first |
| dropped/late results | cap-one/cap-four queues, deadlines, PUB HWM | reduce task input FPS or processing cost |
| video but no exact correlation | FrameIndex broker path and identity counters | repair data path; do not reuse stale result |
| receiver exits when recorder fails | missing five-second session file | repair recorder/disk or intentionally change config/unit design |
| clock invalid | chrony source/leap/offset and tether | restore sync; ignore cross-host latency until valid |
| service stopped after retries | `NRestarts`, start-limit journal | fix root cause, `systemctl reset-failed <unit>`, restart |

Exit codes: 0 clean, 64 arguments, 70 internal software, 74 I/O, 75 temporary/restartable, and 78 invalid config/non-restarting.

## 17. Safe development and release workflow

```text
edit → focused unit test → config validate → simulated process integration
     → custom recorded-input/replay regression → hardware test → production preflight
     → reviewed mission deployment → HIL acceptance evidence
```

Before editing, check status and avoid overwriting unrelated work:

```bash
git status --short --branch
```

Run focused tests while developing, then the repository gates:

```bash
.venv/bin/python -m pytest tests/unit/test_phase5_module_runner.py -q
.venv/bin/python -m pytest tests/integration/test_phase5_module_runner_processes.py -q
.venv/bin/python scripts/smoke_imports.py
.venv/bin/python scripts/verify_dependencies.py
.venv/bin/python -m mypy
.venv/bin/python -m ruff check src tests scripts
.venv/bin/python -m ruff format --check src tests scripts
.venv/bin/python -m pytest -m "not hardware and not extended and not requires_net_admin"
```

After protobuf changes:

```bash
PATH="$PWD/.venv/bin:$PATH" ./scripts/generate_proto.sh
git diff --exit-code -- generated/python
```

Privileged network acceptance is isolated behind:

```bash
bash scripts/run_transport_tests.sh
```

Run physical/HIL tests only on the intended hosts with explicit opt-ins. The normative Phase 11 commands are documented in `docs/phase11-deployment.md`; camera-hub is at least 1,800 seconds, and full stability needs at least 3,600 seconds of matching two-host overlap. Preserve JSON evidence with revision, config hash, host inventory, and timing. Do not convert a smoke run or CI pass into HIL `PASS`.

## 18. Quick reference

| Item | Value |
| --- | --- |
| broker publisher / subscriber | mission `192.168.50.2:5555` / `:5556` |
| control client / module | mission `192.168.50.2:5560` / `ipc:///run/purdue-rov-cv/module-control.sock` |
| replay publisher / subscriber | `127.0.0.1:5655` / `:5656` |
| RTP/RTCP | `5000 + 2×stream_index` / next port |
| RTP payload type | `96 + stream_index` |
| production config | `/etc/purdue-rov-cv/mission.yaml` |
| canonical preflight | `/var/lib/purdue-rov-cv/preflight.json` |
| health / mission decision | `/run/purdue-rov-cv/*-health.json`, `mission-state.json` |
| SHM | `purdue_rov_cv_<camera_id>` in `/dev/shm` |
| recording | `<directory>/<session>/structured.mcap` and `<camera>/<UTC>.mkv` |
| start order | surface, then Pi |
| stop order | Pi, then surface |
| module operator | `rov-cv operator get_status|start|stop <task> --config <path>` |
| config check | `rov-cv config validate <path> [--probe-hardware]` |
| production preflight | `rov-cv preflight <path> --json-report <path>` |
| normal test gate | `pytest -m "not hardware and not extended and not requires_net_admin"` |
| acceptance state | all Phase 11 matrix rows currently `UNVERIFIED` |

State meanings: `STARTING` is dependency/initialization work; `READY` can accept START; `RUNNING` processes mission frames; `DEGRADED` is impaired/recovering; `ERROR` is terminal until reset/restart; `STOPPING` and `STOPPED` are shutdown phases.

Where to add code:

| Extension | Location |
| --- | --- |
| new task module | `src/purdue_rov_cv/modules/` plus task YAML and Phase 5 tests |
| new payload | `proto/purdue_rov/cv/v1/`, `src/purdue_rov_cv/wire/payloads.py`, generated bindings, wire tests |
| new camera | `cameras.<id>` in YAML; new backend under `src/purdue_rov_cv/camera/` only if not V4L2 |
| new config field | `config/models.py`, `validation.py`, `policy.py`, loader/update tests and reference docs |
| new unit/process/hardware test | `tests/unit`, `tests/integration`, or `tests/hardware` according to the boundary |

Common machine-readable errors include `CONFIG_INVALID`, `CAMERA_NOT_FOUND`, `CAMERA_MODE_UNSUPPORTED`, `SHARED_MEMORY_INVALID`, `MODEL_HASH_MISMATCH`, `MODEL_LOAD_FAILED`, `RUNTIME_UNAVAILABLE`, `TARGET_UNAVAILABLE`, `COMMAND_OUTCOME_UNKNOWN`, `DUPLICATE_COMMAND_ID`, `PROCESSING_FAILURE`, `PROCESSING_WATCHDOG_EXCEEDED`, `VIDEO_STREAM_LOST`, `FRAME_INDEX_MISS`, `RECORDER_QUEUE_FULL`, `DISK_SPACE_LOW`, and `INTERNAL_ERROR`. The complete contract is `src/purdue_rov_cv/wire/errors.py`.

## Repository Evidence Index

| Concept | Implementation | Tests | Documentation |
| --- | --- | --- | --- |
| Installation/dependencies/CLI | `pyproject.toml`, `requirements.lock`, `scripts/setup_system_deps.sh`, `scripts/setup_venv.sh`, `src/purdue_rov_cv/**/entrypoints.py` | CI smoke/import/dependency and CLI tests | `README.md` |
| Configuration | `config/mission.yaml`, `config/development.yaml`, `src/purdue_rov_cv/config/models.py`, `validation.py`, `loader.py` | `tests/unit/test_config.py`, `test_configuration_contracts.py`, `test_hardware_probe.py` | `docs/configuration-reference.md`, `docs/configuration-examples.md` |
| Local simulation | `src/purdue_rov_cv/camera/entrypoints.py`, `src/purdue_rov_cv/preflight/process_role.py` | `tests/integration/test_phase6_processes.py`, `test_phase9_full_system.py` | `README.md`, this guide section 4 |
| Camera provisioning/runtime | `scripts/configure_udev.py`, `src/purdue_rov_cv/camera/v4l2.py`, `backend.py`, `service.py` | `tests/unit/test_phase10_v4l2.py`, `tests/hardware/test_phase10_uvc.py` | `docs/camera-profiles.md` |
| Module API/runner | `src/purdue_rov_cv/modules/base.py`, `echo.py`, `src/purdue_rov_cv/module_runner/service.py`, `artifacts.py` | `tests/unit/test_phase5_module_runner.py`, `tests/integration/test_phase5_module_runner_processes.py` | `docs/module-development.md`, `docs/module-runner.md` |
| Result consumption/wire | `src/purdue_rov_cv/runtime/envelope.py`, `src/purdue_rov_cv/wire/payloads.py`, `validators.py` | `tests/unit/test_wire_contracts.py`, `test_runtime_envelope_logging_shutdown.py` | `docs/runtime-primitives.md` |
| Control | `proto/purdue_rov/cv/v1/control.proto`, `src/purdue_rov_cv/messaging/client.py`, `router.py`, `src/purdue_rov_cv/module_runner/service.py` | `tests/unit/test_phase4_control_primitives.py`, `tests/integration/test_phase4_processes.py` | `docs/broker-control-routing.md` |
| Surface video/correlation | `src/purdue_rov_cv/video/gstreamer.py`, `service.py`, `subscriber.py`, `correlation.py` | `tests/unit/test_phase7_video_receiver.py`, `tests/integration/test_phase7_gstreamer.py` | `docs/surface-video-receiver.md` |
| Deployment/operations | `scripts/install_deployment.sh`, `start_deployment.sh`, `configure_deployment.py`, `systemd/*` | `test_phase11_deployment.py`, systemd acceptance tool | `docs/operations.md`, `phase11-deployment.md` |
| Recording/replay | `src/purdue_rov_cv/recording/*`, `src/purdue_rov_cv/replay/*` | `tests/unit/test_phase8_recording_replay.py`, `tests/integration/test_phase8_processes.py` | `docs/recording-replay.md` |
| Preflight/gating | `src/purdue_rov_cv/preflight/checks.py`, `runner.py`, `probes.py`, `authorization.py`, `operator.py` | `tests/unit/test_phase9_preflight.py`, `tests/integration/test_phase9_full_system.py` | `docs/phase9-preflight.md` |
| Monitoring/recovery | `src/purdue_rov_cv/runtime/metrics.py`, `json_logging.py`, `src/purdue_rov_cv/deployment/entrypoints.py`, `src/purdue_rov_cv/wire/errors.py` | runtime metric/log tests and component service tests | `docs/recovery-runbook.md`, `docs/error-code-contract.md` |
| CI/acceptance | `.github/workflows/*.yml`, `scripts/run_phase11_hil.py` | `tests/unit`, `tests/integration`, `tests/transport`, `tests/hardware`; current HIL matrix unverified | `docs/phase11-acceptance-matrix.md`, `docs/transport-resilience.md` |
