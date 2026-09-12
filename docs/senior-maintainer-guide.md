# Purdue ROV CV Runtime — Senior Maintainer & Architecture Guide

This guide describes the repository as it exists at the end of the Phase 11 implementation effort. It is deliberately implementation-led: a checked-in schema or design note is not treated as a working production feature unless a runtime owner and verification evidence also exist.

Status labels used throughout follow the repository-review convention:

- **Implemented and verified** — production code owns the behavior and automated tests exercise it.
- **Implemented but weakly tested** — a production owner exists, but coverage is narrow.
- **Implemented but hardware-dependent** — production code exists, but physical evidence is required.
- **Partially implemented** — a useful portion exists, but an advertised workflow or runtime owner is missing.
- **Legacy/deprecated** — retained code that is not the primary path.
- **Documented but not implemented** — a schema/design/document advertises behavior with no production owner.
- **Unable to determine** — repository evidence is insufficient.

Simulation is called out separately because deterministic simulated proof is not physical acceptance.

## 1. Executive mental model

The system is a Linux, process-isolated computer-vision pipeline for a Raspberry Pi 5 ROV computer and a tethered surface computer. A camera process creates decoded frames for onboard CV and, when configured, sends the same source stream as RTP video. One task process consumes one configured camera through shared memory and publishes typed protobuf results through a ZeroMQ broker. A separate control router mediates commands. The surface can receive video, correlate it with source identities, and record structured traffic plus encoded H.264.

This is not a collection of OpenCV scripts because the mission problem includes stable USB identity, bounded latency under overload, process fault containment, typed cross-process/cross-host contracts, remote lifecycle control, timestamp validity, video/result correlation, recording, and supervised recovery. A CV developer owns deterministic frame-to-protobuf computation, model preprocessing/postprocessing, model compatibility, dynamic algorithm settings, and task tests. The runtime owns camera acquisition, decoded-frame transport, queues, lifecycle, control, envelopes, sockets, health, shutdown, recording, and systemd supervision. Keeping that boundary lets an algorithm fail without turning every platform concern into task-specific code.

The architecture and most core mechanisms are **Implemented and verified** by automated tests. The product is not yet deployment-accepted: all 20 rows in `docs/phase11-acceptance-matrix.md` are `UNVERIFIED`. The production mission config also points `gate_detection` at `EchoModule`; it does not contain a gate detector. Therefore “Phase 11 code present” must not be reported as “the ROV CV deployment is accepted.”

### Actual topology

```text
                                  ROV Pi 5
  physical UVC camera
          |
          v
  camera process ------------------- H.264/MJPEG RTP/UDP -------------------+
      | decoded BGR                                                        |
      | POSIX SHM (triple buffer)                                          v
      +--> module process --> protobuf envelope --> PUB --> XSUB/XPUB broker --> surface consumers
              ^                              |                              |     |
              |                              +--> health + FrameIndex -------+     +--> MCAP recorder
              |
  surface operator --> ROUTER control router --> DEALER module target              +--> video receiver
                                                                                         |
                                                           exact RTP/FrameIndex correlation
                                                                                         |
                                                                                encoded H.264 MKV branch
```

The data plane and control plane use distinct sockets and protocols. Video is not carried through ZeroMQ. Pixels are not carried through ZeroMQ. Task code does not own cameras, sockets, envelopes, lifecycle, or recording.

## 2. Architecture by process

| Host/process | Cardinality | Important internal owners | Restart boundary |
| --- | ---: | --- | --- |
| `purdue-cv-broker` | one on Pi | one thread owns XSUB and XPUB | process/systemd service |
| `purdue-cv-control-router` | one on Pi | one thread owns both ROUTER sockets and module registry | process/systemd service |
| `purdue-cv-camera --camera ID` | one per configured camera | service loop, backend/GStreamer activity, SHM writer, health publisher | camera only, subject to systemd dependencies |
| `purdue-cv-module-runner --task ID` | one per enabled task | main/control/watchdog owner, frame-ingress thread, worker thread, PUB thread | task only, subject to systemd dependencies |
| `purdue-cv-video-receiver --camera ID` | one per surface stream | receiver lifecycle, GStreamer callbacks, FrameIndex subscriber, bounded fan-out | receiver only |
| `purdue-cv-recorder` | nominally one on surface | structured subscriber/writer plus per-camera encoded recording branches | recorder service |
| `purdue-cv-system-health` | one on Pi | periodic local clock/resource snapshot | service |
| `purdue-cv-surface-health` | one on surface | periodic local clock/resource snapshot | service |
| `rov-cv operator ...` | on demand on surface | one DEALER client | command invocation |
| replay broker/replayers | on demand, normally isolated | dedicated broker, MCAP scheduler, or local video decoder | command invocation |

Every production process creates its own ZeroMQ context. Sockets are thread-confined and must not be used after `fork()`. The systemd topology qualifies pure process isolation: camera and module units `Require=` both broker and router. An unexpected dependency exit by itself is not propagated by `Requires=`, but explicitly stopping or restarting broker/router also stops or restarts dependent units; failed dependency activation prevents their startup.

The operational ledger behind that summary is:

- `DataBrokerService` in `src/purdue_rov_cv/messaging/broker.py` owns only forwarding sockets, transitions directly through `READY` to `RUNNING`, logs rate-limited `BROKER_FORWARD_DROPPED`, and shuts down through `ShutdownCoordinator`. It must not deserialize payloads. A crash interrupts structured traffic and subscriptions while systemd retries it; an explicit service stop/restart propagates through declared `Requires=` dependencies. Unit and process coverage is in `test_phase4_*` and transport coverage in `tests/transport`.
- `ControlRouterService` in `src/purdue_rov_cv/messaging/router.py` owns registration, availability, pending routes, and START authorization. It must not own module state transitions. A crash loses volatile registrations/routes; module DEALER sockets reconnect/register after the service returns, while accepted-command uncertainty must be resolved from each module's cache. Systemd retries it. Phase 4/9/11 unit and integration tests cover this path.
- `CameraService` with `V4L2CaptureBackend` owns the physical/GStreamer handle, session/frame counter, `SharedMemoryFrameWriter`, RTP/FrameIndex, and `CameraHealthPublisher`. It must not run task inference. Recoverable capture loss stays in-process as `DEGRADED`; terminal/config failure exits and systemd applies the exit-code policy. Other camera processes and surface consumers are code-isolated, but broker/router unit dependencies qualify that isolation. Phase 6/7/10 tests cover simulation and opt-in physical behavior.
- `ModuleRunnerService` owns dynamic module loading, artifact gate, SHM reader, `CVModule` hooks, `ProcessingSupervisor`, `WorkerWatchdog`, DEALER control, and `ResultPublisher`. The module must not own platform resources. A task crash does not directly crash camera/video/other task processes; systemd retries exit 75, but not exit 78. Phase 5 process tests are the main regression suite.
- `VideoReceiverService` owns UDP/GStreamer receive, `FrameIndexSubscriber`, `FrameCorrelator`, `LocalVideoFanout`, health, and optional `EncodedMatroskaRecorder`. It must not open the camera or infer CV. RTP loss rebuilds locally; terminal thread/config failures let systemd restart or prevent restart according to exit code. Phase 7/8 integration tests cover it.
- `RecorderService` owns the broad `cv.`/`system.` SUB socket, bounded recorder queue, and `McapSessionWriter`; encoded video remains owned by each receiver. It must not alter envelopes. A recorder crash ends structured capture and can block configured receiver startup through the shared session-file contract. Phase 8 tests cover MCAP, disk, overflow, and shutdown.
- Health entry points in `src/purdue_rov_cv/deployment/entrypoints.py` own local chrony/resource sampling and atomic JSON writes. They do not aggregate service health or broker messages. A crash removes fresh gate evidence but does not directly stop CV/video/control; systemd retries it. Phase 9/11 tests cover freshness and gate behavior.
- On-demand operator and replay commands have no supervisor. Their caller owns retries and output. Structured replay uses `StructuredReplayer`; video replay uses `MatroskaVideoReplay`. Failures end only that invocation.

All long-lived services log structured JSON to stdout and expose a `RuntimeMetrics` instance internally or through health payloads. SIGTERM/SIGINT requests ordered cleanup; service units enforce `TimeoutStopSec=5`. The common restart contract is exit 75 for temporary failure and exit 78 for invalid configuration that must not loop.

## 3. End-to-end frame lifecycle

1. The configured camera backend resolves a stable device identity and proves that the exact format, dimensions, and rational frame rate can open.
2. The camera process creates `purdue_rov_cv_<camera_id>` and a process-lifetime camera-session UUID. Frame numbers start at zero.
3. The GStreamer source assigns one source-boundary identity to the decoded-CV and RTP branches.
4. A decoded frame is copied into the next of three SHM slots and committed by an even generation value.
5. A module ingress reader attaches by stable SHM name, detects recreation, obtains a private NumPy copy, and samples to `max_input_fps`.
6. Its capacity-one queue keeps the newest work. The worker calls `CVModule.process(frame)` only while the module is `RUNNING`.
7. Returned protobuf payloads enter a capacity-four result queue. The publisher adds the camera/session/frame identity, task/source identity, clocks, publisher-session UUID, and monotonic publication sequence.
8. The PUB socket sends `[topic, serialized MessageEnvelope]` non-blockingly to the broker. Backpressure drops live output rather than growing memory.
9. Independently, the camera RTP branch transmits to the surface and publishes `FrameIndex` messages mapping RTP `(SSRC, timestamp)` to the same camera-session/frame identity.
10. The surface receiver correlates an RTP frame exactly, optionally labels a near-timestamp debug match as approximate, or marks it unmatched. It never silently reuses stale metadata.

A camera pipeline rebuild in the same process preserves the camera-session UUID and increasing frame number. A camera process restart creates a new session UUID and resets numbering. Consumers must key frame identity by camera ID, camera-session UUID, and frame number, not frame number alone.

The concrete call path is `V4L2DeviceProbe.probe()` → `V4L2CaptureBackend`/`GStreamerCaptureBackend` → `CameraService._accept()` → `SharedMemoryFrameWriter.write()` → `SharedMemoryFrameReader.read()` through `SharedMemoryFrameSource` → `ModuleRunnerService._frame_ingress()`/`_process_frame()` → `CVModule.process()` → `ResultPublisher._send()` → `EnvelopeBuilder.build()` → `DataBrokerService._forward()` → `ReceivedMultipartValidator.validate()`. The parallel video path is `GStreamerRtpSender` + `FrameIndexPublisher` → `GStreamerRtpReceiver` + `FrameIndexSubscriber` → `FrameCorrelator` → `VideoReceiverService._deliver()`. `RecorderSubscriber` validates the same broker frames and `McapSessionWriter` stores them.

```text
Camera   V4L2/Gst   SHM writer   Module runner   Module   PUB/Broker   Consumer/Recorder   Video receiver
  |          |          |              |            |         |               |                 |
  | resolve + exact-open|              |            |         |               |                 |
  |--------->| frame + source identity |            |         |               |                 |
  |          |--------->| odd/write/even            |         |               |                 |
  |          |          |<-------------| attach/read/copy      |               |                 |
  |          |          |              |----------->| process |               |                 |
  |          |          |              |<-----------| payload |               |                 |
  |          |          |              | envelope + PUB------>| two frames---->| validate/store  |
  |          | RTP + FrameIndex--------------------------------------------------------------->| correlate
```

## 4. Data plane and wire contract

The broker binds an XSUB socket at `messaging.broker.publisher_endpoint` and an XPUB socket at `subscriber_endpoint`. Publishers connect to the former; subscribers connect to the latter. The broker is payload-agnostic and also forwards XPUB subscription frames back to XSUB. Broker sockets use HWM 100. Destination congestion causes a warning and a drop.

The multipart contract is exactly two frames: topic bytes and one serialized `MessageEnvelope`. Topics are lowercase ASCII, at most 128 bytes, and one of:

- `cv.result.<task_id>.<camera_id>`
- `cv.health.<source_id>`
- `cv.state.<source_id>`
- `cv.frame_index.<camera_id>`
- `cv.debug_snapshot.<camera_id>`
- `system.clock.<device_id>`
- `system.health.<device_id>`
- `system.event.<event_type>`

The static payload registry accepts `bounding_boxes_v1`, `classification_result_v1`, `target_pose_v1`, `diagnostic_status_v1`, `module_state_v1`, `frame_index_v1`, `debug_snapshot_v1`, `clock_status_v1`, and `system_event_v1`. Normal semantic envelope validation is capped at 1 MiB even though transport configuration permits 4 MiB. Publisher sequence values are consumed before validation/send and therefore expose rejected or dropped attempts as gaps. Receivers track gaps by `(publisher_session_id, source_id)` and reject duplicates/reordering.

> **Implementation note:** The schemas describe `module_state_v1`, `cv.state.*`, and brokered system health, while the current repository has no production state publisher and the health executables write local JSON/stdout in `src/purdue_rov_cv/deployment/entrypoints.py`. The current runtime behavior is local health plus camera/module `cv.health.*`; the other surfaces are **Documented but not implemented**.

## 5. Control plane and command semantics

The router binds ROUTER sockets at the client and module endpoints. DEALER application frames are `[kind, payload]`; ROUTER views `[identity, kind, payload]`. Client identities are `client:<UUID>` and task identities are `module:<module_id>:<UUID>`, with a 128-byte limit.

Modules register, receive a 500 ms acknowledgement, retry once per second, and exit 75 after ten failed attempts. They heartbeat every second and are unavailable after 3.5 seconds without a heartbeat. The router preserves one current session per module ID; a newly observed session replaces the prior one.

The production task runner advertises only:

- `get_status`, `start`, `stop`
- `set_dynamic_config`, `reset`
- `get_command_status`

> **Implementation note:** The control schema describes `set_mode`, `request_debug_snapshot`, `start_recording`, and `stop_recording`, while the current runner advertises only status/start/stop/dynamic/reset/status-lookup in `src/purdue_rov_cv/module_runner/service.py`; the CLI exposes only status/start/stop. The current runtime behavior is rejection of the unadvertised commands; they are **Documented but not implemented**.

Commands are UUID-deduplicated. Final results remain cached for ten minutes with a capacity of 1,024; pending commands are not expired or evicted in the module cache. Router routes expire after 60 seconds. A client waits 500 ms for the initial acknowledgement and never resends the original command. A timeout returns `OUTCOME_UNKNOWN`, reconnects the socket, and permits one status query for that unknown command. Accepted commands use command-specific completion deadlines, followed by bounded status polling for at most ten seconds.

The router does not mutate module state. It rejects unavailable targets and unadvertised commands and invokes the production start authorizer for every `START`.

Control scenarios, following `ControlClient`, `ControlRouterService._handle_client_command()`, and `ModuleRunnerService._handle_command()`:

- **Successful START:** the client sends a new UUID; the router confirms target availability/support and calls `PreflightStartAuthorizer`; the module reserves the UUID, returns `RECEIVED`, transitions to `RUNNING`, executes `on_start()` on its worker, caches `COMPLETED`, and returns it. A hook failure moves the module to `ERROR`. The router relays acknowledgement and completion.
- **Target unavailable:** no fresh registry heartbeat means immediate `REJECTED/TARGET_UNAVAILABLE`; the command never reaches a task.
- **ACK timeout:** after 500 ms, `ControlClient` reconnects and returns `OUTCOME_UNKNOWN`. It does not resend. The caller may perform one `get_command_status` lookup using the original UUID.
- **Completion timeout:** after a command-specific deadline, the client issues fresh status-query commands once per second for at most ten seconds. If no final cache result appears, the outcome remains unknown.
- **Module crashes after acceptance:** the router route may remain until its 60-second expiry, but there is no completion response. The module's in-memory command cache dies with the process, so a post-restart status lookup cannot recover the old final result. The safe outcome is unknown; operator reconciliation is required.
- **Duplicate UUID:** a pending or finalized state-changing UUID is rejected as `DUPLICATE_COMMAND_ID`; callers query status rather than execute it again. The router also rejects a UUID already pending.

## 6. Component state machine and readiness

States are `STARTING`, `READY`, `RUNNING`, `DEGRADED`, `ERROR`, `STOPPING`, and `STOPPED`. Normal transitions are:

```text
STARTING -> READY | DEGRADED | ERROR | STOPPING
READY    -> RUNNING | DEGRADED | ERROR | STOPPING
RUNNING  -> READY | DEGRADED | ERROR | STOPPING
DEGRADED -> RUNNING | READY | ERROR | STOPPING
ERROR    -> STOPPING --(reset_from_error only)--> STARTING
STOPPING -> STOPPED
```

A module is not ready merely because its PID is active. It must initialize, register with control, see its SHM input, and receive a first frame. `START` invokes `on_start()` and enters `RUNNING`; `STOP` invokes `on_stop()` and returns to `READY`. Source loss does not automatically change a running module out of `RUNNING`; SHM disconnect/reattach is represented in metrics. Processing exceptions degrade on the first and enter `ERROR` after three consecutive failures. Five consecutive deadline misses degrade; twenty cause restartable exit 75. No progress for `max(10 s, 5 × deadline)` also exits 75.

The camera becomes `RUNNING` after its first accepted frame. Two seconds without accepted frames, or a backend error, causes `DEGRADED` and pipeline reconstruction.

`ComponentStateMachine` in `src/purdue_rov_cv/runtime/state.py` owns transition legality under an `RLock`; components, not the router, choose transitions. Observers update metrics. Camera/module state reaches subscribers as the `state` field inside `DiagnosticStatus`; there is no production `cv.state.*` publisher. `RESET` is accepted only by the task runner and uses `reset_from_error()` to return from `ERROR` to `STARTING` before re-readiness. Common extension mistakes are jumping directly from `ERROR` to `RUNNING`, publishing state from a second owner, confusing a live PID with `READY`, or swallowing a terminal failure without requesting process escalation.

## 7. Shared-memory frame buffer

The camera writer exclusively owns creation and unlinking. Readers attach and never unlink. The binary layout is a 128-byte header plus three fixed-capacity slots. Header fields include magic `PROVCV01`, version, slot count/capacity, active slot, generation, frame identity and both capture clocks, geometry/stride/length/pixel format, owner PID, session UUID, and reserved bytes.

Publication is lock-free per frame: odd generation means “write in progress”; after payload and metadata are complete, the writer commits an even generation. A reader retries at most three times and accepts only a matching, even before/after generation. It always returns a private copy. Supported pixel formats are `BGR8`, `RGB8`, `GRAY8`, and `DEPTH16_MM`.

An advisory POSIX lock is used only for startup/recovery. A stale segment is removed only when a trusted positive owner PID is provably dead. Malformed ownership is refused, and a live PID is never displaced. Readers inspect the `/dev/shm` inode to detect writer recreation. PID reuse remains a residual risk because ownership is PID-based rather than boot-ID/start-time based.

Simplified `SharedMemoryFrameWriter.write()`/`SharedMemoryFrameReader.read()` behavior:

```text
writer:
  slot = (active_slot + 1) mod 3
  publish header with generation = previous_even + 1   # odd
  copy pixels into slot; write frame metadata
  publish active_slot and generation = previous_even + 2  # commit

reader (up to three attempts):
  g1 = generation
  reject attempt if g1 is odd
  copy header, then active slot bytes into private memory
  g2 = generation
  accept only when g1 == g2 and g2 is even and header/length validate
```

The generation check prevents a torn read in which metadata comes from one frame while pixels are still being replaced by another. A cross-process mutex is unnecessary on the hot path because there is one writer, readers never mutate, and generation validation detects overlap without blocking capture. The guarantee is a self-consistent snapshot from one completed publication, not delivery of every frame and not a promise that the accepted frame is still the newest by return time.

## 8. Queues, backpressure, and drop policy

| Boundary | Capacity | Full behavior | Meaning |
| --- | ---: | --- | --- |
| camera/SHM to module ingress | 1 | drop oldest | process newest frame |
| worker to result publisher | 4 | drop oldest | preserve fresh CV output |
| generic priority publication | 32 | wait up to 50 ms; repeated failures degrade/exit 75 | protect priority messages |
| router to module control commands | 16 | reject with `MODULE_BUSY` | never silently lose control |
| module control results | 16 | wait 100 ms, cache, then exit 75 | make result recoverable by status query |
| recorder ingress | 4,096 | drop newest and degrade | preserve already accepted recording order |
| video local consumer | 1 per consumer | keep latest | no display backlog |

Receive waits are bounded (normally no more than 250 ms), and all shutdown paths are cooperative. Live results and video are intentionally lossy; recording and command results use stronger, but still bounded, policies. Never replace these with unbounded Python queues.

## 9. Camera abstraction, UVC, and reconnect behavior

> **Implementation note:** The configuration schema describes `gstreamer_v4l2`, `depthai`, and `realsense`, while `src/purdue_rov_cv/camera/entrypoints.py` constructs only the V4L2 backend. The current runtime behavior for DepthAI/RealSense is exit 78; they are **Documented but not implemented** beyond identity validation.

V4L2 configuration must use `/dev/v4l/by-id/...` or a generated `/dev/purdue-rov-cv/<id>` fallback; `/dev/videoN` is rejected. Provisioning resolves in order: USB-serial by-id, exact udev `ID_PATH`, then labeled physical port. Validation uses `v4l2-ctl --list-formats-ext` and a one-buffer GStreamer mode-open for the exact FourCC, width, height, and rational frame rate.

Native H.264 and MJPEG are split to decoded BGR appsink and RTP branches. Raw modes are CV-only unless `allow_software_encode: true`, in which case x264 zerolatency/ultrafast encoding is used. H.264 payload type is `96 + stream_index`; destination RTP port is `5000 + 2 × stream_index`, with the next port reserved for RTCP. The repository allocates/validates the RTCP port but does not implement an RTCP session.

On loss, the camera stops the backend, re-resolves stable identity, revalidates the exact mode, and retries forever with 0.5, 1, 2, 4, then 5 second delays. This reconnect loop is **Implemented and verified** in simulation. Its behavior with an actual hub/camera disconnect is **Implemented but hardware-dependent** and remains `UNVERIFIED` in the authoritative matrix. The H.264 profile checker likewise reports allowed profiles as `UNVALIDATED`; it does not prove B-frame, keyframe, or bitrate constraints.

## 10. Surface video and frame correlation

The receiver uses a 4 MiB UDP buffer and 50 ms jitterbuffer (`drop-on-latency`, loss reporting), then depayloads/parses before a tee. A capacity-one leaky decode branch produces BGR frames. A capacity-eight encoded branch feeds H.264 recording without re-encoding. MJPEG can be received but cannot use the MKV recording branch.

RTP source probes preserve exact `(SSRC, RTP timestamp)` values. The FrameIndex subscriber uses receive HWM 5, a 250 ms timeout, a 512-entry/two-second cache, and exact topic validation. Correlation internals use bounded 256-entry maps. Conflicting mappings poison a key. Up to 64 pending frames wait at most 100 ms. Exact identities are consumed once. Optional `--approximate-debug` accepts only a same-SSRC wrap-aware difference of at most 4,500 ticks (50 ms at 90 kHz) and labels it `APPROXIMATE`; otherwise the result is `UNMATCHED`.

Two seconds without parseable RTP degrades and rebuilds the pipeline after one second. Five valid decoded frames recover `RUNNING`. A background-thread failure is fatal to the process.

> **Implementation note:** The intended operator flow describes surface display/overlays, while `src/purdue_rov_cv/video/entrypoints.py` installs no display consumer and the replay entrypoint supplies a no-op frame callback. The current behavior stops at validated correlation/fan-out; a visual UI is **Documented but not implemented**.

## 11. Configuration ownership and updates

Configuration precedence is explicit path, `PURDUE_ROV_CV_CONFIG`, then `/etc/purdue-rov-cv/mission.yaml`. Every YAML field is required and unknown fields are rejected. `PURDUE_ROV_CV_LOG_LEVEL` is the only supported field-level environment override. Cross-field validation covers identities, topology, stream indices, camera limits, exact topics, execution targets, artifact metadata, and fixed recording constants.

A representative task/camera slice using current field names is:

```yaml
device: {device_id: rov_pi5, execution_target: rov_pi5}
network: {tether_interface: eth0, rov_ip: 192.168.50.2, surface_ip: 192.168.50.1}
messaging:
  broker: {publisher_endpoint: tcp://192.168.50.2:5555, subscriber_endpoint: tcp://192.168.50.2:5556}
  control: {client_endpoint: tcp://192.168.50.2:5560, module_endpoint: ipc:///run/purdue-rov-cv/module-control.sock}
cameras:
  front_camera:
    adapter: gstreamer_v4l2
    device_path: /dev/v4l/by-id/usb-purdue-rov-front-camera
    device_path_kind: by_id
    resolution_tier: by_id
    format: h264
    width: 1920
    height: 1080
    frame_rate: 30
    stream_index: 0
    stream_to_surface: true
    cv_enabled: true
    allow_software_encode: false
    slot_capacity_bytes: 6220800
tasks:
  gate_detection:
    module_class: purdue_rov_cv.modules.echo.EchoModule
    enabled: true
    input_camera: front_camera
    execution_target: rov_pi5
    max_input_fps: 15
    processing_deadline_ms: 100
    publish_topic: cv.result.gate_detection.front_camera
    payload_type: bounding_boxes_v1
    dynamic: {confidence_threshold: 0.60}
    artifact: {format: onnx, path: /opt/purdue-rov-cv/models/gate_detector.onnx,
      sha256: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef,
      runtime: onnxruntime}
```

See `config/mission.yaml` for required clock, diagnostics, debug-snapshot, recording, message-limit, and camera-limit sections omitted from this slice. Configuration removes hardcoded wiring by making every process select the same camera/task/topic/endpoint graph. `config_hash()` in `src/purdue_rov_cv/config/loader.py` hashes the canonical validated model for preflight/start authorization. Phase 11 HIL's `configuration_hash()` in `src/purdue_rov_cv/deployment/evidence.py` hashes raw file bytes when comparing two hosts; semantically equivalent but byte-different YAML will therefore fail the HIL cross-host match.

Static values require process/deployment restart. The runner accepts only these live fields:

- `tasks.<current>.max_input_fps`
- `tasks.<current>.dynamic.confidence_threshold`
- `diagnostics.publish_interval_ms`
- `debug_snapshots.enabled`, `.maximum_rate_hz`, and `.jpeg_quality`

Candidate values are validated and merged before the module callback. Callback failure rolls back. Static updates return `RESTART_REQUIRED` and degrade the module.

> **Implementation note:** The specification describes a transactional runtime reload, while `src/purdue_rov_cv/config/transactions.py` only plans changes and has no executor. The current runtime behavior is task-local validated update/rollback in `ModuleRunnerService`; system-wide YAML reload and persistence are **Partially implemented**.

## 12. Model artifact and deployment validation

`ArtifactValidator` checks task execution target, file existence/readability, SHA-256, and importability of the named runtime. An optional injected load probe can add compatibility checks. In production the default load probe is `None`; actual model load, input/output compatibility, and warm-up belong to `CVModule.initialize()`.

The base package does not depend on ONNX Runtime or TensorRT even though those runtime names are allowed by config. Deployment must install the selected runtime separately and the module must prove its own model contract.

> **Implementation note:** The task name/artifact describe gate detection, while `config/mission.yaml` loads `purdue_rov_cv.modules.echo.EchoModule`, which sets `requires_artifact = False` and emits one full-frame `echo` box. The current runtime behavior is Echo output and skipped runner artifact validation (although preflight still hashes the configured artifact); a real gate detector is **Documented but not implemented**.

Keep three validation questions separate:

1. **Model validity:** does the path exist, does content match the pinned SHA-256, and can the selected runtime parse/load it? Existence/hash/runtime import are implemented in `ArtifactValidator`; load probing is optional and not wired in production.
2. **Runtime/hardware compatibility:** are operators, providers, memory, input dtype/shape, output names/shapes, and acceleration support correct on the Pi? The framework does not know this contract. A model-backed module must validate it in `initialize()` and perform warm-up there; no generic production warm-up exists.
3. **Deployment compatibility:** does task `execution_target` match config, is the artifact installed at the absolute path, and can the configured task meet its deadline on the target? Config/preflight cover target/path/hash; HIL must prove timing and hardware.

These gates respectively prevent silent artifact substitution, late runtime/provider failure, and deploying a nominally valid model to the wrong or too-slow target. Artifact-related initialization errors map to invalid-configuration exit 78; non-artifact unexpected runtime failures follow the service's internal/temporary escalation paths. Preprocessing and postprocessing are module-owned and are not encoded in the current task schema.

## 13. Diagnostics, metrics, logs, and health

Runtime metrics cover frame receipt/timeouts, SHM writes/conflicts/disconnect/reattach, read/process/drop counts, exceptions/deadlines, result publication/drop, ZeroMQ drops and reconnects, invalid/unknown/gap messages, RTP receive/loss/decode/correlation/restarts, priority and recorder overflow, CPU/RSS/thread/uptime, FPS/geometry, USB status, processing latency, frame age, input source, temperature/memory/disk/clock/tether, state, and last error.

Camera and module processes publish `diagnostic_status_v1` on `cv.health.<source>`. Structured logs are one JSON object per stdout line with component and correlation context; systemd/journald captures them. The repository does not configure persistent journald storage, so retention depends on the host.

The Pi and surface health executables periodically evaluate local resources and chrony and write `/run/purdue-rov-cv/system-health.json` or `surface-health.json`. They do not aggregate every service and do not publish ZeroMQ diagnostics. The mission-state file is an authorization observation, not a replacement for a complete runtime supervisor.

Start diagnosis from the boundary counter, then move upstream: frame starvation means compare camera `frames_received`/`frame_timeouts` and SHM write/read/reattach; slow inference means processing average/p95, deadline misses, input FPS, CPU and thermal evidence; message loss means local result drops, PUB drops, broker forwarding warnings, then consumer sequence gaps; reconnect loops mean USB presence, exact-mode probe and `pipeline_restarts`; control failure means router target registry/heartbeats, registration retries, command UUID and acknowledgement/completion result; clock loss means chrony tracking plus health freshness/failure count; disk pressure means recorder start/runtime thresholds, queue overflow, file growth and filesystem free bytes. Preserve publisher/camera session IDs and command UUIDs when joining logs.

Warning emission uses `WarningRateLimiter`; suppressed repetitions increment `warnings_suppressed`. Error strings come from `src/purdue_rov_cv/wire/errors.py` and are stable monitoring contracts. Journald is the operational query surface, while `/run` JSON is ephemeral current state and MCAP/MKV are durable mission artifacts.

## 14. Recording and replay

The recorder subscribes to `cv.` and `system.` prefixes, validates every multipart message, and writes unchanged envelope bytes to `<root>/<session>/structured.mcap`. MCAP uses one envelope schema, per-topic channels, zstd 1 MiB chunks, indexes and CRCs, and flushes after 100 messages or 250 ms. Receiver wall time is MCAP `log_time`; original publish time remains in the envelope. MCAP sequence is limited to 32 bits.

Encoded H.264 is recorded before decode to `<session>/<camera>/<UTC>.mkv` with 300-second `splitmuxsink` segments and no re-encode. Start requires at least 10 GiB free; runtime recording stops below 2 GiB. Exactly-threshold values pass. Shutdown stops ingress, drains for up to three seconds, and remains inside the five-second service bound.

Structured replay requires an indexed MCAP, validates each envelope, preserves topic/envelope bytes, and schedules from recorded monotonic deltas at 0.25x, 0.5x, 1x, 2x, or `max`. Its default broker endpoints are isolated at `127.0.0.1:5655/5656`; targeting a live endpoint needs explicit authorization. Unlike the live path, replay retries ZeroMQ HWM until sent.

> **Implementation note:** The module workflow describes replaying a session through production readers, while `src/purdue_rov_cv/replay/video.py` only demuxes/decodes and its CLI discards output. The current behavior does not republish FrameIndex, populate SHM, invoke a module, display frames, or join structured/video replay; turnkey module replay is **Documented but not implemented**.

Replay is still valuable for deterministic consumer/debug work: MCAP can reproduce exact validated topics/envelopes at original relative timing or max speed without touching the live broker, and MKV can prove that encoded mission video is readable. For algorithm regression, build a test adapter around `MatroskaVideoReplay` (or extract representative frames) and call the module with `Frame` objects; document that custom seam because it is not the production runner path.

## 15. Clock domains and latency validity

Wall clock (`time.time_ns`) is used for capture/publish/event timestamps and cross-host interpretation. Monotonic clocks are used for deadlines, queue waits, retries, cache expiry, and replay scheduling. Capture records both domains.

Chrony is configured with the surface as local stratum 8 and the Pi polling `192.168.50.1`. Clock validity requires a reachable source, normal leap status, absolute offset strictly below the configured maximum (10 ms in mission config), and a successful observation no older than 15 seconds. Three consecutive failures invalidate cross-device latency; three valid recovery checks restore health. CV, video, and control continue while clock health is degraded. Consumers must not compute cross-host latency while `cross_device_latency_valid` is false.

- Use Unix nanoseconds to label capture/publication for recording and cross-process interpretation; never use them to enforce a timeout or measure an interval.
- Use monotonic nanoseconds/seconds for deadlines, elapsed processing, retries, cache expiry, and replay scheduling; never compare monotonic values from different hosts or process boots.
- Compute cross-device latency from Unix timestamps only while clock health is fresh and valid. A negative/implausible result is a clock-evidence failure, not permission to clamp and report it.

## 16. Preflight and mission-start authorization

Production preflight runs 20 checks: strict config; artifact hashes; tether; surface ping; broker round-trip; control status for enabled modules; chrony; device identity; exact mode-open; simultaneous camera duration; FPS; maximum frame gap; RTP; exact correlation; CPU/memory; temperature; thermal throttle; root/recording disk; module frame processing; and required component states. Temperature and throttling are diagnostic/nonfatal; the others are required. Exit codes are 0 pass, 1 required check failed, and 2 preflight could not execute or lacks required evidence. `--simulate` is deterministic test evidence, never physical acceptance.

| Category | Why it exists | Production evidence owner | Failure surface |
| --- | --- | --- | --- |
| Config + artifact | prevents ambiguous wiring or substituted/missing model bytes | strict loader and `LocalSystemPreflightProbe._hashes()` | named PFL result, human summary, JSON; required failure |
| Network + broker/control | proves tether, surface reachability, message route, and every enabled target | OS link/address/ping probes, broker round-trip, `ControlClient` | required failure before mission |
| Clock | makes cross-host timestamps defensible | `ChronyClockProbe`/`ClockMonitor` | required preflight failure; runtime later continues with latency invalid |
| Camera identity/mode/run | prevents `/dev/videoN` drift, unsupported tuples, low FPS, and long gaps | `V4L2DeviceProbe`, SHM/health observations over at least ten seconds | required failure with per-camera measurements |
| RTP/correlation | proves video and metadata refer to the same physical frames | `BrokerRuntimeEvidenceProvider` plus UDP observations | required per-stream failure |
| Capacity/environment | prevents launch under unsafe memory/root/recording pressure | psutil/disk probes; CPU sample | required except thermal temperature/throttle observations |
| Module/component readiness | proves each task processed frames and every required role reports `RUNNING` | brokered `cv.health.*`, control GET_STATUS, required-state inventory | required failure |

`rov-cv config validate` proves schema/cross-field consistency only. Adding `--probe-hardware` proves configured device identity and exact mode at that moment. Production preflight composes those with live multi-component/network/clock/performance evidence and creates a start-gate report. HIL acceptance is longer, scenario-specific, physical two-host evidence tied to revision/config; no preflight result substitutes for it.

Every production `START` is reauthorized against:

- a canonical preflight report at `/var/lib/purdue-rov-cv/preflight.json`, at most 30 seconds old;
- a matching canonical configuration hash and all nonthermal checks passing;
- health JSON at most 15 seconds old, with memory/root/tether/ping/clock gates valid;
- each physical device and a fresh SHM frame no more than two seconds old;
- current artifact hashes and all enabled task registrations.

The decision is written to `/run/purdue-rov-cv/mission-state.json`. Failure to persist an authorized decision disables it.

Two limitations matter:

1. Production preflight currently requires a `recorder` component even when `recording.enabled` is false. This can make a valid non-recording deployment fail PFL-020.
2. `MissionEnableGate.observe_runtime()` exists, but the router invokes authorization on each `START`; it does not continuously aggregate all components after enablement. Do not describe the mission-state file as an always-on safety interlock.
3. Preflight marks synthetic component entries `network`, `chronyd`, `broker`, `control_router`, and `operator` from probe outcomes; `operator` is inferred from successful task status queries rather than observed as a long-lived process. Interpret PFL-020 as a composed readiness decision, not a literal process inventory.

## 17. Deployment and systemd supervision

The reference platform is Ubuntu 24.04, Python 3.12, systemd, GStreamer 1.22+, Pi 5 ARM64 onboard, and x86-64/ARM64 surface. Installation creates user `purdue-cv`, `/opt/purdue-rov-cv`, `/etc/purdue-rov-cv`, `/run/purdue-rov-cv`, and `/var/lib/purdue-rov-cv`; installs an isolated venv with system GI/GStreamer packages; validates the environment; installs a chrony drop-in; reconciles camera/module/receiver instances; reloads and enables but does not start the role target.

All service units use `Restart=on-failure`, two-second retry, five-second stop timeout, SIGTERM, exit 78 restart prevention, and a five-starts-per-60-seconds limit. Runtime directory preservation prevents one service from removing shared `/run` state.

Deployment order is surface first, then Pi. Stop Pi first, then surface. `active (running)` is not application readiness; use health, task status, preflight, and mission-state evidence.

The production boot path is `multi-user.target` → enabled role target → network-online/chrony and role services. The surface target starts health, statically starts recorder, and dynamically wants one `purdue-cv-video-receiver@<camera>` generated by `configure_deployment.py`. The onboard target starts broker/router/health and dynamically wants every camera and enabled module instance; generated module drop-ins add `After=`/`Requires=` for that task's camera. Operators then run production preflight and issue `START`; systemd never enables mission processing itself. A unit stopped by the start limit remains failed until the root cause is fixed and `systemctl reset-failed` is run.

> **Implementation note:** The configuration/deployment code describes conditional recording, while `systemd/purdue-cv-surface.target` statically `Wants=purdue-cv-recorder.service` and the recorder entrypoint does not reject disabled config. The current runtime behavior is recorder startup with the surface target even when `recording.enabled: false`.

## 18. Verification layers and acceptance

The merge-blocking CI job installs on Ubuntu 24.04, verifies dependencies/imports, regenerates protobuf and requires a clean diff, runs mypy, Ruff lint/format, and pytest excluding `hardware`, `extended`, and `requires_net_admin`. Coverage gates are 80% for core and 70% for task code. A separate privileged workflow runs Linux network-impairment/slow-consumer acceptance. A scheduled/manual workflow runs the actual 60-minute simulated process soak.

Use the hierarchy deliberately:

- `tests/unit` catches pure schema, validator, state, queue, SHM algorithm, cache, timeout, disk, and service-step regressions quickly. Put a regression here when no OS process boundary is necessary.
- `tests/integration` catches real multi-thread/process ZeroMQ, SHM recreation, GStreamer simulation, control routing, recorder/replay, preflight harness, shutdown, and startup interactions. Put ownership, lifecycle, and IPC bugs here.
- `tests/transport/test_phase75_transport.py` uses isolated Linux network namespaces/tc netem for loss, delay, jitter, rate, interruption, and slow-consumer behavior. It catches kernel/socket/HWM behavior mocks cannot establish.
- Phase 8 replay tests catch MCAP index/timing/endpoint-guard and Matroska decode contracts. They do not prove replay-to-module because that path does not exist.
- `tests/hardware/test_phase10_uvc.py` catches actual udev/V4L2/GStreamer/USB and physical reconnect differences. It must skip rather than fabricate evidence when hardware variables are absent.
- `Phase9ProcessHarness` in `src/purdue_rov_cv/preflight/harness.py` catches complete simulated startup, command, runtime-evidence, and bounded shutdown behavior using real processes.
- `scripts/run_phase11_hil.py camera-hub` catches long physical FPS/gap/restart/RTP/correlation faults; `clock-loss` catches real chrony invalidation and continuity.
- per-host `stability` plus `stability-merge` catches restart/memory/health/recording/runtime drift over at least one concurrent hour.
- `docs/phase11-acceptance-matrix.md` is the release-level ledger. It includes manual system behaviors not fully owned by one automated harness.

Choose the lowest layer that reproduces the defect, then add a process or physical test whenever the defect depends on scheduling, kernel transport, GStreamer, systemd, USB, or two-host timing. Never weaken a physical acceptance criterion into a mock merely to make it green.

Physical UVC tests are opt-in through `PURDUE_ROV_CV_HARDWARE_CONFIG` and `PURDUE_ROV_CV_HARDWARE_CAMERA`; manual disconnect additionally requires `PURDUE_ROV_CV_HARDWARE_DISCONNECT=1`. They cover stable identity/mode open, camera SHM service behavior, RTP/FrameIndex correlation, degraded shutdown, and disconnect/reconnect continuity.

Phase 11 HIL tooling provides camera-hub (normative 1,800 s), per-host stability (at least 3,600 s), two-host overlap merge (at least 3,600 s), and destructive clock-loss tests. It does not provide dedicated automated HIL subcommands for every acceptance row, such as two subscriber processes, module kill/video continuity, control, recording readback, or corrupt-model restoration. Those rows need carefully captured manual evidence or new harnesses.

The authoritative matrix is currently entirely `UNVERIFIED`; CI success does not change it. A smoke artifact intentionally remains `UNVERIFIED`.

## 19. Failure-scenario cookbook

| Scenario | Expected state/code/counter | Automatic behavior and intervention | Primary source |
| --- | --- | --- | --- |
| Camera missing at startup | camera `DEGRADED`, `CAMERA_NOT_FOUND`, USB false/retry metrics | stable identity is re-resolved with capped backoff; restore the same camera/hub path | `src/purdue_rov_cv/camera/service.py`, `src/purdue_rov_cv/camera/v4l2.py` |
| Unsupported capture tuple | `ERROR`, `CAMERA_MODE_UNSUPPORTED`, exit 78 | no fallback; edit the full format/width/height/FPS tuple and restart | `src/purdue_rov_cv/camera/v4l2.py`, `src/purdue_rov_cv/camera/entrypoints.py` |
| Camera disconnect mid-run | camera `DEGRADED`, frame timeout/disconnect and `pipeline_restarts` increment | backend is destroyed/rebuilt; operator acts if identity/power does not return | `CameraService._lose_backend()`, `_start_backend()` |
| Stale valid SHM segment | startup detects dead owner and recreates | automatic only when owner PID is trusted and dead; inspect/remove manually only after proving ownership if malformed | `SharedMemoryFrameWriter._open_locked()` |
| Module process crashes | task disappears/heartbeat ages out; systemd records restart | exit 75 restarts after ~2 s; exit 70 also retries until start limit; inspect exception first | `src/purdue_rov_cv/module_runner/entrypoints.py`, `systemd/purdue-cv-module@.service` |
| Module freezes | `PROCESSING_WATCHDOG_EXCEEDED`, deadline counters, `DEGRADED`, exit 75 | watchdog restarts process; optimize/blocking call or deadline before clearing failed state | `src/purdue_rov_cv/module_runner/supervision.py` |
| Three consecutive process exceptions | `PROCESSING_FAILURE`, `processing_exceptions`, then `ERROR` | control remains alive but processing stops; `RESET` or relaunch only after fixing module | `ModuleRunnerService._process_frame()` |
| Model absent/hash mismatch | `MODEL_NOT_FOUND` or `MODEL_HASH_MISMATCH`, `ERROR`, exit 78 | no retry/fallback; deploy trusted bytes, update hash through review, restart | `src/purdue_rov_cv/module_runner/artifacts.py`, `entrypoints.py` |
| Model/runtime cannot load | `MODEL_LOAD_FAILED`/`RUNTIME_UNAVAILABLE`, `ERROR`, exit 78 | no automatic recovery; install compatible runtime/model and validate module initialization | `ArtifactValidator`, `_initialize_module()` |
| Broker unavailable/congested | publisher reconnects; `zmq_send_dropped`, broker `BROKER_FORWARD_DROPPED` | live messages drop; systemd restarts broker, but dependencies may stop; operator restores topology | `src/purdue_rov_cv/messaging/broker.py`, `src/purdue_rov_cv/module_runner/publisher.py` |
| Control router unavailable | registration retries; client timeout/`COMMAND_OUTCOME_UNKNOWN`; heartbeat registry absent | module exits 75 after ten failed registration attempts; repair router before issuing new commands | `src/purdue_rov_cv/messaging/router.py`, `client.py`, module service |
| Heartbeats stop | target unavailable after 3.5 s; `TARGET_UNAVAILABLE` on new command | router removes availability; systemd/module recovery should re-register; inspect task process | `src/purdue_rov_cv/messaging/registry.py`, `router.py` |
| Result queue overloaded | `results_dropped_local_queue` rises; module may remain `RUNNING` | drop-oldest preserves freshness; reduce input FPS/inference cost, never unbound queue | `src/purdue_rov_cv/runtime/queues.py`, module service |
| Slow subscriber | broker or subscriber HWM drops; observed sequence gaps rise | no replay/retry on live plane; make consumer faster or accept loss | `src/purdue_rov_cv/messaging/sockets.py`, `DataBrokerService._forward()` |
| Video stream lost | receiver `DEGRADED`, `VIDEO_STREAM_LOST`, RTP restart/loss metrics | rebuild after two-second timeout; restore UDP/tether/sender if retries persist | `VideoReceiverService.step()`, `_degrade()` |
| FrameIndex misses spike | state unchanged, `FRAME_INDEX_MISS`, exact miss/unmatched counters | frames remain unmatched; fix broker/index timing, never attach prior metadata | `src/purdue_rov_cv/video/correlation.py`, `cache.py` |
| Chrony loses sync | health `DEGRADED`, `CLOCK_UNSYNCHRONIZED`, cross-device latency false | CV/video/control continue; restore tether/chrony and wait for valid recovery samples | `src/purdue_rov_cv/preflight/clock.py`, deployment health entrypoint |
| Disk low before/during recording | recorder refuses <10 GiB at start or degrades/stops below 2 GiB with `DISK_SPACE_LOW` | existing files remain; free space and start a new session | `src/purdue_rov_cv/recording/disk.py`, `service.py` |
| Recorder queue overflow | `RECORDER_QUEUE_FULL`, `recorder_queue_overflow`, `DEGRADED` | drops newest and emits rate-limited critical health; reduce traffic/fix storage writer | `src/purdue_rov_cv/runtime/queues.py`, `src/purdue_rov_cv/recording/service.py` |
| Repeated systemd restart | after five starts/60 s unit becomes failed; inspect `NRestarts` | no recovery after limit; fix root cause, `systemctl reset-failed`, then start | `systemd/*.service`, systemd acceptance tool |
| Preflight fails with recording disabled | PFL-020 reports missing recorder | no safe automatic bypass; known inventory bug must be fixed before non-recording acceptance | `src/purdue_rov_cv/preflight/probes.py::_required_component_states()` |
| Surface records despite disabled config | recorder is active through target dependency | stop only under an approved operational plan; fix static target dependency | `systemd/purdue-cv-surface.target` |

Exit statuses are 0 clean, 64 invalid arguments, 70 internal software error, 74 I/O error, 75 temporary/restartable failure, and 78 invalid configuration/non-restarting failure.

## 20. Extension guide for senior maintainers

For a new module: reuse or define a versioned protobuf, register it statically, regenerate bindings, implement `CVModule`, add strict task config, validate pixel/model assumptions in `initialize()`, return only the configured payload class, add known-answer and invalid-output tests, then exercise runner/control/SHM/publication integration. Do not create a second runner or open camera, SHM, ZeroMQ, database, or video resources from task code.

For a new camera backend: implement the existing backend contract, preserve stable hardware identity, exact mode validation, source-boundary frame identity, decoded SHM output, optional RTP/FrameIndex output, bounded teardown, and reconnect semantics. Adding an enum/identity field alone is not implementation.

For a new payload: add the `.proto`, update the static `PAYLOAD_REGISTRY` and topic-kind rules, regenerate checked-in Python, and add payload validation plus envelope round-trip tests. Never dynamically import a class from wire text.

For a new control command: define validation and deadline, identify a production target owner, advertise it only there, implement deduplication/final caching and state transitions, expose an operator/API path, and add timeout/no-resend process tests.

The complete change map is:

| Change | Required implementation work | Required verification/docs | Compatibility sensitivity |
| --- | --- | --- | --- |
| evolve/add protobuf payload | edit `proto/.../v1`, preserve existing field numbers, register class/topic kind, regenerate bindings | proto round-trip, wire validator, producer/consumer integration; configuration and payload docs | very high; never reuse/remove field numbers silently |
| add topic form | extend `TopicKind`/`validate_topic` and payload registry authorization | valid/invalid topic tests, broker subscriber process test, wire docs | high; filtering and recorder subscriptions may change |
| add command | extend proto only if needed, validators, deadline, target advertisement/owner, cache/state semantics and operator surface | unit plus real router/module timeout, duplicate, crash tests; control docs | very high for idempotency and old clients |
| add module | subclass `CVModule`; validate payload/pixels/model; add strict task entry | known-answer unit, runner integration, recorded-input custom test, hardware preflight/HIL, module docs | low if existing contracts are reused |
| add camera backend | implement `CaptureBackend`, identity/mode probe, decoded and optional RTP/FrameIndex seams, bounded close/rebuild | parser/unit, GStreamer integration, physical disconnect and long hub HIL, camera docs | high for frame identity and teardown |
| add execution target | extend target/runtime policy and environment validation; supply model/runtime install | config negatives, target artifact load/warm-up, deployment/HIL and operations docs | high; current task target must match host config |
| add health metric | add canonical `RuntimeMetrics` value and protobuf field if transported; populate one owner | metric unit, health serialization and consumer compatibility tests; diagnostics docs | medium/high if wire schema changes |
| add config field | Pydantic model, validation, example values, field policy, every consumer | loader/cross-field/unknown-field/update tests; configuration reference and both guides | high because all fields are required |
| choose static vs dynamic | identify whether all owners can apply atomically and roll back; dynamic only when safe | success, rejection, callback failure and rollback tests | high; misclassification can leave split-brain config |
| add error code | enum plus complete `ERROR_CODE_CONTRACTS` metadata and owning emitter | error-contract and scenario test; error-code/runbook docs | medium; external monitoring consumes strings |
| change queue behavior | update one named boundary and metrics/escalation, preserving boundedness | overload/concurrency/process test and transport run; runtime docs | very high for latency/memory semantics |
| change timeout | update the canonical owner/constant and all dependent deadlines | deterministic boundary tests plus impaired process/HIL test; control/operations docs | high; alters failure and retry behavior |
| change transport semantics | update socket setup, wire validation, recorder/replay behavior together | unit, process, tc/netem, slow-consumer, backward-compatibility and HIL evidence | highest; coordinate all producers/consumers |

Any wire, identity, queue, timeout, or retry change requires an explicit compatibility review. Generated protobuf Python is checked in and CI requires reproducible regeneration. Update `docs/phase11-acceptance-matrix.md` only with new evidence; never infer a physical PASS from implementation changes.

## 21. Do Not Break These Invariants

| Invariant | What breaks if violated |
| --- | --- |
| one camera and one task per fault-containment process | a driver/model crash or leak can take unrelated streams/tasks with it and defeats independent supervision |
| exactly one owning thread per ZeroMQ socket, no post-fork use | libzmq thread-safety/context assumptions fail as intermittent corruption, hangs, or shutdown races |
| modules own computation only; they do not open cameras, SHM, sockets, databases, or video | duplicate platform paths bypass validation, lifecycle, metrics, and fault containment |
| pixels remain in SHM/local memory; broker carries typed metadata | continuous full frames saturate broker/network and invalidate bounded message assumptions |
| frame identity is camera ID + session UUID + frame number | process restart can make an old result look like it belongs to a new frame |
| topic identity, envelope identity, and payload identity agree | filtering routes one logical frame while consumers deserialize another identity |
| all queues and waits are bounded | overload becomes memory growth, stale latency, unresponsive control, or shutdown timeout |
| live overload favors freshness and does not retry ordinary CV/video | retry/backlog makes steering consume old observations long after they matter |
| state-changing control is UUID-idempotent and never blindly resent after uncertainty | a lost acknowledgement can execute START/STOP/config twice with unknown state |
| only the camera writer unlinks its SHM object | a reader can destroy an active producer's IPC object and split readers across mappings |
| module output passes the static payload registry/envelope validator | incompatible or malicious wire types escape into every subscriber/recording |
| camera mode/model selection has no silent fallback | deployment may appear healthy while running the wrong resolution, frame rate, artifact, or target |
| cross-device latency is invalid while clock health is invalid | clock offset is misreported as pipeline performance and drives false decisions |
| replay broker is isolated by default | test traffic can contaminate live mission consumers/recordings |
| simulated/CI evidence never becomes physical/HIL acceptance by inference | hardware, kernel, two-host, duration, and operational failures remain hidden behind a false PASS |

## 22. Technical debt and known limitations

**Acceptable v1 constraints:** Python 3.12/Ubuntu 24.04 reference lock; one process per camera/task; freshness-oriented loss; H.264-only encoded recording; fixed 300-second/10-GiB/1-MiB recording constants; no RTCP service despite reserved ports; and 32-bit MCAP sequence storage. These are deliberate if maintained/documented, but changing them is architectural.

**Genuine technical debt:** no explicit SDK pixel-format declaration; task-local rather than system-wide config updates; no general high-level result client; contract-only state/debug/control surfaces; incomplete HIL automation for several matrix rows; broad broker/router `Requires=` coupling; phase-oriented/stale docs; empty `docs/architecture.md`; the tracked zero-byte root file `=0.9,`; and legacy `ci-cd.yml`, which is only a pointer to `.github/workflows/ci.yml`.

**Future features:** a display/overlay application, joined structured/video replay into modules, DepthAI/RealSense backends, full H.264 profile qualification, and richer production model compatibility/warm-up tooling. These must not be advertised as current behavior.

**Unresolved correctness/acceptance risks, in priority order:**

1. replace Echo with a real, tested gate detector and deployable artifact/runtime;
2. correct unconditional recorder requirements in preflight and `purdue-cv-surface.target`;
3. complete and record all 20 Phase 11 HIL acceptance rows;
4. add continuous mission runtime aggregation or narrow its documented safety claim;
5. prove physical UVC reconnect, two-host clock behavior, recording readback/replay, and one-hour stability on the final revision.

## 23. Maintainer learning path and first 15 files

### First hour

Read the mission config, strict config model/validation, envelope proto, topic/payload registry, and state/queue contracts. Draw the actual process graph and write down the three identities that must agree on a frame. The goal is to distinguish algorithm code from runtime ownership before touching either.

### First day

Trace one synthetic frame through `CameraService`, SHM, `ModuleRunnerService`, `EchoModule`, `ResultPublisher`, the broker, and a validated subscriber. Then trace START through `ControlClient`, `ControlRouterService`, `PreflightStartAuthorizer`, and the module command cache. Run focused Phase 4–7 process tests and inspect structured logs/counters during one injected disconnect.

### First week

Reproduce SHM recreation, processing exceptions/deadlines, broker backpressure, command timeout/duplicate behavior, RTP/FrameIndex correlation, recorder disk/overflow behavior, structured replay, production preflight simulation, and systemd acceptance fixtures. On available hardware, run the opt-in UVC suite and a short HIL smoke without relabeling it PASS. Review every `UNVERIFIED` acceptance row with its evidence owner.

### Before approving major architecture changes

Be able to explain socket thread ownership, identity/session boundaries, queue/drop semantics, command idempotency under crash, clock validity, systemd dependency propagation, and why replay/physical acceptance differ from CI. Require compatibility analysis for protobuf/topic/config/error/timeout changes, an overload/failure test at the right layer, and an explicit migration/rollback plan.

### The 15 files every senior maintainer should know

| Path | Purpose | Why it matters |
| --- | --- | --- |
| `config/mission.yaml` | concrete production graph | exposes actual endpoints, camera/task wiring, and the Echo/model mismatch |
| `src/purdue_rov_cv/config/models.py` | typed public config schema | controls what every process may receive |
| `src/purdue_rov_cv/config/validation.py` | cross-field/topology checks | prevents inconsistent task/topic/camera/port deployments |
| `proto/purdue_rov/cv/v1/envelope.proto` | common data-plane identity | defines the cross-process/cross-host compatibility boundary |
| `src/purdue_rov_cv/wire/validators.py` | canonical wire semantics | decides what is accepted, rejected, and counted |
| `src/purdue_rov_cv/runtime/state.py` | reusable lifecycle | prevents ad hoc and unsafe state transitions |
| `src/purdue_rov_cv/runtime/queues.py` | overload contracts | fixes bounded-memory/freshness behavior |
| `src/purdue_rov_cv/frame_buffer/buffer.py` | decoded-frame IPC | contains the most concurrency-sensitive local data path |
| `src/purdue_rov_cv/camera/service.py` | camera lifecycle/reconnect | owns source identity and failure recovery |
| `src/purdue_rov_cv/camera/v4l2.py` | stable UVC identity/mode proof | separates safe physical configuration from device-number guessing |
| `src/purdue_rov_cv/module_runner/service.py` | task execution/control/watchdog | is the main boundary between algorithm and platform |
| `src/purdue_rov_cv/module_runner/publisher.py` | result and health publication | enforces payload/envelope/drop behavior |
| `src/purdue_rov_cv/messaging/router.py` | command routing/registry/gate | owns distributed command uncertainty and start authorization |
| `src/purdue_rov_cv/preflight/checks.py` | canonical go/no-go report | defines mission eligibility evidence |
| `docs/phase11-acceptance-matrix.md` | deployment acceptance ledger | prevents implementation or simulation from being mistaken for accepted hardware behavior |

## Repository Evidence Index

| Concept | Implementation | Tests | Documentation |
| --- | --- | --- | --- |
| Package/entry points | `pyproject.toml`, `requirements.lock`, `src/purdue_rov_cv/**/entrypoints.py` | `scripts/smoke_imports.py`, `scripts/verify_dependencies.py` via CI | `README.md` |
| Canonical topology/config | `config/mission.yaml`, `config/development.yaml`, `src/purdue_rov_cv/config/models.py`, `validation.py`, `loader.py`, `transactions.py` | `tests/unit/test_config.py`, `test_configuration_contracts.py`, `test_hardware_probe.py` | `docs/configuration-reference.md`, `docs/configuration-examples.md` |
| Envelope/topics/payloads | `proto/purdue_rov/cv/v1/envelope.proto`, `src/purdue_rov_cv/wire/topics.py`, `payloads.py`, `validators.py`, `src/purdue_rov_cv/runtime/envelope.py` | `tests/unit/test_wire_contracts.py`, `tests/unit/test_proto_round_trip.py` | `docs/runtime-primitives.md` |
| State/queues/metrics/shutdown | `src/purdue_rov_cv/runtime/state.py`, `queues.py`, `metrics.py`, `shutdown.py`, `json_logging.py` | `tests/unit/test_runtime_state_metrics.py`, `test_runtime_queues.py`, `test_runtime_envelope_logging_shutdown.py` | `docs/runtime-primitives.md`, `docs/error-code-contract.md` |
| Broker/control | `src/purdue_rov_cv/messaging/broker.py`, `router.py`, `client.py`, `registry.py`, `protocol.py`, `deadlines.py` | `tests/unit/test_phase4_control_primitives.py`, `tests/integration/test_phase4_processes.py`, `tests/transport` | `docs/broker-control-routing.md` |
| Task API/runner/model gate | `src/purdue_rov_cv/modules/base.py`, `echo.py`, `src/purdue_rov_cv/module_runner/service.py`, `publisher.py`, `artifacts.py` | `tests/unit/test_phase5_module_runner.py`, `tests/integration/test_phase5_module_runner_processes.py` | `docs/module-runner.md`, `docs/module-development.md` |
| Shared memory | `src/purdue_rov_cv/frame_buffer/header.py`, `buffer.py`, `src/purdue_rov_cv/module_runner/frame_source.py` | `tests/unit/test_phase6_frame_buffer.py`, `tests/integration/test_phase6_processes.py` | `docs/shared-memory-frame-buffer.md` |
| Camera/UVC | `src/purdue_rov_cv/camera/service.py`, `backend.py`, `v4l2.py`, `entrypoints.py`, `scripts/configure_udev.py` | `tests/unit/test_phase6_camera_service.py`, `test_phase10_v4l2.py`, `tests/hardware/test_phase10_uvc.py` | `docs/camera-profiles.md`, `docs/shared-memory-frame-buffer.md` |
| RTP/correlation | `src/purdue_rov_cv/video/sender.py`, `gstreamer.py`, `service.py`, `subscriber.py`, `correlation.py`, `mapping.py` | `tests/unit/test_phase7_video_receiver.py`, `tests/integration/test_phase7_gstreamer.py`, `tests/transport` | `docs/surface-video-receiver.md`, `docs/transport-resilience.md` |
| Recording/replay | `src/purdue_rov_cv/recording/service.py`, `mcap_writer.py`, `video.py`, `src/purdue_rov_cv/replay/structured.py`, `video.py` | `tests/unit/test_phase8_recording_replay.py`, `tests/integration/test_phase8_processes.py` | `docs/recording-replay.md` |
| Clock/preflight/start gate | `src/purdue_rov_cv/preflight/clock.py`, `checks.py`, `probes.py`, `authorization.py`, `health.py` | `tests/unit/test_phase9_preflight.py`, `tests/integration/test_phase9_full_system.py`, `test_phase11_deployment.py` | `docs/phase9-preflight.md`, `docs/phase11-deployment.md` |
| Deployment/systemd | `systemd/*`, `scripts/install_deployment.sh`, `scripts/configure_deployment.py`, `scripts/install_chrony.sh` | deployment unit tests, `scripts/run_systemd_acceptance.py` | `docs/operations.md`, `docs/phase11-deployment.md` |
| CI/transport/soak | `.github/workflows/ci.yml`, `transport.yml`, `phase9-soak.yml` | `tests/unit`, `integration`, `transport` | `README.md`, `docs/transport-resilience.md` |
| Physical/final acceptance | `tests/hardware/test_phase10_uvc.py`, `scripts/run_phase11_hil.py`, `config/phase11-acceptance-matrix.json` | opt-in hardware/HIL execution; currently no checked-in PASS | `docs/phase11-acceptance-matrix.md`, `docs/phase11-deployment.md` |
