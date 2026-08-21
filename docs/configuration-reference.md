# Configuration reference

Production loads `/etc/purdue-rov-cv/mission.yaml`. An explicit path has highest
precedence, then `PURDUE_ROV_CV_CONFIG`, then that default. The only permitted
field override is `PURDUE_ROV_CV_LOG_LEVEL`, which replaces
`diagnostics.log_level` before complete validation. No individual camera,
network, task, model, safety, or messaging field is environment-overridable.

All fields are required; there are no implicit YAML defaults. `Static` means a
change requires a restart. `Dynamic` fields may be included in a transaction
plan. `Unsupported` changes are valid but cannot be applied to a running system.
Static validation never reads a file, resolves a symlink, opens a socket, or
contacts hardware.

Hardware preflight is separate and opt-in. It first completes static validation,
then reads the deployed Linux host. It never changes a camera mode: the V4L2
query is read-only. It is not part of normal CI or laptop validation because it
needs the deployed device paths, model artifacts, and inference runtime.

## CLI

```bash
rov-cv config validate config/mission.yaml
rov-cv config validate config/mission.yaml --probe-hardware
```

The command returns `0` when valid, `74` when an existing configuration cannot
be read because of an I/O failure, and `78` for a missing/malformed/invalid
configuration or incompatible hardware deployment. Invalid command-line syntax
returns `64`; an unexpected installed-CLI failure returns `70`. Errors start
with canonical `CONFIG_INVALID`, followed by a diagnostic kind and YAML path.
`--probe-hardware` returns `0` only when live checks pass and `78` when the host
cannot run the probe or deployed hardware, artifacts, runtimes, or endpoints do
not meet the configuration.

## Fields

| Field path | Type / valid values | Policy | Static validation | Hardware-aware validation / failure behavior | Example |
|---|---|---|---|---|---|
| `schema_version` | integer `1` | Static | Reject all other versions. | None. `UNSUPPORTED_SCHEMA_VERSION`. | `1` |
| `device.device_id`, `device.execution_target` | lowercase identifier | Static | Required, 1-64 chars, starts with a letter. | None. | `rov_pi5` |
| `network.tether_interface` | lowercase identifier | Static | Identifier grammar. | Interface/link checks are not part of this probe. | `eth0` |
| `network.rov_ip`, `network.surface_ip`, `clock.server_ip` | literal IPv4 address | Static | DNS names and malformed addresses reject. | Reachability/time-sync checks are not part of this probe. | `192.168.50.2` |
| `clock.maximum_offset_ms`, `clock.check_interval_seconds`, `clock.invalid_after_failures` | bounded positive integers | Static | Respectively <=60000, <=3600, and <=100. | Clock monitoring is a runtime responsibility. | `10`, `5`, `3` |
| `messaging.broker.publisher_endpoint`, `messaging.broker.subscriber_endpoint`, `messaging.control.client_endpoint` | `tcp://<IPv4>:<port>` | Static | Literal IPv4, port 1-65535, and wildcard-aware TCP collision checking. | Linux preflight attempts a bind then closes it. This follows the current contract that every listed TCP endpoint is local; add endpoint-role fields before changing that assumption. | `tcp://192.168.50.2:5555` |
| `messaging.control.module_endpoint` | absolute `ipc:///` socket path | Static | Reject root, traversal, trailing separators, and malformed IPC endpoints. | Preflight checks that the parent directory is usable and the socket pathname is free. | `ipc:///run/purdue-rov-cv/module-control.sock` |
| `messaging.max_message_bytes`, `result_send_hwm`, `result_receive_hwm` | positive integers | Static | Message limit <=4 MiB; HWM <=10000. | None. | `4194304`, `5` |
| `diagnostics.publish_interval_ms` | integer 500-5000 | Dynamic | Health publication interval contract. | Sent to every enabled task module. | `1000` |
| `diagnostics.log_level` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` | Unsupported | Enum only; may be supplied by the allowed environment override. | Requires later runtime integration. | `INFO` |
| `debug_snapshots.enabled`, `maximum_rate_hz`, `jpeg_quality` | boolean, 0-60 Hz, 1-95 | Dynamic | Bounds and type checks. | Sent to every enabled task module. | `true`, `1.0`, `70` |
| `debug_snapshots.maximum_width`, `maximum_height` | integer 1-640 / 1-360 | Static | JPEG dimension bounds. | Camera/image support is a runtime responsibility. | `640`, `360` |
| `recording.enabled`, `directory`, `video_segment_seconds`, `minimum_free_space_gib`, `structured.*` | boolean, absolute path, bounded integers, compression string | Unsupported | Lexically safe absolute paths and value types. | Free-space and recorder checks are later runtime work. | `/var/lib/purdue-rov-cv/recordings` |
| `camera_limits.maximum_configured`, `maximum_active` | positive integers | Static | `maximum_active` cannot exceed configured; there is no implicit eight-camera cap. RTP payload types allow stream indexes 0-31. | None. | `16`, `8` |
| `cameras.<camera_id>` | dynamic keyed mapping | Static | Key uses shared identifier grammar; additions/removals require restart. | Every configured camera is preflighted. | `front_camera:` |
| `cameras.*.adapter` | `v4l2` or `oakd` | Static | Enum. | The current Linux probe requires the selected path to resolve to a V4L2 video device, then checks its listed capture modes. | `v4l2` |
| `cameras.*.device_path`, `device_path_kind` | lexical absolute Linux path; `by_id` / `fallback` | Static | Reject `/dev/videoN`, traversal, empty parts, and trailing separators. `by_id` is under `/dev/v4l/by-id/`; `fallback` is `/dev/purdue-rov-cv/<camera_id>`. | Preflight checks existence, symlink stability, and resolution to a V4L2 `/dev/videoN` character device. | `/dev/v4l/by-id/usb-camera` |
| `cameras.*.format`, `width`, `height`, `frame_rate`, `allow_software_encode`, `slot_capacity_bytes` | enum, bounded dimensions/rate, boolean, positive integer | Static | H264/MJPEG/YUYV/NV12; width <=7680, height <=4320, FPS <=240. | Preflight reads `v4l2-ctl --list-formats-ext` and requires the exact format, resolution, and frame rate. | `h264`, `1920`, `1080`, `30` |
| `cameras.*.stream_index`, `stream_to_surface`, `cv_enabled` | nonnegative integer, booleans | Static | Indexes are unique; derive RTP `5000+2i`, RTCP `RTP+1`, PT `96+i`; PT must be 96-127. Active means CV or surface streaming. | Preflight briefly binds derived UDP ports, then closes them. | `0`, `true`, `true` |
| `tasks.<task_id>` | dynamic keyed mapping | Static except rows below | Key grammar; additions/removals require restart. | None. | `gate_detection:` |
| `tasks.*.module_class`, `enabled`, `input_camera`, `execution_target`, `processing_deadline_ms`, `publish_topic`, `payload_type` | nonblank class, boolean, identifiers, positive integer, registered topic/payload | Static | Enabled task must use a CV-enabled configured camera; target must match host; topic must match task/camera; payload must be a CV-result registry entry. | Module loading is later runtime work. | `cv.result.gate_detection.front_camera` |
| `tasks.*.max_input_fps`, `tasks.*.dynamic.confidence_threshold` | integer 1-240; float 0-1 | Dynamic | Exact listed fields only; unknown future task fields safely fall back to static. | Only enabled task modules receive updates. | `15`, `0.6` |
| `tasks.*.artifact.format`, `path`, `sha256`, `runtime` | `onnx`/`tensorrt`, absolute path, lowercase SHA-256, runtime enum | Static | SHA-256 is exactly 64 lowercase hex characters; ONNX supports ONNX Runtime or TensorRT, TensorRT artifacts require TensorRT. | For enabled tasks, preflight checks readable artifact existence, SHA-256, and that the requested runtime module is importable. | `onnx`, `/opt/.../model.onnx` |

## Camera and artifact concepts

`maximum_active: 8` in `config/mission.yaml` is a deployment capacity choice,
not a hidden application limit. This initial ROV profile can run at most eight
CV- or surface-streaming cameras at once, while it may define sixteen cameras.
The protocol permits 32 distinct stream indexes (payload types `96` through
`127`). Increase the active limit only after bandwidth, encoder, CPU, and memory
testing on the actual Pi.

`v4l2` is the standard Linux Video4Linux2 camera interface used by generic
USB/UVC cameras and many video devices. `oakd` identifies a Luxonis OAK-D
integration; the current contract still expects its selected video stream to
resolve to a V4L2 device. A future non-V4L2 OAK-D path needs a separate probe,
not a weakened check.

`device_path` is the stable name the pipeline opens. It must never be a volatile
enumerated name such as `/dev/video0`, because USB enumeration may change after
reconnect. `device_path_kind: by_id` selects the udev hardware-identity link
under `/dev/v4l/by-id/`; `fallback` selects a deployment-provisioned stable link
under `/dev/purdue-rov-cv/<camera_id>`. The paths describe link/provisioning
strategy, not a different video format.

`stream_index` is a stable media-stream identity, independent of YAML mapping
order. For index `i`, the system derives RTP `5000 + 2i`, RTCP `5001 + 2i`, and
RTP payload type `96 + i`. Index `0` therefore uses ports `5000`/`5001` and
payload type `96`.

The YAML `sha256` is not generated at application startup. Generating a digest
from the artifact the application is about to trust would make integrity checking
self-fulfilling. A release/provisioning job should hash the immutable built
artifact, write that reviewed lowercase digest into the mission manifest, and
the deployed process should only verify it. For a manual release, use
`sha256sum model.onnx` and copy the result to the task artifact entry.

## Cross-field rules and transaction planning

The configuration hash is SHA-256 over canonical sorted JSON from an already
validated model. Diff paths are sorted and leaf-level. Static changes produce
`RESTART_REQUIRED`; unclassified runtime changes reject as `INVALID_COMMAND`.
Plans contain old and new values for each module, apply in sorted module order,
and roll back in reverse order with a one-second per-module timeout. A later
executor must apply every module, restore prior values after a later rejection
or timeout, persist the proposed hash only after success, and only then emit the
plan's `configuration_changed` event template.

`LinuxHardwareProbe` in `src/purdue_rov_cv/config/probes.py` is never called by
static validation. It requires Linux plus `v4l2-ctl` from the `v4l-utils` system
package. A port preflight is point-in-time: successful test sockets close
immediately and cannot reserve a port against later service startup.
