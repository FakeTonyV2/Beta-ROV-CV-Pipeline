# Phase 10 camera provisioning and profiles

Phase 10 owns generic UVC cameras through `adapter: gstreamer_v4l2`. A camera is
opened only by its configured stable identity; `/dev/videoN` is never a valid
production configuration. DepthAI uses `mxid`, and the reserved RealSense
boundary uses `serial_number`; neither backend is probed with `v4l2-ctl`.

## Provisioning workflow

Install `v4l-utils`, GStreamer, the good/bad/ugly/libav plugin sets, and
PyGObject. Inspect without root or changing the host:

```text
python scripts/configure_udev.py config/mission.yaml
python scripts/configure_udev.py config/mission.yaml --json
```

The tool enumerates capture nodes, resolves `/dev/v4l/by-id` links, queries
udev properties, rejects duplicate physical assignments, and prints camera ID,
candidate node, tier, stable property, and resulting symlink. If no by-id link
exists, review the generated rule and install/reload it explicitly:

```text
sudo .venv/bin/python scripts/configure_udev.py config/mission.yaml --install
```

Resolution priority is (1) USB serial through `/dev/v4l/by-id`, (2) exact
`ID_PATH`, then (3) a documented/labeled physical hub port. The provisioning
tool rejects a fallback profile whenever the candidate exposes a serial or
by-id link. Tiers 2 and 3 use `/dev/purdue-rov-cv/<camera_id>`, and tier 3
requires `physical_port_label`. Generated rules match `SUBSYSTEM`, exact
`ID_PATH`, exact V4L2 `ATTR{index}`, and VID/PID when available; they never
match `/dev/videoN`. Rules
also attach `PURDUE_ROV_CV_CAMERA_ID`, `PURDUE_ROV_CV_TIER`, and
`PURDUE_ROV_CV_IDENTITY`; tier 3 also records
`PURDUE_ROV_CV_PORT_LABEL`. Runtime compares all applicable metadata with
configuration, so a
stale link or wrong-device substitution is a configuration failure (exit 78).
A tier-3 camera moved to another port is a hardware configuration change.

## Capability and mode verification

The production probe runs exactly:

```text
v4l2-ctl --device <resolved-device> --list-formats-ext
```

Discrete FourCC/size/FPS combinations become `V4L2Mode` tuples. Mappings are
`H264 -> h264`, `MJPG`/`JPEG -> mjpeg`, `YUYV`/`YUY2 -> yuyv`, and
`NV12 -> nv12`. Decimal FPS is represented as an exact rational: `30.000` is
30, while `29.970` is 2997/100 and does not match 30. Fields from different
advertised modes are never combined. The configured tuple must exist; no
nearest-FPS, lower-resolution, alternate-format, or first-mode fallback occurs.
Hardware validation then runs a one-buffer `gst-launch-1.0` pipeline with the
same exact caps to prove the advertised mode actually opens.

## Production pipelines

Native H.264 uses `v4l2src -> exact video/x-h264 caps -> h264parse -> stamp ->
tee`. Its RTP queue is 2 buffers, zero byte/time limits, downstream-leaky;
`rtph264pay` has configured PT, MTU 1200, and `config-interval=1`. Its CV queue
is one buffer downstream-leaky and decodes through `avdec_h264 -> videoconvert
-> BGR -> appsink(max-buffers=1,drop=true,sync=false)`.

Native MJPEG uses `v4l2src -> exact image/jpeg caps -> stamp -> tee`, the same
queue bounds, `rtpjpegpay` with configured PT and MTU 1200, and `jpegdec ->
videoconvert -> BGR` for CV. The surface receiver selects the matching
`rtpjpegdepay -> jpegparse -> jpegdec` branch, so MJPEG is supported end to end.

Raw YUYV/NV12 may run CV-only and converts to BGR. Surface streaming is rejected
unless that camera alone sets `allow_software_encode: true`; the opted-in path
uses bounded queues and `x264enc`. Mission use of software encoding still
requires the ten-second simultaneous-camera Phase 9 preflight measurements.

The source stamp uses `time.time_ns()` for UTC and monotonic time for recovery.
The camera session and source frame sequence survive pipeline rebuilds; only a
process restart resets them. Native H.264 remains `UNVALIDATED` even when caps
name baseline/constrained-baseline/main/high, because caps do not prove the
absence of B-frames, the keyframe interval, or bitrate behavior. An unknown or
unsupported reported profile is `FAILED`. `VERIFIED` is reserved for a future
hardware stream/control analysis that proves every required property.

## Deployment profile record

Complete one row per physical fallback device after validation. Do not replace
unknown hardware facts with assumed values.

| Camera ID | Backend | Model | VID:PID | Stable identity | Tier | Port-dependent | Labeled port | Validated modes | H.264/MJPEG behavior | Profile status |
|---|---|---|---|---|---|---|---|---|---|---|
| `usb_webcam` (development example) | `gstreamer_v4l2` | deployment-specific | deployment-specific | `pci-0000:00:14.0-usb-0:1.2:1.0` | `id_path` | no | n/a | `mjpeg 1280x720@30` must be verified on target | MJPEG configured | `UNVALIDATED — HARDWARE UNAVAILABLE` |

## Hardware and disconnect evidence

Set `PURDUE_ROV_CV_HARDWARE_CONFIG` and `PURDUE_ROV_CV_HARDWARE_CAMERA`, then
run `pytest -q -m hardware tests/hardware/test_phase10_uvc.py -s`. The suite
prints OS, kernel, Python, GStreamer, v4l2-ctl, stable/resolved paths, tier,
identity, VID:PID, model, advertised modes, and tested mode. It exercises the
camera service, canonical shared-memory/module reader, three rebuilds, running
and degraded shutdown, RTP receive, and exact RTP/FrameIndex key matching while
bounding FD/RSS growth.

Physical disconnect remains an opt-in manual HIL action: set
`PURDUE_ROV_CV_HARDWARE_DISCONNECT=1`, positively identify the configured
stable link, start the test with `-s`, and unplug only that camera when prompted,
observe `usb_device_present=0`, `DEGRADED`, and 0.5/1/2/4/5-second capped retry,
then reconnect it at the same stable identity. Verify the same process session,
strictly increasing frame numbers, shared-memory delivery, RTP reception, and
FrameIndex correlation resume. Never unbind/reset a device selected only by a
`/dev/videoN` name.
