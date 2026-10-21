# System architecture

This page is the short orientation to the V1 runtime. The [Senior Maintainer &
Architecture Guide](senior-maintainer-guide.md) is the detailed reference for
contracts, failure handling, and subsystem ownership.

## Runtime topology

```text
Pi / onboard host
  V4L2 cameras -> camera services -> shared-memory frame buffers
                                      |                  |
                                      |                  +-> module runner -> CVModule -> protobuf results
                                      |                                           |
                                      +-> H.264 RTP/UDP --------------------------+-> ZeroMQ broker
                                                                                       |
Surface host                                                                        subscribers
  RTP receivers -> FrameIndex correlation -> video consumers
  ZeroMQ broker/control router <----------------------------------------------> Pi
  optional recorder: MCAP structured messages + encoded H.264 video
```

The Pi owns camera capture and task inference. Each camera and task runs in a
separate supervised process. Camera pixels cross local process boundaries in a
bounded shared-memory frame buffer. Structured results and control messages use
versioned Protocol Buffers over ZeroMQ. Encoded video uses RTP/UDP; it does not
travel through the structured messaging broker.

## Process and contract boundaries

- **Camera service:** opens a configured V4L2 device, publishes frames and
  `FrameIndex` metadata, and reconnects after capture failures.
- **Module runner:** owns frame subscriptions, bounded input queues, task
  lifecycle, configuration updates, result envelopes, and health reporting.
- **CVModule:** implements deterministic `initialize()` and `process(Frame)`
  behavior. It does not open cameras or create transport sockets.
- **Broker and control router:** carry validated result envelopes and explicit
  lifecycle commands on separate data and control paths.
- **Surface receiver:** receives video and correlates it with structured
  `FrameIndex` messages by exact identity; it does not guess when a match is
  missing.
- **Recorder:** when enabled, writes structured traffic to MCAP and encoded
  H.264 to segmented Matroska files. Recording is optional in the generated
  surface systemd topology.
- **Preflight and systemd:** preflight checks readiness before each production
  `START`; systemd supervises process availability and restarts eligible
  failures. The saved mission-state decision is point-in-time evidence, not a
  continuous system-wide safety interlock.

## Current production task

The checked-in production mission runs `CrabDetectionModule`, using a pinned
ONNX artifact and the `bounding_boxes_v1` payload. `EchoModule` is a development
reference task. Module task behavior and artifact compatibility are described
in [crab detection](crab-detection.md) and [module development](module-development.md).

## Configuration and deployment

`config/mission.yaml` is the production mission example. `config/development.yaml`
keeps a lightweight runner example. Strict validation checks cross-field
constraints before deployment. The installer generates role-specific systemd
topology and chrony settings from the validated mission. See the [setup and
usage guide](setup-and-usage-guide.md), [operations guide](operations.md), and
[Phase 11 acceptance matrix](phase11-acceptance-matrix.md).

## Read next

- [Senior Maintainer & Architecture Guide](senior-maintainer-guide.md): full
  subsystem contracts, verification status, and failure behavior.
- [Shared-memory frame buffer](shared-memory-frame-buffer.md): frame ownership
  and process transport.
- [Broker and control routing](broker-control-routing.md): message and command
  paths.
- [Recording and replay](recording-replay.md): MCAP/MKV behavior and limits.
- [Phase 11 deployment](phase11-deployment.md): installation and physical
  acceptance workflow.
