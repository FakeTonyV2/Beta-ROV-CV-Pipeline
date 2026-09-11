# Module development

## Required v1 workflow

1. Define or reuse the versioned protobuf message.
2. Register its payload type in the canonical payload registry.
3. Generate and check in Python bindings with `scripts/generate_proto.sh`.
4. Add protobuf and envelope round-trip tests.
5. Implement `CVModule`; do not create another runner.
6. Declare accepted pixel formats and validate source compatibility.
7. Separate static configuration from the permitted dynamic fields.
8. Add the task to canonical mission configuration.
9. Add a deterministic known-input unit test.
10. Exercise representative recorded video through the production reader.
11. Set and verify the processing deadline.
12. Emit standard frame, error, deadline, publish, and health metrics.
13. Run unit, integration, full, coverage, type, lint, and format checks.
14. Replay a recorded session through the Phase 8 path.
15. Run hardware preflight on the intended Pi/camera/model.
16. Document payload meaning, configuration, limits, and operator behavior.

A module must not bypass the shared frame buffer, envelope builder, payload
registry, standard publisher, control state machine, configuration validation,
or health reporting. The reference Echo module follows these boundaries; adding
an architectural side channel is not a valid extension workflow.

Task implementations subclass `purdue_rov_cv.modules.CVModule`. They implement
`initialize(context)` and `process(frame)` and may override dynamic configuration
and lifecycle hooks. `process()` receives a process-private NumPy-backed `Frame`
and returns protobuf payload objects matching the task's configured
`payload_type`.

A module must not open a camera, shared-memory object, PUB/DEALER socket,
database, or video stream. It must not build or serialize `MessageEnvelope`.
Those resources belong to the runner and adjacent platform services.

`EchoModule` is the minimal reference implementation. For
`bounding_boxes_v1`, it emits one deterministic full-frame `echo` detection with
the current dynamic confidence threshold. It uses exactly the same worker,
queue, envelope, PUB, control, and shutdown path as other task modules.

Dynamic configuration arrives as native Python values converted with protobuf
`MessageToDict`. The runner validates updates against the Phase 2 policy and
accepts the current task's `max_input_fps` and `dynamic` fields plus runner-owned
`diagnostics.publish_interval_ms` and dynamic `debug_snapshots` fields. Updates
are merged into the active settings and delivered atomically to the module; an
empty update is a no-op, and a callback failure rolls back the candidate values.
Static deployment changes are rejected with `RESTART_REQUIRED` before invoking
the module, and the degraded module cannot be restarted until reset/relaunch.

Each configured task is launched independently:

```bash
purdue-cv-module-runner --task gate_detection --config /etc/purdue-rov-cv/mission.yaml
```

The `purdue-cv-module@.service` systemd template preserves one task instance per
OS process.
