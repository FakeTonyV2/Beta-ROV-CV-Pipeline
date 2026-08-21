# Module development

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
