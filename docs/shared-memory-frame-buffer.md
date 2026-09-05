# Simulated camera and shared-memory frame contract

Phase 6 provides one camera-service process and one stable shared-memory segment
per configured camera. The simulated backend uses a live GStreamer
`videotestsrc`, explicit raw BGR caps, and an `appsink` configured with
`max-buffers=1 drop=true`. A source-pad probe records wall and monotonic capture
timestamps before color conversion. Rebuild and shutdown transition the owned
pipeline to `NULL` and remove the probe.

When `stream_to_surface` is enabled, that same source probe assigns the
canonical frame number before a tee. The raw branch writes the assigned number
to shared memory while the bounded encoded branch sends H.264/RTP and publishes
the corresponding `FrameIndex`. A dropped raw appsink frame may therefore
produce a legitimate gap in shared-memory frame numbers; numbers never move
backward or reset during an in-process pipeline rebuild.

The production service command is:

```text
purdue-cv-camera --camera <camera_id> --config <mission.yaml>
```

It uses the configured width, height, frame rate, and `slot_capacity_bytes`.
Phase 6 deliberately does not probe or open the configured physical V4L2 or OAK
device. Those responsibilities belong to the physical-camera/provisioning
subsystem.

## Stable segment identity and ownership

The segment name is exactly:

```text
purdue_rov_cv_<camera_id>
```

Camera IDs must match `[a-z][a-z0-9_]{0,63}`; invalid identifiers are rejected,
not sanitized. The camera process creates and normally unlinks the segment.
Readers attach, copy frames, close their own `SharedMemory` handle, and never
unlink camera-owned memory.

At startup, an absent segment is created. For an existing segment, a valid live
owner PID rejects duplicate startup with exit 78. A dead positive owner PID
allows unlink and clean recreation. A malformed segment is cleaned only when it
still contains a positive owner PID that the centralized process-existence
check proves dead; missing/invalid ownership is rejected conservatively. An
`EPERM` process check is treated as alive. PID reuse remains an unavoidable v1
limitation because the fixed header has no process-start timestamp.

Creator arbitration and consumer attachment/header validation share one short
POSIX advisory startup lock. This prevents simultaneous stale replacements from
unlinking each other and prevents a consumer from observing the interval between
name creation and initial header encoding. The lock is never held during frame
publication or reading; the data path remains lock-free.

## Binary layout

All integers are little-endian. The header is exactly 128 bytes and is followed
by exactly three fixed-capacity slots. Segment size is
`128 + 3 * slot_capacity_bytes`.

| Offset | Size | Encoding | Field |
| ---: | ---: | --- | --- |
| 0 | 8 | bytes | magic `PROVCV01` |
| 8 | 4 | uint32 | version `1` |
| 12 | 4 | uint32 | slot count `3` |
| 16 | 4 | uint32 | slot capacity bytes |
| 20 | 4 | uint32 | active slot index |
| 24 | 8 | uint64 | generation |
| 32 | 8 | uint64 | frame number |
| 40 | 8 | int64 | capture UNIX ns |
| 48 | 8 | int64 | capture monotonic ns |
| 56 | 4 | uint32 | width |
| 60 | 4 | uint32 | height |
| 64 | 4 | uint32 | stride bytes |
| 68 | 4 | uint32 | data length bytes |
| 72 | 4 | uint32 | pixel format |
| 76 | 4 | uint32 | owner PID |
| 80 | 16 | bytes | camera-service session UUID |
| 96 | 32 | zero bytes | reserved |

Pixel formats are `1=BGR8`, `2=RGB8`, `3=GRAY8`, and `4=DEPTH16_MM`. Rows may
contain padding described by stride. Readers construct BGR/RGB arrays as
`height × width × 3`, gray arrays as `height × width`, and depth arrays as
little-endian uint16 `height × width`; padding is excluded from the returned
process-private NumPy array. No implicit color conversion occurs in the reader.

## Publication and read consistency

The writer validates the complete candidate before changing published state.
It selects `(active_slot + 1) mod 3`, publishes an odd generation, copies the
complete slot, writes all metadata and the new active slot while generation is
odd, then publishes the next even generation. Oversized or malformed input is
rejected without truncating or disturbing the previous valid publication.
Generation uses safe uint64 wrap arithmetic; ambiguity across a theoretical
full wrap is documented rather than extending the v1 header.

A reader makes at most three consistency attempts. It reads generation, rejects
odd values, copies and validates the header, bounds-checks the active slot,
copies bytes into private memory, then accepts only if the second generation is
the same even value. Three conflicts return `CONFLICT` and increment
`shared_memory_read_conflicts` once. No process-shared mutex is used. On Ubuntu,
the reader compares the mapped `/dev/shm` inode with the visible stable name so
an unlinked/recreated camera segment causes detach and reattachment rather than
indefinite use of the old mapping.

## Camera identity, state, retry, and metrics

One random 16-byte camera session UUID is generated per camera-service process.
The first accepted frame is number 0. Backend rebuilds retain the session and
continue numbering; process restart generates a new session and resets to 0.
The wall timestamp uses `time.time_ns()`. Timeout, retry, and frame age use
monotonic time.

Startup progresses `STARTING → READY`, then the first accepted frame moves to
`RUNNING`. A pipeline error or two seconds without an accepted frame moves to
`DEGRADED`, destroys the backend, and rebuilds after interruptible delays of
0.5, 1, 2, 4, 5, 5… seconds. Retries continue after ten failures. SIGTERM
interrupts backoff, tears down GStreamer, closes and unlinks owned memory, and
uses the canonical `STOPPING → STOPPED` lifecycle.

Metric boundaries are:

- `frames_received`: after one source frame is validated and committed;
- `shared_memory_write_count`: after the final even generation is published;
- `frame_timeouts`: once per two-second timeout/rebuild episode;
- `pipeline_restarts`: once per replacement-backend start attempt, successful
  or failed;
- `shared_memory_read_conflicts`: once per read operation whose three attempts
  all conflict.

`current_width`, `current_height`, `current_pixel_format`, frame rate, and frame
age are updated by the camera service. `usb_device_present` is false for the
non-hardware simulated backend. Repeated equivalent retry warnings use the
shared warning limiter.

No protobuf or configuration-schema change is required for Phase 6. Existing
camera dimensions, rate, ID, and slot capacity are sufficient.
