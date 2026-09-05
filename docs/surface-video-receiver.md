# Surface video receiver and frame correlation

Phase 7 runs one independently supervised process for each configured
surface-visible camera:

```text
purdue-cv-video-receiver --camera <camera_id> --config <mission.yaml>
```

The systemd template is `purdue-cv-video-receiver@.service`. Ports and dynamic
payload types come only from the configured `stream_index`; no fixed camera
count is embedded in the receiver.

## Pipeline and RTP identity

The production receive path is:

```text
udpsrc(buffer-size=4194304, configured RTP caps)
  -> rtpjitterbuffer(latency=50, drop-on-latency=true, do-lost=true)
  -> rtph264depay -> h264parse -> tee
     -> capacity-one leaky queue -> avdec_h264 -> BGR -> dropping appsink
     -> bounded encoded queue -> appsink (future recorder/relay seam)
```

The `udpsrc` probe counts only packets with a parseable RTP header and updates
`last_rtp_packet_monotonic_ns`. A second probe after the jitterbuffer reads the
still-present RTP `(ssrc, timestamp)` and associates it with the PTS assigned by
the jitterbuffer. Depayload, parse, and decode preserve that PTS. The decoded
appsink therefore recovers the exact RTP key from a bounded 512-entry PTS map;
it never synthesizes RTP identity from arrival or wall time. Jitterbuffer
`GstRTPPacketLost` events increment packet loss independently of decoded-frame
count.

The production camera service assigns frame identity at its GStreamer source
probe before a local/encoded tee. The local branch writes the same source frame
number and capture clocks to shared memory. The encoded branch probes encoder
input/output and `rtph264pay`, then sends UDP RTP and queues one canonical
`FrameIndex` through the broker. One mapper is retained across camera-pipeline
rebuilds so the camera session and frame numbering do not reset. Source and
encoded maps plus RTP-key deduplication are bounded to 256 entries.
Deduplication uses `(ssrc, timestamp)`, so a fragmented H.264 access unit
publishes exactly one canonical `FrameIndex`. A standalone sender using the
same mapper remains available only for focused transport tests.

## Validated FrameIndex input

The receiver connects a real `SUB` socket to the broker subscriber endpoint and
subscribes only to `cv.frame_index.<camera_id>`. Its settings are `RCVHWM=5`,
`RCVTIMEO=250 ms`, `LINGER=0`, reconnect intervals 250–2000 ms, TCP keepalive
1/5/1/3, and `MAXMSGSIZE=4 MiB`. `ZMQ_CONFLATE` is not enabled.

Every receive goes through `ReceivedMultipartValidator`: exactly two frames,
canonical topic/envelope identity, schema, payload registry, UUIDs, sizes, and
the `frame_index_v1` protobuf are validated before cache insertion. The shared
publisher-session/source sequence tracker owns `observed_sequence_gaps` here.
A gap is transport continuity loss; it is distinct from a decoded-frame cache
miss. Normal 250 ms receive timeouts only perform shutdown and expiry checks.

## Cache and correlation

Each receiver owns one `OrderedDict` cache. The key is
`(rtp_ssrc, rtp_timestamp)`, capacity is 512, and monotonic TTL is exactly two
seconds. Capacity evicts oldest first. An identical duplicate is ignored. A
different camera session/frame identity on the same key poisons that key until
expiry, preventing either stale identity from being claimed. Exact mappings
are consumed once, and stream teardown clears cache and pending work before a
replacement pipeline can deliver frames.

Decoded frames first attempt exact lookup. If the index is in flight, the frame
waits in an independent bounded pending set for at most 100 ms; the GStreamer
callback never sleeps. Exact arrival immediately resolves every matching
pending frame. Deadline expiry delivers the frame as `UNMATCHED` and never
reuses an older mapping.

Debug approximation is opt-in. It compares only 90 kHz RTP timestamps within
the same SSRC, uses wrap-aware uint32 distance, and accepts at most 4500 ticks
(50 ms). The result is explicitly `APPROXIMATE`, never `EXACT`.

The downstream object is a decoded immutable frame plus one explicit
`FrameCorrelation`: `EXACT`, `APPROXIMATE`, or `UNMATCHED`. Exact/approximate
results include canonical camera ID, camera session UUID, frame number, and
capture UTC time. Phase 7 does not draw CV graphics or attach stale CV results.

## Local fan-out and recording boundary

Decoded operator/debug/surface-CV consumers each receive an independent
capacity-one keep-latest subscription. One slow consumer therefore cannot block
GStreamer or accumulate history. A separate bounded encoded-access-unit branch
is the seam for recorder/relay consumers, preserving H.264 without re-encoding.
Final segment naming, disk protection, recording control, and MCAP/video
coordination remain owned by the recorder subsystem.

## Lifecycle, health, and metrics

Initialization transitions `STARTING -> READY`; the first valid decoded frame
moves to `RUNNING`. Packet and decoded clocks are distinct. If no parseable RTP
packet arrives for two monotonic seconds, one loss episode moves to `DEGRADED`,
exposes `STREAM LOST`, sets canonical `VIDEO_STREAM_LOST` health, transitions
the pipeline to `NULL`, waits an interruptible one second, and builds a fresh
pipeline with new probes and callbacks. Each rebuild attempt increments
`stream_restarts`. Recovery requires five consecutive valid decoded frames; an
invalid decode resets the streak.

Metric boundaries are:

- `rtp_packets_received`: one parseable packet at the UDP source probe;
- `rtp_packets_lost`: one jitterbuffer lost-packet event;
- `decoded_frames`: one valid frame accepted from decoded appsink;
- `frame_index_hits`: one delivered `EXACT` result;
- `frame_index_misses`: one delivered `UNMATCHED` result after the wait;
- `stream_restarts`: one actual replacement-pipeline construction attempt;
- `last_frame_age_ms`: monotonic age since the last valid decoded frame.

Health is published as the existing `DiagnosticStatus.video` and `.messaging`
groups on `cv.health.video_receiver_<stream_index>`. SIGTERM stops subscriber
and health loops, destroys GStreamer, cancels pending correlation, closes both
fan-outs, and uses `LINGER=0` before owned contexts terminate.
An unexpected subscriber or health-worker exit is supervised, records canonical
`INTERNAL_ERROR`, transitions the service to `ERROR`, and fails the process for
the systemd restart policy rather than leaving a silently degraded receiver.

## Deferred-risk disposition

| Risk | Phase 7 classification | Action / owner | Blocking |
| --- | --- | --- | --- |
| Running module during temporary source loss | SPECIFICATION AMBIGUITY | Receiver lifecycle remains independent; module behavior awaits an architecture decision. | No |
| Shared-memory owner PID reuse | OWNED BY FUTURE VERSIONED SHARED-MEMORY PROTOCOL | The v1 128-byte header is unchanged. | No |
| Python/GStreamer environment | ALREADY RESOLVED on Ubuntu 24.04 | Reuse system `python3-gi`/`python3-gst-1.0` through the project Python 3.12 venv; smoke verification now includes `GstRtp`. | No on reference platform |
| Physical V4L2/DepthAI capture/provisioning | OWNED BY PHYSICAL CAMERA / PROVISIONING SUBSYSTEM | Phase 7 opens only surface UDP/GStreamer resources. | No |
| `observed_sequence_gaps` | FIX IN PHASE 7 | Production `FrameIndex` SUB uses the shared `ReceivedMultipartValidator`. | No |
| Final video persistence | CORRECTLY DEFERRED TO RECORDER SUBSYSTEM | Bounded encoded H.264 handoff is present; no re-encode. | No |
| Operator CV overlay rendering | OWNED BY OPERATOR SUBSYSTEM | Decoded frames expose explicit identity/quality; no graphics are burned in. | No |

No protobuf or configuration-schema changes were necessary.
