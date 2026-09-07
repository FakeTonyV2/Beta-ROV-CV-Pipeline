# Recording and replay

## Adopted structured-recording decision

The complete v1 specification contained a conflict: detailed §27.1 requires
MCAP, while the §34 summary mentioned SQLite. The Phase 8 detailed requirement
is the more specific adopted decision. Structured mission messages use only
MCAP 1.x; SQLite is not a structured-recording backend. The repository had no
SQLite recorder or SQLite dependency to migrate.

Each session contains `structured.mcap`. It is a chunked, indexed MCAP with
zstd compression and 1,048,576-byte target chunks. All topics share the
`purdue_rov.cv.v1.MessageEnvelope` protobuf schema and each original ZeroMQ
topic has one reused MCAP channel. MCAP `log_time` is recorder receive time,
`publish_time` is the original envelope publish time, and message data is the
unaltered serialized envelope. The writer finalizes its current chunk after
100 messages or 250 ms, whichever happens first, including an idle tail.
MCAP v1 stores its sequence header as `uint32`, while the canonical envelope
field is `uint64`; the recorder fails closed above `2^32 - 1` instead of
truncating or rewriting the envelope. A long-term rollover policy remains a
wire-contract decision.

The recorder subscribes through the production broker and validates the full
two-frame/envelope/payload contract before offering a record to the canonical
capacity-4096 queue. A full queue drops the newest arrival, preserves queued
records, increments `recorder_queue_overflow`, degrades recorder health, and
rate-limits the critical log to one per second.

## Session layout and video

Session and camera identifiers use the canonical lowercase identifier rule.
Files are collision-safe and cannot traverse outside the configured root:

```text
<recording.directory>/<session>/structured.mcap
<recording.directory>/<session>/<camera_id>/<UTC-start>.mkv
```

The session directory is shared: structured and per-camera video processes may
attach in either start order. Exclusive creation of `structured.mcap` prevents a
second structured recorder from overwriting an existing session.

`purdue-cv-video-receiver --record-session <session>` attaches recording to
the Phase 7 encoded H.264 seam. The branch is H.264 parser → `splitmuxsink`
with `matroskamux`; it contains no decoder or encoder. Production segmentation
is 300 seconds. The current segment receives EOS and is finalized during
bounded receiver shutdown.
If the recording branch encounters disk-low, muxer, EOS, or write failure, that
failure is latched: display/decoding may continue, but health remains DEGRADED
and recording does not silently restart or recover to RUNNING.

Structured and video starts require at least 10 GiB (`1024^3` bytes per GiB)
free on the target filesystem. Exactly 10 GiB is allowed. Active recording
stops below 2 GiB; exactly 2 GiB is allowed. The one-second health cadence is
also the disk-monitor cadence. Disk-low produces `DISK_SPACE_LOW`, degraded
health, and `system.event.disk_space_low`; recording does not auto-restart.

Structured shutdown stops ingestion and drains for at most three seconds before
final MCAP flush/finish. If that bounded deadline would abandon queued records,
the count is logged as `RECORDER_SHUTDOWN_DROPPED` and the process exits with a
failure rather than silently claiming a complete recording.

## Commands

Start the dedicated replay broker and recorder:

```text
purdue-cv-replay-broker
purdue-cv-recorder --config /etc/purdue-rov-cv/mission.yaml --session dive_one
```

Replay structured data at 0.25×, 0.5×, 1×, 2×, or maximum speed:

```text
purdue-cv-replay recording.mcap --rate 1
purdue-cv-replay recording.mcap --rate max
```

The replay broker defaults to publisher endpoint `tcp://127.0.0.1:5655` and
subscriber endpoint `tcp://127.0.0.1:5656`. Original topics and envelope
identities are preserved. A configured live endpoint is rejected unless each
invocation includes `--allow-live-broker`. A non-default endpoint without a
mission config is also rejected unless explicitly opted in.

Video replay demuxes Matroska, parses and decodes H.264, and emits the Phase 7
decoded-frame model without re-encoding:

```text
purdue-cv-video-replay segment.mkv --rate 2
```

Finite structured replay uses absolute monotonic deadlines anchored to the
first recorded `log_time`, preventing cumulative scheduler drift. Maximum
speed adds no intentional inter-message delay. All waits poll the canonical
shutdown token at no more than 250 ms.
