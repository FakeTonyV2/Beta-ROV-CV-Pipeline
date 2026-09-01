# Module runner contract

Phase 5 hosts one configured task and one `CVModule` instance per operating
system process. It composes the existing wire, configuration, runtime, and
control contracts; it does not introduce alternate lifecycle states, queues,
envelopes, or command schemas.

## Ownership and data flow

The main thread owns the control DEALER socket and evaluates registration,
heartbeat, response delivery, readiness, and the worker watchdog. Frame ingress
alone owns the read-only shared-memory attachment. The worker alone calls module
hooks and `process()`. The publisher thread alone owns the PUB socket.

```text
camera-owned shared memory -> frame ingress -> FrameInputQueue(1)
    -> module worker -> CvResultQueue(4) -> PUB owner -> broker

control DEALER owner -> ControlCommandQueue(16) -> module worker
    -> ControlResultQueue(16) -> control DEALER owner
```

The frame ingress adapter delegates to the canonical Phase 6
`SharedMemoryFrameReader`; it does not parse `SharedMemory.buf`. The reader
validates the fixed 128-byte header, triple-buffer bounds, format, stride, and
odd/even generation contract before returning a process-private snapshot. It
reattaches when a camera restart recreates the stable segment, closes only its
consumer handle during shutdown, and never unlinks. The camera service owns
creation, writes, stale-owner recovery, and normal unlink. See
`docs/shared-memory-frame-buffer.md` for the language-independent binary layout.

The available v1 lifecycle contract does not define a post-start module state
transition for temporary camera loss. The current safe behavior therefore keeps
an already running module `RUNNING`, stops receiving new frames, and transparently
reattaches. It never reaccepts a stale generation. Loss and recovery are exposed
through `input_source_present`, `shared_memory_disconnects`, and
`shared_memory_reattach_count`, one structured warning/recovery pair per episode,
and the externally published `frames_read` counter ceasing/resuming. Readiness
events remain startup prerequisites and are not reinterpreted as recovery state.
A future specification revision must choose explicitly between this behavior and
a module-state degradation transition before downstream phases rely on it.

`frames_read` increments once when ingress accepts a sampled source frame.
`frames_processed` increments once after `process()` returns a valid output list.
Queue replacement, processing exception/deadline, successful result send, and
nonblocking ZeroMQ drop each increment their canonical Phase 3 counter at the
corresponding boundary.

## Readiness and control

The state remains `STARTING` until configuration and module initialization have
succeeded, registration is acknowledged, the camera attachment exists, and one
valid frame has arrived. It then becomes `READY`; a valid `START` invokes
`on_start()` through the worker and moves to `RUNNING`. `STOP` returns to
`READY`. Three consecutive processing failures move to `ERROR`; ingress pauses
while GET_STATUS, health, GET_COMMAND_STATUS, and RESET remain available.

State-changing UUIDs are atomically placed in `CommandStatusCache` with
`COMMAND_STATUS_RECEIVED` before queue dispatch. The same atomic reservation
rejects a concurrent duplicate. Final completed, rejected, failed, or unknown
outcomes replace the reservation. Pending reservations never expire or evict;
when all 1024 entries are pending, a new reservation is rejected as
`MODULE_BUSY`. Final entries retain the canonical ten-minute TTL and are evicted
oldest-first when capacity is needed.

Registration uses one module session UUID per execution, the canonical DEALER
identity, one-second production retry cadence, a 500 ms acknowledgement wait,
ten attempts, and exit 75 on exhaustion. Heartbeats use the same socket owner at
one-second production intervals.

## Processing and publication

Processing duration is measured with `time.monotonic_ns()`. A successful call
resets both consecutive deadline and exception streaks as applicable. Five
consecutive deadline misses degrade the component; twenty request exit 75.
Three consecutive exceptions enter `ERROR`. Worker progress is updated only at
worker loop/cycle boundaries. A stall beyond `max(10 seconds, 5 x
processing_deadline_ms)` requests exit 75.

The PUB socket connects to the Phase 4 broker with HWM 5, nonblocking send,
zero linger, immediate reconnect behavior, TCP keepalive, and the 4 MiB message
limit. The canonical envelope builder validates the configured payload class and
allocates sequence before every send attempt. `zmq.Again` drops once, increments
`zmq_send_dropped`, and is never retried. Module health uses the same publisher
session and the canonical diagnostic payload. Health includes the latest
canonical error code and message, including the terminal processing failure
that moves a module to `ERROR`.

## Shutdown and later ownership

SIGTERM stops command/frame acceptance, requests `STOPPING`, lets ingress,
worker, and publisher close resources they own, invokes module stop/shutdown
hooks on the worker, closes DEALER with zero linger, terminates the runner's ZMQ
context, and completes as `STOPPED`. Cleanup is bounded; a stuck thread requests
exit 75.

Phase 6 owns simulated GStreamer teardown and shared-memory creation/unlink. A
later production data-plane subscriber/recorder
receiver owns `observed_sequence_gaps`; this runner reads frames from shared
memory and has no MessageEnvelope subscriber. Full installed-systemd deployment
testing belongs to the deployment/provisioning phase; Phase 5 supplies and tests
the console boundary and systemd unit template.

The registration schema still has no generation value with which to order two
never-seen sessions racing for the same stable module ID. The router therefore
retains the documented arrival-order authority rule and permanently rejects
retired sessions; guessing a generation would change the wire contract.
