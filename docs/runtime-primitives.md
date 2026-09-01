# Shared runtime primitives

`purdue_rov_cv.runtime` is the common mechanism layer for later camera, module,
broker, recorder, and control services. It does not open hardware, sockets,
model runtimes, or GStreamer pipelines, and it never exits the interpreter.

## Queue contracts

| Primitive | Capacity | Producer and overflow behavior | Counter | State/log effect | Escalation | Consumer timeout |
|---|---:|---|---|---|---|---:|
| `FrameInputQueue` | 1 | Nonblocking; private NumPy copy; replace oldest with newest | `frames_dropped_before_processing` | None | None | <=250 ms |
| `CvResultQueue` | 4 | Nonblocking; replace oldest with newest | `results_dropped_local_queue` | None | None | <=250 ms |
| `PriorityPublicationQueue` | 32 | Wait at most 50 ms, then drop incoming item | `priority_messages_dropped` | `DEGRADED`; `PRIORITY_QUEUE_FULL` | One exit-75 request when failures reach five in a rolling 10 s window | <=250 ms |
| `ControlCommandQueue` | 16 | Nonblocking; reject incoming command | None | `REJECTED / MODULE_BUSY` | None | <=250 ms |
| `ControlResultQueue` | 16 | Wait at most 100 ms; cache result before escalation | None | `CONTROL_RESULT_QUEUE_FULL` | Exit-75 request | <=250 ms |
| `RecorderQueue` | 4096 | Nonblocking; drop newest incoming record | `recorder_queue_overflow` | `DEGRADED`; rate-limited CRITICAL | None | <=250 ms |

Required queue callbacks receive structured events, degradation reasons, or
`EscalationRequest` values; queue construction cannot silently omit a required
effect. If a callback fails, the queue attempts the remaining required actions
and returns a structured `CallbackFailure`. Service supervisors translate
escalation data into a process exit. Helpers never call `sys.exit()`.

## Process exit statuses

| Status | Meaning |
|---:|---|
| `0` | Clean shutdown |
| `64` | Invalid command-line arguments |
| `70` | Internal software failure |
| `74` | I/O failure |
| `75` | Temporary failure; supervisor restart is permitted |
| `78` | Invalid configuration or incompatible deployment |

Reusable primitives return `EscalationRequest` or `ShutdownResult`; only an
installed CLI/service boundary converts the status into process termination.

## State, publisher, and envelopes

`ComponentStateMachine` enforces the exact normal transition graph. Rejected
transitions preserve state and return `INVALID_STATE_TRANSITION`. Only
`reset_from_error()` permits `ERROR -> STARTING`. Runtime state maps explicitly
to the existing protobuf enum; there is no second wire representation.

`PublisherSequence` creates one UUID per instance and consumes sequence zero,
one, two, and so on before each attempted publication. Failed validation or
transport sends therefore cannot reuse a number. `EnvelopeBuilder` builds the
existing `MessageEnvelope`, uses the static payload registry, and calls the
canonical wire validator, including exact serialized-size checks, before
exposing the two transport frames.

## Metrics and warning suppression

`RuntimeMetrics` defines canonical counters, gauges, and metadata. Counters
reject negative increments, metadata validates canonical state/error values,
and snapshots have deterministic key ordering. Processing average is
lifetime-based; p95 uses the latest 1024 observations so a long-running process
has bounded retention. All mutations and snapshots are thread-safe. Services
own health scheduling; the Phase 2 configuration is the single source of truth
for the supported 500-5000 ms publication interval.

`WarningRateLimiter` keys identical warnings by a stable hashable tuple such as
`(event_code, source_id, discriminator)`. The first warning emits immediately;
repeats within one monotonic second are suppressed, and the next emitted warning
contains `suppressed_count`. Key storage is bounded.

## Logging and shutdown

`StructuredJsonLogger` emits one JSON object per line. Required base fields are
always present; unavailable frame/command context is JSON `null`. UUID fields
use canonical UUID text, and nonfinite context values use JSON-safe strings. UTC
is used for external timestamps and a monotonic clock for runtime durations.
Library import does not configure the root logger. Deployment should capture
stdout/stderr with journald. If persistent files are configured by deployment,
use a 50 MiB rotation target and retain five files.

`ShutdownCoordinator` stops acceptance of new work, exposes a token to bounded
queue polls, and runs ordered cleanup hooks once. Hook registration is serialized
against the first request. Exceptions are returned as structured failures and do
not block later hooks. A hook exceeding the overall deadline produces an exit-75
result; the timed-out daemon cleanup thread may remain alive only until the
owning process exits. Signal integration is explicit.

Service code remains responsible for closing GStreamer, setting ZeroMQ
`LINGER=0`, detaching shared memory, persisting command status, flushing sinks,
and translating the final `ExitCode` at its process-supervisor boundary.
