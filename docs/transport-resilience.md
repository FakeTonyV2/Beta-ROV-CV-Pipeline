# Phase 7.5 transport resilience and backpressure

Phase 7.5 verifies the intentionally lossy live transport. It does not add
retransmission, durable result queues, or larger high-water marks. The
production settings remain PUB `SNDHWM=5`, SUB `RCVHWM=5`, and broker
XSUB/XPUB `HWM=100`.

## Safe topology and prerequisites

The privileged suite runs only on Linux with `ip`, `tc`, network-namespace
privileges, Python 3.12, and the Phase 7 GStreamer elements. The preferred local
command runs everything inside a disposable privileged Docker container:

```bash
bash scripts/run_transport_tests.sh
```

The container creates two private namespaces joined by a random, harness-owned
veth pair. Publisher, broker, router, target module, and RTP sender run in the
first namespace. Subscriber, control client, and video receiver run in the
second. Symmetric qdiscs affect the real TCP control/data and UDP RTP paths.
The harness API provides loss, delay, jitter, rate, interruption, and clear
operations. It refuses user-supplied interfaces. Every command and resulting
`tc qdisc show` output is recorded.

Do not copy `tc` commands from artifacts onto a workstation interface. Direct
execution is supported only in an already isolated Linux environment with
CAP_NET_ADMIN/root:

```bash
sudo --preserve-env=PATH .venv/bin/python -m pytest -ra tests/transport/test_phase75_transport.py
```

Each test tracks ownership after every successful namespace creation, clears
both qdiscs in `finally`, verifies no netem rule remains, terminates every child
within five seconds, deletes only namespaces it proved it created, and verifies
their absence. Cleanup continues across individual failures. A failed or
capability-skipped scenario still writes JSON and per-process event/log files
beneath a run-scoped directory in `test-results/transport/`; skips are marked
`UNVERIFIED`, never `PASS`. That directory is intentionally ignored by Git.

## Scenarios and measurements

The suite runs separate cases for 1% loss, 5% loss, 20 ms delay, 20 ms jitter,
100 Mbit/s, and an exact two-second 100% loss interruption. A separate sustained
case publishes 30 large but valid CV envelopes per second, periodically pauses
and burst-drains a canonical HWM-5 SUB into one replaceable latest-value slot,
and performs application processing once per second. The pause forces real HWM
loss; the bounded drain proves that old transport data does not become an
application FIFO. The rate scenario uses a 450,000-byte valid payload at 30 Hz.
It records baseline and impaired per-veth byte-counter throughput plus
`tc -s qdisc` state, and passes only when baseline traffic reaches the configured
100 Mbit/s limit and impaired egress is measurably constrained. Production
capacities are unchanged.

Durations and recovery boundaries use `time.monotonic()`. CV age uses
`time.time_ns()` because all publishers and consumers share one kernel clock,
even across namespaces. The first post-impairment result below 250 ms must
arrive within two seconds, followed by five consecutive near-current results.
Control samples report count/minimum/mean/p95/maximum. Every slow-consumer
attempt must return a valid, command/target-correlated, completed
acknowledgement below 500 ms; failed attempts are retained rather than filtered
from the distribution.

RSS is sampled every 500 ms after topology warm-up and every raw sample is
retained in the artifact. The boundedness evaluator requires at least eight
samples over five seconds, limits retained growth to the larger of 8 MiB or 25%
of baseline, records transient peaks, and requires both a robust final-window
Theil-Sen slope no greater than 64 KiB/s and a final-window span no greater than
the larger of 2 MiB or 5% of baseline. The final window uses at least four
samples and normally the final quarter, so a transport-buffer peak is accepted
only when the evidence shows it was released and a plateau followed. A
continuous 100 KiB/s growth regression is rejected.

Live owner-thread instrumentation records effective publisher, subscriber,
broker, router, FrameIndex publisher, and FrameIndex subscriber HWM values. The
slow-consumer artifact separately records validated receive-boundary
sequence-gap pairs and reconciles their missing-count sum with
`observed_sequence_gaps`; one-per-second application-processed jumps are not
presented as transport gaps. Video evidence independently records decoded-frame,
pipeline-rebuild, five-frame RUNNING, and exact FrameIndex-correlation recovery.

## Acceptance matrix

| Scenario | Control | Memory | Queue bounds | CV age recovery | Video recovery | Sequence gaps | Broker/router | Overall |
|---|---|---|---|---|---|---|---|---|
| 1% loss | Required while connected | Required | Required | <250 ms within 2 s | Frames and exact correlation resume/continue | N/A; dedicated slow-consumer proof | Alive and functional | All cells |
| 5% loss | Required while connected | Required | Required | <250 ms within 2 s | Frames and exact correlation resume/continue | N/A; dedicated slow-consumer proof | Alive and functional | All cells |
| 20 ms delay | Required while connected | Required | Required | <250 ms within 2 s | Frames and exact correlation resume/continue | N/A; dedicated slow-consumer proof | Alive and functional | All cells |
| 20 ms jitter | Required while connected | Required | Required | <250 ms within 2 s | Frames and exact correlation resume/continue | N/A; dedicated slow-consumer proof | Alive and functional | All cells |
| 100 Mbit/s | Required while connected | Required | Required | <250 ms within 2 s | Frames and exact correlation resume/continue | N/A; dedicated slow-consumer proof | Alive and functional | All cells plus measured rate constraint |
| 2 s interruption | Required after restoration; outage is expected while severed | Required | Required | <250 ms within 2 s | Rebuild <=3 s; five frames restore RUNNING; exact correlation resumes | N/A; dedicated slow-consumer proof | Alive and functional | All cells |
| 30 Hz -> 1 Hz | Every attempt valid and <500 ms | Publisher and broker required | Required | Current processed tail <250 ms | N/A; video is unrelated | Actual receive-boundary gap | Alive and functional | All five slow-consumer criteria plus matrix |

Skipped privileged tests are not passes. A missing capability is reported as a
specific skip reason and leaves transport acceptance unverified.

## Coverage gate

The canonical coverage command is:

```bash
.venv/bin/python -m pytest --cov=purdue_rov_cv --cov-report=term-missing
.venv/bin/python -m coverage json -o coverage.json
.venv/bin/python scripts/check_coverage.py coverage.json --core-min 80 --task-min 70
```

Core is the weighted set of `camera`, `config`, `frame_buffer`, `messaging`,
`module_runner`, `runtime`, `video`, and `wire`. Every non-`__init__.py` file in
`modules` is checked individually; a single task file below 70% fails the gate.
Generated protobuf code remains excluded, but failure, queue, subscriber,
timeout, and recovery paths are not excluded.

The existing test suite covers all twelve §30.1 categories: protobuf round
trips, envelope validation, registry dispatch, topic validation, queue overflow,
shared-memory consistency, configuration validation, dynamic/static comparison,
command transitions, duplicate command handling, timestamp validation, and
FrameIndex cache expiration.
