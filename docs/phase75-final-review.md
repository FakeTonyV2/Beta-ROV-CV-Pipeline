# Phase 7.5 independent final review

This report records the adversarial pre-remediation findings before any Phase
7.5 implementation code was changed.  Line references in this section are to
the pre-remediation worktree based on Phase 7 commit `841d76c`.

## Initial findings

### HIGH

#### F-01 — RSS evaluator accepts continuous positive growth

- Location: `tests/transport/harness.py:228-250`;
  `tests/unit/test_phase75_transport_helpers.py:69-79`.
- Requirement: sustained pressure must distinguish a bounded plateau from
  persistent positive growth for each participating process.
- Actual behavior: the late-window condition is
  `slope <= 256 KiB/s OR last-quarter span <= a 4 MiB/10% tolerance`.  A
  process growing continuously at 100 KiB/s, and many faster short-run leaks,
  passes.  The regression only rejects an extreme 3 MiB/s synthetic leak.
- Required behavior: retained growth and a defensible late-window plateau test
  must both pass; raw samples and the decision basis must be retained.
- Failure scenario: a publisher retaining one modest object per message grows
  throughout the 20-second test but remains below both broad allowances.
- Existing tests detect it: no.
- Proposed correction: require both bounded retained growth and a much tighter
  low-slope/late-window plateau, add slow-growth and late-growth regressions,
  and serialize every RSS sample.

#### F-02 — Slow-consumer control acceptance drops failed attempts

- Location: `tests/transport/test_phase75_transport.py:529-541`;
  `tests/transport/roles.py:286-306`.
- Requirement: multiple real `GET_STATUS` acknowledgements during active
  30 Hz to 1 Hz pressure must be valid and each must be below 500 ms, with
  command/target correlation proved.
- Actual behavior: only already-valid `COMPLETED` responses are selected before
  assertions and statistics.  `OUTCOME_UNKNOWN`, malformed, wrong-target, or
  timed-out attempts disappear when at least one later request succeeds.  The
  event also omits correlation evidence.
- Required behavior: every attempt in the measurement window must be valid,
  correlated, completed, and below 500 ms; statistics must state attempts and
  failures.
- Failure scenario: 94 commands complete in 5 ms and one times out at 500 ms;
  the test reports 94/94 success and passes.
- Existing tests detect it: no.
- Proposed correction: record request/response IDs and targets, retain every
  attempt, fail on any unsuccessful attempt, and report total/valid counts.

#### F-03 — Partial setup cleanup can delete an unowned namespace

- Location: `tests/transport/harness.py:108-128,198-205`.
- Requirement: direct privileged execution must never manipulate arbitrary
  developer networking; partial setup and teardown failures must clean up only
  harness-owned resources and continue best-effort cleanup.
- Actual behavior: ownership is represented by one `_setup` boolean.  If the
  first namespace is created and creation of the second collides/fails,
  `cleanup()` deletes both random names, including a pre-existing second
  namespace.  If `clear_impairment()` raises, namespace deletion is skipped.
- Required behavior: record ownership after each successful creation, refuse
  pre-existing names, delete only owned namespaces, continue cleanup after
  qdisc errors, and verify absence.
- Failure scenario: a rare six-hex name collision during direct host execution
  deletes a namespace owned by another workload; alternatively a failed qdisc
  delete leaves both test namespaces behind.
- Existing tests detect it: no.
- Proposed correction: per-resource ownership tracking, preflight collision
  checks, best-effort aggregated cleanup, post-delete verification, and partial
  setup/cleanup-failure regressions.

#### F-04 — Aggregate artifacts can assert PASS from incomplete evidence

- Location: `scripts/summarize_transport_reports.py:35-88`;
  `tests/transport/harness.py:318-355`;
  `scripts/run_transport_tests.sh:7-13`.
- Requirement: every mandatory matrix cell must be explicit PASS, FAIL, N/A
  with reason, or UNVERIFIED with reason; skips are not passes; video rebuild,
  sequence gaps, and broker/router functional health are mandatory evidence.
- Actual behavior: summary cells are weakly inferred.  Any truthy `control`
  object becomes PASS, any post-recovery video frame becomes PASS even when the
  interruption rebuild contract is absent, sequence-gap and broker/router
  columns are missing, and the runner never generates the summary.  Markdown
  supports only Boolean PASS/FAIL.
- Required behavior: scenario artifacts carry explicit outcomes and complete
  cells; aggregate generation rejects missing/unverified/invalid cells and runs
  even when pytest fails.
- Failure scenario: an interruption artifact with frames but no compliant
  rebuild is summarized as video PASS, or a prior stale PASS is selected after
  the current run skipped.
- Existing tests detect it: no.
- Proposed correction: explicit outcome/matrix schema, skip artifacts,
  strict aggregation, complete columns, run-scoped evidence, and summary
  generation in a shell trap/final step.

### MEDIUM

#### F-05 — 100 Mbit/s pressure is calculated, not measured

- Location: `tests/transport/test_phase75_transport.py:316,457-461`;
  `tests/transport/harness.py:178-183`.
- Requirement: report baseline throughput, configured rate, and observed
  constraint evidence; attaching a qdisc is insufficient.
- Actual behavior: the report estimates 96 Mbit/s from payload size and does
  not sample interface byte counters or qdisc statistics.
- Required behavior: measure pre-impairment and impaired veth throughput and
  retain `tc -s qdisc` counters.
- Failure scenario: publisher scheduling or serialization delivers far below
  100 Mbit/s, so the limiter never constrains traffic while the scenario passes.
- Existing tests detect it: no.
- Proposed correction: namespace interface counters plus qdisc statistics and
  an assertion that the workload approaches the limit and the limiter constrains
  observed egress.

#### F-06 — HWM evidence is declarative rather than effective

- Location: `tests/transport/roles.py:139-145,221-228`;
  `tests/transport/test_phase75_transport.py:442-451,543-552`.
- Requirement: effective PUB/SUB/broker socket HWM values must be checked and
  test fixtures must not enlarge them.
- Actual behavior: subscriber `RCVHWM` is queried, but publisher and broker
  values are constants/hard-coded report fields rather than observations from
  the sockets carrying test traffic.
- Required behavior: instrument each actual socket in its owner thread after
  canonical configuration and assert the emitted values.
- Failure scenario: a configuration regression changes an effective socket
  setting while imported/hard-coded values still claim 5/100.
- Existing tests detect it: owner unit tests may detect canonical helper
  changes, but the transport harness does not detect fixture/runtime drift.
- Proposed correction: test-side wrappers around canonical configure helpers
  that record actual `getsockopt` values from the live sockets.

#### F-07 — Sustained CV freshness evidence filters away stale results

- Location: `tests/transport/harness.py:283-297`;
  `tests/transport/test_phase75_transport.py:357-369`.
- Requirement: first age below 250 ms must occur within two seconds of full
  removal and several subsequent results must remain current.
- Actual behavior: `first_fresh_recovery()` returns only samples already below
  250 ms.  The subsequent check therefore cannot see stale samples interleaved
  after the first recovery.
- Required behavior: locate the first fresh sample but return the contiguous
  subsequent receive sequence, then assert sustained freshness on that sequence.
- Failure scenario: fresh, stale, stale, fresh, fresh is reported as three
  fresh results and passes.
- Existing tests detect it: no.
- Proposed correction: return the suffix beginning at first fresh sample and
  add an interleaved-stale regression.

#### F-08 — Sequence evidence conflates application replacement with transport loss

- Location: `tests/transport/roles.py:238-267`;
  `tests/transport/test_phase75_transport.py:505-525`.
- Requirement: `observed_sequence_gaps` must be driven by real envelopes and
  the artifact must show representative received sequence pairs in one
  publisher session; metric semantics count missing values.
- Actual behavior: the validator correctly owns the metric, but the only
  serialized sequence list contains one-per-second application-processed
  values.  Its 31/32 jumps arise from the intentional latest-value slot even
  when transport receives all intervening messages.  The actual receive pair
  that changed the canonical counter is not recorded.
- Required behavior: emit a gap event at the validated transport receive
  boundary with previous/current sequence, missing count, session, and metric.
- Failure scenario: report readers attribute an application drop to ZeroMQ HWM
  loss and cannot reproduce the canonical count.
- Existing tests detect it: the counter must be positive, but evidence origin
  is not tested.
- Proposed correction: retain real receive-boundary gap evidence and assert
  missing-count reconciliation.

#### F-09 — Exact FrameIndex correlation recovery is not tested

- Location: `tests/transport/test_phase75_transport.py:405-440`;
  `tests/transport/roles.py:313-350`.
- Requirement: where Phase 7 correlation is active, exact correlation must
  resume after restoration; pixels alone are insufficient.
- Actual behavior: the scenario checks decoded frames and state/rebuild events
  but never asserts post-restoration `frame_index_hits` or reports correlation
  recovery timing.
- Required behavior: record and assert the first post-restoration increase in
  exact-hit metrics independently of pixel and RUNNING recovery.
- Failure scenario: UDP video recovers while the brokered FrameIndex stream is
  permanently wedged; all current assertions pass.
- Existing tests detect it: no.
- Proposed correction: emit correlation counters with frame events and require
  a post-restoration exact hit.

#### F-10 — Full-interruption duration has no acceptance assertion

- Location: `tests/transport/test_phase75_transport.py:329-354`.
- Requirement: the harness-caused complete link interruption lasts at least the
  specified two seconds while processes remain alive; timing uses monotonic
  application/removal boundaries.
- Actual behavior: requested and observed duration are recorded, but no failure
  is added if scheduling or a future helper change shortens the interval.
- Required behavior: explicitly assert the full-impairment interval and record
  qdisc application/removal command boundaries.
- Failure scenario: an early deadline/clock regression restores the link at
  1.7 seconds and still passes every later recovery assertion.
- Existing tests detect it: no.
- Proposed correction: hard duration assertion with monotonic command evidence.

#### F-11 — Artifact provenance cannot identify the uncommitted implementation

- Location: `tests/transport/harness.py:300-315`.
- Requirement: evidence must contain a reproducible git/worktree identifier.
- Actual behavior: all Phase 7.5 files are uncommitted, while provenance stores
  only Phase 7 `HEAD` and `working_tree_dirty=true`.
- Required behavior: include status and a deterministic fingerprint covering
  tracked diffs and non-ignored untracked implementation files.
- Failure scenario: materially different dirty Phase 7.5 implementations emit
  indistinguishable provenance.
- Existing tests detect it: no.
- Proposed correction: SHA-256 worktree fingerprint plus status/file list.

#### F-12 — Mandatory skip produces no UNVERIFIED artifact

- Location: `tests/transport/test_phase75_transport.py:56-75,303-305,489-491`.
- Requirement: missing privilege/GStreamer produces explicit UNVERIFIED
  evidence, never PASS; failed/skipped scenarios retain artifacts.
- Actual behavior: `_require_environment()` skips before allocating or writing
  an artifact.  It also requires GStreamer for the non-video slow-consumer case.
- Required behavior: write an UNVERIFIED report before skip and check only the
  capabilities each scenario uses.
- Failure scenario: slow-consumer transport is never attempted on a valid
  net-admin host lacking GStreamer, and no artifact explains the omission.
- Existing tests detect it: the host run reports seven skips but creates no
  current-run evidence.
- Proposed correction: capability-specific preflight and skip artifact helper.

### LOW

#### F-13 — Launch ordering depends on arbitrary sleeps

- Location: `tests/transport/test_phase75_transport.py:141-206`.
- Requirement: setup/reconnect coordination should prefer events over guessed
  sleeps to reduce transport-test flakiness.
- Actual behavior: broker/module and video/sender launch ordering uses fixed
  0.4/0.3 second sleeps.  The later baseline poll limits impact but does not
  prove each prerequisite is ready.
- Required behavior: wait on role readiness/socket observations with a bounded
  deadline.
- Failure scenario: a loaded runner starts more slowly and loses initial
  subscriptions or registration, consuming most of the baseline timeout.
- Existing tests detect it: only eventual baseline timeout.
- Proposed correction: bounded event-based role readiness waits.

#### F-14 — New verification scripts are omitted from CI Ruff checks

- Location: `.github/workflows/ci.yml:28-31`.
- Requirement: all configured static checks cover the Phase 7.5 harness and
  evidence tooling.
- Actual behavior: Ruff checks only `src tests`; `scripts/check_coverage.py` and
  `scripts/summarize_transport_reports.py` are not checked in CI.
- Required behavior: include the two production verification scripts (or the
  complete scripts directory) in lint and format checks.
- Failure scenario: a broken/poorly formatted acceptance summarizer merges
  while CI remains green.
- Existing tests detect it: functional helper coverage is partial, style is not.
- Proposed correction: include the Phase 7.5 scripts in both Ruff commands.

## Pre-remediation requirement traceability summary

| Requirement | Status | Evidence / note |
|---|---|---|
| Isolated namespaces/veth and real impaired routing | PARTIAL | Correct two-namespace placement and veth endpoints, but no route/counter proof and unsafe partial ownership cleanup. |
| 1% loss, 5% loss, fixed delay, jitter | PARTIAL | Distinct valid netem commands and prior executions; artifact acceptance remains too weak. |
| 100 Mbit/s | INCORRECT | Qdisc attached, pressure inferred rather than measured. |
| Two-second complete interruption | PARTIAL | `loss 100%` and monotonic record exist; no duration assertion. |
| Real PUB/XSUB/XPUB/SUB | COMPLETE | Production `ResultPublisher`, `DataBrokerService`, and real SUB run in separate processes. |
| Real DEALER/ROUTER/DEALER control | COMPLETE | Production client/router and target DEALER run in separate processes. |
| Real RTP sender/Phase 7 receiver | COMPLETE | Production GStreamer sender/receiver use UDP across the veth. |
| Canonical lossy non-retried CV publication | COMPLETE | `DONTWAIT`, one drop counter, no retry/spool/ACK path. |
| Effective HWM and queue proof | PARTIAL | Subscriber and CV queue observed; publisher/broker evidence declarative. |
| Sequence-gap proof | PARTIAL | Canonical missing-count metric is used, but receive-boundary evidence is absent. |
| Current-data recovery | COMPLETE | Latest-value application slot and processed age below 250 ms. |
| CV age under 250 ms within two seconds | PARTIAL | Correct same-kernel wall-age and monotonic boundary; sustained check can hide stale samples. |
| Video rebuild within three seconds | COMPLETE | Correct full-restoration to backend-present boundary is asserted. |
| Video RUNNING and exact correlation recovery | PARTIAL | Five-frame state recovery asserted; exact FrameIndex recovery absent. |
| Slow-consumer control under 500 ms | INCORRECT | Failed attempts are filtered out. |
| Publisher/broker memory bounded | INCORRECT | Samples exist in memory only; evaluator permits continuous growth. |
| Broker/router alive and functional | PARTIAL | Implicitly exercised by post-data/control success; not explicit in matrix/artifact. |
| Cleanup proof | PARTIAL | Normal qdisc clear verified; partial/failing cleanup is unsafe and not regression-tested. |
| Failed/skipped artifact truthfulness | PARTIAL | Failures serialize; skips have no artifact and aggregate inference is weak. |
| Core >=80%, each task >=70% | COMPLETE | Separate weighted/per-file gate fails below threshold. |
| Dedicated privileged execution path | COMPLETE | Manual GitHub workflow invokes a disposable privileged container. |

## Initial deferred-risk disposition

| Risk | Classification | Owner | Blocking Phase 7.5? |
|---|---|---|---|
| Physical V4L2/DepthAI | CORRECTLY DEFERRED | Physical camera/provisioning phase | No |
| Recording/replay | CORRECTLY DEFERRED | Recorder/replay phase | No |
| Camera-hub endurance | CORRECTLY DEFERRED | Phase 11/HIL | No |
| Clock-failure injection and cross-host synchronization | CORRECTLY DEFERRED | Clock/system-health/HIL | No; this harness is same-kernel |
| Running module state during temporary camera loss | SPECIFICATION AMBIGUITY | Architecture owner | No; unrelated to transport harness acceptance |
| Exact statistical tolerance for an RSS plateau | SPECIFICATION AMBIGUITY | Phase 7.5 measurement policy | No ambiguity about the present false pass; correction must document a conservative test policy |

---

# Final remediation and independent acceptance record

## Overall verdict: PASS

The final implementation passes Phase 7.5.  All fourteen findings above were
remediated, every mandatory privileged scenario passed in one run, the complete
repository suite and coverage gates passed, and no Phase 3 through Phase 7
production behavior or public contract was changed.  The final privileged run
executed the real broker, router, publisher, subscriber, RTP sender, and Phase 7
video receiver through isolated Linux namespaces and the impaired veth path.

This verdict is based on the supplied Phase 7.5 review brief and the canonical
architecture, protocol, error-code, and phase documents in this repository.  A
separate external “detailed technical outline/v1 specification” was not present
in the review workspace; no unpublished requirement is represented as tested.

## Finding disposition

| Finding | Severity | Final status | Remediation and proof |
|---|---:|---|---|
| F-01 | HIGH | FIXED | Plateau evaluation now requires both low Theil–Sen slope and a tight final-quarter span after a minimum sample/window duration.  Unit tests reject a continuous 100 KiB/s leak and accept a released transient peak. |
| F-02 | HIGH | FIXED | Every issued slow-consumer control request is retained and must complete, be structurally valid, correlate by command ID/target, and remain below 500 ms.  Final result: 93/93 valid and complete, max 5.795 ms. |
| F-03 | HIGH | FIXED | Namespace creation is collision-preflighted and ownership-tracked.  Cleanup attempts every qdisc and namespace operation, deletes only owned namespaces, aggregates errors, and verifies absence.  Failure-path unit tests cover partial setup and qdisc cleanup failure. |
| F-04 | HIGH | FIXED | Aggregation requires exactly seven valid scenario artifacts, explicit outcomes, and every required matrix column.  Missing, duplicate, malformed, FAIL, and UNVERIFIED input prevents PASS. |
| F-05 | MEDIUM | FIXED | The rate scenario now measures baseline and impaired veth throughput from kernel byte counters.  Final baseline was 109.200 Mbit/s and impaired throughput 95.834 Mbit/s for a 100 Mbit/s configuration. |
| F-06 | MEDIUM | FIXED | Test-side wrappers record effective `getsockopt` values from each socket’s owning thread.  Publisher/subscriber are 5, broker XSUB/XPUB are 100, router sockets are 100, and FrameIndex PUB/SUB are 5 where present. |
| F-07 | MEDIUM | FIXED | Freshness validation starts at the first qualifying result and evaluates the unfiltered suffix; a later stale sample fails.  All final scenarios produced five consecutive current results. |
| F-08 | MEDIUM | FIXED | The subscriber emits receive-boundary sequence-gap events.  Slow-consumer evidence reconciled 39 evidenced missing results exactly with the canonical missing-count delta of 39. |
| F-09 | MEDIUM | FIXED | Video artifacts record exact FrameIndex hit/miss events.  The interruption test requires a new exact hit after restoration; recovery was 2.403 s. |
| F-10 | MEDIUM | FIXED | Complete interruption duration is explicitly asserted from monotonic qdisc boundaries.  Final observed interval was 2.04 s for the requested 2.00 s. |
| F-11 | MEDIUM | FIXED | Provenance includes commit, dirty status, porcelain status, untracked implementation files, tool/runtime versions, and a SHA-256 tracked-diff/untracked-content fingerprint. |
| F-12 | MEDIUM | FIXED | Capability-specific preflight writes an explicit UNVERIFIED artifact before skipping.  The non-video slow-consumer case no longer requires GStreamer. |
| F-13 | LOW | FIXED | Broker/router/video prerequisites use bounded readiness and socket-observation events rather than fixed launch-order sleeps. |
| F-14 | LOW | FIXED | CI Ruff check and format coverage now includes both Phase 7.5 verification scripts. |

## Final requirement traceability

| Requirement | Status | Final evidence |
|---|---|---|
| Two isolated namespaces and a real veth impairment boundary | PASS | Per-scenario route evidence identifies the intended veth peer; baseline/impairment interface counters and `tc -s` statistics prove traffic crossed it. |
| 1% loss, 5% loss, fixed delay, jitter, and 100 Mbit/s rate | PASS | Five distinct netem/TBF scenarios completed with qdisc state, counters, recovery, and complete matrices. |
| Two-second complete link interruption | PASS | 100% loss applied for 2.04 s; processes stayed alive and post-restoration recovery passed. |
| Real PUB/XSUB/XPUB/SUB structured-result path | PASS | Canonical `ResultPublisher` and `DataBrokerService`, plus a real SUB, ran in distinct processes with live socket observations. |
| Real DEALER/ROUTER/DEALER control path | PASS | Canonical control client/router and target service executed every scenario; request/response IDs, target, schema validity, and latency were recorded. |
| Real RTP sender and Phase 7 receiver | PASS | GStreamer RTP crossed the veth in all six video-bearing scenarios and recovered after impairment. |
| Lossy, non-retried CV publication semantics | PASS | No retry, acknowledgment, spool, or replay path was added; canonical DONTWAIT publication and drop metric remain intact. |
| Effective HWM and queue bounds | PASS | Live HWM values match canonical policy; `CvResult` application queue maximum was 1 with capacity 4 in all scenarios. |
| Transport sequence-gap proof | PASS | Dedicated receive-boundary events exactly reconcile with the canonical missing-count delta during slow-consumer overload. |
| Current-data recovery and five consecutive results below 250 ms | PASS | Every impairment recovered within 2 s; worst measured recovery was 1.23 s and every final qualifying age was below 250 ms. |
| Video backend rebuild below 3 s | PASS | Interruption rebuild completed in 0.975 s. |
| Video RUNNING state and exact correlation recovery | PASS | RUNNING recovery completed in 2.591 s, exact correlation in 2.403 s, followed by 55 post-restoration frames. |
| Control below 500 ms with no discarded failures | PASS | All attempts are accounted for.  Worst scenario maximum was 247.37 ms; slow-consumer maximum was 5.795 ms. |
| Bounded publisher and broker memory | PASS | Raw per-role RSS/CPU series are serialized.  Both strict slope and final-span plateau conditions pass. |
| Broker/router remain alive and functional | PASS | Explicit liveness plus post-impairment data/control functionality passed in every matrix. |
| Cleanup is complete and ownership-safe | PASS | Each qdisc returned to `noqueue`; every owned namespace was absent after teardown; failure paths are unit-tested. |
| Failed/skipped artifacts remain truthful | PASS | Scenario reports carry explicit PASS/FAIL/UNVERIFIED outcomes; strict run-scoped aggregation cannot convert incomplete evidence to PASS. |
| Core coverage at least 80%; every task at least 70% | PASS | Core weighted coverage 81.6266%; task minima all pass (lowest reported task coverage 91.8367%). |
| Dedicated privileged execution path | PASS | The privileged runner and CI workflow execute the seven scenarios in a disposable privileged Linux container. |

## Network and process architecture verified

The surface namespace hosted the broker/router and receiving consumers.  The
vehicle namespace hosted the publisher/module and RTP sender.  Each namespace
was joined by a unique veth pair; routing records showed the peer address using
that veth and its namespace-local source address.  Structured ZeroMQ traffic,
control traffic, and RTP crossed the pair.  Process evidence contains role,
PID, namespace, command/log path, readiness, exit state, and raw event stream.
Local application queueing remains bounded at the canonical capacity and was
not substituted for the network path.

## Privileged scenario matrix

Run: `test-results/transport/20260905T220455Z-40942`

| Scenario | Control | Memory | Queue bounds | CV age recovery | Video recovery | Sequence gaps | Broker/router | Overall |
|---|---|---|---|---|---|---|---|---|
| loss-1pct | PASS | PASS | PASS | PASS | PASS | N/A — dedicated slow-consumer assertion | PASS | PASS |
| loss-5pct | PASS | PASS | PASS | PASS | PASS | N/A — dedicated slow-consumer assertion | PASS | PASS |
| delay-20ms | PASS | PASS | PASS | PASS | PASS | N/A — dedicated slow-consumer assertion | PASS | PASS |
| jitter-20ms | PASS | PASS | PASS | PASS | PASS | N/A — dedicated slow-consumer assertion | PASS | PASS |
| rate-100mbit | PASS | PASS | PASS | PASS | PASS | N/A — dedicated slow-consumer assertion | PASS | PASS |
| link-interruption-2s | PASS | PASS | PASS | PASS | PASS | N/A — dedicated slow-consumer assertion | PASS | PASS |
| slow-consumer-30hz-to-1hz | PASS | PASS | PASS | PASS | N/A — no video in this scenario | PASS | PASS | PASS |

### Recovery and control measurements

| Scenario | Impairment duration | CV recovery | Final CV age | Control attempts | Control p95 | Control maximum |
|---|---:|---:|---:|---:|---:|---:|
| loss-1pct | 8.04 s | 0.03 s | 8.44 ms | 37 | 4.17 ms | 211.05 ms |
| loss-5pct | 8.03 s | 0.02 s | 5.38 ms | 33 | 214.38 ms | 241.81 ms |
| delay-20ms | 8.02 s | 0.04 s | 11.61 ms | 32 | 44.30 ms | 44.76 ms |
| jitter-20ms | 8.04 s | 0.10 s | 16.58 ms | 30 | 88.74 ms | 89.20 ms |
| rate-100mbit | 8.02 s | 0.53 s | 8.79 ms | 20 | 246.96 ms | 247.37 ms |
| link-interruption-2s | 2.04 s | 1.23 s | 1.78 ms | 20 post-restore | 3.12 ms | 3.31 ms |
| slow-consumer | 20.00 s measured | current tail PASS | 56.14 ms | 93 | 4.58 ms | 5.80 ms |

The loss-1% control mean is larger than its p95 because of a rare long outlier;
the complete distribution and every attempt remain in the raw artifact, and the
maximum is still well below the 500 ms limit.

### Rate-limit evidence

The rate test generated enough payload to exceed the configured limit without
changing a production queue or protocol.  Kernel counters measured 109.200
Mbit/s before impairment and 95.834 Mbit/s while the 100 Mbit/s limiter was
active.  The qdisc report recorded approximately 100.2 MB transmitted and a
3.25 MB backlog, independently demonstrating active pressure rather than merely
echoing configuration text.

### Slow-consumer result

The publisher produced 29.9996 results/s and the consumer processed 0.999985
results/s for 20.0003 s.  The run therefore exercised overload at approximately
30:1 while preserving bounded queues and current-data behavior.  Receive-boundary
events evidenced 39 missing results and the canonical missing-count metric grew
by exactly 39.  The subscriber ended on a current result (56.14 ms old), while
all 93 control attempts completed and correlated correctly.

### Memory and CPU evidence

The acceptance algorithm requires at least eight samples over five seconds,
limits retained growth, and requires both a final-quarter RSS span no greater
than `max(2 MiB, 5% of baseline)` and a median pairwise Theil–Sen slope no
greater than 64 KiB/s.  CPU is derived from deltas in per-process CPU time rather
than a point-in-time placeholder.  All raw timestamped samples are in each JSON
artifact.

| Scenario/role | Initial RSS | Peak RSS | Final RSS | Plateau slope | Mean / max CPU |
|---|---:|---:|---:|---:|---:|
| slow/broker | 50.95 MiB | 51.01 MiB | 51.01 MiB | 0 KiB/s | 1.99% / 3.99% |
| slow/publisher | 51.50 MiB | 51.52 MiB | 51.52 MiB | 0 KiB/s | 4.81% / 5.99% |
| slow/subscriber | 51.63 MiB | 51.70 MiB | 51.70 MiB | 0 KiB/s | 5.57% / 7.98% |
| interruption/broker | 50.75 MiB | 51.98 MiB | 51.98 MiB | 0 KiB/s | 2.15% / 3.99% |
| interruption/publisher | 50.42 MiB | 50.42 MiB | 50.42 MiB | 0 KiB/s | 3.26% / 5.96% |
| interruption/video | 105.68 MiB | 107.86 MiB | 107.86 MiB | 5.31 KiB/s | 12.51% / 23.94% |

Every other sampled role/scenario also passed the same bounds.  A transient
peak that is released is accepted only when the final window plateaus; a
continuous modest leak is rejected by unit test.

## Queue, HWM, freshness, video, and control conclusions

- The observed `CvResult` application queue maximum was 1 against capacity 4
  in all seven scenarios.
- Effective live values were publisher SNDHWM 5, subscriber RCVHWM 5 with no
  CONFLATE, broker XSUB/XPUB 100, router RCVHWM/SNDHWM 100, and FrameIndex
  PUB/SUB 5 when video was active.
- Freshness is computed from the canonical same-kernel wall timestamp; recovery
  starts when the impairment-removal command completes, and five consecutive
  unfiltered results must remain below 250 ms.
- Link interruption recovered video backend presence in 0.975 s, an exact
  FrameIndex correlation hit in 2.403 s, and sustained RUNNING frames in 2.591
  s.  All are below their specified three-second bound.
- Control acceptance covers every request rather than a successful subset and
  validates response structure, target, command ID, completion, and latency.

## Coverage, static analysis, and regression verification

| Verification | Result |
|---|---|
| Privileged Phase 7.5 runner | 7 passed in 231.43 s; aggregate PASS |
| Full repository suite in CI-equivalent ext4 container | 409 passed, 7 explicitly UNVERIFIED/skipped in 62.71 s |
| Aggregate Python coverage | 82% |
| Core weighted gate | 5189/6357 = 81.6266% (minimum 80%) |
| Per-task gate | PASS; base 91.8367%, echo 92.5%, no failures (minimum 70%) |
| Phase 1 regression set | 93 passed in 1.21 s |
| Phase 2 regression set | 52 passed in 11.92 s |
| Phase 3 regression set | 100 passed in 3.26 s |
| Phase 4 regression set | 38 passed in 6.05 s in CI-equivalent ext4 container |
| Phase 5 regression set | 42 passed in 31.08 s |
| Phase 6 regression set | 52 passed in 12.06 s |
| Phase 7 regression set | 18 passed in 27.37 s |
| Phase 7.5 helper unit tests | 14 passed in 2.46 s |
| mypy | Success, 51 source files |
| Ruff check / format check | Passed; 92 files already formatted |
| Proto regeneration comparison | Passed; no generated contract diff |
| Dependency/import smoke | Passed; no broken requirements |
| `git diff --check` / shell syntax | Passed |

The seven skips in the ordinary non-privileged full-suite container are
explicit UNVERIFIED outcomes caused by absent network-administration
capabilities.  They are not counted as scenario passes; the separate privileged
run above is the acceptance authority.  A Phase 4 attempt directly from the
WSL `/mnt/c` mount encountered uninterruptible child-process import I/O under
concurrent load; the exact test selection passed on ext4 and is classified as
an environment/mount contention issue, not a product failure.

## Artifact audit

The final run directory contains a JSON report, Markdown rendering, per-role
event stream, and per-role log for every scenario, followed by a strict
run-scoped aggregate:

- `transport-loss-1pct-20260905T220537Z.{json,md}`
- `transport-loss-5pct-20260905T220618Z.{json,md}`
- `transport-delay-20ms-20260905T220650Z.{json,md}`
- `transport-jitter-20ms-20260905T220717Z.{json,md}`
- `transport-rate-100mbit-20260905T220749Z.{json,md}`
- `transport-link-interruption-2s-20260905T220820Z.{json,md}`
- `transport-slow-consumer-30hz-to-1hz-20260905T220848Z.{json,md}`
- `phase75-acceptance-summary.{json,md}`

The aggregate contains exactly the seven expected scenario identities and
rejects incomplete matrices or non-PASS outcomes.  Evidence provenance records
commit `841d76c007d8eac4b2c38739ab9f2926fe6d9ddd`, dirty-worktree state, status,
untracked implementation files, and fingerprint
`7056e8821785ddf869902d0e0d574adc85e7b7c1e26057638c3b0d396f97cad6`.
Runtime provenance includes Python 3.12.3, pyzmq 27.1.0, libzmq 4.3.5, Ubuntu
24.04.4, WSL2 kernel 6.18.33.2, iproute2/tc 6.1.0, and GStreamer 1.24.2.
The final prose added to this review after execution is documentation-only and
therefore is not included in that run-time worktree fingerprint.

Failed diagnostic runs and host UNVERIFIED reports were retained in their own
older run directories; they were not overwritten or included in the passing
aggregate.  During remediation, an early privileged rate run correctly failed
an overly conservative memory window after a released broker peak, and a second
focused run exposed OLS sensitivity to a single oscillatory spike.  The final
Theil–Sen/final-quarter policy was unit-tested and the complete seven-scenario
run was repeated from scratch.

## Cleanup verification

Every final scenario recorded `qdisc noqueue` after teardown and false presence
for both owned namespace names.  Cleanup continues after an individual qdisc
failure, reports every error, and attempts deletion only for namespaces whose
creation succeeded in that harness instance.  A pre-existing name collision is
rejected before setup.  These behaviors are covered by unit tests, so the proof
does not depend only on a successful happy-path teardown.

## Files changed and generated

No file under `src/`, no `.proto`, and no production configuration schema was
modified.  Canonical queue capacities, HWM values, state-machine rules, error
codes, protobuf compatibility, and lossy publication semantics are unchanged.

Modified tracked files:

- `.github/workflows/ci.yml` — include Phase 7.5 tooling in static checks.
- `.gitignore` — retain the intended artifact-ignore policy.
- `README.md` — document the Phase 7.5 entry points.
- `pyproject.toml` — Phase 7.5 test/tool configuration.

Created review/harness files:

- `.dockerignore` and `.github/workflows/transport.yml`.
- `docs/transport-resilience.md` and this independent review.
- `scripts/__init__.py`, `scripts/check_coverage.py`,
  `scripts/run_transport_tests.sh`, and
  `scripts/summarize_transport_reports.py`.
- `tests/__init__.py`, `tests/transport/__init__.py`,
  `tests/transport/harness.py`, `tests/transport/roles.py`,
  `tests/transport/test_phase75_transport.py`, and
  `tests/unit/test_phase75_transport_helpers.py`.
- Ignored generated evidence under
  `test-results/transport/20260905T220455Z-40942/`.

## Cross-phase correction record

All corrections were confined to the Phase 7.5 harness, tests, evidence
summarizer/runner, CI lint scope, and documentation.  No Phase 3–7 production
implementation was changed.  Consequently there is no production behavior
correction requiring migration, rollback, schema regeneration, or compatibility
exception.  Regression and coverage results above provide the cross-phase proof.

## Remaining risks and deferred scope

| Risk | Classification | Effect on verdict |
|---|---|---|
| Physical V4L2/DepthAI devices and camera-hub endurance | Correctly deferred to HIL/provisioning phases | Non-blocking; no hardware was claimed as exercised. |
| Recording/replay | Correctly deferred to recorder/replay phase | Non-blocking; no recording path was introduced. |
| Cross-host clock failure/synchronization | Correctly deferred to clock/system-health/HIL work | Non-blocking; freshness evidence is explicitly same-kernel. |
| WSL `/mnt/c` multiprocessing import contention | Environment limitation | Non-blocking; exact tests pass on CI-equivalent ext4. |
| Docker legacy-builder deprecation warning | Tooling maintenance | Non-blocking; build and tests pass. |
| PyGI `GLib.unix_signal_add_full` deprecation warning | Upstream/API maintenance | Non-blocking; Phase 7 tests pass. |
| Separate external outline/v1 text absent from workspace | Documentation availability | Non-blocking for the supplied brief/repository contract; unpublished conflicting requirements would require a new review. |

No unresolved finding in this review is a Phase 7.5 blocker.  The artifact
aggregate, not this prose verdict alone, is the machine-verifiable acceptance
authority.
