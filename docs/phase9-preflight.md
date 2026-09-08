# Phase 9 simulated full-system preflight

Phase 9 composes the existing strict configuration loader, broker, control
router and client, camera service, module runner, bounded queues, recorder,
replay reader, component state machine, and shutdown coordinator. The synthetic
camera is a capture-backend implementation; it does not bypass camera-service
reconnect or shared-memory behavior. The simulated recorder changes only the
external disk-capacity probe so the real recorder can run on small CI volumes.

## Run preflight

Run commands from the repository's WSL environment:

```bash
source .venv/bin/activate
rov-cv preflight config/mission.yaml --simulate \
  --json-report /tmp/preflight.json
```

The command prints every check and writes a versioned report with a stable check
order and one deterministic run identifier derived from that report's contents.
Exit statuses are `0` for pass, `1` for required-check failure, and `2` when
the probes could not execute. Representative deterministic failures are:

```bash
rov-cv preflight config/mission.yaml --simulate --scenario invalid_model_hash
rov-cv preflight config/mission.yaml --simulate --scenario invalid_camera_mode
rov-cv preflight config/mission.yaml --simulate --scenario unsynchronized_clock
rov-cv preflight config/mission.yaml --simulate --scenario missing_component
rov-cv preflight config/mission.yaml --simulate --scenario component_error
```

Omit `--simulate` to query Linux/chrony/network/hardware state. Runtime camera,
video, module, and component telemetry is supplied to `LocalSystemPreflightProbe`
through its production broker/shared-memory evidence provider. A probe that ran
and observed an unsafe value is `FAIL` (exit `1`); telemetry that could not be
collected is `UNAVAILABLE` (exit `2`). Neither is treated as success.

## Surface operator

The operator uses the existing ROUTER/DEALER control protocol and canonical
Protobuf messages:

```bash
rov-cv operator get_status echo --config /path/to/mission.yaml
rov-cv operator start echo --config /path/to/mission.yaml --preflight-report /tmp/preflight.json
rov-cv operator stop echo --config /path/to/mission.yaml --preflight-report /tmp/preflight.json
```

It reports command state and error information through the canonical control
path. A supplied preflight report is validated and shown as eligibility
evidence, not as authoritative mission state. Because this repository has no
long-lived deployment orchestrator, the standalone CLI reports mission state as
`UNOBSERVED`; the in-process `MissionEnableGate` remains the single authority in
the Phase 9 harness.

## Checklist contract

All checks are fatal for Phase 9. The JSON report includes each probe, its
inputs, exact calculation, measurements, threshold, and actionable reason.

| ID | Probe | Pass calculation |
|---|---|---|
| PFL-001 | strict configuration loader | parse succeeds; unknown fields forbidden |
| PFL-002 | artifact SHA-256 | every enabled-task hash equals its manifest |
| PFL-003 | network link | tether `operstate` is `up` |
| PFL-004 | reachability | one bounded surface request succeeds |
| PFL-005 | broker PUB/SUB | unique canonical envelope returns by deadline |
| PFL-006 | `ControlClient` | enabled modules complete `GET_STATUS` |
| PFL-007 | chrony abstraction | reachable, normal leap, `abs(offset) < 10 ms`, success age `<= 15 s` |
| PFL-008 | USB/device path | every required path exists and is a video device |
| PFL-009 | camera mode | every requested tuple opens |
| PFL-010 | camera soak | all cameras overlap for at least 10 seconds |
| PFL-011 | camera metrics | achieved/configured FPS `>= 0.95` |
| PFL-012 | camera metrics | maximum frame gap `<= 500 ms` |
| PFL-013 | video receiver | each required RTP stream supplies a frame |
| PFL-014 | `FrameCorrelator` | exact matches/received frames `>= 0.95` |
| PFL-015 | resource sampler | average CPU `< 85%` |
| PFL-016 | thermal probe | maximum temperature `< 80 C` |
| PFL-017 | throttle probe | all throttle flags clear |
| PFL-018 | `DiskSpaceGuard` | free space `>= 10 * 1024^3` bytes |
| PFL-019 | module metrics | each enabled module processes `>= 10` frames |
| PFL-020 | health aggregator | every required component present and none `ERROR` |

## Tests and extended acceptance

```bash
pytest -q tests/unit/test_phase9_preflight.py
pytest -q tests/integration/test_phase9_full_system.py -m 'not extended'
pytest -q tests/integration/test_phase9_full_system.py
```

The normal integration suite covers real process boundaries, two independent
subscribers, a deliberately slow subscriber, camera disconnect/recovery, module
crash isolation, clock-loss continuity, MCAP recording/readback, control
transitions, TCP/IPC/RTP/shared-memory cleanup, and the five-second shutdown
bound. Phase 7.5 remains the canonical bounded-backpressure acceptance owner.
Process logs are retained in the pytest temporary directory and included in
assertion failures.

The actual 60-minute criterion is deliberately not shortened. Run it with:

```bash
PURDUE_ROV_CV_RUN_60_MIN_SOAK=1 \
  pytest -q tests/integration/test_phase9_full_system.py -m extended
```

The soak records per-process RSS baselines, checks liveness and RSS every 30
seconds for one hour, fails if any process grows by more than 64 MiB, and writes
`phase9-60-minute-report.json` in its pytest temporary directory. Physical USB
identity, real camera modes, true tether RTP, Pi temperature/throttle behavior,
and cross-device chrony quality remain hardware acceptance items. Surface and
Pi chrony examples are in `config/chrony/`.
