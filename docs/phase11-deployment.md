# Phase 11 deployment and HIL acceptance

## Adopted policy

Active cooling is excluded from v1 requirements. Temperature and throttling are
collected and reported as diagnostic evidence; they are not independent startup,
mission-enable, or mission-disable gates. Available memory ≥512 MiB, root free
space ≥2 GiB, required camera availability, operational tether, compatible
runtime/configuration, successful preflight, and absence of unrecoverable required
ERROR conditions remain hard gates.

## Deployment topology

The Pi target owns exactly one broker, one router, one health process, one camera
process per configured camera, and one module runner per enabled task. The surface
target owns one receiver per surface-streaming camera, one recorder when enabled,
and one surface-health process. Operator and control-client are separate logical
roles of the installed `rov-cv operator` executable and are invoked on demand.

Every production process constructs its own ZeroMQ context. Existing socket
owners remain single-threaded; deployment uses independent systemd processes and
never forks a constructed context/socket.

All services use `Restart=on-failure`, `RestartSec=2`, `TimeoutStopSec=5`,
`KillSignal=SIGTERM`, `RestartPreventExitStatus=78`,
`StartLimitIntervalSec=60`, `StartLimitBurst=5`, and
`RuntimeDirectoryPreserve=yes` so one service cannot remove the shared runtime
directory while peers remain active. Validate checked-in policy:

```bash
.venv/bin/python scripts/validate_systemd.py systemd
systemd-analyze verify systemd/*
```

On the deployed Linux host, safely exercise transient project-named fixtures
without crashing production mission services:

```bash
sudo /opt/purdue-rov-cv/.venv/bin/python /opt/purdue-rov-cv/tools/run_systemd_acceptance.py \
  --allow-systemd-test --config /etc/purdue-rov-cv/mission.yaml \
  --output /var/lib/purdue-rov-cv/hil/systemd-supervision.json
```

The harness verifies the approximately two-second restart, exit-78 prevention,
five-attempt start limit, SIGTERM delivery, five-second bound, and cleanup.

## Chrony

`scripts/install_chrony.sh` installs only
`/etc/chrony/conf.d/purdue-rov-cv.conf`; it does not overwrite the main chrony
configuration. The surface uses local stratum 8, allows `192.168.50.0/24`, binds
`192.168.50.1`, and permits initial stepping. The Pi polls `192.168.50.1` with
`iburst minpoll 2 maxpoll 4`, `makestep 0.1 3`, and `rtcsync`.

System health polls the production `chronyc` evaluator at the configured five
seconds. Valid means reachable source, normal leap, absolute offset below 10 ms,
and success no older than 15 seconds. Three failures invalidate cross-device
latency; success resets the sequence. Runtime continues in DEGRADED health.

## HIL commands and artifacts

Run on the actual deployed hosts. These commands write versioned JSON including
revision, dirty state, configuration hash, software/hardware inventory, times,
measurements, events, restarts, result, and normative-duration flag:

```bash
# Pi, normative 30 minutes
sudo -u purdue-cv /opt/purdue-rov-cv/.venv/bin/python /opt/purdue-rov-cv/tools/run_phase11_hil.py camera-hub \
  --config /etc/purdue-rov-cv/mission.yaml --duration 1800 \
  --output /var/lib/purdue-rov-cv/hil/camera-hub.json

# Pi, disruptive opt-in; chrony is restored in finally
sudo /opt/purdue-rov-cv/.venv/bin/python /opt/purdue-rov-cv/tools/run_phase11_hil.py clock-loss \
  --role pi --allow-disruptive-clock-test --config /etc/purdue-rov-cv/mission.yaml \
  --output /var/lib/purdue-rov-cv/hil/clock-loss.json

# Run concurrently on Pi and surface for the normative hour
sudo /opt/purdue-rov-cv/.venv/bin/python /opt/purdue-rov-cv/tools/run_phase11_hil.py stability \
  --role pi --duration 3660 --cadence 30 --config /etc/purdue-rov-cv/mission.yaml \
  --output /var/lib/purdue-rov-cv/hil/stability-pi.json
sudo /opt/purdue-rov-cv/.venv/bin/python /opt/purdue-rov-cv/tools/run_phase11_hil.py stability \
  --role surface --duration 3660 --cadence 30 --config /etc/purdue-rov-cv/mission.yaml \
  --output /var/lib/purdue-rov-cv/hil/stability-surface.json

# After copying both artifacts to one host, require >=3600 seconds of overlap
sudo /opt/purdue-rov-cv/.venv/bin/python /opt/purdue-rov-cv/tools/run_phase11_hil.py stability-merge \
  --config /etc/purdue-rov-cv/mission.yaml \
  --pi-artifact /var/lib/purdue-rov-cv/hil/stability-pi.json \
  --surface-artifact /var/lib/purdue-rov-cv/hil/stability-surface.json \
  --output /var/lib/purdue-rov-cv/hil/stability-full-system.json
```

Each single-host stability capture remains `UNVERIFIED`; only the merge can
produce the full-system PASS after validating role, revision, configuration,
host result, duration, and concurrent overlap. Short developer runs require
`--smoke` and always report `UNVERIFIED`, never `PASS`. The clock fault requires
explicit disruptive opt-in and targets only
`chrony.service`; its `finally` block requests restoration even after failure.
Normal CI excludes `hardware`, `extended`, and privileged tests. CI PASS is not
HIL acceptance.

Capture observed systemd activation timestamps plus application readiness for
the ten-stage startup sequence with `/opt/purdue-rov-cv/tools/capture_startup_trace.py`. Supply
the surface trace, preflight report, and authoritative mission-state evidence;
missing inputs remain `UNVERIFIED` rather than being inferred from active PIDs.

## Acceptance execution

Follow [the acceptance matrix](phase11-acceptance-matrix.md). Attach actual
artifact paths and replace `UNVERIFIED` only when the stated environment and
normative duration were observed. If the Pi, surface, cameras, or required time
are unavailable, implementation can be complete while v1 remains NOT ACCEPTED.
