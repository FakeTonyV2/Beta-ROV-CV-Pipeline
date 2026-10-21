# Production operations guide

The production configuration is `/etc/purdue-rov-cv/mission.yaml`; the installed
application is `/opt/purdue-rov-cv`; runtime state is under `/run/purdue-rov-cv`;
recordings are under the directory configured in `recording.directory` (normally
`/var/lib/purdue-rov-cv/recordings`). No password or credential is stored here.

## Physical connection and access

Connect every camera to its provisioned hub port, connect the dedicated Ethernet
tether, then power the surface and Pi. The reference addresses are surface
`192.168.50.1` and Pi `192.168.50.2`. From the surface, use the site-managed account:

```bash
ssh <pi-user>@192.168.50.2
```

Confirm the link and clock on both hosts:

```bash
ip -brief link show
ip -brief address show eth0
chronyc -n tracking
chronyc -n sources -v
```

## Installation

On a clean Ubuntu 24.04 host, clone or transfer a trusted release checkout. Run
these commands from that checkout. On the Pi:

```bash
sudo ./scripts/setup_system_deps.sh
sudo ./scripts/install_deployment.sh pi config/mission.yaml
```

Before the Pi installer, keep the approved model file in the checkout's
`models/` directory and set its real SHA-256 in `mission.yaml`. The installer
copies an enabled task artifact to its configured deployment path only after
the local file matches that digest. It does not download model artifacts.

```bash
sha256sum models/crab_dection.onnx
```

On the surface:

```bash
sudo ALLOW_NON_REFERENCE_PLATFORM=1 ./scripts/setup_system_deps.sh
sudo ./scripts/install_deployment.sh surface config/mission.yaml
```

Before replacing the installed mission, the installer validates the candidate
YAML, enabled module classes, and (on the Pi) each local model digest and model
initialization. It snapshots the current mission, model directory, chrony
drop-in, and project-owned systemd units/links. If a later installation or
environment-validation step fails, it restores that snapshot and reloads
systemd. The installer uses a non-editable install, generates role-specific
chrony and unit topology, and enables exactly one role target after environment
validation. It does not start the target.

## Startup and readiness

Start surface services first, then onboard services:

```bash
# Surface
sudo ./scripts/start_deployment.sh surface

# Pi
sudo ./scripts/start_deployment.sh pi
```

Systemd ordering is network → chrony → broker → router → cameras → modules. The
module remains non-RUNNING until configuration and artifact loading, control
registration, shared-memory attachment, and a valid frame all succeed. A process
being `active` is not application readiness.

Inspect the complete topology:

```bash
systemctl status purdue-cv-surface.target
systemctl status 'purdue-cv-video-receiver@*' purdue-cv-recorder purdue-cv-surface-health
systemctl status purdue-cv-onboard.target
systemctl status purdue-cv-broker purdue-cv-control-router 'purdue-cv-camera@*' 'purdue-cv-module@*' purdue-cv-system-health
journalctl -u purdue-cv-system-health -n 50 --no-pager
```

Run production preflight on the Pi only after the surface receivers and all
runtime roles are ready:

```bash
sudo -u purdue-cv /opt/purdue-rov-cv/.venv/bin/rov-cv preflight \
  /etc/purdue-rov-cv/mission.yaml \
  --json-report /var/lib/purdue-rov-cv/preflight.json
```

Mission enablement is application-owned. Neither systemd nor deployment scripts
enable mission mode. Every `START`, including one from a direct control client,
is authorized inside the control router against the fresh canonical preflight,
matching configuration hash, current hard gates, synchronized clock, live camera
frames, model hashes, and complete enabled-module registration. The decision is
written to `/run/purdue-rov-cv/mission-state.json` for observation.

## Control, camera status, and recording

The operator and control-client roles intentionally share the installed `rov-cv`
executable while retaining separate logical duties. From the surface:

```bash
/opt/purdue-rov-cv/.venv/bin/rov-cv operator get_status crab_detection --config /etc/purdue-rov-cv/mission.yaml
/opt/purdue-rov-cv/.venv/bin/rov-cv operator start crab_detection --config /etc/purdue-rov-cv/mission.yaml
/opt/purdue-rov-cv/.venv/bin/rov-cv operator stop crab_detection --config /etc/purdue-rov-cv/mission.yaml
```

The recorder starts with the surface target when `recording.enabled` is true and
creates a timestamped `mission-YYYYMMDDTHHMMSSZ` session. Check it with:

```bash
systemctl status purdue-cv-recorder
journalctl -fu purdue-cv-recorder
find /var/lib/purdue-rov-cv/recordings -maxdepth 2 -type f -printf '%TY-%Tm-%Td %TH:%TM %s %p\n'
```

## Normal shutdown

Stop the Pi target, then the surface target. Services receive SIGTERM and must
finish within five seconds:

```bash
# Pi
sudo systemctl stop purdue-cv-onboard.target

# Surface
sudo systemctl stop purdue-cv-surface.target
```

Check for forced kills or incomplete recorder finalization:

```bash
journalctl --since '-5 minutes' -u 'purdue-cv-*' | grep -E 'Killing|timed out|ERROR|CRITICAL'
```

Thermal observations are operational evidence, not independent gates. Inspect
temperature/throttling in `/run/purdue-rov-cv/system-health.json` and journald;
respond to sustained heat or performance loss as described in troubleshooting.
