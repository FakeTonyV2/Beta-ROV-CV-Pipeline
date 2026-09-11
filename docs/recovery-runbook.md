# Production recovery runbook

## Camera failure

Identify the failing instance and inspect `journalctl -u purdue-cv-camera@<id>`.
Compare its configured path with `udevadm info`. Observe automatic reacquisition
and pipeline rebuild; verify unrelated cameras/modules/video remain active.
Reprovision only if the hardware identity or assigned physical port intentionally
changed.

## Module failure

Use surface `rov-cv operator get_status <id>`, inspect the module journal, and use
STOP/START (or the canonical RESET workflow) when appropriate. If the process is
stuck, restart only `purdue-cv-module@<id>`. Confirm camera and video never stopped.

## Broker or router failure

Inspect both fixed units. Restart the failed service, broker first when both are
down. Verify camera/module publishers, control clients, and modules reconnect and
re-register before rerunning preflight.

## Clock failure

Run `chronyc -n tracking` and `chronyc -n sources -v` on both hosts. Restore the
surface source or tether, then wait for three valid checks. Cross-device latency
is unavailable while invalid, but CV/video/control remain running. Rerun preflight
before a new mission enable decision.

## Recorder or disk failure

Inspect `df -B1` and the recorder journal. Preserve and archive completed files;
do not delete an active session. Restart recording only through its normal service
or control path after space and permissions are safe.

## Full restart

Stop onboard, stop surface, start surface, then start onboard using
`scripts/start_deployment.sh`. Observe systemd ordering and application readiness,
run production preflight, and enable mission only through the authoritative
application gate. Systemd and scripts never manipulate mission state.
