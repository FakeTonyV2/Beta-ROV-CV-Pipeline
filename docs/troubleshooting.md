# Troubleshooting

Start with `systemctl status <unit>` and `journalctl -u <unit> -n 100 --no-pager`.
Follow live output with `journalctl -fu <unit>`. Recovery must target only the
affected project service.

| Symptom | Likely cause | Diagnostic / evidence | Safe recovery and operator action |
|---|---|---|---|
| Service will not start | Invalid config, dependency, path, or permission | `systemctl status <unit>`; `journalctl -u <unit>`; run `purdue-cv-validate-environment` | Correct the named failure, then `systemctl restart <unit>`; do not reinstall by default. |
| Exit code 78 | Permanent configuration/deployment incompatibility | `systemctl show <unit> -p ExecMainStatus`; journal error code | Correct config/artifact/device identity, rerun validation, then manually restart. Exit 78 intentionally does not auto-restart. |
| Camera missing | Power/cable/hub/device absent | `ls -l /dev/v4l/by-id /dev/purdue-rov-cv`; `udevadm info --query=property --name <path>` | Reseat the named camera safely and observe automatic recovery. |
| Stable path mismatch | Camera identity or physical port changed | Compare `udevadm info` with `mission.yaml` and `docs/camera-profiles.md` | Restore the original camera/port; reprovision only for an intentional hardware change. |
| Unsupported camera mode | Exact format/size/FPS absent | `v4l2-ctl -d <path> --list-formats-ext`; preflight PFL-009 | Select a truly supported canonical mode in reviewed config; do not combine unrelated advertised fields. |
| Camera reconnect loop | USB instability, power, invalid mode, backend error | `journalctl -fu purdue-cv-camera@<id>`; camera `pipeline_restarts`, `frame_timeouts`, USB metric | Inspect cable/hub/power and mode. Restart only the affected instance after correcting cause. |
| Module stuck STARTING | Artifact not loaded, router not registered, shared memory absent, or no first frame | `journalctl -u purdue-cv-module@<id>`; GET_STATUS; camera state | Restore its camera/router/artifact. Do not force RUNNING or bypass readiness. |
| Module ERROR | Repeated processing/deadline fault | GET_STATUS; module exception/deadline metrics | STOP/RESET through the normal control path when safe, or restart only its unit; confirm video is unaffected. |
| Broker unavailable | Broker stopped or bind collision | `systemctl status purdue-cv-broker`; `ss -lntp`; journal | Remove the project endpoint conflict, restart broker, verify publishers/subscribers reconnect. |
| Router unavailable | Router stopped, broker dependency, IPC path issue | `systemctl status purdue-cv-control-router`; `ls -ld /run/purdue-rov-cv`; journal | Restart router after broker is ready and verify every module re-registers. |
| Control timeout | Route loss or target unavailable; outcome may be unknown | Operator JSON status/error; router/module journals | Query GET_STATUS. Never automatically resend a non-idempotent command after `COMMAND_OUTCOME_UNKNOWN`. |
| Video STREAM LOST | RTP stopped, receiver failure, tether loss | receiver journal; RTP packet/last-frame/restart metrics; `ip -s link` | Restore tether/camera, observe receiver rebuild; restart only the affected receiver if rebuild fails. |
| FrameIndex misses | Index path loss, late/out-of-window identity, restart boundary | receiver hit/miss and sequence-gap metrics | Verify broker and source identity; allow canonical cache/rebuild behavior, never correlate by timestamp alone. |
| Clock unsynchronized | Surface source unreachable, leap not normal, or offset ≥10 ms | `chronyc -n tracking`; `chronyc -n sources -v`; health JSON | Restore chrony/source/network. CV/video/control continue; wait for three valid 5-second checks and rerun preflight before mission enable. |
| Recorder failure | Low disk, write/finalization, or path permission | recorder journal; recording health; `df -h` | Protect existing recordings, free space outside active files, restart only through the normal recording path. |
| Low disk | Root <2 GiB or recording guard reached | `df -B1 / /var/lib/purdue-rov-cv/recordings`; PFL-018 | Archive completed recordings safely; never fill or bulk-delete the root filesystem. |
| High memory | Leak, backlog, or process fault | `systemd-cgtop`; `ps -eo pid,rss,cmd --sort=-rss`; HIL RSS slope | Stop optional work, identify the growing process, preserve evidence, then restart only that component. Available memory <512 MiB blocks preflight. |
| High CPU | Camera encode/inference pressure | `systemd-cgtop`; health CPU samples; FPS/deadline metrics | Reduce configured workload only through reviewed config and watch FPS/deadlines. |
| High temperature or throttling warning | Enclosure airflow, ambient heat, power, or sustained load | health JSON; `cat /sys/class/thermal/thermal_zone0/temp`; throttle field | Inspect enclosure, airflow, and power; reduce workload/camera load if needed and watch FPS/process degradation. Active cooling is not mandatory and thermal state alone does not disable mission. |

Escalate to hardware reprovisioning only when stable identity/port assignment has
intentionally changed. Escalate to a full restart only when broker/router recovery
does not restore registrations or multiple dependent services remain unhealthy.
