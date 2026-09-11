#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 || ( "$1" != "pi" && "$1" != "surface" ) ]]; then
    echo "usage: sudo $0 pi|surface [mission.yaml]" >&2
    exit 64
fi
if [[ "${EUID}" -ne 0 ]]; then
    echo "error: deployment installation requires root" >&2
    exit 77
fi

role="$1"
config_source="${2:-config/mission.yaml}"
[[ -r "${config_source}" ]] || { echo "error: configuration is not readable: ${config_source}" >&2; exit 78; }
command -v python3.12 >/dev/null || { echo "error: Python 3.12 is required" >&2; exit 78; }
command -v systemctl >/dev/null || { echo "error: systemd is required" >&2; exit 78; }

getent group purdue-cv >/dev/null || groupadd --system purdue-cv
getent passwd purdue-cv >/dev/null || useradd --system --gid purdue-cv --home-dir /var/lib/purdue-rov-cv --shell /usr/sbin/nologin purdue-cv
getent group video >/dev/null || groupadd --system video
usermod --append --groups video purdue-cv
install -d -m 0755 /opt/purdue-rov-cv /opt/purdue-rov-cv/models /etc/purdue-rov-cv
install -d -m 0755 /opt/purdue-rov-cv/tools
install -d -o purdue-cv -g purdue-cv -m 0750 /var/lib/purdue-rov-cv /var/lib/purdue-rov-cv/recordings /run/purdue-rov-cv
python3.12 -m venv --system-site-packages /opt/purdue-rov-cv/.venv
/opt/purdue-rov-cv/.venv/bin/python -m pip install --requirement requirements.lock
/opt/purdue-rov-cv/.venv/bin/python -m pip install --no-deps .
/opt/purdue-rov-cv/.venv/bin/python -m pip check
install -m 0755 scripts/run_phase11_hil.py scripts/run_systemd_acceptance.py \
    scripts/systemd_fixture.py scripts/capture_startup_trace.py /opt/purdue-rov-cv/tools/
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git rev-parse HEAD > /opt/purdue-rov-cv/REVISION
    git status --porcelain > /opt/purdue-rov-cv/INSTALL_WORKTREE_STATUS
else
    echo "UNVERIFIED" > /opt/purdue-rov-cv/REVISION
    echo "source was not a Git worktree" > /opt/purdue-rov-cv/INSTALL_WORKTREE_STATUS
fi
if [[ -f /etc/purdue-rov-cv/mission.yaml ]] && ! cmp -s "${config_source}" /etc/purdue-rov-cv/mission.yaml; then
    cp --preserve=mode,ownership,timestamps /etc/purdue-rov-cv/mission.yaml /etc/purdue-rov-cv/mission.yaml.previous
    echo "preserved prior project configuration: /etc/purdue-rov-cv/mission.yaml.previous"
fi
install -o root -g purdue-cv -m 0640 "${config_source}" /etc/purdue-rov-cv/mission.yaml
install -m 0644 systemd/* /etc/systemd/system/
/opt/purdue-rov-cv/.venv/bin/python scripts/configure_deployment.py \
    --config /etc/purdue-rov-cv/mission.yaml --unit-root /etc/systemd/system --role "${role}"
./scripts/install_chrony.sh "${role}"
systemctl daemon-reload
target="purdue-cv-surface.target"
if [[ "${role}" == "pi" ]]; then
    target="purdue-cv-onboard.target"
fi
systemctl enable "${target}"
other_target="purdue-cv-onboard.target"
if [[ "${role}" == "pi" ]]; then
    other_target="purdue-cv-surface.target"
fi
systemctl disable "${other_target}" >/dev/null 2>&1 || true
/opt/purdue-rov-cv/.venv/bin/purdue-cv-validate-environment \
    --role "${role}" --config /etc/purdue-rov-cv/mission.yaml \
    --json-report /var/lib/purdue-rov-cv/deployment-environment.json
echo "deployment installed; start explicitly with: sudo ./scripts/start_deployment.sh ${role}"
