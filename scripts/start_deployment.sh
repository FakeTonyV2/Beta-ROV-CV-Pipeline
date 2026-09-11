#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ( "$1" != "pi" && "$1" != "surface" ) ]]; then
    echo "usage: sudo $0 pi|surface" >&2
    exit 64
fi
if [[ "${EUID}" -ne 0 ]]; then
    echo "error: deployment startup requires root" >&2
    exit 77
fi
role="$1"
target="purdue-cv-surface.target"
if [[ "${role}" == "pi" ]]; then
    target="purdue-cv-onboard.target"
fi
/opt/purdue-rov-cv/.venv/bin/purdue-cv-validate-environment \
    --role "${role}" --config /etc/purdue-rov-cv/mission.yaml \
    --json-report /var/lib/purdue-rov-cv/deployment-environment.json
systemctl start "${target}"
systemctl --no-pager --full status "${target}"
