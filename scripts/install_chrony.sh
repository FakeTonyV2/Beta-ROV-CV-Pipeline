#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 || ( "$1" != "pi" && "$1" != "surface" ) ]]; then
    echo "usage: sudo $0 pi|surface [mission.yaml]" >&2
    exit 64
fi
if [[ "${EUID}" -ne 0 ]]; then
    echo "error: chrony installation requires root" >&2
    exit 77
fi

role="$1"
config_source="${2:-/etc/purdue-rov-cv/mission.yaml}"
temporary="$(mktemp)"
trap 'rm -f "${temporary}"' EXIT
python3.12 scripts/configure_chrony.py --config "${config_source}" --role "${role}" --output "${temporary}"
target="/etc/chrony/conf.d/purdue-rov-cv.conf"
install -d -m 0755 /etc/chrony/conf.d
install -m 0644 "${temporary}" "${target}"
echo "installed project-managed chrony drop-in: ${target}"
systemctl restart chrony.service
chronyc -n tracking
chronyc -n sources
