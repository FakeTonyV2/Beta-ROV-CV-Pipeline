#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ( "$1" != "pi" && "$1" != "surface" ) ]]; then
    echo "usage: sudo $0 pi|surface" >&2
    exit 64
fi
if [[ "${EUID}" -ne 0 ]]; then
    echo "error: chrony installation requires root" >&2
    exit 77
fi

role="$1"
source_file="config/chrony/rov-pi.conf"
if [[ "${role}" == "surface" ]]; then
    source_file="config/chrony/surface.conf"
fi
target="/etc/chrony/conf.d/purdue-rov-cv.conf"
install -d -m 0755 /etc/chrony/conf.d
install -m 0644 "${source_file}" "${target}"
echo "installed project-managed chrony drop-in: ${target}"
systemctl restart chrony.service
chronyc -n tracking
chronyc -n sources
