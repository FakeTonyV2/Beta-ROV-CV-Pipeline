#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
image="purdue-rov-cv-transport:phase75"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
artifact_dir="/workspace/test-results/transport/${run_id}"

docker build --file "${repo_root}/tests/transport/Dockerfile" --tag "${image}" "${repo_root}"
docker run --rm --privileged \
    --volume "${repo_root}:/workspace" \
    --workdir /workspace \
    --env TRANSPORT_ARTIFACT_DIR="${artifact_dir}" \
    "${image}" \
    bash -lc 'python -m pip install --quiet --editable . && set +e; python -m pytest -ra tests/transport/test_phase75_transport.py; test_status=$?; python scripts/summarize_transport_reports.py "$TRANSPORT_ARTIFACT_DIR"; summary_status=$?; if (( test_status != 0 )); then exit "$test_status"; fi; exit "$summary_status"'
