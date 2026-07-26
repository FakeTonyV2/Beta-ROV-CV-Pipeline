#!/usr/bin/env bash
set -euo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly EXPECTED="$(tr -d '[:space:]' < "${ROOT_DIR}/.protoc-version")"
readonly ACTUAL="$(protoc --version | awk '{print $2}')"
if [[ "${ACTUAL}" != "${EXPECTED}" ]]; then
  echo "error: expected protoc ${EXPECTED}, found ${ACTUAL}" >&2
  exit 1
fi
mapfile -t PROTO_FILES < <(find "${ROOT_DIR}/proto" -type f -name '*.proto' -print | sort)
(( ${#PROTO_FILES[@]} > 0 )) || { echo "error: no proto files found" >&2; exit 1; }
protoc --proto_path="${ROOT_DIR}/proto" --python_out="${ROOT_DIR}/generated/python" \
  --pyi_out="${ROOT_DIR}/generated/python" "${PROTO_FILES[@]}"
