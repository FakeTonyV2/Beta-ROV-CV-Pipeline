#!/usr/bin/env bash
set -euo pipefail

# Keep generated Python code reproducible with the repository's pinned protoc
# major version. Do not silently generate with an incompatible compiler.
readonly PINNED_PROTOC_MAJOR=3
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ROOT_DIR="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
readonly PROTO_DIR="${ROOT_DIR}/proto"
readonly OUTPUT_DIR="${ROOT_DIR}/generated/python"

PROTOC_VERSION="$(protoc --version)"
if [[ "${PROTOC_VERSION}" != "libprotoc ${PINNED_PROTOC_MAJOR}."* ]]; then
  echo "error: expected protoc major ${PINNED_PROTOC_MAJOR}, found: ${PROTOC_VERSION}" >&2
  exit 1
fi

mapfile -t PROTO_FILES < <(find "${PROTO_DIR}" -type f -name '*.proto' -print | sort)
if (( ${#PROTO_FILES[@]} == 0 )); then
  echo "error: no .proto files found under ${PROTO_DIR}" >&2
  exit 1
fi

protoc \
  --proto_path="${PROTO_DIR}" \
  --python_out="${OUTPUT_DIR}" \
  --pyi_out="${OUTPUT_DIR}" \
  "${PROTO_FILES[@]}"
