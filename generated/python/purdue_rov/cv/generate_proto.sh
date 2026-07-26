#!/usr/bin/env bash
set -euo pipefail

# Compatibility entry point retained for existing operator documentation.
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)/scripts/generate_proto.sh" "$@"
