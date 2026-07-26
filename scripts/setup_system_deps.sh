#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -m)" != "aarch64" && "${ALLOW_NON_REFERENCE_PLATFORM:-0}" != "1" ]]; then
    echo "error: reference Pi platform requires aarch64 (set ALLOW_NON_REFERENCE_PLATFORM=1 for development)" >&2
    exit 1
fi
if [[ ! -r /etc/os-release ]] || ! grep -qE '^ID=ubuntu$' /etc/os-release || ! grep -qE '^VERSION_ID="24.04"$' /etc/os-release; then
    echo "error: reference platform requires Ubuntu 24.04 (set ALLOW_NON_REFERENCE_PLATFORM=1 for development)" >&2
    exit 1
fi

echo "=== Installing System & GStreamer Dependencies ==="

# Update package lists
sudo apt update

# Define package manifest
SYSTEM_PACKAGES=(
    # Core CLI & Networking
    chrony
    iproute2
    v4l-utils

    # GStreamer Engine & Plugins
    gstreamer1.0-tools
    gstreamer1.0-plugins-base
    gstreamer1.0-plugins-good
    gstreamer1.0-plugins-bad
    gstreamer1.0-plugins-ugly
    gstreamer1.0-libav

    # PyGObject / GObject Introspection Bindings
    gir1.2-gstreamer-1.0
    gir1.2-gst-plugins-base-1.0
    python3-gi
    python3-gst-1.0
    python3-cairo
    gir1.2-gstreamer-1.0

    # protobuf Compiler
    protobuf-compiler
)

# Install packages non-interactively
sudo apt install -y "${SYSTEM_PACKAGES[@]}"

echo "=== System setup complete ==="
