#!/usr/bin/env bash
set -euo pipefail

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