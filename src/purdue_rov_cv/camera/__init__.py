"""Camera service public API, loaded lazily to avoid configuration cycles."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_BACKEND_EXPORTS = {
    "CaptureBackend",
    "CaptureBackendError",
    "CaptureBackendUnavailable",
    "CapturedFrame",
    "DisconnectAfterFramesBackend",
    "GStreamerCaptureBackend",
    "SurfaceRtpStream",
    "SyntheticCaptureBackend",
    "V4L2CaptureBackend",
}
_SERVICE_EXPORTS = {"CameraService", "RetryController"}


def __getattr__(name: str) -> Any:
    if name in _BACKEND_EXPORTS:
        return getattr(import_module(".backend", __name__), name)
    if name in _SERVICE_EXPORTS:
        return getattr(import_module(".service", __name__), name)
    raise AttributeError(name)


__all__ = sorted(_BACKEND_EXPORTS | _SERVICE_EXPORTS)
