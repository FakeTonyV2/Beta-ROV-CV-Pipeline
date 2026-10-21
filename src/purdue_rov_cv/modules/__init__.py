"""Task computation interfaces and built-in reference modules."""

from .base import CVModule, Frame, ModuleContext
from .crab_detection import CrabDetectionModule
from .echo import EchoModule

__all__ = ["CVModule", "CrabDetectionModule", "EchoModule", "Frame", "ModuleContext"]
