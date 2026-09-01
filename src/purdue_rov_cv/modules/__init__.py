"""Task computation interfaces and built-in reference modules."""

from .base import CVModule, Frame, ModuleContext
from .echo import EchoModule

__all__ = ["CVModule", "EchoModule", "Frame", "ModuleContext"]
