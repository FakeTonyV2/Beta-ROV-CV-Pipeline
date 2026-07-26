"""Stable validation and dispatch contracts for CV wire messages."""

from .errors import ErrorCode, is_error_code
from .identities import validate_dealer_identity
from .payloads import PAYLOAD_REGISTRY, PayloadSpec
from .topics import TopicKind, validate_topic
from .validators import (
    validate_command_request,
    validate_command_response,
    validate_envelope,
    validate_module_registration,
    validate_module_registration_response,
    validate_multipart,
    validate_serialized_envelope,
)

__all__ = [
    "ErrorCode",
    "PAYLOAD_REGISTRY",
    "PayloadSpec",
    "TopicKind",
    "validate_command_request",
    "validate_command_response",
    "validate_dealer_identity",
    "validate_envelope",
    "validate_multipart",
    "validate_module_registration",
    "validate_module_registration_response",
    "validate_serialized_envelope",
    "validate_topic",
    "is_error_code",
]
