from typing import Dict, Type
from runtime._generated.errors_pb2 import ErrorProto
from runtime.errors import (
    RuntimeError,
    RuntimeConfigError,
    RuntimeProviderError,
    RuntimeTimeoutError,
    RuntimeCancelledError,
    RuntimeValidationError,
    RuntimeInternalError,
    RuntimeSaturationError,
    RuntimeAuthError,
    RuntimeBudgetExceededError,
    RuntimeRateLimitedError,
)

_CATEGORY_TO_EXCEPTION: Dict[str, Type[RuntimeError]] = {
    "config": RuntimeConfigError,
    "provider": RuntimeProviderError,
    "timeout": RuntimeTimeoutError,
    "cancelled": RuntimeCancelledError,
    "validation": RuntimeValidationError,
    "internal": RuntimeInternalError,
    "saturation": RuntimeSaturationError,
    "auth": RuntimeAuthError,
    "budget_exceeded": RuntimeBudgetExceededError,
    "rate_limited": RuntimeRateLimitedError,
}

def from_proto(proto: ErrorProto) -> RuntimeError:
    """
    Reconstruct a Python-native exception type from ErrorProto.
    """
    exc_class = _CATEGORY_TO_EXCEPTION.get(proto.category, RuntimeInternalError)
    exc = exc_class(
        message=proto.message, 
        retryable=proto.retryable, 
        fields=dict(proto.fields)
    )
    if proto.cause_message:
        # Wrap cause as a generic RuntimeError to preserve it in standard traceback chaining
        exc.__cause__ = RuntimeError(proto.cause_message)
    return exc
