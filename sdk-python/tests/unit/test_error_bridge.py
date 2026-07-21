from runtime._client.error_bridge import from_proto
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

def test_error_mapping():
    categories = {
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
        "unregistered_category": RuntimeInternalError,  # fallback case
    }
    
    for cat, exc_class in categories.items():
        proto = ErrorProto(
            category=cat,
            message=f"Test message for {cat}",
            retryable=True,
            fields={"key": "val"},
            cause_message="Root cause message"
        )
        
        exc = from_proto(proto)
        
        assert isinstance(exc, exc_class)
        assert exc.category == ("internal" if cat == "unregistered_category" else cat)
        assert exc.retryable is True
        assert exc.fields == {"key": "val"}
        assert str(exc) == f"Test message for {cat}"
        
        # Verify cause propagation
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, RuntimeError)
        assert str(exc.__cause__) == "Root cause message"
