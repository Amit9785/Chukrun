from typing import Mapping

class RuntimeError(Exception):
    """Base class for all Runtime-originated errors."""
    def __init__(
        self, 
        message: str, 
        category: str = "internal", 
        retryable: bool = False, 
        fields: Mapping[str, str] = None
    ):
        super().__init__(message)
        self.category = category
        self.retryable = retryable
        self.fields = fields or {}

class RuntimeConfigError(RuntimeError):
    """Error representing bad or missing configuration."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "config", retryable, fields)

class RuntimeProviderError(RuntimeError):
    """Error representing provider-side failure."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "provider", retryable, fields)

class RuntimeTimeoutError(RuntimeError):
    """Error representing execution timeout."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "timeout", retryable, fields)

class RuntimeCancelledError(RuntimeError):
    """Error representing execution cancellation."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "cancelled", retryable, fields)

class RuntimeValidationError(RuntimeError):
    """Error representing validation failure of the request or parameters."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "validation", retryable, fields)

class RuntimeInternalError(RuntimeError):
    """Error representing internal runtime errors or panics."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "internal", retryable, fields)

class RuntimeSaturationError(RuntimeError):
    """Error representing runtime resource saturation (concurrency limit hit)."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "saturation", retryable, fields)

class RuntimeAuthError(RuntimeError):
    """Error representing authentication/authorization failure."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "auth", retryable, fields)

class RuntimeBudgetExceededError(RuntimeError):
    """Error representing budget exhaustion."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "budget_exceeded", retryable, fields)

class RuntimeRateLimitedError(RuntimeError):
    """Error representing provider or runtime rate limiting."""
    def __init__(self, message: str, retryable: bool = False, fields: Mapping[str, str] = None):
        super().__init__(message, "rate_limited", retryable, fields)
