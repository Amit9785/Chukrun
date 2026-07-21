from runtime._client.async_client import AsyncRuntimeClient
from runtime._client.sync_client import SyncRuntimeClient
from runtime._client.channel import RuntimeChannel
from runtime._client.retry import ReconnectPolicy
from runtime._client.context_bridge import Context, Priority
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

class Client:
    """
    The top-level entry point for the Chukrun AI Runtime Python SDK.
    Wraps AsyncRuntimeClient and SyncRuntimeClient internally.
    """
    def __init__(self, target: str, credentials: str | None = None, options = None):
        from runtime._client.async_client import ClientOptions
        self._target = target
        self._credentials = credentials
        self._options = options or ClientOptions()
        
        # Instantiate internal components
        self._channel = RuntimeChannel(self._target, self._credentials, self._options)
        self._async_client = AsyncRuntimeClient(self._channel)
        self._sync_client = SyncRuntimeClient(self._async_client)

    @property
    def async_(self) -> AsyncRuntimeClient:
        return self._async_client

    @property
    def sync(self) -> SyncRuntimeClient:
        return self._sync_client

    async def connect(self) -> None:
        await self._channel.connect()

    async def close(self) -> None:
        await self._channel.close()

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def __enter__(self) -> "Client":
        # For sync context manager, we trigger the sync client connection
        self._sync_client.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._sync_client.close()

__all__ = [
    "Client",
    "Context",
    "Priority",
    "ReconnectPolicy",
    "RuntimeError",
    "RuntimeConfigError",
    "RuntimeProviderError",
    "RuntimeTimeoutError",
    "RuntimeCancelledError",
    "RuntimeValidationError",
    "RuntimeInternalError",
    "RuntimeSaturationError",
    "RuntimeAuthError",
    "RuntimeBudgetExceededError",
    "RuntimeRateLimitedError",
]
