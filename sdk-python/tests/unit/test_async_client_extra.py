import pytest
from datetime import datetime, timezone, timedelta
import grpc
from runtime._client.async_client import AsyncRuntimeClient, _seconds_until, ClientOptions
from runtime._client.channel import RuntimeChannel
from runtime._client.context_bridge import Context
from runtime.errors import (
    RuntimeCancelledError,
    RuntimeAuthError,
    RuntimeRateLimitedError,
    RuntimeSaturationError,
    RuntimeInternalError,
    RuntimeValidationError,
)

def test_seconds_until():
    assert _seconds_until(None) is None
    
    # Future deadline
    future_dt = datetime.now(timezone.utc) + timedelta(seconds=10)
    diff = _seconds_until(future_dt)
    assert 8.0 <= diff <= 11.0
    
    # Naive future deadline
    naive_dt = datetime.now() + timedelta(seconds=10)
    diff_naive = _seconds_until(naive_dt)
    assert diff_naive is not None and diff_naive > 0.0
    
    # Past deadline
    past_dt = datetime.now(timezone.utc) - timedelta(seconds=10)
    assert _seconds_until(past_dt) == 0.0

class DummyRpcError(grpc.RpcError):
    def __init__(self, code, details=""):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details

def test_map_rpc_errors():
    ch = RuntimeChannel("localhost:50051", options=ClientOptions(allow_insecure_channel=True))
    client = AsyncRuntimeClient(ch)
    
    err_cancelled = client._map_rpc_error(DummyRpcError(grpc.StatusCode.CANCELLED, "task cancelled"))
    assert isinstance(err_cancelled, RuntimeCancelledError)
    
    err_auth = client._map_rpc_error(DummyRpcError(grpc.StatusCode.UNAUTHENTICATED, "invalid token"))
    assert isinstance(err_auth, RuntimeAuthError)
    
    err_perm = client._map_rpc_error(DummyRpcError(grpc.StatusCode.PERMISSION_DENIED, "forbidden"))
    assert isinstance(err_perm, RuntimeAuthError)

    err_rate = client._map_rpc_error(DummyRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Rate limit exceeded"))
    assert isinstance(err_rate, RuntimeRateLimitedError)

    err_sat = client._map_rpc_error(DummyRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED, "Queue full"))
    assert isinstance(err_sat, RuntimeSaturationError)

    err_int = client._map_rpc_error(DummyRpcError(grpc.StatusCode.INTERNAL, "internal failure"))
    assert isinstance(err_int, RuntimeInternalError)

@pytest.mark.asyncio
async def test_async_client_invalid_method():
    ch = RuntimeChannel("localhost:50051", options=ClientOptions(allow_insecure_channel=True))
    client = AsyncRuntimeClient(ch)
    ctx = Context(trace_id="test-invalid")
    
    with pytest.raises(RuntimeValidationError) as exc_info:
        await client.call("NonExistentMethod", {}, ctx)
    assert "not found on RuntimeService" in str(exc_info.value)

    with pytest.raises(RuntimeValidationError) as exc_info:
        async for _ in client.call_streaming("NonExistentStreamingMethod", {}, ctx):
            pass
    assert "not found on RuntimeService" in str(exc_info.value)
