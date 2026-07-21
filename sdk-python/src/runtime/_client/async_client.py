import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Optional
import grpc
from runtime._client.channel import RuntimeChannel
from runtime._client.retry import ReconnectPolicy
from runtime._client.context_bridge import Context
from runtime._client.codec import Codec
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
from runtime._generated.runtime_pb2_grpc import RuntimeServiceStub
from runtime._generated.runtime_pb2 import StreamChunk, ExecuteResponse

logger = logging.getLogger("runtime.client.async_client")

@dataclass
class ClientOptions:
    pool_size: Optional[int] = None
    reconnect_policy: ReconnectPolicy = field(default_factory=ReconnectPolicy)
    allow_insecure_channel: bool = False
    default_timeout_seconds: float = 60.0
    metrics_callback: Optional[Callable[[str, float], None]] = None

def _seconds_until(deadline: Optional[datetime]) -> Optional[float]:
    if deadline is None:
        return None
    # Ensure deadline is timezone-aware
    if deadline.tzinfo is None:
        deadline = deadline.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    diff = (deadline - now).total_seconds()
    return max(0.0, diff)

class AsyncRuntimeClient:
    def __init__(self, channel: RuntimeChannel):
        self._channel = channel
        self._options = channel._options
        self._codec = Codec()

    async def call(self, method: str, request: Any, ctx: Context, timeout: Optional[float] = None) -> Any:
        """
        Invoke a unary RPC method on the Runtime Core.
        """
        # Determine timeout
        ctx_timeout = _seconds_until(ctx.deadline)
        effective_timeout = timeout
        if effective_timeout is None:
            effective_timeout = ctx_timeout if ctx_timeout is not None else self._options.default_timeout_seconds
        elif ctx_timeout is not None:
            # child effective deadline is min(timeout, ctx.deadline)
            effective_timeout = min(effective_timeout, ctx_timeout)

        # Retry loop for connection failures
        attempt = 0
        reconnect_policy = self._options.reconnect_policy
        start_time = time.perf_counter()
        
        while True:
            try:
                channel = await self._channel.get_channel()
                stub = RuntimeServiceStub(channel)
                
                # Get the stub method
                method_fn = getattr(stub, method, None)
                if method_fn is None:
                    raise RuntimeValidationError(
                        f"RPC method '{method}' not found on RuntimeService"
                    )

                # Encode request
                proto_req = self._codec.encode_request(method, request, ctx)

                # Execute with asyncio timeout
                # Note: grpc.aio call supports timeout parameter natively
                res_proto = await method_fn(proto_req, timeout=effective_timeout)
                
                # If the execution returned a business error, raise it
                if hasattr(res_proto, "error") and res_proto.HasField("error"):
                    raise self._codec.decode_error(res_proto.error)

                # Decode response
                result, response_ctx = self._codec.decode_response(res_proto)
                
                # Record metrics if configured
                duration = time.perf_counter() - start_time
                if self._options.metrics_callback:
                    try:
                        self._options.metrics_callback(method, duration)
                    except Exception:
                        pass
                        
                return result

            except grpc.RpcError as e:
                # Retryable network/channel errors
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    attempt += 1
                    if attempt <= reconnect_policy.max_attempts:
                        logger.warning(
                            f"gRPC call failed with {e.code()} (attempt {attempt}/{reconnect_policy.max_attempts}). Retrying..."
                        )
                        await reconnect_policy.async_sleep_before_retry(attempt)
                        continue
                
                raise self._map_rpc_error(e)

    async def call_streaming(self, method: str, request: Any, ctx: Context) -> AsyncIterator[Any]:
        """
        Invoke a streaming RPC method on the Runtime Core.
        """
        # Connect if needed
        channel = await self._channel.get_channel()
        stub = RuntimeServiceStub(channel)

        method_fn = getattr(stub, method, None)
        if method_fn is None:
            raise RuntimeValidationError(
                f"RPC streaming method '{method}' not found on RuntimeService"
            )

        # Encode request
        proto_req = self._codec.encode_request(method, request, ctx)
        
        # Calculate streaming timeout if context deadline is present
        effective_timeout = _seconds_until(ctx.deadline)

        try:
            # We call the gRPC streaming RPC
            stream_call = method_fn(proto_req, timeout=effective_timeout)
            
            async for chunk in stream_call:
                # If chunk is a StreamChunk and has an error, decode and raise it
                if hasattr(chunk, "error") and chunk.HasField("error"):
                    raise self._codec.decode_error(chunk.error)
                
                # Yield decoded chunk
                # For stream chunk, we decode it
                yield chunk
                
        except grpc.RpcError as e:
            raise self._map_rpc_error(e)

    def _map_rpc_error(self, e: grpc.RpcError) -> RuntimeError:
        """
        Map a low-level gRPC RpcError into a taxonomic Python RuntimeError.
        """
        code = e.code()
        details = e.details() or str(e)
        
        if code == grpc.StatusCode.UNAVAILABLE:
            return RuntimeProviderError(
                f"Runtime Core unreachable: {details}", 
                retryable=True, 
                fields={"grpc_code": code.name}
            )
        elif code == grpc.StatusCode.DEADLINE_EXCEEDED:
            return RuntimeTimeoutError(
                f"gRPC call timed out: {details}", 
                retryable=False, 
                fields={"grpc_code": code.name}
            )
        elif code == grpc.StatusCode.CANCELLED:
            return RuntimeCancelledError(
                f"gRPC call cancelled: {details}", 
                retryable=False, 
                fields={"grpc_code": code.name}
            )
        elif code == grpc.StatusCode.INVALID_ARGUMENT:
            return RuntimeValidationError(
                f"Invalid argument: {details}", 
                retryable=False, 
                fields={"grpc_code": code.name}
            )
        elif code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
            return RuntimeAuthError(
                f"Authentication failed: {details}", 
                retryable=False, 
                fields={"grpc_code": code.name}
            )
        elif code == grpc.StatusCode.RESOURCE_EXHAUSTED:
            # Can be rate limit or concurrency saturation
            if "rate limit" in details.lower():
                return RuntimeRateLimitedError(
                    f"Rate limit exceeded: {details}", 
                    retryable=True, 
                    fields={"grpc_code": code.name}
                )
            return RuntimeSaturationError(
                f"Runtime saturated: {details}", 
                retryable=True, 
                fields={"grpc_code": code.name}
            )
        else:
            return RuntimeInternalError(
                f"Internal gRPC error: {details}", 
                retryable=False, 
                fields={"grpc_code": code.name}
            )
