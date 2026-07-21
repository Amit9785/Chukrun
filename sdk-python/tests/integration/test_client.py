import asyncio
import time
from datetime import datetime, timezone
import pytest
import grpc
from concurrent import futures
from runtime import Client, Context, Priority, RuntimeError, RuntimeProviderError
from runtime._generated import runtime_pb2_grpc, runtime_pb2
from runtime._generated.context_pb2 import ContextProto
from runtime._generated.errors_pb2 import ErrorProto

class MockRuntimeService(runtime_pb2_grpc.RuntimeServiceServicer):
    def Execute(self, request, context):
        # Retrieve trace_id from request context
        trace_id = request.context.trace_id if request.HasField("context") else "no-trace"
        
        if "trigger_error" in request.payload:
            # Return response containing business error
            error_proto = ErrorProto(
                category="provider",
                message="Mock provider failure",
                retryable=False,
                cause_message="Low-level API crash"
            )
            return runtime_pb2.ExecuteResponse(
                context=request.context,
                id=request.id,
                status="Failed",
                state="FAILED",
                error=error_proto
            )
            
        elif "trigger_grpc_error" in request.payload:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Invalid payload arguments passed")
            return runtime_pb2.ExecuteResponse()
            
        elif "trigger_delay" in request.payload:
            # Simulate a slow execution to test timeout/cancellation
            time.sleep(1.0)
            
        # Standard successful response
        return runtime_pb2.ExecuteResponse(
            context=request.context,
            id=request.id,
            status="Succeeded",
            state="SUCCEEDED",
            output='{"status": "ok", "echo": "' + request.payload + '"}'
        )

    def Stream(self, request, context):
        trace_id = request.context.trace_id if request.HasField("context") else "no-trace"
        
        if "trigger_stream_error" in request.payload:
            error_proto = ErrorProto(
                category="internal",
                message="Mock stream internal crash",
                retryable=False
            )
            yield runtime_pb2.StreamChunk(id=request.id, error=error_proto)
            return

        # Yield 3 standard chunks
        for i in range(3):
            yield runtime_pb2.StreamChunk(
                id=request.id,
                content=f"chunk-{i}"
            )

    def Health(self, request, context):
        from google.protobuf.timestamp_pb2 import Timestamp
        since = Timestamp()
        since.FromDatetime(datetime.now(timezone.utc))
        
        comp_health = runtime_pb2.ComponentHealthProto(
            state="Healthy",
            details="Mock details",
            fatal=True,
            checked_at=since
        )
        
        return runtime_pb2.HealthResponse(
            overall="Healthy",
            state="READY",
            components={"execution": comp_health},
            since=since,
            reason=""
        )

@pytest.fixture(scope="module")
def grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    runtime_pb2_grpc.add_RuntimeServiceServicer_to_server(MockRuntimeService(), server)
    port = server.add_insecure_port('[::]:0')
    server.start()
    yield f"localhost:{port}"
    server.stop(0)

@pytest.mark.asyncio
async def test_async_client_execute(grpc_server):
    async with Client(grpc_server, options=None) as client:
        ctx = Context(trace_id="test-trace-123")
        
        # Test basic successful execution
        payload = {"id": "req-1", "payload": "hello-world"}
        result = await client.async_.call("Execute", payload, ctx)
        
        assert result["id"] == "req-1"
        assert result["status"] == "Succeeded"
        assert result["output"] == {"status": "ok", "echo": "hello-world"}

        # Test business error propagation
        payload_err = {"id": "req-2", "payload": "trigger_error"}
        with pytest.raises(RuntimeProviderError) as exc_info:
            await client.async_.call("Execute", payload_err, ctx)
        
        assert exc_info.value.category == "provider"
        assert "Mock provider failure" in str(exc_info.value)
        assert exc_info.value.__cause__ is not None
        assert "Low-level API crash" in str(exc_info.value.__cause__)

        # Test transport gRPC error mapping
        payload_grpc_err = {"id": "req-3", "payload": "trigger_grpc_error"}
        with pytest.raises(RuntimeError) as exc_info:
            await client.async_.call("Execute", payload_grpc_err, ctx)
        assert exc_info.value.category == "validation"
        assert "Invalid payload arguments passed" in str(exc_info.value)

@pytest.mark.asyncio
async def test_async_client_streaming(grpc_server):
    async with Client(grpc_server) as client:
        ctx = Context(trace_id="test-trace-stream")
        payload = {"id": "req-stream", "payload": "stream-test"}
        
        chunks = []
        async for chunk in client.async_.call_streaming("Stream", payload, ctx):
            chunks.append(chunk.content)
            
        assert chunks == ["chunk-0", "chunk-1", "chunk-2"]

        # Test error mid-stream
        payload_err = {"id": "req-stream-err", "payload": "trigger_stream_error"}
        with pytest.raises(RuntimeError) as exc_info:
            async for chunk in client.async_.call_streaming("Stream", payload_err, ctx):
                pass
        assert exc_info.value.category == "internal"
        assert "Mock stream internal crash" in str(exc_info.value)

def test_sync_client_execute(grpc_server):
    with Client(grpc_server) as client:
        ctx = Context(trace_id="sync-trace-123")
        payload = {"id": "req-sync", "payload": "hello-sync"}
        
        result = client.sync.call("Execute", payload, ctx)
        
        assert result["id"] == "req-sync"
        assert result["status"] == "Succeeded"
        assert result["output"] == {"status": "ok", "echo": "hello-sync"}

def test_sync_client_streaming(grpc_server):
    with Client(grpc_server) as client:
        ctx = Context(trace_id="sync-trace-stream")
        payload = {"id": "req-sync-stream", "payload": "stream-test"}
        
        chunks = []
        for chunk in client.sync.call_streaming("Stream", payload, ctx):
            chunks.append(chunk.content)
            
        assert chunks == ["chunk-0", "chunk-1", "chunk-2"]

def test_sync_client_health(grpc_server):
    with Client(grpc_server) as client:
        ctx = Context(trace_id="sync-health-trace")
        result = client.sync.call("Health", {}, ctx)
        
        assert result["overall"] == "Healthy"
        assert result["state"] == "READY"
        assert "execution" in result["components"]
        assert result["components"]["execution"]["state"] == "Healthy"
