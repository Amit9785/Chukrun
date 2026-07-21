import pytest
from datetime import datetime, timezone
from google.protobuf.timestamp_pb2 import Timestamp
from runtime._client.codec import Codec
from runtime._client.context_bridge import Context
from runtime.errors import RuntimeError, RuntimeValidationError
from runtime._generated.runtime_pb2 import ExecuteRequest, ExecuteResponse, HealthRequest, TokenUsageProto, CostEstimateProto, AttemptProto
from runtime._generated.errors_pb2 import ErrorProto

def test_codec_encode_execute_request():
    codec = Codec()
    ctx = Context(trace_id="trace-codec")
    
    payload = {
        "id": "req-1",
        "provider_ref": "openai/gpt-4",
        "payload": {"prompt": "test"},
        "priority": "High",
        "parent_id": "parent-1",
        "metadata": {"env": "prod"},
        "retry_policy": {"max_attempts": 3, "backoff_strategy": "linear"},
        "timeout_policy": {"total_ns": 1000000000}
    }
    
    req = codec.encode_request("Execute", payload, ctx)
    assert isinstance(req, ExecuteRequest)
    assert req.id == "req-1"
    assert req.provider_ref == "openai/gpt-4"
    assert '"prompt": "test"' in req.payload
    assert req.priority == "High"
    assert req.parent_id == "parent-1"
    assert req.metadata["env"] == "prod"
    assert req.retry_policy.max_attempts == 3
    assert req.timeout_policy.total_ns == 1000000000

def test_codec_encode_execute_request_instance():
    codec = Codec()
    ctx = Context(trace_id="trace-inst")
    existing_req = ExecuteRequest(id="existing-1")
    
    req = codec.encode_request("Execute", existing_req, ctx)
    assert req.id == "existing-1"
    assert req.context.trace_id == "trace-inst"

def test_codec_encode_health_and_invalid():
    codec = Codec()
    ctx = Context(trace_id="trace-h")
    
    h_req = codec.encode_request("Health", {}, ctx)
    assert isinstance(h_req, HealthRequest)
    
    with pytest.raises(RuntimeError) as exc_info:
        codec.encode_request("Execute", "invalid-string-payload", ctx)
    assert exc_info.value.category == "validation"

    with pytest.raises(RuntimeError) as exc_info:
        codec.encode_request("UnknownMethod", {}, ctx)
    assert exc_info.value.category == "validation"

def test_codec_decode_execute_response():
    codec = Codec()
    
    now = Timestamp()
    now.FromDatetime(datetime.now(timezone.utc))
    
    res_proto = ExecuteResponse(
        id="res-1",
        status="Succeeded",
        state="SUCCEEDED",
        output='{"result": 42}',
        token_usage=TokenUsageProto(prompt_tokens=10, completion_tokens=20, total_tokens=30),
        cost=CostEstimateProto(amount_usd=0.005),
        duration_ns=1000000,
        attempt_count=1,
        attempts=[AttemptProto(number=1, started_at=now, ended_at=now, provider_ref="ref-1")]
    )
    
    res_dict, ctx = codec.decode_response(res_proto)
    assert res_dict["id"] == "res-1"
    assert res_dict["output"] == {"result": 42}
    assert res_dict["token_usage"]["total_tokens"] == 30
    assert res_dict["cost"]["amount_usd"] == 0.005
    assert len(res_dict["attempts"]) == 1
    assert ctx is None
