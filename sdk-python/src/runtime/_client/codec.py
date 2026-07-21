import json
from typing import Any, Tuple, Optional
from google.protobuf.message import Message
from runtime._client.context_bridge import Context, to_proto, from_proto
from runtime._client.error_bridge import from_proto as error_from_proto
from runtime.errors import RuntimeError, RuntimeInternalError
from runtime._generated.runtime_pb2 import (
    ExecuteRequest, 
    ExecuteResponse, 
    HealthRequest, 
    HealthResponse,
    RetryPolicyProto,
    TimeoutPolicyProto,
    TokenUsageProto,
    CostEstimateProto,
    AttemptProto,
)
from runtime._generated.context_pb2 import ContextProto
from runtime._generated.errors_pb2 import ErrorProto

class Codec:
    def encode_request(self, method: str, payload: Any, ctx: Context) -> Message:
        """
        Encode Python-native request payload and context into the appropriate Protobuf request message.
        """
        ctx_proto = to_proto(ctx)
        method_lower = method.lower()
        
        if method_lower in ("execute", "stream"):
            return self._encode_execute_request(payload, ctx_proto)
        elif method_lower == "health":
            return HealthRequest()
        else:
            return self._encode_generic_request(method, payload, ctx_proto)

    def _encode_execute_request(self, payload: Any, ctx_proto: ContextProto) -> ExecuteRequest:
        if isinstance(payload, ExecuteRequest):
            payload.context.CopyFrom(ctx_proto)
            return payload
            
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"Unsupported execute payload type: {type(payload)}", 
                category="validation"
            )
            
        req = ExecuteRequest(context=ctx_proto)
        req.id = payload.get("id", "")
        req.provider_ref = payload.get("provider_ref", "")
        
        inner_payload = payload.get("payload")
        if inner_payload is not None:
            req.payload = inner_payload if isinstance(inner_payload, str) else json.dumps(inner_payload)
            
        req.priority = payload.get("priority", "Normal")
        req.parent_id = payload.get("parent_id", "")
        
        if "metadata" in payload:
            req.metadata.update(payload["metadata"])
            
        if "retry_policy" in payload and payload["retry_policy"]:
            self._apply_retry_policy(req, payload["retry_policy"])
            
        if "timeout_policy" in payload and payload["timeout_policy"]:
            self._apply_timeout_policy(req, payload["timeout_policy"])
            
        return req

    def _apply_retry_policy(self, req: ExecuteRequest, rp: dict) -> None:
        req.retry_policy.CopyFrom(RetryPolicyProto(
            max_attempts=rp.get("max_attempts", 5),
            backoff_strategy=rp.get("backoff_strategy", "exponential"),
            base_delay_ns=int(rp.get("base_delay_ns", 500000000)), # 0.5s default
            max_delay_ns=int(rp.get("max_delay_ns", 10000000000)), # 10s default
            jitter=rp.get("jitter", True),
        ))

    def _apply_timeout_policy(self, req: ExecuteRequest, tp: dict) -> None:
        req.timeout_policy.CopyFrom(TimeoutPolicyProto(
            total_ns=int(tp.get("total_ns", 60000000000)), # 60s default
            per_attempt_ns=int(tp.get("per_attempt_ns", 0)),
        ))

    def _encode_generic_request(self, method: str, payload: Any, ctx_proto: ContextProto) -> Message:
        if isinstance(payload, Message):
            if hasattr(payload, "context"):
                payload.context.CopyFrom(ctx_proto)
            return payload
        raise RuntimeError(
            f"Unknown method {method} or unsupported payload type {type(payload)}", 
            category="validation"
        )

    def decode_response(self, proto: Message) -> Tuple[Any, Optional[Context]]:
        """
        Decode Protobuf response message into Python-native result and Context.
        """
        ctx = None
        if hasattr(proto, "context") and proto.HasField("context"):
            ctx = from_proto(proto.context)
        
        if isinstance(proto, ExecuteResponse):
            return self._decode_execute_response(proto), ctx
        elif isinstance(proto, HealthResponse):
            return self._decode_health_response(proto), ctx
        else:
            return proto, ctx

    def _decode_execute_response(self, proto: ExecuteResponse) -> dict:
        output = proto.output
        if output:
            try:
                output = json.loads(output)
            except Exception:
                pass
        
        error = error_from_proto(proto.error) if proto.HasField("error") else None
        
        token_usage = None
        if proto.HasField("token_usage"):
            token_usage = {
                "prompt_tokens": proto.token_usage.prompt_tokens,
                "completion_tokens": proto.token_usage.completion_tokens,
                "total_tokens": proto.token_usage.total_tokens,
            }
            
        cost = {"amount_usd": proto.cost.amount_usd} if proto.HasField("cost") else None
        
        attempts = []
        for att in proto.attempts:
            att_err = error_from_proto(att.error) if att.HasField("error") else None
            attempts.append({
                "number": att.number,
                "started_at": att.started_at.ToDatetime(),
                "ended_at": att.ended_at.ToDatetime() if att.HasField("ended_at") else None,
                "error": att_err,
                "provider_ref": att.provider_ref,
            })
            
        return {
            "id": proto.id,
            "status": proto.status,
            "state": proto.state,
            "output": output,
            "error": error,
            "token_usage": token_usage,
            "cost": cost,
            "duration_ns": proto.duration_ns,
            "attempt_count": proto.attempt_count,
            "attempts": attempts,
        }

    def _decode_health_response(self, proto: HealthResponse) -> dict:
        components = {}
        for name, ch in proto.components.items():
            components[name] = {
                "state": ch.state,
                "details": ch.details,
                "fatal": ch.fatal,
                "last_error": ch.last_error,
                "checked_at": ch.checked_at.ToDatetime(),
            }
        return {
            "overall": proto.overall,
            "state": proto.state,
            "components": components,
            "since": proto.since.ToDatetime(),
            "reason": proto.reason,
        }

    def decode_error(self, proto: ErrorProto) -> RuntimeError:
        """
        Decode ErrorProto to Python-native RuntimeError exception.
        """
        return error_from_proto(proto)
