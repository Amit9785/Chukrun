from datetime import datetime, timezone
from runtime._client.context_bridge import (
    Context, 
    UserContext, 
    CostBudget, 
    Priority, 
    to_proto, 
    from_proto
)

def test_context_roundtrip():
    # Setup full context
    user = UserContext(
        user_id="user-123",
        email="user@example.com",
        metadata={"role": "admin"}
    )
    
    budget = CostBudget(
        limit_usd=10.0,
        spent_usd=2.5,
        limit_tokens=1000,
        spent_tokens=250
    )
    
    deadline = datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    
    ctx = Context(
        trace_id="trace-abc",
        session_id="session-xyz",
        user=user,
        variables={"var1": "value1", "var2": "value2"},
        deadline=deadline,
        priority=Priority.HIGH,
        cost_budget=budget
    )
    
    # Serialize to proto
    proto = to_proto(ctx)
    
    assert proto.trace_id == "trace-abc"
    assert proto.session_id == "session-xyz"
    assert proto.user.user_id == "user-123"
    assert proto.user.email == "user@example.com"
    assert proto.user.metadata["role"] == "admin"
    assert proto.request_variables["var1"] == "value1"
    assert proto.request_variables["var2"] == "value2"
    assert proto.priority == 3 # Priority.HIGH mapped to 3
    assert proto.cost_budget.limit_usd == 10.0
    assert proto.cost_budget.spent_usd == 2.5
    assert proto.cost_budget.limit_tokens == 1000
    assert proto.cost_budget.spent_tokens == 250
    
    # Deserialize back from proto
    ctx_reconstructed = from_proto(proto)
    
    assert ctx_reconstructed.trace_id == ctx.trace_id
    assert ctx_reconstructed.session_id == ctx.session_id
    assert ctx_reconstructed.user.user_id == ctx.user.user_id
    assert ctx_reconstructed.user.email == ctx.user.email
    assert ctx_reconstructed.user.metadata == ctx.user.metadata
    assert ctx_reconstructed.variables == ctx.variables
    assert ctx_reconstructed.deadline == ctx.deadline
    assert ctx_reconstructed.priority == ctx.priority
    assert ctx_reconstructed.cost_budget.limit_usd == ctx.cost_budget.limit_usd
    assert ctx_reconstructed.cost_budget.spent_usd == ctx.cost_budget.spent_usd
    assert ctx_reconstructed.cost_budget.limit_tokens == ctx.cost_budget.limit_tokens
    assert ctx_reconstructed.cost_budget.spent_tokens == ctx.cost_budget.spent_tokens

def test_context_optional_fields():
    # Context with minimal fields
    ctx = Context(trace_id="trace-minimal")
    proto = to_proto(ctx)
    
    assert proto.trace_id == "trace-minimal"
    assert proto.session_id == ""
    assert not proto.HasField("user")
    assert not proto.HasField("deadline")
    assert not proto.HasField("cost_budget")
    assert proto.priority == 2 # Priority.NORMAL mapped to 2
    
    reconstructed = from_proto(proto)
    assert reconstructed.trace_id == "trace-minimal"
    assert reconstructed.session_id is None
    assert reconstructed.user is None
    assert reconstructed.deadline is None
    assert reconstructed.cost_budget is None
    assert reconstructed.priority == Priority.NORMAL
