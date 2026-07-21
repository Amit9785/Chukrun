from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Mapping, Optional
from google.protobuf.timestamp_pb2 import Timestamp
from runtime._generated.context_pb2 import ContextProto, UserContextProto, CostBudgetProto

class Priority(str, Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    NORMAL = "Normal"
    LOW = "Low"
    BACKGROUND = "Background"

@dataclass(frozen=True)
class UserContext:
    user_id: str
    email: str = ""
    metadata: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_proto(cls, proto: UserContextProto) -> "UserContext":
        return cls(
            user_id=proto.user_id,
            email=proto.email,
            metadata=dict(proto.metadata),
        )

    def to_proto(self) -> UserContextProto:
        return UserContextProto(
            user_id=self.user_id,
            email=self.email,
            metadata=dict(self.metadata),
        )

@dataclass(frozen=True)
class CostBudget:
    limit_usd: float = 0.0
    spent_usd: float = 0.0
    limit_tokens: int = 0
    spent_tokens: int = 0

    @classmethod
    def from_proto(cls, proto: CostBudgetProto) -> "CostBudget":
        return cls(
            limit_usd=proto.limit_usd,
            spent_usd=proto.spent_usd,
            limit_tokens=proto.limit_tokens,
            spent_tokens=proto.spent_tokens,
        )

    def to_proto(self) -> CostBudgetProto:
        return CostBudgetProto(
            limit_usd=self.limit_usd,
            spent_usd=self.spent_usd,
            limit_tokens=self.limit_tokens,
            spent_tokens=self.spent_tokens,
        )

# Mapping between Priority enum and protobuf int32 values
_PRIORITY_TO_INT = {
    Priority.BACKGROUND: 0,
    Priority.LOW: 1,
    Priority.NORMAL: 2,
    Priority.HIGH: 3,
    Priority.CRITICAL: 4,
}

_INT_TO_PRIORITY = {
    0: Priority.BACKGROUND,
    1: Priority.LOW,
    2: Priority.NORMAL,
    3: Priority.HIGH,
    4: Priority.CRITICAL,
}

@dataclass(frozen=True)
class Context:
    trace_id: str
    session_id: Optional[str] = None
    user: Optional[UserContext] = None
    variables: Mapping[str, str] = field(default_factory=dict)
    deadline: Optional[datetime] = None
    priority: Priority = Priority.NORMAL
    cost_budget: Optional[CostBudget] = None

def from_proto(proto: ContextProto) -> Context:
    """
    Reconstruct a Python-native Context object from ContextProto.
    """
    user = None
    if proto.HasField("user"):
        user = UserContext.from_proto(proto.user)
        
    deadline = None
    if proto.HasField("deadline"):
        deadline = proto.deadline.ToDatetime()
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        
    cost_budget = None
    if proto.HasField("cost_budget"):
        cost_budget = CostBudget.from_proto(proto.cost_budget)
        
    # Map int32 priority to enum, default to NORMAL on invalid
    priority = _INT_TO_PRIORITY.get(proto.priority, Priority.NORMAL)
    
    return Context(
        trace_id=proto.trace_id,
        session_id=proto.session_id or None,
        user=user,
        variables=dict(proto.request_variables),
        deadline=deadline,
        priority=priority,
        cost_budget=cost_budget,
    )

def to_proto(ctx: Context) -> ContextProto:
    """
    Serialize a Python-native Context back into a ContextProto.
    """
    user_proto = ctx.user.to_proto() if ctx.user is not None else None
    cost_budget_proto = ctx.cost_budget.to_proto() if ctx.cost_budget is not None else None
    
    deadline_proto = None
    if ctx.deadline is not None:
        deadline_proto = Timestamp()
        deadline_proto.FromDatetime(ctx.deadline)
        
    priority_int = _PRIORITY_TO_INT.get(ctx.priority, 2)
    
    # ContextProto construction
    kwargs = {
        "trace_id": ctx.trace_id,
        "session_id": ctx.session_id or "",
        "request_variables": dict(ctx.variables),
        "priority": priority_int,
    }
    if user_proto is not None:
        kwargs["user"] = user_proto
    if cost_budget_proto is not None:
        kwargs["cost_budget"] = cost_budget_proto
    if deadline_proto is not None:
        kwargs["deadline"] = deadline_proto
        
    return ContextProto(**kwargs)
