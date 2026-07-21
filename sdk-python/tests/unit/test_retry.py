import pytest
from runtime._client.retry import ReconnectPolicy

def test_reconnect_policy_delay_calculation():
    policy = ReconnectPolicy(
        max_attempts=3,
        base_delay_seconds=0.5,
        max_delay_seconds=2.0,
        jitter=False
    )
    
    assert policy.get_delay(0) == 0.0
    assert policy.get_delay(1) == 0.5
    assert policy.get_delay(2) == 1.0
    assert policy.get_delay(3) == 2.0
    assert policy.get_delay(4) == 2.0  # capped at max_delay_seconds

def test_reconnect_policy_jitter():
    policy = ReconnectPolicy(
        max_attempts=3,
        base_delay_seconds=1.0,
        max_delay_seconds=10.0,
        jitter=True
    )
    
    for attempt in range(1, 4):
        delay = policy.get_delay(attempt)
        max_possible = min(1.0 * (2 ** (attempt - 1)), 10.0)
        assert 0.0 <= delay <= max_possible

def test_reconnect_policy_sync_sleep():
    policy = ReconnectPolicy(base_delay_seconds=0.01, jitter=False)
    policy.sleep_before_retry(0)  # should not sleep or delay
    policy.sleep_before_retry(1)

@pytest.mark.asyncio
async def test_reconnect_policy_async_sleep():
    policy = ReconnectPolicy(base_delay_seconds=0.001, jitter=False)
    await policy.async_sleep_before_retry(0)
    await policy.async_sleep_before_retry(1)
