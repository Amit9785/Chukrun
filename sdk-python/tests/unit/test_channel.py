import pytest
from runtime._client.channel import RuntimeChannel
from runtime._client.async_client import ClientOptions
from runtime.errors import RuntimeError

@pytest.mark.asyncio
async def test_channel_pool_lifecycle():
    options = ClientOptions(pool_size=2, allow_insecure_channel=True)
    ch = RuntimeChannel("localhost:50051", options=options)
    
    assert not ch.is_connected
    
    # Eager connect
    await ch.connect()
    assert ch.is_connected
    
    # Idempotent connect
    await ch.connect()
    assert ch.is_connected
    
    # Round robin channel retrieval
    ch1 = await ch.get_channel()
    ch2 = await ch.get_channel()
    ch3 = await ch.get_channel()
    assert ch1 is ch3
    
    # Close
    await ch.close()
    assert not ch.is_connected
    
    # Idempotent close
    await ch.close()

@pytest.mark.asyncio
async def test_channel_lazy_connect():
    options = ClientOptions(pool_size=1, allow_insecure_channel=True)
    ch = RuntimeChannel("localhost:50051", options=options)
    
    # get_channel triggers connect automatically if not connected
    channel = await ch.get_channel()
    assert channel is not None
    assert ch.is_connected
    await ch.close()

@pytest.mark.asyncio
async def test_channel_empty_pool_error():
    options = ClientOptions(pool_size=1, allow_insecure_channel=True)
    ch = RuntimeChannel("localhost:50051", options=options)
    await ch.connect()
    ch._channels.clear()  # simulate empty pool state
    
    with pytest.raises(RuntimeError) as exc_info:
        await ch.get_channel()
    assert "Channel pool is empty" in str(exc_info.value)
