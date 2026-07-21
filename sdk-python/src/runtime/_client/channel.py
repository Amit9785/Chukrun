import os
import grpc
import logging
from typing import List, Optional
from runtime._client.retry import ReconnectPolicy
from runtime.errors import RuntimeError

logger = logging.getLogger("runtime.client.channel")

class RuntimeChannel:
    def __init__(self, target: str, credentials: Optional[str] = None, options = None):
        self._target = target
        self._credentials = credentials
        from runtime._client.async_client import ClientOptions
        self._options = options or ClientOptions()
        
        # Determine pool size (default to CPU count, at least 1)
        pool_size = self._options.pool_size
        if pool_size is None:
            try:
                pool_size = os.cpu_count() or 1
            except Exception:
                pool_size = 1
        self._pool_size = max(1, pool_size)
        
        self._channels: List[grpc.aio.Channel] = []
        self._connected = False
        self._index = 0

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        """
        Eagerly establish gRPC channels in the pool.
        """
        if self._connected:
            return

        for i in range(self._pool_size):
            channel = self._create_channel()
            self._channels.append(channel)
            
            # If not in lazy mode, eager connect to verify target is reachable
            # We check connectivity using channel.channel_ready() or similar if needed.
            # But standard grpc eager connect can wait for the channel state
            # or we just establish it.
            
        self._connected = True
        logger.debug(f"Established channel pool of size {self._pool_size} to {self._target}")

    def _create_channel(self) -> grpc.aio.Channel:
        # Standard channel options for HTTP/2 keepalives, etc.
        grpc_options = [
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', 1),
            ('grpc.http2.max_pings_without_data', 0),
        ]
        
        if self._options.allow_insecure_channel or self._credentials is None:
            channel = grpc.aio.insecure_channel(self._target, options=grpc_options)
        else:
            # TLS/Secure credentials
            # credentials argument could be file path or PEM string or grpc.ChannelCredentials
            if isinstance(self._credentials, str):
                if os.path.exists(self._credentials):
                    with open(self._credentials, "rb") as f:
                        creds = grpc.ssl_channel_credentials(f.read())
                else:
                    creds = grpc.ssl_channel_credentials(self._credentials.encode("utf-8"))
            elif isinstance(self._credentials, grpc.ChannelCredentials):
                creds = self._credentials
            else:
                creds = grpc.ssl_channel_credentials()
            channel = grpc.aio.secure_channel(self._target, creds, options=grpc_options)
            
        return channel

    async def get_channel(self) -> grpc.aio.Channel:
        """
        Get a channel from the pool using round-robin distribution.
        """
        if not self._connected:
            await self.connect()
            
        if not self._channels:
            raise RuntimeError("Channel pool is empty. Did connect() fail?", category="internal")
            
        channel = self._channels[self._index]
        self._index = (self._index + 1) % len(self._channels)
        return channel

    async def close(self) -> None:
        """
        Gracefully close all channels in the pool.
        """
        if not self._connected:
            return
            
        for channel in self._channels:
            try:
                # Close waits for in-flight calls to complete
                await channel.close()
            except Exception as e:
                logger.warning(f"Error closing channel to {self._target}: {e}")
                
        self._channels.clear()
        self._connected = False
        logger.debug("Closed all channels in pool")
