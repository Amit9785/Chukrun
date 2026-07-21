import asyncio
import threading
from typing import Any, Iterator, Optional
from runtime._client.context_bridge import Context
from runtime._client.async_client import AsyncRuntimeClient

class SyncStreamIterator:
    def __init__(self, async_gen, loop):
        self._async_gen = async_gen
        self._loop = loop
        self._done = False

    def __iter__(self):
        return self

    def __next__(self):
        if self._done:
            raise StopIteration

        async def _next_item():
            try:
                # Get the next item from the async generator
                return await self._async_gen.__anext__()
            except StopAsyncIteration:
                return StopAsyncIteration
            except Exception as e:
                return e

        future = asyncio.run_coroutine_threadsafe(_next_item(), self._loop)
        res = future.result()

        if res is StopAsyncIteration:
            self._done = True
            raise StopIteration
        elif isinstance(res, Exception):
            self._done = True
            raise res
            
        return res

class SyncRuntimeClient:
    def __init__(self, async_client: AsyncRuntimeClient):
        self._async_client = async_client
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def _start_loop(self) -> None:
        """
        Lazily start the background event loop thread.
        """
        with self._lock:
            if self._loop is None:
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(
                    target=self._run_loop, 
                    args=(self._loop,), 
                    daemon=True
                )
                self._thread.start()

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def connect(self) -> None:
        self._start_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._async_client._channel.connect(), 
            self._loop
        )
        future.result()

    def close(self) -> None:
        with self._lock:
            if self._loop is not None:
                # Gracefully close channels from within the loop
                future = asyncio.run_coroutine_threadsafe(
                    self._async_client._channel.close(), 
                    self._loop
                )
                try:
                    future.result(timeout=5.0)
                except Exception:
                    pass
                
                # Stop and cleanup loop and thread
                self._loop.call_soon_threadsafe(self._loop.stop())
                self._thread.join(timeout=5.0)
                self._loop.close()
                self._loop = None
                self._thread = None

    def call(self, method: str, request: Any, ctx: Context, timeout: Optional[float] = None) -> Any:
        self._start_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._async_client.call(method, request, ctx, timeout),
            self._loop
        )
        return future.result()

    def call_streaming(self, method: str, request: Any, ctx: Context) -> Iterator[Any]:
        self._start_loop()
        async_gen = self._async_client.call_streaming(method, request, ctx)
        return SyncStreamIterator(async_gen, self._loop)
