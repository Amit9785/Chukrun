import random
import time
from dataclasses import dataclass

@dataclass
class ReconnectPolicy:
    max_attempts: int = 5
    base_delay_seconds: float = 0.5
    max_delay_seconds: float = 10.0
    jitter: bool = True

    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay using exponential backoff with jitter.
        """
        if attempt <= 0:
            return 0.0
        
        delay = self.base_delay_seconds * (2 ** (attempt - 1))
        delay = min(delay, self.max_delay_seconds)
        
        if self.jitter:
            # Full jitter
            delay = random.uniform(0, delay)
            
        return delay

    def sleep_before_retry(self, attempt: int) -> None:
        """
        Sleep for the delay calculated for this attempt.
        """
        delay = self.get_delay(attempt)
        if delay > 0:
            time.sleep(delay)

    async def async_sleep_before_retry(self, attempt: int) -> None:
        """
        Async sleep for the delay calculated for this attempt.
        """
        import asyncio
        delay = self.get_delay(attempt)
        if delay > 0:
            await asyncio.sleep(delay)
