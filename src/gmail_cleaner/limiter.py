import threading
import time
from gmail_cleaner.logger import get_logger

logger = get_logger("limiter")


class TokenBucketRateLimiter:
    """
    Thread-safe token bucket rate limiter to prevent 429 Too Many Requests errors.
    Smoothly paces outgoing requests with sub-second precision.
    """

    def __init__(self, max_rpm=1000, burst_limit=None):
        self.max_rpm = max_rpm
        self.rate = max_rpm / 60.0  # tokens per second
        self.capacity = burst_limit if burst_limit is not None else min(max_rpm, 30)
        self.tokens = float(self.capacity)
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self, tokens=1):
        """Blocks until the requested number of tokens are available."""
        with self.lock:
            while True:
                now = time.time()
                elapsed = now - self.last_update
                self.last_update = now

                # Refill tokens based on elapsed time
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                # Calculate sleep duration needed for next token
                needed = tokens - self.tokens
                sleep_time = needed / self.rate
                if sleep_time > 0.2:
                    logger.debug(f"Rate limiter active: waiting {sleep_time:.2f}s for tokens...")
                time.sleep(sleep_time)


def get_rate_limiter(tier="paid"):
    """
    Returns a rate limiter tuned for the specified Google Gemini tier.
    - 'paid' (Pay-As-You-Go): 1,000 RPM (up to 16 req/sec with burst 25)
    - 'free' (Free Tier): 15 RPM (1 req every 4.0 seconds, burst 1)
    """
    if tier.lower() == "free":
        return TokenBucketRateLimiter(max_rpm=14, burst_limit=1)
    return TokenBucketRateLimiter(max_rpm=950, burst_limit=25)
