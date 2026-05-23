"""
AdaptAgent — Auto-scaling & optimization.
Flow control, backpressure, circuit breaker, and resource management.
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """Circuit breaker for fault tolerance."""

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.state = "closed"  # closed, open, half-open
        self.last_failure_time = 0

    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker."""
        if self.state == "open":
            if time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "half-open"
            else:
                raise Exception("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failures = 0
            return result
        except Exception as e:
            self.failures += 1
            self.last_failure_time = time.time()
            if self.failures >= self.failure_threshold:
                self.state = "open"
                logger.warning("Circuit breaker opened!")
            raise


class AdaptAgent:
    """Auto-scaling and flow control optimization."""

    def __init__(self):
        self.flow_rate = 100  # events per second
        self.backpressure_active = False
        self.circuit_breaker = CircuitBreaker()
        self.optimizations = 0

    async def optimize_flow(self, current_load: int):
        """Dynamically adjust flow based on load."""
        if current_load > self.flow_rate * 1.5:
            self.backpressure_active = True
            logger.warning(f"Backpressure activated: load={current_load}")
        elif current_load < self.flow_rate * 0.5:
            self.backpressure_active = False

        self.optimizations += 1

    def adjust_rate(self, new_rate: int):
        """Adjust flow rate."""
        self.flow_rate = new_rate
        logger.info(f"Flow rate adjusted: {new_rate}")

    def status(self) -> dict:
        return {"flow_rate": self.flow_rate, "backpressure": self.backpressure_active,
                "circuit_breaker": self.circuit_breaker.state, "optimizations": self.optimizations}
