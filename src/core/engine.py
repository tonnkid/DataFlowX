"""DataFlowX FlowEngine — Orchestrates event-driven pipeline."""
import asyncio
import logging
import time
from typing import Any
from ..pipeline.stream_agent import StreamAgent
from ..pipeline.process_agent import ProcessAgent
from ..pipeline.route_agent import RouteAgent
from ..pipeline.adapt_agent import AdaptAgent
from ..pipeline.recover_agent import RecoverAgent

logger = logging.getLogger(__name__)


class FlowEngine:
    """Central engine coordinating all 5 agents."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.stream = StreamAgent()
        self.process = ProcessAgent()
        self.route = RouteAgent()
        self.adapt = AdaptAgent()
        self.recover = RecoverAgent()
        self.running = False
        self.events_processed = 0
        self.start_time = None

    async def start_stream(self, source: str, target: str):
        """Start streaming pipeline."""
        self.running = True
        self.start_time = time.time()
        logger.info(f"Engine started: {source} -> {target}")

        while self.running:
            try:
                # 1. Stream events
                events = await self.stream.ingest(source, batch_size=100)

                # 2. Process events
                processed = await self.process.transform(events)

                # 3. Route events
                routed = await self.route.distribute(processed, target)

                # 4. Adapt flow control
                await self.adapt.optimize_flow(len(events))

                self.events_processed += len(events)
                await asyncio.sleep(0.1)  # Flow control

            except Exception as e:
                logger.error(f"Stream error: {e}")
                await self.recover.handle_failure(e)

    async def start_monitor(self):
        """Start real-time monitoring dashboard."""
        logger.info("Monitor started (press Ctrl+C to stop)")
        try:
            while True:
                status = self.get_status()
                logger.info(f"Events: {status['events_processed']} | Uptime: {status['uptime']:.0f}s")
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Monitor stopped")

    async def replay(self, from_time: str, to_time: str):
        """Replay events from event store."""
        logger.info(f"Replaying events from {from_time} to {to_time}")
        # Replay logic would go here
        return {"status": "replayed", "from": from_time, "to": to_time}

    def get_status(self) -> dict:
        """Get current engine status."""
        uptime = time.time() - self.start_time if self.start_time else 0
        return {
            "status": "running" if self.running else "stopped",
            "events_processed": self.events_processed,
            "uptime": uptime,
            "agents": {
                "stream": self.stream.status(),
                "process": self.process.status(),
                "route": self.route.status(),
                "adapt": self.adapt.status(),
                "recover": self.recover.status()
            }
        }
