"""DataFlowX Engine Tests."""
import asyncio
import pytest
from src.core.engine import FlowEngine


@pytest.mark.asyncio
async def test_engine_status():
    """Test engine status."""
    engine = FlowEngine()
    status = engine.get_status()
    assert status["status"] == "stopped"
    assert status["events_processed"] == 0


@pytest.mark.asyncio
async def test_stream_agent():
    """Test stream agent ingestion."""
    from src.pipeline.stream_agent import StreamAgent
    agent = StreamAgent()
    events = await agent.ingest("kafka://test", batch_size=10)
    assert len(events) == 10
    assert agent.events_ingested == 10
