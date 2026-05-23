"""DataFlowX CLI — Real-time event processing interface."""
import argparse
import asyncio
import json
import logging
from .core.engine import FlowEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="DataFlowX — Event-Driven Processing")
    sub = parser.add_subparsers(dest="command")

    # Stream command
    stream_p = sub.add_parser("stream", help="Start streaming pipeline")
    stream_p.add_argument("--source", required=True, help="Source URI (kafka://, redis://, ws://)")
    stream_p.add_argument("--target", required=True, help="Target URI")

    # Monitor command
    sub.add_parser("monitor", help="Start real-time dashboard")

    # Replay command
    replay_p = sub.add_parser("replay", help="Replay events")
    replay_p.add_argument("--from", dest="from_time", required=True, help="Start time")
    replay_p.add_argument("--to", dest="to_time", required=True, help="End time")

    # Status command
    sub.add_parser("status", help="Show system status")

    args = parser.parse_args()
    engine = FlowEngine()

    if args.command == "stream":
        logger.info(f"Starting stream: {args.source} -> {args.target}")
        await engine.start_stream(args.source, args.target)
    elif args.command == "monitor":
        logger.info("Starting dashboard...")
        await engine.start_monitor()
    elif args.command == "replay":
        logger.info(f"Replaying: {args.from_time} -> {args.to_time}")
        await engine.replay(args.from_time, args.to_time)
    elif args.command == "status":
        status = engine.get_status()
        print(json.dumps(status, indent=2))
    else:
        parser.print_help()
