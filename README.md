# DataFlowX

Real-Time Event-Driven Data Processing System — 5 autonomous agents with adaptive flow control, event sourcing, and self-healing capabilities.

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ StreamAgent │───▶│ ProcessAgent│───▶│ RouteAgent  │
│ (Ingest)    │    │ (Transform) │    │ (Distribute)│
└─────────────┘    └─────────────┘    └─────────────┘
                          │                   │
                          ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐
                   │ AdaptAgent  │    │ RecoverAgent│
                   │ (Optimize)  │    │ (Heal)      │
                   └─────────────┘    └─────────────┘
```

| Agent | Role | Capabilities |
|-------|------|--------------|
| StreamAgent | Real-time event ingestion | Kafka, RabbitMQ, WebSocket, gRPC, MQTT |
| ProcessAgent | Adaptive data transformation | Schema evolution, ML-based cleaning |
| RouteAgent | Intelligent event routing | Priority queues, load balancing, sharding |
| AdaptAgent | Auto-scaling & optimization | Flow control, backpressure, circuit breaker |
| RecoverAgent | Self-healing & fault tolerance | Retry strategies, dead letter queues, rollback |

## Key Features
- **Event Sourcing**: Full audit trail with replay capability
- **Adaptive Flow Control**: Dynamic rate limiting based on system load
- **Self-Healing**: Automatic recovery from failures with exponential backoff
- **Schema Evolution**: Handle schema changes without pipeline restart
- **Priority Routing**: Route events based on priority, type, or custom rules

## Quick Start
```bash
# Start streaming pipeline
python -m src.main stream --source kafka://events --target redis://cache

# Monitor in real-time
python -m src.main monitor --dashboard

# Replay events
python -m src.main replay --from "2024-01-01" --to "2024-01-02"
```

## Token Consumption
~8M tokens/day — real-time event processing, adaptive optimization, anomaly detection, and automated incident response.

Built with: Hermes Agent, MiMo + Claude series
