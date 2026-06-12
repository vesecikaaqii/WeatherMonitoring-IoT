# Performance Analysis & Stress Testing

This document describes how performance is measured in the Weather Monitoring
IoT pipeline, how to run a stress test, example results, and optimization
recommendations.

## What is measured

| Metric | Where measured | Stored in |
|--------|----------------|-----------|
| Messages sent | Simulator (producer counter) | `/metrics` API |
| Messages/second (produce) | Simulator (1s sliding window) | `/metrics` API |
| Messages processed | Spark `process_batch` per micro-batch | `performance_metrics` |
| Messages/second (process) | Spark (batch size / batch time) | `performance_metrics` |
| Average latency (ms) | `now_ms - producer_timestamp_ms` per record | `performance_metrics` |
| Max latency (ms) | max over batch | `performance_metrics` |
| Cassandra write time (ms) | wall-clock around the readings write | `performance_metrics` |
| Mode | `stream` / `stress` / `normal` | `performance_metrics` |

**End-to-end latency** is the key IoT metric: every message carries
`producer_timestamp_ms`, and Spark subtracts it from its own processing clock,
giving the full Kafka → Spark latency.

## How to run a stress test

### Option A — via the dashboard
1. Open the dashboard → **Simulator Control** page.
2. Set *Target msg/s* (e.g. 500) and *Duration* (e.g. 30s).
3. Click ** Run stress test**.
4. Watch live throughput on the **Performance** page.

### Option B — via the API
```bash
curl -X POST http://localhost:8000/stress-test \
  -H "Content-Type: application/json" \
  -d '{"messages_per_second": 1000, "duration_seconds": 60, "anomaly_rate": 0.1}'

# poll live rate
curl http://localhost:8000/metrics
```

### Option C — CLI burst
```bash
# set stress config, then run the simulator
curl -X POST http://localhost:8000/config \
  -d '{"mode":"stress","messages_per_second":1000}' -H "Content-Type: application/json"
curl -X POST http://localhost:8000/start
```

## Example results

> Single laptop, Docker (Kafka + Cassandra + Spark local), 3 Kafka partitions.
> Replace with your measured numbers after running.

| Mode | Target msg/s | Achieved msg/s | Avg latency (ms) | Max latency (ms) | Cassandra write (ms) |
|------|-------------|----------------|------------------|------------------|----------------------|
| normal | ~2 | ~2 | 35 | 90 | 40 |
| stress | 100 | ~100 | 120 | 350 | 70 |
| stress | 500 | ~480 | 410 | 1100 | 160 |
| stress | 1000 | ~850 | 900 | 2400 | 320 |

Observations:
- Latency rises with throughput once Spark batches and Cassandra writes
  become the bottleneck.
- Achieved rate approaches target until the single-node broker/consumer
  saturates (~800–1000 msg/s on a laptop).

## Optimization recommendations

1. **Kafka partitions** — increase `weather_data` beyond 3 partitions so more
   Spark consumer tasks run in parallel. Partition by `sensor_id` (already the
   message key) for ordering per sensor.
2. **Spark trigger interval** — tune `SPARK_TRIGGER_INTERVAL`. Shorter (1–2s)
   lowers latency; longer (5–10s) increases batch size and throughput.
3. **Cassandra schema** — bounded partitions `((sensor_id, date), timestamp)`
   prevent wide-partition hotspots; `latest_readings` keyed by `sensor_id`
   gives O(1) upserts.
4. **Checkpointing** — enabled via `checkpointLocation`; place it on fast local
   disk (not a network share) to avoid stalling micro-batches.
5. **Batching writes** — the Cassandra connector batches per micro-batch.
   Push per-row Python logic (alarms/AI) into Spark UDFs / mapPartitions at
   higher scale instead of `collect()` to the driver.
6. **Producer tuning** — `linger_ms` + `acks=1` already trade a little
   durability for throughput; raise `linger_ms` for larger produce batches.
