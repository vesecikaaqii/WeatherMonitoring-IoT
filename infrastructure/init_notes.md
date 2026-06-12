# Infrastructure initialization notes

This folder bootstraps the backing services for the Weather Monitoring IoT
pipeline: **Kafka**, **Cassandra**, and the topic / schema setup.

## Files
| File | Purpose |
|------|---------|
| `cassandra_setup.cql` | Creates `weather_ks` keyspace + all tables/indexes |
| `kafka_topics.sh` | Creates `weather_data`, `weather_alerts`, `performance_metrics` |
| `seed_metadata.py` | Inserts sensor metadata rows (Kosovo cities) into Cassandra |

## Order of operations

1. **Start infrastructure**
   ```bash
   docker compose up -d zookeeper kafka cassandra
   ```
   Wait ~30–60s for Cassandra to accept CQL (`docker logs -f cassandra`).

2. **Create Cassandra schema**
   ```bash
   docker exec -it cassandra cqlsh -f /scripts/cassandra_setup.cql
   ```
   (The compose file mounts `infrastructure/` to `/scripts` in the container.)

3. **Create Kafka topics**
   ```bash
   docker exec -it kafka bash /scripts/kafka_topics.sh
   # or from host:
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092 bash infrastructure/kafka_topics.sh
   ```

4. **Seed sensor metadata** (optional but recommended)
   ```bash
   CASSANDRA_HOST=127.0.0.1 python infrastructure/seed_metadata.py
   ```

5. **Seed historical readings** (DEMO ONLY — optional)
   Pre-fills `sensor_readings` for the last 7 days so the dashboard's
   "Historical trends" charts and date-partition selector have data without
   running the live pipeline. Does NOT touch Kafka/Spark.
   ```bash
   CASSANDRA_HOST=127.0.0.1 python infrastructure/seed_historical_readings.py
   # optional: DAYS=7 INTERVAL_MINUTES=30 ANOMALY_RATE=0.25
   ```

## Notes
- All hosts/ports come from environment variables (`KAFKA_BOOTSTRAP_SERVERS`,
  `CASSANDRA_HOST`, …) so the exact same code runs locally and in Docker.
- `replication_factor = 1` is for single-node dev. On a real cluster use
  `NetworkTopologyStrategy` and RF >= 3.
- Topic partition counts (`weather_data`=3, `weather_alerts`=3) allow Spark to
  consume in parallel; increase them to scale the stress test.
