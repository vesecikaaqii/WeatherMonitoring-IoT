# Implementation Summary

This document maps each professor requirement to where and how it is satisfied
in the codebase.

## Requirement → Implementation map

| # | Requirement | Implemented in | Notes |
|---|-------------|----------------|-------|
| 1 | Kafka message queue | `docker-compose.yml`, `infrastructure/kafka_topics.sh`, `sensor-service/simulator.py` | Topics: `weather_data`, `weather_alerts`, `performance_metrics` |
| 2 | Spark Structured Streaming | `stream-processing/spark_processor.py` | Kafka source, `foreachBatch`, checkpointing |
| 3 | Cassandra storage | `infrastructure/cassandra_setup.cql` | 5 tables in `weather_ks` |
| 4 | UI visualization | `frontend-dashboard/app.py` | 6 Streamlit pages |
| 5 | Configurable simulator | `sensor-service/simulator.py`, `simulator_api.py` | normal/stress, rate, anomaly, cities |
| 6 | Sensor metadata | `sensor-service/config.py`, `infrastructure/seed_metadata.py` | `sensor_metadata` table |
| 7 | Alarm system | `spark_processor.py` (`classify_parameter`, `build_alarms`, `publish_alarms_to_kafka`) | INFO/WARNING/CRITICAL → published to Kafka `weather_alerts`, then stored in `alarms` |
| 8 | AI / anomaly detection | `stream-processing/ai_model.py` | IsolationForest + statistical fallback |
| 9 | Performance metrics | `spark_processor.py`, `simulator_api.py` | `performance_metrics` table + `/metrics` |
| 10 | Stress test | `simulator_api.py` (`/stress-test`) | Timed high-throughput burst |
| 11 | Docker / server deploy | `docker-compose.yml`, Dockerfiles | All services containerized |
| 12 | Report material | `reports/` | This file + outline + perf results |
| 13 | Weather topic consistency | Whole project | Kosovo cities, weather params throughout |
| 14 | Bonus LSTM | `stream-processing/lstm_forecast.py` | Keras LSTM with moving-avg fallback |

## Key code references

### Message schema (simulator)
`sensor-service/simulator.py` → `WeatherSimulator.generate_message()` produces
the exact JSON spec including `event_id`, `location`, and
`producer_timestamp_ms` for latency measurement.

### Severity / alarm logic (Spark)
`stream-processing/spark_processor.py`:
- `THRESHOLDS` — all parameter bands from the spec.
- `classify_parameter(param, value)` → (severity, threshold, fragment).
- `build_alarms(reading)` → one alarm row per crossed threshold.
- `process_batch(batch_df, epoch_id)` → validation, latency, AI, writes to all
  Cassandra tables, performance metrics, logging.

### AI anomaly detection
`stream-processing/ai_model.py`:
- `WeatherAnomalyDetector` uses IsolationForest trained on synthetic normal
  weather; `is_anomaly(reading)` returns True/False.
- Falls back to a pure-Python statistical/threshold check if sklearn/numpy are
  absent, so the Spark workers never crash.

### Cassandra schema
`infrastructure/cassandra_setup.cql`:
- `sensor_metadata` (PK `sensor_id`, indexes on city/status)
- `sensor_readings` (PK `((sensor_id, date), timestamp)`)
- `latest_readings` (PK `sensor_id`)
- `alarms` (PK `((city), timestamp, alarm_id)`, index on severity)
- `performance_metrics` (PK `((mode), timestamp, metric_id)`)

### Consistency guarantees
- Sensor IDs and city names are defined once in `config.CITY_SENSORS` and reused
  by simulator, metadata seeding, and dashboard filters — no `S1/S2/S3` mismatch.
- All hosts/ports come from environment variables — no hardcoded `localhost`
  that would break Docker.
- Topic names are constants in `config.py`.

## How to verify quickly
```bash
# Unit tests (no infra needed)
python tests/test_simulator_message.py

# AI model smoke test
python stream-processing/ai_model.py

# Syntax check everything
python -m py_compile sensor-service/*.py stream-processing/*.py frontend-dashboard/*.py
```
