# Final Report Outline — Weather Monitoring in the Internet of Things

> Phase-2 practical report outline, continuing the Phase-1 topic
> **"Monitorimi i Motit në Internet of Things / Weather Monitoring IoT"**.
> University of Prishtina — Faculty of Electrical and Computer Engineering —
> Computer and Software Engineering (Master) — Course: Internet of Things —
> Prof. Besmir Sejdiu.

---

## Abstract
A concise (~200 word) summary of the project: an end-to-end IoT weather
monitoring system for Kosovo cities that streams simulated weather-station data
through Kafka, processes it in real time with Spark Structured Streaming, stores
it in Cassandra, raises threshold-based alarms, applies AI anomaly detection,
and visualizes everything in a Streamlit dashboard, with a stress-test harness
for performance evaluation.

## Keywords
Internet of Things, Weather Monitoring, Apache Kafka, Apache Spark, Structured
Streaming, Cassandra, Anomaly Detection, Time-Series, Real-Time Analytics.

## 1. Introduction
- Motivation for weather monitoring as an IoT domain.
- Problem statement and objectives (real-time ingestion, storage, alarms, AI).
- Scope: continuation of Phase-1 topic; what Phase-2 adds.

## 2. IoT Domain: Weather Monitoring
- Importance of weather data for agriculture, transport, civil protection.
- Kosovo context: nine monitored cities (Prishtina, Prizren, Peja, Gjakova,
  Mitrovica, Gjilan, Ferizaj, Podujeva, Vushtrri).

## 3. Sensor and Parameter Analysis
- Monitored parameters: temperature, humidity, pressure, wind speed, rainfall,
  air quality, battery level.
- Representative hardware: Bosch BME280 weather station class sensors.
- Sampling frequency and accuracy considerations.

## 4. Data Transmission and IoT Gateway
- Sensor → gateway → cloud model.
- The simulator/gateway role: JSON messages with `producer_timestamp_ms`.
- Reliability: retries, batching, message keys.

## 5. Communication Protocols
- MQTT vs HTTP vs Kafka producer protocol — why a streaming log (Kafka) suits
  high-throughput telemetry.
- JSON payload format and schema.

## 6. IoT Architectures
- Three-layer (perception / network / application) and edge-fog-cloud models.
- Where each component of this project fits.

## 7. Proposed System Architecture
- Diagram: Simulator → Kafka → Spark → Cassandra → Dashboard + AI/Alarms.
- Component responsibilities and data flow.

## 8. Kafka, Spark Streaming and Cassandra Pipeline
- Kafka topics: `weather_data`, `weather_alerts`, `performance_metrics`.
- Spark Structured Streaming: parsing, validation, latency, severity, alarms.
- Cassandra as the time-series sink.

## 9. Cassandra Database Schema
- Keyspace `weather_ks` and the five tables.
- Primary-key design rationale (query-driven modeling).

## 10. Simulator Implementation
- Configurable simulator + FastAPI control plane.
- Normal vs stress mode, anomaly injection, metadata publishing.

## 11. Alarm System
- Threshold tables, severity levels (INFO/WARNING/CRITICAL), alarm records.
- Examples ("CRITICAL: Temperature in Prishtina is above 40°C").

## 12. AI Integration
- Anomaly detection (IsolationForest + statistical fallback).
- Bonus: LSTM temperature forecasting scaffold.
- Why anomaly detection suits IoT weather monitoring.

## 13. Performance Analysis and Stress Testing
- Metrics measured, methodology, example results (see `performance_results.md`).
- Optimization recommendations.

## 14. User Interface
- Streamlit dashboard pages (Overview, Live, Alarms, Simulator, Performance, AI).
- Screenshots placeholder.

## 15. Results and Discussion
- Throughput/latency findings, alarm accuracy, anomaly-detection behavior.
- Limitations of single-node setup.

## 16. Conclusion
- Summary of achievements vs objectives; future work (real sensors, LSTM in
  production, multi-node cluster).

## References
- [ ] Apache Kafka documentation
- [ ] Apache Spark Structured Streaming documentation
- [ ] Apache Cassandra documentation
- [ ] scikit-learn IsolationForest
- [ ] Phase-1 report (Weather Monitoring IoT)
- [ ] Additional academic references (placeholder)
