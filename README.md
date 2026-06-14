<table border="0">
 <tr>
    <td><img src="https://github.com/user-attachments/assets/9002855f-3f97-4b41-a180-85d1e24ad34a" alt="University Logo" width="110" align="left"/></td>
    <td>
      <p>Universiteti i Prishtinës</p>
      <p>Fakulteti i Inxhinierisë Elektrike dhe Kompjuterike</p>
      <p>Inxhinieri Kompjuterike dhe Softuerike - Programi Master</p>
      <p>Profesor: Prof. Besmir Sejdiu</p>
    </td>
 </tr>
</table>

# Weather Monitoring IoT — Kosovo

End-to-end **Internet of Things weather monitoring** system that ingests
simulated weather-station data from nine Kosovo cities, streams it through
**Apache Kafka**, processes it in real time with **Apache Spark Structured
Streaming**, stores it in **Apache Cassandra**, raises **threshold alarms**,
applies **AI anomaly detection**, and visualizes everything in a **Streamlit**
dashboard — with a built-in **stress-test** harness for performance analysis.

> Phase-2 practical implementation continuing the Phase-1 topic
> *"Monitorimi i Motit në Internet of Things / Weather Monitoring IoT"*.

**Authors:**Dafina Keqmezi · Vesë Cikaqi · Uranik Hodaj

---

## Architecture

```
 ┌──────────────────┐     ┌───────────────┐     ┌──────────────────────────┐     ┌─────────────────────┐
 │ Weather Sensor   │     │  Apache Kafka │     │ Apache Spark             │     │  Apache Cassandra   │
 │ Simulator /      │ ──▶ │  topics:      │ ──▶ │ Structured Streaming     │ ──▶ │  keyspace weather_ks│
 │ Gateway (FastAPI)│     │  • weather_   │     │ Query 1 (per record):    │     │  • sensor_metadata  │
 └──────────────────┘     │    data       │     │  • parse + validate      │     │  • sensor_readings  │
        ▲                 │  • weather_   │     │  • filter + latency       │     │  • latest_readings  │
        │ control         │    alerts ◀───┼─────┤  • severity + alarms      │     │  • alarms           │
        │ (start/stop/    │  • performance│     │  • AI anomaly detection  │     │  • performance_     │
        │  config/stress) │    _metrics   │     │ Query 2 (windowed):      │     │    metrics          │
 ┌──────┴───────────┐     └───────────────┘     │  • 1-min tumbling window │     │  • sensor_aggregates│
 │  Web Dashboard   │                           │    agg per city          │     └──────────┬──────────┘
 │  (Streamlit, 7p) │ ◀──────────────────────────────────── reads ────────────────────────┤
 ├──────────────────┤                           └──────────────────────────┘                │
 │  Mobile Dashboard│ ◀───────────────────────────────────── reads ───────────────────────┘
 │  (FastAPI PWA,8t)│        UI / AI / Alarms / Performance / Metadata / Aggregates
 └──────────────────┘
```
Spark runs **two streaming queries** on the same Kafka source: per-record
processing (validate → latency → severity → alarms → AI) and a **windowed
aggregation** (tumbling 1-min per city → `sensor_aggregates`). Detected alarms
are published back to the `weather_alerts` topic before being stored. Both the
web (Streamlit) and mobile (PWA) dashboards read from Cassandra.

## Technologies

| Layer | Technology |
|-------|-----------|
| Sensor / gateway | Python, FastAPI, kafka-python |
| Message queue | Apache Kafka (+ Zookeeper) |
| Stream processing | Apache Spark Structured Streaming (PySpark) |
| Storage | Apache Cassandra |
| AI | scikit-learn IsolationForest (+ statistical fallback); Keras **LSTM** temperature forecast trained offline (moving-average fallback if the model/TensorFlow is absent) |
| Dashboard (web) | Streamlit, pandas |
| Dashboard (mobile) | FastAPI + responsive HTML/JS PWA |
| Orchestration | Docker Compose |

## Folder structure

```
WeatherMonitoring-IoT/
├── docker-compose.yml
├── README.md
├── requirements.txt
├── .env.example
├── infrastructure/
│   ├── cassandra_setup.cql      
│   ├── kafka_topics.sh          
│   ├── seed_metadata.py         
│   ├── seed_historical_readings.py  
│   └── init_notes.md
├── sensor-service/
│   ├── simulator.py            
│   ├── simulator_api.py         
│   ├── config.py                
│   ├── Dockerfile
│   └── requirements.txt
├── stream-processing/
│   ├── spark_processor.py       
│   ├── ai_model.py              
│   ├── lstm_forecast.py         
│   ├── train_lstm.py            
│   ├── models/                  
│   ├── checkpoints/
│   └── requirements.txt
├── frontend-dashboard/
│   ├── app.py                  
│   ├── Dockerfile
│   └── requirements.txt
├── mobile-dashboard/           
│   ├── mobile_api.py           
│   ├── static/index.html        
│   ├── Dockerfile
│   └── requirements.txt
└── tests/
    ├── test_simulator_message.py
    └── sample_messages.json
```

---

## Quick start (Windows PowerShell + Docker)

> Run every command in **PowerShell** from the project root, with **Docker
> Desktop** running. The steps are sequential — finish each before the next.

**1. Configure the environment**
```powershell
Copy-Item .env.example .env
```

**2. Start the infrastructure (Zookeeper, Kafka, Cassandra)**
```powershell
docker compose up -d zookeeper kafka cassandra
```
Wait ~45 s for Cassandra to report healthy: `docker compose ps`.

**3. Initialize the Cassandra schema**
```powershell
docker exec -it cassandra cqlsh -f /scripts/cassandra_setup.cql
```

**4. Create the Kafka topics** (partitions: `weather_data`=6, `weather_alerts`=3, `performance_metrics`=3)
```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_data --partitions 6 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_alerts --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic performance_metrics --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**5. Build and start the application services**
```powershell
docker compose up -d --build simulator-api dashboard mobile-dashboard spark-master spark-worker
```

**6. Submit the Spark streaming job** (leave it running; open a new PowerShell tab for the next steps)
```powershell
docker exec -it spark-master bash -lc "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=6 --conf spark.cassandra.output.concurrent.writes=4 --conf spark.cassandra.output.batch.size.rows=200 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 /app/stream-processing/spark_processor.py"
```

**7. Seed sensor metadata** (and, optionally, 7 days of demo history for the charts)
```powershell
$env:CASSANDRA_HOST="127.0.0.1"; python infrastructure/seed_metadata.py
$env:CASSANDRA_HOST="127.0.0.1"; python infrastructure/seed_historical_readings.py   # optional demo data
```

**8. Train the LSTM forecast (Docker-based)** — optional; without it the forecast uses the moving-average fallback
```powershell
docker exec dashboard python /app/stream-processing/train_lstm.py
```
(The model is written inside the running `dashboard` container, which then serves it. To enable the LSTM on the phone app too, run the same against `mobile-dashboard`.)

**9. Start the simulator and open the dashboards**
```powershell
Invoke-RestMethod -Method Post http://localhost:8000/start
Start-Process "http://localhost:8501"   # web dashboard
Start-Process "http://localhost:8600"   # mobile dashboard
```

**Optional — quick data check**
```powershell
docker exec cassandra cqlsh -e "SELECT sensor_id, city, temperature, severity, is_anomaly FROM weather_ks.latest_readings;"
```

The web dashboard is at **http://localhost:8501**, the simulator API docs at
**http://localhost:8000/docs**, and the **mobile dashboard** at
**http://localhost:8600** (on a phone use the host IP, e.g.
`http://<server-ip>:8600`, then *Add to Home Screen* for an app-like PWA).

---

## Local run (without Docker)

Requires local Kafka + Cassandra (or point env vars at remote ones).

```bash

pip install -r requirements.txt

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export CASSANDRA_HOST=127.0.0.1
export SIMULATOR_API_URL=http://localhost:8000

cqlsh -f infrastructure/cassandra_setup.cql

kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_data --partitions 6 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic weather_alerts --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic performance_metrics --partitions 3 --replication-factor 1

python infrastructure/seed_metadata.py

python sensor-service/simulator_api.py

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  stream-processing/spark_processor.py

streamlit run frontend-dashboard/app.py
```

Then in the dashboard **Simulator Control** page click **Start**, or:

```bash
curl -X POST http://localhost:8000/start
```

> Prefer the standalone producer (no API)? Run `python sensor-service/simulator.py`.

---

## Running a stress test

```bash
curl -X POST http://localhost:8000/stress-test \
  -H "Content-Type: application/json" \
  -d '{"messages_per_second": 1000, "duration_seconds": 60, "anomaly_rate": 0.1}'
```

or use the dashboard **Simulator Control → Run stress test** button, and watch
results on the **Performance** page.

---

## Cassandra tables

| Table | Primary key | Purpose |
|-------|-------------|---------|
| `sensor_metadata` | `sensor_id` (+ city/status indexes) | Static station description |
| `sensor_readings` | `((sensor_id, date), timestamp)` | Historical time-series |
| `latest_readings` | `sensor_id` | Newest value per sensor |
| `alarms` | `((city), timestamp, alarm_id)` | Threshold-crossing events |
| `performance_metrics` | `((mode), timestamp, metric_id)` | Throughput + latency samples |
| `sensor_aggregates` | `((city), window_start)` | Windowed aggregates (1-min/city) from Spark |

## Kafka topics

| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `weather_data` | 6 | Raw sensor readings (simulator → Spark) |
| `weather_alerts` | 3 | Alarm events stream |
| `performance_metrics` | 3 | Throughput / latency samples |

## AI component

**What it does:** flags sensor readings whose multi-parameter pattern is
unusual, using a scikit-learn **IsolationForest** trained on synthetic "normal"
weather vectors. If scikit-learn/numpy are unavailable it falls back to a
pure-Python statistical/threshold check, so the pipeline never crashes.

**Why it suits IoT weather monitoring:** field sensors drift, lose battery, or
report impossible spikes. Anomaly detection separates genuine extreme-weather
events from sensor faults, complementing the static alarm thresholds. Each
reading is stored with an `is_anomaly` flag and shown on the **AI / Prediction**
dashboard page. The next temperature value is predicted by a **Keras LSTM**
(`lstm_forecast.py`). The model is **trained offline** by
`stream-processing/train_lstm.py`, which pulls each sensor's historical
temperature series from Cassandra, trains the LSTM, and saves it to
`stream-processing/models/`. The dashboard only **loads** the saved model and
predicts (no per-render training), so rendering stays fast. If TensorFlow or the
trained model file is missing, the forecaster transparently falls back to a
**moving average** — and the dashboard's **Model** field always reports the
method actually used (`lstm` or `moving_average`), so it never overclaims. Both
the web dashboard and the mobile **AI** tab (served by `/api/forecast`) load the
*same* trained model, so forecasts are identical across clients.

```powershell

docker exec dashboard python /app/stream-processing/train_lstm.py
```

## Alarm system

Severity levels: **NORMAL / WARNING / CRITICAL**. When a parameter crosses a
threshold, Spark writes an alarm row with `parameter`, `value`, `threshold`,
`severity`, `message`, and `status`. Examples:

- `CRITICAL: Temperature in Prishtina is above 40°C`
- `WARNING: Humidity in Peja is above 85%`
- `CRITICAL: Battery level in Gjilan is below 10%`

Thresholds (per spec): temperature ±35/±40, humidity 85/95, pressure 1000/980,
wind 10/15 m/s, rainfall 10/30 mm, air quality 100/150, battery 20/10.

Alarm rows are kept for history (`status='active'`; they are not auto-resolved),
so the Overview KPI shows **Recent alarms** — the count raised in the last
`ALARM_WINDOW_MIN` minutes (default 15) — rather than the unbounded all-time
total. It is computed with a server-side `COUNT` over the per-city clustering
range on `timestamp`, so it stays fast as the table grows.

## Sensor metadata

Each weather station's static description (manufacturer, model, serial number,
sensor type, installation date, status, sampling frequency) lives in the
`sensor_metadata` table, seeded from `sensor-service/config.py` via
`infrastructure/seed_metadata.py`. It is shown on the **Sensor Metadata** page
(web dashboard) and the **Meta** tab (mobile).

### Active vs reporting sensors

The Overview (and Sensor Metadata) views show **two complementary live counts**,
so a healthy pipeline is visible at a glance:

- **Active sensors (simulator)** — how many sensors the simulator is *configured
  to produce right now*. Read in **real time** from the simulator API
  (`/status` + `/config`): the selected cities capped by `num_sensors`, or `0`
  when stopped. It updates the instant you change the config — no waiting.
- **Reporting sensors (Cassandra)** — how many sensors *actually wrote* to
  `latest_readings` within the last `ACTIVE_WINDOW_MIN` minutes (default 10) —
  i.e. data that really flowed through **Spark → Cassandra**.

A gap (**Active > Reporting**) flags that the Spark → Cassandra path is lagging
or down, even though the simulator is producing. Both the web dashboard and the
mobile **Overview** tab show the pair; the static `status` field in
`sensor_metadata` is no longer used for these counts.

## Spark processing & windowed aggregation

Per micro-batch the Spark job **validates** (drops impossible/missing records),
**filters** (metadata envelopes), **analyses** (severity + AI anomaly + latency)
and writes the processed rows to Cassandra. A **second streaming query** runs a
**windowed aggregation** — tumbling **1-minute windows per city** (avg/min/max
temperature, avg humidity & pressure, max wind, avg air quality, reading count)
with a 2-minute watermark — and upserts the result into `sensor_aggregates`.
This demonstrates Spark aggregation + windowing on the live stream.

## Performance metrics & optimization

**What is measured.** Every message carries `producer_timestamp_ms`; Spark
computes end-to-end `latency_ms = now_ms - producer_timestamp_ms`. Per
micro-batch it records messages processed, msg/s, avg/max latency, and Cassandra
write time into `performance_metrics` (mode `stream`); a stress test writes a
summary row (mode `stress`). The dashboard **Performance** page charts these
over time ("Spark Streaming Metrics" vs "Stress Test Metrics").

**Measured results** (single laptop, Docker, 6 Kafka partitions, optimized
config). Refresh the table any time from your own run with:

```powershell
$env:CASSANDRA_HOST="127.0.0.1"; python infrastructure/perf_report.py
```

It reads `performance_metrics` from Cassandra and prints a ready-to-paste
markdown table, so the numbers are measured, not invented.

| Mode | Target msg/s | Achieved | Avg latency | Max latency |
|------|-------------|----------|-------------|-------------|
| stress | 500 | ~492 | ~3410 ms | ~6067 ms |

At 500 msg/s the throughput target is sustained, but end-to-end latency rises to
~3.4 s (peak ~6 s) — the driver-side micro-batch path (`collect()` + per-row
Python in `process_batch`) is the bottleneck under load. Run `perf_report.py`
after a `normal` run and a 1000 msg/s stress test to add those rows.

**Optimization recommendations:**
1. **Kafka partitions** — the project provisions **6** `weather_data` partitions
   so up to 6 Spark consumer tasks run in parallel (partitioned by `sensor_id`
   for per-sensor ordering); raise it further for more parallelism.
2. **Spark trigger interval** — tune `SPARK_TRIGGER_INTERVAL`: shorter (1–2s)
   lowers latency, longer (5–10s) increases batch size and throughput.
3. **Cassandra schema** — bounded partitions `((sensor_id, date), timestamp)`
   avoid wide-partition hotspots; `latest_readings` keyed by `sensor_id` gives O(1) upserts.
4. **Checkpointing** — keep `checkpointLocation` on fast local disk (not a
   network share) so micro-batches don't stall.
5. **Batching writes** — the Cassandra connector batches per micro-batch; at
   higher scale push per-row logic into Spark UDFs/`mapPartitions` instead of `collect()`.
6. **Producer tuning** — `linger_ms` + `acks=1` trade a little durability for
   throughput; raise `linger_ms` for larger produce batches.

### Before vs after optimization

The optimization knobs are applied in code (and are env-tunable). The table
shows what changed:

| Setting | Before (default) | After (optimized) | Where |
|---------|------------------|-------------------|-------|
| Producer compression | none | `gzip` | `simulator.py` / `PRODUCER_COMPRESSION` |
| Producer `linger_ms` | 5 | 10 | `PRODUCER_LINGER_MS` |
| Producer `batch_size` | 16 KB (default) | 32 KB | `PRODUCER_BATCH_SIZE` |
| Cassandra concurrent writes | 1 (default) | 4 | `CASS_CONCURRENT_WRITES` |
| Cassandra batch size (rows) | default | 200, grouped by partition | `CASS_BATCH_SIZE_ROWS` |
| Back-pressure (`maxOffsetsPerTrigger`) | unbounded | optional cap | `MAX_OFFSETS_PER_TRIGGER` |

**Measured impact.** The optimized configuration above was measured at a stress
test of 500 msg/s: **~492 msg/s achieved, ~3410 ms avg latency, ~6067 ms peak**.
A before/after delta is intentionally **not** fabricated here — to quantify it,
run the stress test twice and compare the two `perf_report.py` outputs: once with
the tuning **off** (baseline) and once **on**.

To capture the "before" baseline, disable the optimizations in `.env`, recreate
the services, and re-run the stress test:

```powershell
# in .env: PRODUCER_COMPRESSION=none and CASS_CONCURRENT_WRITES=1
docker compose up -d --force-recreate simulator-api spark-master spark-worker
```

> The **stored data is identical** before/after — compression and write tuning
> change *speed*, not the readings/alarms/values.

## Mobile dashboard (PWA)

In addition to the desktop web dashboard, a **mobile-first** app is served by
`mobile-dashboard/` (FastAPI backend + responsive HTML/JS frontend). It reads
the same Cassandra data and proxies simulator control server-side, so a phone
only needs one URL.

- Open **http://localhost:8600** (or `http://<server-ip>:8600` from a phone).
- **Full feature parity with the web dashboard.** Tabs:
  - **Overview** — KPIs: active (simulator) + reporting (Cassandra) sensors, alarms, latency, msg/s
  - **Sensors** — latest reading per city with severity colors
  - **Meta** — sensor metadata (manufacturer, model, serial, install date…)
  - **Trends** — historical SVG line charts + daily summary + alarms per city
  - **Alarms** — filter WARNING / CRITICAL
  - **AI** — anomaly-flagged sensors + LSTM temperature forecast (moving-average fallback)
  - **Perf** — Spark Streaming & Stress Test metrics (msg/s + latency charts)
  - **Control** — start/stop, full config (mode, sensors, interval, msg/s,
    anomaly rate, cities) **and** stress test
- Charts are drawn with inline SVG (no external libraries → works offline).
- Auto-refreshes every 5s; supports *Add to Home Screen* (web app manifest).

### Mobile — step by step

The mobile dashboard reads from Cassandra and controls the simulator, so the
infrastructure + data must be up first.

**Option A — Docker (recommended).** Follow the main **Quick start (Windows
PowerShell + Docker)** above — it already builds and starts `mobile-dashboard`
alongside the web dashboard, creates the Kafka topics manually, seeds the data,
and (optionally) trains the LSTM. Once the simulator is started (step 9), the
mobile app is live at **http://localhost:8600**.

**Open it on a phone** — find your machine's LAN IP:
```powershell
ipconfig
```
- Computer: **http://localhost:8600**
- Phone (same WiFi): **http://<your-ip>:8600** (e.g. `http://192.168.1.20:8600`)
- Phone browser menu → **Add to Home Screen** → opens like an app (PWA)

**Option B — local (no Docker)** — requires Kafka + Cassandra already running:
```bash
pip install -r mobile-dashboard/requirements.txt
export CASSANDRA_HOST=127.0.0.1
export SIMULATOR_API_URL=http://localhost:8000
cd mobile-dashboard
uvicorn mobile_api:app --host 0.0.0.0 --port 8600   
```

**Verify**
```bash
curl http://localhost:8600/health        
curl http://localhost:8600/api/overview  
```

**Troubleshooting**
- *Tabs empty* → start the simulator (`curl -X POST http://localhost:8000/start`); for Trends charts run `seed_historical_readings.py`.
- *Phone can't connect* → same WiFi, allow port 8600 in the Windows firewall, and use `--host 0.0.0.0`.
- *Mobile API unreachable* → `docker compose ps` should show `mobile-dashboard` = Up.

---

## Tests

```bash
python tests/test_simulator_message.py      
python stream-processing/ai_model.py       
```
