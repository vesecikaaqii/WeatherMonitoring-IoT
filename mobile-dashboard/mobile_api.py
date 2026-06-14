"""
mobile_api.py
=============
Mobile-friendly backend + web app for the Weather Monitoring IoT project.

Why a separate mobile app?
--------------------------
The professor's requirement says "web is enough, mobile is better". The
Streamlit dashboard covers the full desktop/web experience; this service adds a
lightweight **mobile-first** Progressive Web App (PWA) that opens in any phone
browser and can be "Added to Home Screen" like a native app.

Architecture (consistent with the rest of the stack):
    phone browser
        -> mobile_api (FastAPI)         this file
            -> Cassandra (read data)    overview / latest / alarms
            -> simulator-api (control)  start / stop / status   (server-side)

Keeping the simulator + Cassandra calls server-side means the phone only talks
to ONE public URL and we avoid the "internal docker hostname" problem.

Endpoints
---------
GET  /                 -> the mobile web app (index.html)
GET  /api/overview     -> KPIs (sensors, active alarms, avg latency, msg/s)
GET  /api/latest       -> latest reading per sensor
GET  /api/alarms       -> recent alarms (optional ?severity=CRITICAL)
GET  /api/sim/status   -> simulator status (proxied)
POST /api/sim/start    -> start simulator (proxied)
POST /api/sim/stop     -> stop simulator (proxied)

Run:
    uvicorn mobile_api:app --host 0.0.0.0 --port 8600
or:
    python mobile_api.py
"""

import os
from datetime import datetime, timezone, timedelta

import requests
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.staticfiles import StaticFiles

# --------------------------------------------------------------------------
# Configuration (environment driven -> works locally and in Docker)
# --------------------------------------------------------------------------
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")
SIMULATOR_API_URL = os.getenv("SIMULATOR_API_URL", "http://localhost:8000")
PORT = int(os.getenv("MOBILE_API_PORT", "8600"))

# Fallback only: if the simulator API is unreachable, a sensor counts as
# reporting when its most recent reading arrived within this many minutes.
ACTIVE_WINDOW_MIN = int(os.getenv("ACTIVE_WINDOW_MIN", "10"))

ALL_CITIES = ["Prishtina", "Prizren", "Peja", "Gjakova", "Mitrovica",
              "Gjilan", "Ferizaj", "Podujeva", "Vushtrri"]

# city -> sensor_id (same mapping as the simulator/config + web dashboard)
SENSOR_MAP = {
    "Prishtina": "PR-001", "Prizren": "PZ-001", "Peja": "PE-001",
    "Gjakova": "GJ-001", "Mitrovica": "MI-001", "Gjilan": "GL-001",
    "Ferizaj": "FE-001", "Podujeva": "PD-001", "Vushtrri": "VU-001",
}

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(HERE, "static")

app = FastAPI(title="Weather Monitoring IoT - Mobile", version="1.0.0")

# Serve static assets (manifest, icons) under /static
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# --------------------------------------------------------------------------
# Cassandra (lazy, resilient connection)
# --------------------------------------------------------------------------
_session = None


def get_session():
    """Connect to Cassandra on demand; returns None if unavailable."""
    global _session
    if _session is None:
        try:
            from cassandra.cluster import Cluster
            cluster = Cluster([CASSANDRA_HOST])
            _session = cluster.connect(KEYSPACE)
        except Exception as exc:
            print(f"[mobile_api] Cassandra unavailable: {exc}")
            _session = None
    return _session


def rows(cql):
    """Run CQL and return a list of dicts (empty on any error)."""
    session = get_session()
    if session is None:
        return []
    try:
        return [dict(r._asdict()) for r in session.execute(cql)]
    except Exception as exc:
        print(f"[mobile_api] query failed: {exc}")
        return []


# --------------------------------------------------------------------------
# Active-sensor counts: two complementary views (mirrors the web dashboard)
#  - configured: what the simulator is set to produce now (real-time)
#  - reporting : what actually wrote to Cassandra recently (pipeline output)
# --------------------------------------------------------------------------
def _parse_iso(ts):
    """Parse an ISO timestamp (e.g. '2026-06-14T12:34:56Z') -> aware datetime."""
    if not isinstance(ts, str):
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def reporting_sensor_ids():
    """Sensor_ids that wrote to latest_readings within ACTIVE_WINDOW_MIN."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=ACTIVE_WINDOW_MIN)
    out = set()
    for r in rows("SELECT sensor_id, timestamp FROM latest_readings"):
        dt = _parse_iso(r.get("timestamp"))
        if dt is not None and dt >= cutoff and r.get("sensor_id"):
            out.add(r["sensor_id"])
    return out


def configured_sensor_ids():
    """Sensor_ids the simulator is configured to produce now (real-time).

    Selected cities capped by num_sensors, only while running. Falls back to the
    Cassandra reporting set if the simulator API is unreachable.
    """
    try:
        status = requests.get(f"{SIMULATOR_API_URL}/status", timeout=3).json()
        cfg = requests.get(f"{SIMULATOR_API_URL}/config", timeout=3).json()
        if (isinstance(status, dict) and "error" not in status
                and isinstance(cfg, dict) and "error" not in cfg):
            if not status.get("running"):
                return set()
            cities = [c for c in cfg.get("cities", ALL_CITIES) if c in SENSOR_MAP]
            num = int(cfg.get("num_sensors", len(cities)))
            return set(SENSOR_MAP[c] for c in cities[:max(num, 0)])
    except Exception:
        pass
    return reporting_sensor_ids()


# --------------------------------------------------------------------------
# Temperature forecaster (same model as the web dashboard)
# Loaded ONCE and reused; predicts server-side. Falls back to moving average
# if TensorFlow / the trained model are absent (handled inside the forecaster).
# --------------------------------------------------------------------------
_forecaster = None


def get_forecaster():
    global _forecaster
    if _forecaster is None:
        import sys
        # container: /app/stream-processing ; local: ../stream-processing
        for p in (os.path.join(HERE, "..", "stream-processing"),
                  os.path.join(HERE, "stream-processing"),
                  "/app/stream-processing"):
            if os.path.isdir(p):
                sys.path.insert(0, p)
                break
        try:
            from lstm_forecast import TemperatureForecaster
            _forecaster = TemperatureForecaster.load_default()
        except Exception as exc:
            print(f"[mobile_api] forecaster unavailable: {exc}")
            _forecaster = None
    return _forecaster


# --------------------------------------------------------------------------
# Pages
# --------------------------------------------------------------------------
@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/health")
def health():
    return {"status": "ok", "service": "mobile-api"}


# --------------------------------------------------------------------------
# Data API (read from Cassandra)
# --------------------------------------------------------------------------
@app.get("/api/overview")
def overview():
    meta = rows("SELECT sensor_id, status FROM sensor_metadata")
    latest = rows("SELECT sensor_id, latency_ms FROM latest_readings")

    # Count active alarms across all city partitions
    active_alarms = 0
    for city in ALL_CITIES:
        for a in rows(f"SELECT status FROM alarms WHERE city='{city}' LIMIT 100"):
            if a.get("status") == "active":
                active_alarms += 1

    latencies = [r["latency_ms"] for r in latest if r.get("latency_ms") is not None]
    avg_latency = round(sum(latencies) / len(latencies), 1) if latencies else 0

    # Live producer rate from the simulator API (best-effort)
    mps = 0
    try:
        m = requests.get(f"{SIMULATOR_API_URL}/metrics", timeout=3).json()
        mps = m.get("messages_per_second", 0)
    except Exception:
        pass

    total_sensors = len(meta) if meta else len(ALL_CITIES)
    # Active = configured by the simulator (real-time); Reporting = actually
    # writing through Spark -> Cassandra. A gap flags a lagging/down pipeline.
    active_sensors_simulator = len(configured_sensor_ids())
    reporting_sensors_cassandra = len(reporting_sensor_ids())

    return {
        "total_sensors": total_sensors,
        "active_sensors_simulator": active_sensors_simulator,
        "reporting_sensors_cassandra": reporting_sensors_cassandra,
        "latest_readings": len(latest),
        "active_alarms": active_alarms,
        "avg_latency_ms": avg_latency,
        "messages_per_second": mps,
    }


@app.get("/api/latest")
def latest():
    data = rows("SELECT * FROM latest_readings")
    # Sort by city for a stable phone list
    data.sort(key=lambda r: r.get("city") or "")
    return JSONResponse(data)


@app.get("/api/metadata")
def metadata():
    """Static sensor description from the sensor_metadata table."""
    data = rows(
        "SELECT sensor_id, city, manufacturer, model, serial_number, "
        "sensor_type, installation_date, status, sampling_frequency_seconds "
        "FROM sensor_metadata"
    )
    data.sort(key=lambda r: r.get("sensor_id") or "")
    return JSONResponse(data)


@app.get("/api/dates")
def dates(sensor_id: str):
    """Return the available date partitions for a sensor (newest first)."""
    parts = rows("SELECT DISTINCT sensor_id, date FROM sensor_readings")
    ds = sorted(
        {p["date"] for p in parts if p.get("sensor_id") == sensor_id and p.get("date")},
        reverse=True,
    )
    return JSONResponse(ds)


@app.get("/api/history")
def history(sensor_id: str, date: str):
    """Historical time-series for one sensor + date partition (oldest first)."""
    data = rows(
        "SELECT timestamp, temperature, humidity, pressure, wind_speed, "
        "rainfall, air_quality, battery_level, severity, is_anomaly "
        f"FROM sensor_readings WHERE sensor_id='{sensor_id}' AND date='{date}' LIMIT 500"
    )
    data.sort(key=lambda r: r.get("timestamp") or "")
    return JSONResponse(data)


@app.get("/api/alarm_counts")
def alarm_counts():
    """Number of alarms per city, split by severity (for the Trends summary)."""
    out = []
    for city in ALL_CITIES:
        rs = rows(f"SELECT severity FROM alarms WHERE city='{city}' LIMIT 500")
        warning = sum(1 for r in rs if r.get("severity") == "WARNING")
        critical = sum(1 for r in rs if r.get("severity") == "CRITICAL")
        out.append({
            "city": city,
            "warning": warning,
            "critical": critical,
            "total": len(rs),
        })
    out.sort(key=lambda r: r["total"], reverse=True)
    return JSONResponse(out)


@app.get("/api/alarms")
def alarms(severity: str = None):
    out = []
    for city in ALL_CITIES:
        out.extend(rows(f"SELECT * FROM alarms WHERE city='{city}' LIMIT 300"))

    if severity:
        out = [a for a in out if a.get("severity") == severity.upper()]

    # newest first
    out.sort(key=lambda r: r.get("timestamp") or "", reverse=True)

    return JSONResponse(jsonable_encoder(out[:200]))


@app.get("/api/performance")
def performance():
    """
    Performance samples per mode (matches the web dashboard Performance page):
    'stream' = Spark Streaming Metrics, 'stress' = Stress Test Metrics.
    """
    result = {}
    for mode in ["stream", "stress", "normal"]:
        rs = rows(
            "SELECT timestamp, messages_processed, messages_per_second, "
            f"avg_latency_ms FROM performance_metrics WHERE mode='{mode}' LIMIT 200"
        )
        rs.sort(key=lambda r: r.get("timestamp") or "")
        result[mode] = {
            "mps": [r.get("messages_per_second") for r in rs],
            "latency": [r.get("avg_latency_ms") for r in rs],
            "total_processed": sum(int(r.get("messages_processed") or 0) for r in rs),
            "count": len(rs),
        }
    return JSONResponse(result)


@app.get("/api/ai")
def ai():
    """Anomaly-detection summary (latest reading per sensor with is_anomaly)."""
    latest = rows(
        "SELECT sensor_id, city, temperature, is_anomaly, severity FROM latest_readings"
    )
    flagged = [r for r in latest if r.get("is_anomaly")]
    # anomalous sensors first, then by city
    latest.sort(key=lambda r: (not bool(r.get("is_anomaly")), r.get("city") or ""))
    return JSONResponse({"flagged_count": len(flagged), "sensors": latest})


@app.get("/api/forecast")
def forecast(city: str = "Prishtina"):
    """Next-temperature forecast for a city, using the SAME LSTM model as the
    web dashboard (moving-average fallback if the model is absent)."""
    sid = SENSOR_MAP.get(city)
    if not sid:
        return JSONResponse({"error": f"unknown city {city}"})

    # Use the most recent date partition that has data for this sensor.
    parts = rows("SELECT DISTINCT sensor_id, date FROM sensor_readings")
    dates = sorted(
        {p["date"] for p in parts if p.get("sensor_id") == sid and p.get("date")},
        reverse=True,
    )
    if not dates:
        return JSONResponse({"city": city, "sensor_id": sid,
                             "series": [], "prediction": None, "mode": None})
    date = dates[0]

    hist = rows(
        "SELECT timestamp, temperature FROM sensor_readings "
        f"WHERE sensor_id='{sid}' AND date='{date}' LIMIT 200"
    )
    hist.sort(key=lambda r: r.get("timestamp") or "")
    series = [float(r["temperature"]) for r in hist if r.get("temperature") is not None]

    if len(series) < 3:
        return JSONResponse({"city": city, "sensor_id": sid, "date": date,
                             "series": series, "prediction": None, "mode": None})

    f = get_forecaster()
    if f is not None:
        pred = f.predict_next(series)
        mode = f.mode
    else:
        # Pure-Python fallback if the forecaster module could not be imported.
        window = series[-10:]
        pred = round(sum(window) / len(window), 2)
        mode = "moving_average"

    return JSONResponse({
        "city": city, "sensor_id": sid, "date": date,
        "series": series, "n": len(series),
        "prediction": pred, "mode": mode,
    })


# --------------------------------------------------------------------------
# Simulator control (proxied server-side -> phone only needs this one host)
# --------------------------------------------------------------------------
@app.get("/api/sim/status")
def sim_status():
    try:
        return requests.get(f"{SIMULATOR_API_URL}/status", timeout=3).json()
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/api/sim/start")
def sim_start():
    try:
        return requests.post(f"{SIMULATOR_API_URL}/start", timeout=5).json()
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/api/sim/stop")
def sim_stop():
    try:
        return requests.post(f"{SIMULATOR_API_URL}/stop", timeout=5).json()
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/api/sim/config")
def sim_get_config():
    try:
        return requests.get(f"{SIMULATOR_API_URL}/config", timeout=3).json()
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/api/sim/config")
async def sim_set_config(req: Request):
    body = await req.json()
    try:
        return requests.post(f"{SIMULATOR_API_URL}/config", json=body, timeout=5).json()
    except Exception as exc:
        return {"error": str(exc)}


@app.post("/api/sim/stress-test")
async def sim_stress_test(req: Request):
    body = await req.json()
    try:
        return requests.post(f"{SIMULATOR_API_URL}/stress-test", json=body, timeout=5).json()
    except Exception as exc:
        return {"error": str(exc)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
