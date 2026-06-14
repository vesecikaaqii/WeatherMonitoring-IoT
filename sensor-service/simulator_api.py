"""
simulator_api.py
================
FastAPI control plane for the weather simulator.

Exposes REST endpoints so the Streamlit dashboard (or curl) can start/stop the
simulator, change its configuration, run a timed stress test, and read live
metrics.

Endpoints
---------
GET  /health        -> liveness probe
GET  /config        -> current simulator configuration
POST /config        -> update configuration
POST /start         -> start generating data
POST /stop          -> stop generating data
GET  /status        -> running flag + live metrics
POST /stress-test   -> run a timed high-throughput burst
GET  /metrics       -> live metrics (alias used by dashboard charts)

Run:
    uvicorn simulator_api:app --host 0.0.0.0 --port 8000
or:
    python simulator_api.py
"""

import os
import time
import uuid
import threading
from typing import List, Optional

from fastapi import FastAPI
from pydantic import BaseModel, Field

import config
from simulator import WeatherSimulator, _now_ms

# Cassandra is optional here: the API still works if the driver/DB is absent;
# the stress test simply skips persisting its summary row in that case.
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")


def _stream_latency_since(start_iso):
    """Read Spark 'stream' metrics written since start_iso and return the REAL
    end-to-end latency under load as (avg_latency_ms, max_latency_ms).

    `performance_metrics` is partitioned by `mode` and clustered by `timestamp`,
    and the ISO timestamp sorts chronologically, so this is an efficient
    clustering-range read (no full scan). Best-effort: (0.0, 0.0) on error.
    """
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        rs = list(session.execute(
            "SELECT avg_latency_ms, max_latency_ms FROM performance_metrics "
            "WHERE mode='stream' AND timestamp >= %s",
            (start_iso,),
        ))
        cluster.shutdown()
        avgs = [r.avg_latency_ms for r in rs if r.avg_latency_ms is not None]
        maxs = [r.max_latency_ms for r in rs if r.max_latency_ms is not None]
        avg = round(sum(avgs) / len(avgs), 2) if avgs else 0.0
        mx = round(max(maxs), 2) if maxs else 0.0
        return avg, mx
    except Exception as exc:  # pragma: no cover - DB optional
        print(f"[stress-test] latency lookup failed: {exc}")
        return 0.0, 0.0


def _write_stress_metric(produced, elapsed, target_mps,
                         avg_latency_ms=0.0, max_latency_ms=0.0):
    """
    Best-effort: persist one stress-test summary row to
    performance_metrics (mode='stress') so the dashboard's stress section and
    requirement #11 are satisfied. Never raises (Cassandra may be unavailable).
    """
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        achieved = round(produced / elapsed, 2) if elapsed > 0 else 0.0
        session.execute(
            """
            INSERT INTO performance_metrics (
                mode, timestamp, metric_id, messages_processed,
                messages_per_second, avg_latency_ms, max_latency_ms,
                cassandra_write_time_ms, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "stress",
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                uuid.uuid4(),
                int(produced),
                achieved,
                float(avg_latency_ms),
                float(max_latency_ms),
                0.0,
                f"target={target_mps}msg/s duration={elapsed:.1f}s",
            ),
        )
        cluster.shutdown()
        print(f"[stress-test] summary persisted: produced={produced} "
              f"achieved={achieved}msg/s avg_latency={avg_latency_ms}ms")
    except Exception as exc:  # pragma: no cover - DB optional
        print(f"[stress-test] could not persist summary (Cassandra unavailable): {exc}")

app = FastAPI(
    title="Weather Monitoring IoT - Simulator API",
    version="1.0.0",
    description="Control plane for the Kosovo weather sensor simulator.",
)

# Single shared simulator instance
sim = WeatherSimulator()

# Publish metadata once at startup (best-effort; Kafka may not be up yet)
@app.on_event("startup")
def _startup():
    try:
        sim.publish_metadata()
    except Exception as exc:  # pragma: no cover - Kafka not ready is non-fatal
        print(f"[startup] metadata publish skipped: {exc}")


# -------------------------------------------------------------------------- #
# Request models
# -------------------------------------------------------------------------- #
class ConfigUpdate(BaseModel):
    mode: Optional[str] = Field(None, description="normal | stress")
    num_sensors: Optional[int] = None
    interval_seconds: Optional[float] = None
    messages_per_second: Optional[int] = None
    anomaly_rate: Optional[float] = None
    cities: Optional[List[str]] = None


class StressTestRequest(BaseModel):
    messages_per_second: int = Field(500, description="Target throughput")
    duration_seconds: int = Field(30, description="How long to run the burst")
    anomaly_rate: float = Field(0.1, description="Fraction of anomalous msgs")


# -------------------------------------------------------------------------- #
# Endpoints
# -------------------------------------------------------------------------- #
@app.get("/health")
def health():
    return {"status": "ok", "service": "simulator-api", "time_ms": _now_ms()}


@app.get("/config")
def get_config():
    return sim.get_config()


@app.post("/config")
def set_config(update: ConfigUpdate):
    new = {k: v for k, v in update.dict().items() if v is not None}
    return {"updated": True, "config": sim.update_config(new)}


@app.post("/start")
def start():
    started = sim.start()
    return {"started": started, "status": sim.get_status()}


@app.post("/stop")
def stop():
    sim.stop()
    return {"stopped": True, "status": sim.get_status()}


@app.get("/status")
def status():
    return sim.get_status()


@app.get("/metrics")
def metrics():
    """Live metrics for dashboard performance charts."""
    s = sim.get_status()
    return {
        "running": s["running"],
        "messages_sent": s["messages_sent"],
        "messages_per_second": s["messages_per_second"],
        "mode": s["mode"],
        "started_at": s["started_at"],
        "last_message_at": s["last_message_at"],
    }


@app.post("/stress-test")
def stress_test(req: StressTestRequest):
    """
    Run a timed stress test in a background thread:
    switch to stress mode at the requested throughput, run for duration_seconds,
    then revert to normal mode. Returns immediately with the plan.
    """
    def _run():
        prev_cfg = sim.get_config()
        sim.update_config({
            "mode": "stress",
            "messages_per_second": req.messages_per_second,
            "anomaly_rate": req.anomaly_rate,
            "num_sensors": len(config.ALL_CITIES),
            "cities": config.ALL_CITIES,
        })
        sim.start()
        start_sent = sim.get_status()["messages_sent"]
        t0 = time.time()
        start_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t0))
        time.sleep(req.duration_seconds)
        end_sent = sim.get_status()["messages_sent"]
        elapsed = time.time() - t0
        # Revert to previous mode (keep running in normal mode)
        sim.update_config({
            "mode": prev_cfg["mode"],
            "messages_per_second": prev_cfg["messages_per_second"],
            "anomaly_rate": prev_cfg["anomaly_rate"],
        })
        produced = end_sent - start_sent
        print(
            f"[stress-test] produced={produced} in {elapsed:.1f}s "
            f"(~{produced / elapsed:.1f} msg/s)"
        )
        # Let Spark flush the last micro-batches, then read the REAL end-to-end
        # latency it measured during the burst (the producer can't see it).
        time.sleep(8)
        avg_lat, max_lat = _stream_latency_since(start_iso)
        # Persist a summary row so it shows on the dashboard Performance page.
        _write_stress_metric(produced, elapsed, req.messages_per_second,
                             avg_lat, max_lat)

    threading.Thread(target=_run, daemon=True).start()
    return {
        "started": True,
        "target_mps": req.messages_per_second,
        "duration_seconds": req.duration_seconds,
        "note": "Stress test running in background. Poll /metrics for live rate.",
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("SIMULATOR_API_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
