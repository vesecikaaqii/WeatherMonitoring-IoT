"""
seed_historical_readings.py  (DEMO ONLY)
========================================
Insert synthetic historical weather readings into Cassandra
(weather_ks.sensor_readings) for the LAST 7 DAYS, for every configured
Kosovo city / sensor.

Why
---
For a presentation you usually want the dashboard's "Historical trends" charts
and the date-partition selector to already contain data, without having to run
the real-time Kafka + Spark pipeline for hours. This script writes directly to
Cassandra and does NOT touch Kafka or Spark, so the live pipeline is unaffected.

What it generates
-----------------
- One reading every `INTERVAL_MINUTES` for the last `DAYS` days.
- Mostly NORMAL values, with a configurable share of WARNING and CRITICAL
  cases (so the Alarms/AI/Historical views show all severities).
- Realistic per-parameter values reusing the ranges defined in config.py.
- The same columns Spark writes: severity, is_anomaly, latency_ms.

Usage
-----
    CASSANDRA_HOST=127.0.0.1 python infrastructure/seed_historical_readings.py

Optional env:
    DAYS=7  INTERVAL_MINUTES=30  ANOMALY_RATE=0.25

Note: this only inserts into sensor_readings (historical time-series). It does
not populate latest_readings/alarms; run the simulator + Spark for those, or
they will fill in once the live pipeline runs.
"""

import os
import sys
import random
from datetime import datetime, timedelta, timezone

# Make sensor-service/config.py importable regardless of CWD
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sensor-service"))

from cassandra.cluster import Cluster  # noqa: E402
import config  # noqa: E402

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")

DAYS = int(os.getenv("DAYS", "7"))
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "30"))
ANOMALY_RATE = float(os.getenv("ANOMALY_RATE", "0.25"))  # 25% of points anomalous
WARNING_SHARE = config.WARNING_ANOMALY_SHARE             # 60% of anomalies = WARNING

PARAMS = ["temperature", "humidity", "pressure", "wind_speed",
          "rainfall", "air_quality", "battery_level"]

INSERT = """
INSERT INTO sensor_readings (
    sensor_id, date, timestamp, city,
    temperature, humidity, pressure, wind_speed, rainfall,
    air_quality, battery_level, severity, is_anomaly, latency_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _normal(param):
    lo, hi = config.NORMAL_RANGES[param]
    return round(random.uniform(lo, hi), 2)


def _banded(param, ranges):
    lo, hi = random.choice(ranges[param])
    return round(random.uniform(lo, hi), 2)


def _make_reading():
    """
    Return (values_dict, severity, is_anomaly).

    A point is NORMAL, or carries ONE anomalous parameter (warning/critical) so
    the historical charts show occasional realistic spikes rather than every
    parameter going extreme at once.
    """
    values = {p: _normal(p) for p in PARAMS}
    if random.random() >= ANOMALY_RATE:
        return values, "NORMAL", False

    level = "WARNING" if random.random() < WARNING_SHARE else "CRITICAL"
    spiked_param = random.choice(PARAMS)
    if level == "WARNING":
        values[spiked_param] = _banded(spiked_param, config.WARNING_RANGES)
        return values, "WARNING", False
    else:
        values[spiked_param] = _banded(spiked_param, config.CRITICAL_RANGES)
        return values, "CRITICAL", True


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    stmt = session.prepare(INSERT)

    now = datetime.now(timezone.utc).replace(microsecond=0)
    start = now - timedelta(days=DAYS)
    step = timedelta(minutes=INTERVAL_MINUTES)

    sensors = [(city, sid) for city, (sid, _, _) in config.CITY_SENSORS.items()]

    total = 0
    counts = {"NORMAL": 0, "WARNING": 0, "CRITICAL": 0}
    for city, sid in sensors:
        ts = start
        while ts <= now:
            iso = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            date_part = iso[:10]
            values, severity, is_anomaly = _make_reading()
            counts[severity] += 1
            session.execute(stmt, (
                sid, date_part, iso, city,
                values["temperature"], values["humidity"], values["pressure"],
                values["wind_speed"], values["rainfall"], values["air_quality"],
                values["battery_level"], severity, is_anomaly,
                random.randint(20, 200),  # synthetic latency_ms
            ))
            total += 1
            ts += step
        print(f"  seeded {city} ({sid})")

    cluster.shutdown()
    print("-" * 50)
    print(f"Inserted {total} historical readings across {len(sensors)} sensors "
          f"({DAYS} days @ every {INTERVAL_MINUTES} min).")
    print(f"Severity mix -> NORMAL={counts['NORMAL']} "
          f"WARNING={counts['WARNING']} CRITICAL={counts['CRITICAL']}")
    print("Open the dashboard 'Live Readings' page and pick a date partition.")


if __name__ == "__main__":
    main()
