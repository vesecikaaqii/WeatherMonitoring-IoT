"""
test_simulator_message.py
=========================
Unit tests for the simulator message schema and the alarm/severity logic.
These run WITHOUT Kafka, Spark or Cassandra (pure functions only).

Run:
    pytest tests/ -v
or:
    python tests/test_simulator_message.py
"""

import os
import sys
import json

# Make project packages importable
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "sensor-service"))
sys.path.insert(0, os.path.join(ROOT, "stream-processing"))

import config                              # noqa: E402
from simulator import WeatherSimulator     # noqa: E402

REQUIRED_FIELDS = [
    "event_id", "sensor_id", "city", "location", "timestamp",
    "temperature", "humidity", "pressure", "wind_speed", "rainfall",
    "air_quality", "battery_level", "producer_timestamp_ms",
]


def _sample_message():
    sim = WeatherSimulator()
    sim.cfg["anomaly_rate"] = 0.0  # deterministic normal values
    city = "Prishtina"
    sid, lat, lon = config.CITY_SENSORS[city]
    return sim.generate_message(city, sid, lat, lon)


def test_message_has_all_required_fields():
    msg = _sample_message()
    for field in REQUIRED_FIELDS:
        assert field in msg, f"missing field: {field}"


def test_message_is_json_serializable():
    msg = _sample_message()
    encoded = json.dumps(msg)
    decoded = json.loads(encoded)
    assert decoded["sensor_id"] == msg["sensor_id"]


def test_location_structure():
    msg = _sample_message()
    assert "latitude" in msg["location"]
    assert "longitude" in msg["location"]


def test_sensor_id_matches_city():
    msg = _sample_message()
    assert config.SENSOR_TO_CITY[msg["sensor_id"]] == msg["city"]


def test_producer_timestamp_is_ms():
    msg = _sample_message()
    # ms timestamp should be a 13-digit-ish integer (> year 2001 in ms)
    assert isinstance(msg["producer_timestamp_ms"], int)
    assert msg["producer_timestamp_ms"] > 1_000_000_000_000


def _is_out_of_normal(msg):
    """True if any parameter crosses at least a WARNING threshold."""
    return (
        msg["temperature"] > 35 or msg["temperature"] < -10
        or msg["humidity"] > 85 or msg["pressure"] < 1000
        or msg["wind_speed"] > 10 or msg["rainfall"] > 10
        or msg["air_quality"] > 100 or msg["battery_level"] < 20
    )


def test_anomaly_rate_produces_extremes():
    sim = WeatherSimulator()
    sim.cfg["anomaly_rate"] = 1.0  # force anomalies
    city = "Peja"
    sid, lat, lon = config.CITY_SENSORS[city]
    # With anomaly_rate=1, every message is anomalous (WARNING or CRITICAL),
    # so at least one parameter must be outside its normal band.
    msg = sim.generate_message(city, sid, lat, lon)
    assert _is_out_of_normal(msg), "anomaly_rate=1.0 should produce out-of-band values"


def test_anomaly_split_produces_both_warning_and_critical():
    """Over many forced anomalies we should see BOTH severities appear."""
    sim = WeatherSimulator()
    sim.cfg["anomaly_rate"] = 1.0
    city = "Prishtina"
    sid, lat, lon = config.CITY_SENSORS[city]
    saw_warning = saw_critical = False
    for _ in range(200):
        m = sim.generate_message(city, sid, lat, lon)
        # Critical if any param crosses a CRITICAL threshold; else warning band.
        critical = (
            m["temperature"] > 40 or m["temperature"] < -20
            or m["humidity"] > 95 or m["pressure"] < 980
            or m["wind_speed"] > 15 or m["rainfall"] > 30
            or m["air_quality"] > 150 or m["battery_level"] < 10
        )
        if critical:
            saw_critical = True
        elif _is_out_of_normal(m):
            saw_warning = True
        if saw_warning and saw_critical:
            break
    assert saw_warning, "expected some WARNING-level anomalies"
    assert saw_critical, "expected some CRITICAL-level anomalies"


def test_severity_classification():
    from spark_processor import classify_parameter, build_alarms
    sev, thr, _ = classify_parameter("temperature", 45)
    assert sev == "CRITICAL"
    sev, thr, _ = classify_parameter("humidity", 88)
    assert sev == "WARNING"
    sev, thr, _ = classify_parameter("temperature", 22)
    assert sev == "NORMAL"

    reading = {
        "sensor_id": "PR-001", "city": "Prishtina",
        "timestamp": "2026-06-12T12:00:00Z",
        "temperature": 45, "humidity": 50, "pressure": 1015,
        "wind_speed": 3, "rainfall": 0, "air_quality": 40, "battery_level": 90,
    }
    alarms = build_alarms(reading)
    assert any(a["parameter"] == "temperature" and a["severity"] == "CRITICAL"
               for a in alarms)


if __name__ == "__main__":
    # Minimal runner so the file works without pytest installed
    funcs = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    for fn in funcs:
        try:
            fn()
            print(f"PASS  {fn.__name__}")
            passed += 1
        except Exception as exc:
            print(f"FAIL  {fn.__name__}: {exc}")
    print(f"\n{passed}/{len(funcs)} tests passed")
