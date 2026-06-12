"""
config.py
=========
Central configuration for the Weather Monitoring IoT simulator.

Holds:
- Kafka connection / topic settings (read from environment variables so the
  same code runs locally AND inside Docker without modification).
- The Kosovo cities + sensor-id mapping used everywhere in the project
  (simulator, Cassandra metadata and the Streamlit dashboard filters).
- Realistic value ranges per weather parameter.
- Static sensor metadata (manufacturer, model, etc.).

IMPORTANT: sensor_id <-> city mapping defined here is the single source of
truth. The dashboard filters by these exact city names, so there is never an
"S1/S2/S3 vs city" mismatch.
"""

import os

# --------------------------------------------------------------------------
# Kafka configuration (environment driven -> works locally and in Docker)
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_WEATHER_DATA = os.getenv("KAFKA_TOPIC", "weather_data")
TOPIC_WEATHER_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "weather_alerts")
TOPIC_PERFORMANCE = os.getenv("KAFKA_TOPIC_PERFORMANCE", "performance_metrics")

# --------------------------------------------------------------------------
# Sensors: one weather station per Kosovo city.
# sensor_id prefix is derived from the city (PR = Prishtina, PZ = Prizren ...)
# --------------------------------------------------------------------------
# city -> (sensor_id, latitude, longitude)
CITY_SENSORS = {
    "Prishtina":  ("PR-001", 42.6629, 21.1655),
    "Prizren":    ("PZ-001", 42.2139, 20.7397),
    "Peja":       ("PE-001", 42.6593, 20.2887),
    "Gjakova":    ("GJ-001", 42.3803, 20.4308),
    "Mitrovica":  ("MI-001", 42.8914, 20.8660),
    "Gjilan":     ("GL-001", 42.4637, 21.4694),
    "Ferizaj":    ("FE-001", 42.3702, 21.1483),
    "Podujeva":   ("PD-001", 42.9110, 21.1922),
    "Vushtrri":   ("VU-001", 42.8231, 20.9675),
}

# Convenient reverse lookups
SENSOR_TO_CITY = {sid: city for city, (sid, _, _) in CITY_SENSORS.items()}
ALL_CITIES = list(CITY_SENSORS.keys())

# --------------------------------------------------------------------------
# Static metadata per sensor (inserted into Cassandra sensor_metadata table)
# --------------------------------------------------------------------------
def build_sensor_metadata():
    """Return a list of metadata dicts, one per sensor."""
    metadata = []
    for city, (sensor_id, lat, lon) in CITY_SENSORS.items():
        metadata.append({
            "sensor_id": sensor_id,
            "city": city,
            "latitude": lat,
            "longitude": lon,
            "manufacturer": "Bosch",
            "model": "BME280",
            "serial_number": f"WX-{sensor_id}",
            "sensor_type": "weather_station",
            "installation_date": "2026-01-01",
            "status": "active",
            "sampling_frequency_seconds": 5,
        })
    return metadata

# --------------------------------------------------------------------------
# Realistic value ranges (used for "normal" generation).
# (min, max) for uniform-ish generation around realistic weather.
# --------------------------------------------------------------------------
NORMAL_RANGES = {
    "temperature":   (-5.0, 35.0),   # °C
    "humidity":      (30.0, 70.0),   # %
    "pressure":      (1000.0, 1030.0),  # hPa
    "wind_speed":    (0.0, 8.0),     # m/s
    "rainfall":      (0.0, 5.0),     # mm
    "air_quality":   (10.0, 80.0),   # AQI-like index
    "battery_level": (40.0, 100.0),  # %
}

# --------------------------------------------------------------------------
# Anomaly value ranges, split by severity so the simulator can generate BOTH
# WARNING and CRITICAL readings (alarm thresholds themselves are NOT changed --
# these ranges are simply chosen to land inside the warning / critical bands
# defined in stream-processing/spark_processor.py).
#
# Thresholds (for reference):
#   temperature : warn <-10 or >35 ; crit <-20 or >40
#   humidity    : warn >85         ; crit >95
#   pressure    : warn <1000       ; crit <980
#   wind_speed  : warn >10         ; crit >15
#   rainfall    : warn >10         ; crit >30
#   air_quality : warn >100        ; crit >150
#   battery     : warn <20         ; crit <10
# --------------------------------------------------------------------------
WARNING_RANGES = {
    "temperature":   [(-19.0, -11.0), (36.0, 39.5)],   # cold/hot warning band
    "humidity":      [(86.0, 94.0)],                   # warning high
    "pressure":      [(981.0, 999.0)],                 # warning low
    "wind_speed":    [(10.5, 14.5)],                   # warning
    "rainfall":      [(11.0, 29.0)],                   # warning
    "air_quality":   [(101.0, 149.0)],                 # warning
    "battery_level": [(11.0, 19.0)],                   # warning low
}

CRITICAL_RANGES = {
    "temperature":   [(-30.0, -21.0), (41.0, 48.0)],   # critical cold / hot
    "humidity":      [(96.0, 100.0)],                  # critical high
    "pressure":      [(960.0, 979.0)],                 # critical low
    "wind_speed":    [(16.0, 30.0)],                   # critical
    "rainfall":      [(31.0, 80.0)],                   # critical
    "air_quality":   [(151.0, 300.0)],                 # critical
    "battery_level": [(0.0, 9.0)],                     # critical low
}

# Backwards-compatible alias (older code referenced ANOMALY_RANGES = critical).
ANOMALY_RANGES = CRITICAL_RANGES

# When a message carries an anomaly, this fraction is WARNING-level and the
# remainder is CRITICAL-level (e.g. 0.6 -> 60% WARNING, 40% CRITICAL).
WARNING_ANOMALY_SHARE = 0.6

# --------------------------------------------------------------------------
# Default simulator runtime configuration
# --------------------------------------------------------------------------
DEFAULT_CONFIG = {
    "mode": "normal",                # normal | stress
    "num_sensors": len(CITY_SENSORS),
    "interval_seconds": 5.0,         # delay between full rounds in normal mode
    "messages_per_second": 0,        # 0 = use interval; >0 = stress (rate based)
    "anomaly_rate": 0.05,            # 5% of messages carry an anomaly
    "cities": ALL_CITIES,            # which cities to emit
}
