"""
seed_metadata.py
================
Insert sensor metadata rows into Cassandra (weather_ks.sensor_metadata).

The metadata is the single source of truth defined in
sensor-service/config.py (Kosovo cities + sensor ids). Running this script
once after cassandra_setup.cql ensures the dashboard's "total sensors" and
city filters are populated even before any reading arrives.

Usage:
    CASSANDRA_HOST=127.0.0.1 python infrastructure/seed_metadata.py
"""

import os
import sys

# Make sensor-service/config.py importable regardless of CWD
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sensor-service"))

from cassandra.cluster import Cluster  # noqa: E402
import config  # noqa: E402

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")

INSERT = """
INSERT INTO sensor_metadata (
    sensor_id, city, latitude, longitude, manufacturer, model,
    serial_number, sensor_type, installation_date, status,
    sampling_frequency_seconds
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def main():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    rows = config.build_sensor_metadata()
    for m in rows:
        session.execute(INSERT, (
            m["sensor_id"], m["city"], m["latitude"], m["longitude"],
            m["manufacturer"], m["model"], m["serial_number"],
            m["sensor_type"], m["installation_date"], m["status"],
            m["sampling_frequency_seconds"],
        ))
    print(f"Inserted {len(rows)} sensor metadata rows into {KEYSPACE}.sensor_metadata")
    cluster.shutdown()


if __name__ == "__main__":
    main()
