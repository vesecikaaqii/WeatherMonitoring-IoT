"""
simulator.py
============
Weather sensor simulator for the Weather Monitoring IoT project.

This module is BOTH:
  1. A standalone CLI producer  ->  `python simulator.py`
  2. A reusable engine imported by `simulator_api.py` (FastAPI control plane).

Responsibilities
----------------
- Generate realistic weather messages for every configured Kosovo city sensor.
- Occasionally inject anomalies (warning / critical values) based on anomaly_rate.
- Attach `producer_timestamp_ms` so Spark can compute end-to-end latency.
- Push sensor metadata + readings to Kafka.
- Support normal mode and a high-throughput stress-test mode.
- Track live metrics (messages sent, msg/s) for the dashboard.

The JSON message schema matches the project spec exactly.
"""

import os
import json
import time
import uuid
import random
import threading
from datetime import datetime, timezone

# NOTE: kafka-python is imported lazily inside _connect() so that the pure
# message-generation logic (and the unit tests) work even on machines where
# kafka-python is not installed.

import config


def _now_iso():
    """UTC timestamp in ISO-8601 with trailing Z (e.g. 2026-06-12T12:30:00Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_ms():
    """Current wall-clock time in milliseconds (for latency measurement)."""
    return int(time.time() * 1000)


class WeatherSimulator:
    """
    Threaded weather data generator + Kafka producer.

    The simulator runs in a background thread so the FastAPI control plane can
    start / stop it without blocking. All shared state is guarded by a lock.
    """

    def __init__(self, bootstrap_servers=None):
        self.bootstrap_servers = bootstrap_servers or config.KAFKA_BOOTSTRAP_SERVERS
        self.producer = None

        # Runtime config (a copy of defaults so the API can mutate it safely)
        self.cfg = dict(config.DEFAULT_CONFIG)

        # Threading / lifecycle
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Live metrics
        self.metrics = {
            "running": False,
            "messages_sent": 0,
            "messages_per_second": 0.0,
            "started_at": None,
            "last_message_at": None,
            "mode": self.cfg["mode"],
        }

    # ------------------------------------------------------------------ #
    # Kafka producer lifecycle
    # ------------------------------------------------------------------ #
    def _connect(self):
        """Lazily create the Kafka producer (retries make startup resilient)."""
        if self.producer is None:
            from kafka import KafkaProducer  # imported here so generation works without kafka-python
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=5,
                linger_ms=5,             # small batching window for throughput
                acks=1,
            )
        return self.producer

    # ------------------------------------------------------------------ #
    # Metadata
    # ------------------------------------------------------------------ #
    def publish_metadata(self):
        """
        Publish each sensor's metadata once. The metadata is sent on the
        weather_data topic with a 'type=metadata' marker AND is also written
        directly to Cassandra by the bootstrap script. Returns the list.
        """
        producer = self._connect()
        metadata = config.build_sensor_metadata()
        for m in metadata:
            envelope = {"type": "metadata", **m}
            producer.send(config.TOPIC_WEATHER_DATA, key=m["sensor_id"], value=envelope)
        producer.flush()
        return metadata

    # ------------------------------------------------------------------ #
    # Message generation
    # ------------------------------------------------------------------ #
    def _generate_value(self, param, level=None):
        """
        Generate one parameter value.

        level=None       -> normal realistic value
        level='warning'  -> value inside the WARNING band (triggers a WARNING alarm)
        level='critical' -> value inside the CRITICAL band (triggers a CRITICAL alarm)
        """
        if level == "warning" and param in config.WARNING_RANGES:
            lo, hi = random.choice(config.WARNING_RANGES[param])
            return round(random.uniform(lo, hi), 2)
        if level == "critical" and param in config.CRITICAL_RANGES:
            lo, hi = random.choice(config.CRITICAL_RANGES[param])
            return round(random.uniform(lo, hi), 2)
        lo, hi = config.NORMAL_RANGES[param]
        return round(random.uniform(lo, hi), 2)

    def generate_message(self, city, sensor_id, lat, lon):
        """Build a single weather reading JSON message."""
        # Decide whether THIS message carries an anomaly, and at what severity.
        # Split anomalies into WARNING vs CRITICAL so both appear in the demo.
        level = None
        if random.random() < self.cfg["anomaly_rate"]:
            level = "warning" if random.random() < config.WARNING_ANOMALY_SHARE else "critical"

        msg = {
            "event_id": str(uuid.uuid4()),
            "sensor_id": sensor_id,
            "city": city,
            "location": {"latitude": lat, "longitude": lon},
            "timestamp": _now_iso(),
            "temperature":   self._generate_value("temperature", level),
            "humidity":      self._generate_value("humidity", level),
            "pressure":      self._generate_value("pressure", level),
            "wind_speed":    self._generate_value("wind_speed", level),
            "rainfall":      self._generate_value("rainfall", level),
            "air_quality":   self._generate_value("air_quality", level),
            "battery_level": self._generate_value("battery_level", level),
            # producer_timestamp_ms is the key field for latency analysis
            "producer_timestamp_ms": _now_ms(),
        }
        return msg

    def _selected_sensors(self):
        """Return [(city, sensor_id, lat, lon)] limited by config cities/num."""
        chosen = []
        for city in self.cfg["cities"]:
            if city in config.CITY_SENSORS:
                sid, lat, lon = config.CITY_SENSORS[city]
                chosen.append((city, sid, lat, lon))
        # Honor num_sensors cap (used by stress test to scale fan-out)
        return chosen[: self.cfg["num_sensors"]]

    # ------------------------------------------------------------------ #
    # Main loop (runs in background thread)
    # ------------------------------------------------------------------ #
    def _run_loop(self):
        producer = self._connect()
        with self._lock:
            self.metrics["running"] = True
            self.metrics["started_at"] = _now_iso()
            self.metrics["messages_sent"] = 0
            self.metrics["mode"] = self.cfg["mode"]

        window_start = time.time()
        window_count = 0

        try:
            while not self._stop_event.is_set():
                batch_start = time.time()

                # Recompute each tick so live config changes (cities /
                # num_sensors) from the API take effect without a restart.
                sensors = self._selected_sensors()
                if not sensors:
                    # No cities selected -> idle until reconfigured.
                    self._stop_event.wait(1.0)
                    continue

                # ----- Decide how many messages to send this tick -----
                mps = self.cfg["messages_per_second"]
                if self.cfg["mode"] == "stress" and mps > 0:
                    # Send `mps` messages spread across the available sensors
                    to_send = mps
                else:
                    # Normal mode: one message per sensor per round
                    to_send = len(sensors)

                for i in range(to_send):
                    city, sid, lat, lon = sensors[i % len(sensors)]
                    msg = self.generate_message(city, sid, lat, lon)
                    producer.send(config.TOPIC_WEATHER_DATA, key=sid, value=msg)

                    with self._lock:
                        self.metrics["messages_sent"] += 1
                        self.metrics["last_message_at"] = msg["timestamp"]
                    window_count += 1

                producer.flush()

                # ----- Update messages/second metric every ~1s -----
                elapsed = time.time() - window_start
                if elapsed >= 1.0:
                    with self._lock:
                        self.metrics["messages_per_second"] = round(window_count / elapsed, 2)
                    window_start = time.time()
                    window_count = 0

                # ----- Throttle -----
                if self.cfg["mode"] == "stress" and mps > 0:
                    # Aim for one batch per second
                    sleep_for = max(0.0, 1.0 - (time.time() - batch_start))
                    self._stop_event.wait(sleep_for)
                else:
                    self._stop_event.wait(self.cfg["interval_seconds"])
        finally:
            with self._lock:
                self.metrics["running"] = False

    # ------------------------------------------------------------------ #
    # Public control API (used by FastAPI)
    # ------------------------------------------------------------------ #
    def start(self):
        if self._thread and self._thread.is_alive():
            return False  # already running
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        return True

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        return True

    def update_config(self, new_cfg: dict):
        """Merge new config; safe to call while running (applied next tick)."""
        with self._lock:
            self.cfg.update({k: v for k, v in new_cfg.items() if k in config.DEFAULT_CONFIG})
        return self.cfg

    def get_status(self):
        with self._lock:
            return dict(self.metrics)

    def get_config(self):
        with self._lock:
            return dict(self.cfg)


# -------------------------------------------------------------------------- #
# CLI entry point (standalone producer, no API needed)
# -------------------------------------------------------------------------- #
def main():
    print("=" * 60)
    print(" Weather Monitoring IoT - Sensor Simulator (CLI mode)")
    print(f" Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f" Topic: {config.TOPIC_WEATHER_DATA}")
    print(f" Sensors: {', '.join(config.ALL_CITIES)}")
    print("=" * 60)

    sim = WeatherSimulator()
    print("Publishing sensor metadata to Kafka...")
    sim.publish_metadata()
    sim.start()

    try:
        while True:
            status = sim.get_status()
            print(
                f"[{_now_iso()}] sent={status['messages_sent']} "
                f"rate={status['messages_per_second']} msg/s mode={status['mode']}"
            )
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopping simulator...")
        sim.stop()
        if sim.producer:
            sim.producer.close()
        print("Simulator stopped.")


if __name__ == "__main__":
    main()
