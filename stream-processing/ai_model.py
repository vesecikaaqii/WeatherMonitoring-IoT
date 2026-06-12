"""
ai_model.py
===========
AI / anomaly-detection component for the Weather Monitoring IoT pipeline.

Why anomaly detection for IoT weather monitoring?
-------------------------------------------------
Weather sensors in the field drift, fail, lose battery, or report physically
impossible spikes (e.g. a sudden +60 degC jump). Anomaly detection flags those
readings so operators trust the data and react to genuine extreme-weather
events instead of sensor glitches. It complements the threshold-based alarm
system: thresholds catch *known* dangerous bands, the model catches *unusual*
patterns the static rules miss.

Design (robust, dependency-optional)
------------------------------------
- PRIMARY: scikit-learn IsolationForest, trained on synthetic "normal" weather
  vectors. Returns is_anomaly for a multi-parameter reading.
- FALLBACK: if scikit-learn / numpy are not installed (common on minimal Spark
  workers), the same `WeatherAnomalyDetector.is_anomaly()` API degrades to a
  pure-Python statistical / threshold check, so the pipeline NEVER crashes.

This module is imported by spark_processor.py and used per micro-batch.
A bonus LSTM temperature-forecast scaffold is provided in `lstm_forecast.py`.
"""

import os
import math

# --- thresholds reused for the statistical fallback (mirror Spark thresholds)
NORMAL_BANDS = {
    "temperature": (-20.0, 40.0),
    "humidity": (0.0, 95.0),
    "pressure": (980.0, 1040.0),
    "wind_speed": (0.0, 15.0),
    "rainfall": (0.0, 30.0),
    "air_quality": (0.0, 150.0),
    "battery_level": (10.0, 100.0),
}

FEATURES = ["temperature", "humidity", "pressure", "wind_speed",
            "rainfall", "air_quality", "battery_level"]

# Detect optional ML stack once
try:
    import numpy as np
    from sklearn.ensemble import IsolationForest
    _ML_AVAILABLE = True
except Exception:  # pragma: no cover
    _ML_AVAILABLE = False


class WeatherAnomalyDetector:
    """
    Unified anomaly detector with a stable public API:

        detector = WeatherAnomalyDetector()
        detector.is_anomaly({"temperature": 50, ...})  -> True/False

    Uses IsolationForest when available, otherwise statistical fallback.
    """

    def __init__(self, contamination=0.05, random_state=42):
        self.contamination = contamination
        self.random_state = random_state
        self.model = None
        self.mode = "fallback"
        if _ML_AVAILABLE:
            self._train_on_synthetic()
            self.mode = "isolation_forest"

    # ------------------------------------------------------------------ #
    def _train_on_synthetic(self):
        """
        Train IsolationForest on synthetic 'normal' weather vectors.
        In production you would `fit` on historical sensor_readings pulled
        from Cassandra; synthetic training keeps the demo self-contained.
        """
        rng = np.random.default_rng(self.random_state)
        n = 2000
        data = np.column_stack([
            rng.uniform(-5, 35, n),     # temperature
            rng.uniform(30, 70, n),     # humidity
            rng.uniform(1000, 1030, n), # pressure
            rng.uniform(0, 8, n),       # wind_speed
            rng.uniform(0, 5, n),       # rainfall
            rng.uniform(10, 80, n),     # air_quality
            rng.uniform(40, 100, n),    # battery_level
        ])
        self.model = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=100,
        )
        self.model.fit(data)

    # ------------------------------------------------------------------ #
    def _vector(self, reading: dict):
        return [float(reading.get(f, 0.0) or 0.0) for f in FEATURES]

    def is_anomaly(self, reading: dict) -> bool:
        """Return True if the multi-parameter reading is anomalous."""
        if self.mode == "isolation_forest" and self.model is not None:
            try:
                pred = self.model.predict([self._vector(reading)])[0]
                return bool(pred == -1)  # IsolationForest: -1 == anomaly
            except Exception:
                pass  # fall through to statistical check
        return self._statistical_anomaly(reading)

    def _statistical_anomaly(self, reading: dict) -> bool:
        """Pure-Python fallback: any parameter outside its normal band."""
        for param, (lo, hi) in NORMAL_BANDS.items():
            val = reading.get(param)
            if val is None:
                continue
            try:
                v = float(val)
            except (TypeError, ValueError):
                return True  # unparseable value is itself anomalous
            if math.isnan(v) or v < lo or v > hi:
                return True
        return False


# Module-level singleton so Spark workers reuse one model per executor
_DETECTOR = None


def get_detector():
    global _DETECTOR
    if _DETECTOR is None:
        _DETECTOR = WeatherAnomalyDetector()
    return _DETECTOR


def detect_anomaly(reading: dict) -> bool:
    """Convenience function used inside Spark UDF / foreachBatch."""
    return get_detector().is_anomaly(reading)


if __name__ == "__main__":
    d = get_detector()
    print(f"AI anomaly detector mode: {d.mode}")
    normal = {"temperature": 22, "humidity": 55, "pressure": 1015,
              "wind_speed": 3, "rainfall": 0, "air_quality": 40, "battery_level": 90}
    extreme = {"temperature": 55, "humidity": 99, "pressure": 950,
               "wind_speed": 25, "rainfall": 50, "air_quality": 250, "battery_level": 5}
    print(f"normal  -> anomaly={d.is_anomaly(normal)}")
    print(f"extreme -> anomaly={d.is_anomaly(extreme)}")
