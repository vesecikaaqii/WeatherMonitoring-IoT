"""
lstm_forecast.py  (BONUS - Option B)
====================================
Simple LSTM temperature-prediction scaffold for the Weather Monitoring IoT
project. The Phase-1 report mentions LSTM; this file shows how it would be
trained on historical temperature data from Cassandra and used to predict the
next temperature value shown on the dashboard.

It is intentionally dependency-optional: if TensorFlow/Keras is not installed
it falls back to a moving-average forecast so the dashboard still works.

How to use
----------
    from lstm_forecast import TemperatureForecaster
    f = TemperatureForecaster()
    f.fit(history_temps)          # list/array of past temperatures
    next_temp = f.predict_next(history_temps)

To train on real data, pull a sensor's temperature series from Cassandra:
    SELECT temperature FROM sensor_readings
    WHERE sensor_id='PR-001' AND date='2026-06-12';
"""

LOOKBACK = 10  # number of past readings used to predict the next one

try:
    import numpy as np
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import LSTM, Dense
    _TF_AVAILABLE = True
except Exception:  # pragma: no cover
    _TF_AVAILABLE = False


class TemperatureForecaster:
    """Predict the next temperature value from a recent window."""

    def __init__(self, lookback=LOOKBACK):
        self.lookback = lookback
        self.model = None
        self.mode = "moving_average"
        if _TF_AVAILABLE:
            self.mode = "lstm"

    def _build(self):
        model = Sequential([
            LSTM(32, activation="tanh", input_shape=(self.lookback, 1)),
            Dense(1),
        ])
        model.compile(optimizer="adam", loss="mse")
        return model

    def fit(self, series, epochs=10, verbose=0):
        """Train the LSTM on a 1-D temperature series (no-op in fallback)."""
        if self.mode != "lstm" or len(series) <= self.lookback:
            return self
        arr = np.asarray(series, dtype="float32")
        X, y = [], []
        for i in range(len(arr) - self.lookback):
            X.append(arr[i:i + self.lookback])
            y.append(arr[i + self.lookback])
        X = np.asarray(X).reshape(-1, self.lookback, 1)
        y = np.asarray(y)
        self.model = self._build()
        self.model.fit(X, y, epochs=epochs, verbose=verbose)
        return self

    def predict_next(self, series):
        """Return the predicted next temperature value."""
        if not series:
            return None
        if self.mode == "lstm" and self.model is not None and len(series) >= self.lookback:
            window = np.asarray(series[-self.lookback:], dtype="float32").reshape(1, self.lookback, 1)
            return float(self.model.predict(window, verbose=0)[0][0])
        # Fallback: simple moving average of last lookback values
        window = series[-self.lookback:]
        return round(sum(window) / len(window), 2)


if __name__ == "__main__":
    import math
    demo = [20 + 5 * math.sin(i / 3.0) for i in range(50)]
    f = TemperatureForecaster()
    print(f"Forecaster mode: {f.mode}")
    f.fit(demo)
    print(f"Predicted next temperature: {f.predict_next(demo):.2f}")
