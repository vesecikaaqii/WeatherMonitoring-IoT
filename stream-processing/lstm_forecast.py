"""
lstm_forecast.py
================
LSTM temperature-forecasting for the Weather Monitoring IoT project.

This module trains a small Keras LSTM on historical temperature series and
uses the SAVED model to predict the next temperature value shown on the
dashboard. Training happens OFFLINE (see `train_lstm.py`); the dashboard only
*loads* the persisted model and predicts, so rendering stays fast.

Two clearly separated phases:

  1. TRAINING (offline, run once / periodically)
        f = TemperatureForecaster()
        f.train([series_sensor_a, series_sensor_b, ...])   # list of 1-D series
        f.save()                                           # -> models/ on disk

  2. PREDICTION (dashboard, every render)
        f = TemperatureForecaster.load_default()           # loads saved model
        next_temp = f.predict_next(recent_temps)           # fast, no training

Robustness
----------
- If TensorFlow is not installed, or no trained model file is present, the
  forecaster transparently falls back to a moving-average prediction so the
  dashboard always works. The active method is always exposed via `.mode`
  ("lstm" or "moving_average"), so the UI never overclaims.
"""

import os
import json

LOOKBACK = 10  # number of past readings used to predict the next one

# Default on-disk location for the trained model + scaler. Lives next to this
# file under models/, which the frontend Dockerfile copies into the dashboard
# image, so a model trained before `docker build` is available at runtime.
MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
MODEL_PATH = os.path.join(MODEL_DIR, "lstm_temperature.keras")
SCALER_PATH = os.path.join(MODEL_DIR, "lstm_temperature.scaler.json")

# TensorFlow is optional: present on the training machine / dashboard image,
# absent on minimal workers. We import lazily and degrade gracefully.
try:
    import numpy as np
    from tensorflow.keras.models import Sequential, load_model
    from tensorflow.keras.layers import LSTM, Dense, Input
    _TF_AVAILABLE = True
except Exception:  # pragma: no cover
    _TF_AVAILABLE = False


class TemperatureForecaster:
    """Predict the next temperature value from a recent window.

    Uses a trained Keras LSTM when one is available (TensorFlow installed AND a
    saved model loaded), otherwise a moving-average fallback. Min/max scaling
    parameters are stored alongside the model so prediction needs no sklearn.
    """

    def __init__(self, lookback=LOOKBACK):
        self.lookback = lookback
        self.model = None
        self.scaler = None  # {"min": float, "max": float}
        self.mode = "moving_average"

    # ------------------------------------------------------------------ #
    # Loading (used by the dashboard)
    # ------------------------------------------------------------------ #
    @classmethod
    def load_default(cls):
        """Return a forecaster with the default saved model loaded if present.

        Never raises: on any problem (no TensorFlow, no model file, corrupt
        file) it returns a moving-average forecaster.
        """
        f = cls()
        f.load(MODEL_PATH, SCALER_PATH)
        return f

    def load(self, model_path=MODEL_PATH, scaler_path=SCALER_PATH):
        """Load a saved LSTM model + scaler. Falls back silently on failure."""
        if not _TF_AVAILABLE:
            return self
        if not (os.path.exists(model_path) and os.path.exists(scaler_path)):
            return self
        try:
            self.model = load_model(model_path)
            with open(scaler_path, "r", encoding="utf-8") as fh:
                self.scaler = json.load(fh)
            self.lookback = int(self.scaler.get("lookback", self.lookback))
            self.mode = "lstm"
        except Exception:  # pragma: no cover - corrupt/incompatible model
            self.model = None
            self.scaler = None
            self.mode = "moving_average"
        return self

    # ------------------------------------------------------------------ #
    # Scaling helpers (manual min-max -> no sklearn dependency)
    # ------------------------------------------------------------------ #
    def _scale(self, arr):
        lo, hi = self.scaler["min"], self.scaler["max"]
        span = (hi - lo) or 1.0
        return (np.asarray(arr, dtype="float32") - lo) / span

    def _inverse(self, value):
        lo, hi = self.scaler["min"], self.scaler["max"]
        span = (hi - lo) or 1.0
        return float(value) * span + lo

    # ------------------------------------------------------------------ #
    # Training (offline)
    # ------------------------------------------------------------------ #
    def _build(self):
        model = Sequential([
            Input(shape=(self.lookback, 1)),
            LSTM(32, activation="tanh"),
            Dense(1),
        ])
        model.compile(optimizer="adam", loss="mse", metrics=["mae"])
        return model

    def _make_windows(self, series_list):
        """Build (X, y) supervised windows from a list of 1-D series.

        Windows never cross series boundaries, so we don't learn a bogus jump
        from one sensor's last reading to another sensor's first.
        """
        X, y = [], []
        for series in series_list:
            arr = [float(v) for v in series if v is not None]
            for i in range(len(arr) - self.lookback):
                X.append(arr[i:i + self.lookback])
                y.append(arr[i + self.lookback])
        return X, y

    def train(self, series_list, epochs=20, batch_size=32, verbose=0):
        """Train the LSTM on many temperature series.

        `series_list` is a list of 1-D sequences (one per sensor). Returns a
        dict of training info. Raises RuntimeError if TensorFlow is missing or
        there is not enough data to form a single window.
        """
        if not _TF_AVAILABLE:
            raise RuntimeError(
                "TensorFlow is not installed; cannot train the LSTM. "
                "Install it (pip install tensorflow) or keep the "
                "moving-average fallback."
            )
        X, y = self._make_windows(series_list)
        if len(X) < 10:
            raise RuntimeError(
                f"Not enough data to train (got {len(X)} windows, need >= 10). "
                "Seed more historical readings first."
            )

        X = np.asarray(X, dtype="float32")
        y = np.asarray(y, dtype="float32")

        # Fit global min/max scaler on the training values, then scale.
        lo = float(min(X.min(), y.min()))
        hi = float(max(X.max(), y.max()))
        self.scaler = {"min": lo, "max": hi, "lookback": self.lookback}
        Xs = self._scale(X).reshape(-1, self.lookback, 1)
        ys = self._scale(y)

        self.model = self._build()
        hist = self.model.fit(
            Xs, ys,
            epochs=epochs, batch_size=batch_size,
            validation_split=0.1, verbose=verbose,
        )
        self.mode = "lstm"
        return {
            "windows": int(len(X)),
            "epochs": epochs,
            "final_loss": float(hist.history["loss"][-1]),
            "final_val_loss": float(hist.history.get("val_loss", [0])[-1]),
            "final_mae_scaled": float(hist.history.get("mae", [0])[-1]),
            "temp_min": lo,
            "temp_max": hi,
        }

    def save(self, model_path=MODEL_PATH, scaler_path=SCALER_PATH):
        """Persist the trained model + scaler to disk."""
        if self.model is None or self.scaler is None:
            raise RuntimeError("Nothing to save: train the model first.")
        os.makedirs(os.path.dirname(model_path), exist_ok=True)
        self.model.save(model_path)
        with open(scaler_path, "w", encoding="utf-8") as fh:
            json.dump(self.scaler, fh)
        return self

    # ------------------------------------------------------------------ #
    # Prediction (dashboard)
    # ------------------------------------------------------------------ #
    def predict_next(self, series):
        """Return the predicted next temperature value.

        Uses the LSTM when a model is loaded; otherwise a moving average of the
        last `lookback` values.
        """
        series = [v for v in (series or []) if v is not None]
        if not series:
            return None
        if (self.mode == "lstm" and self.model is not None
                and self.scaler is not None and len(series) >= self.lookback):
            window = self._scale(series[-self.lookback:]).reshape(1, self.lookback, 1)
            pred = self.model.predict(window, verbose=0)[0][0]
            return round(self._inverse(pred), 2)
        # Fallback: simple moving average of last `lookback` values
        window = series[-self.lookback:]
        return round(sum(window) / len(window), 2)


if __name__ == "__main__":
    import math
    demo = [20 + 5 * math.sin(i / 3.0) for i in range(80)]
    f = TemperatureForecaster()
    if _TF_AVAILABLE:
        info = f.train([demo], epochs=5, verbose=0)
        print(f"Trained LSTM on {info['windows']} windows "
              f"(loss={info['final_loss']:.4f})")
    print(f"Forecaster mode: {f.mode}")
    print(f"Predicted next temperature: {f.predict_next(demo):.2f}")
