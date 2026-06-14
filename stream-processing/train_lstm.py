"""
train_lstm.py
=============
OFFLINE trainer for the temperature-forecasting LSTM.

Pulls historical temperature series per sensor from Cassandra
(weather_ks.sensor_readings), trains the Keras LSTM defined in
`lstm_forecast.TemperatureForecaster`, and saves the model + scaler to
`stream-processing/models/`. The dashboard then loads that saved model and only
predicts (no per-render training).

Run this ONCE after you have historical data (e.g. after the live pipeline has
run for a while, or after seeding demo history):

    CASSANDRA_HOST=127.0.0.1 python infrastructure/seed_historical_readings.py
    CASSANDRA_HOST=127.0.0.1 python stream-processing/train_lstm.py

Requirements: tensorflow, numpy, cassandra-driver (see requirements.txt).
If TensorFlow is absent the script exits with a clear message and the dashboard
keeps using the moving-average fallback.

Optional env:
    CASSANDRA_HOST=127.0.0.1
    CASSANDRA_KEYSPACE=weather_ks
    LSTM_EPOCHS=20
"""

import os
import sys

# Import the forecaster from this directory regardless of CWD.
sys.path.insert(0, os.path.dirname(__file__))
from lstm_forecast import TemperatureForecaster, MODEL_PATH, SCALER_PATH  # noqa: E402

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")
EPOCHS = int(os.getenv("LSTM_EPOCHS", "20"))


def load_series_from_cassandra():
    """Return a list of per-sensor temperature series, ordered in time.

    Each element is one sensor's temperatures concatenated across its date
    partitions in chronological order. Windows are later built per series so we
    never learn an artificial jump between two different sensors.
    """
    from cassandra.cluster import Cluster

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)

    # (sensor_id, date) is the partition key, so DISTINCT is cheap.
    parts = list(session.execute(
        "SELECT DISTINCT sensor_id, date FROM sensor_readings"
    ))
    by_sensor = {}
    for row in parts:
        by_sensor.setdefault(row.sensor_id, []).append(row.date)

    series_list = []
    select = session.prepare(
        "SELECT timestamp, temperature FROM sensor_readings "
        "WHERE sensor_id=? AND date=?"
    )
    for sensor_id, dates in by_sensor.items():
        points = []
        for date in sorted(dates):
            rows = session.execute(select, (sensor_id, date))
            points.extend(
                (r.timestamp, r.temperature) for r in rows
                if r.temperature is not None
            )
        points.sort(key=lambda p: p[0])  # chronological by ISO timestamp
        series = [t for _, t in points]
        if series:
            series_list.append(series)
            print(f"  {sensor_id}: {len(series)} readings")

    cluster.shutdown()
    return series_list


def main():
    print("=" * 60)
    print(" Training temperature-forecasting LSTM")
    print(f" Cassandra: {CASSANDRA_HOST} / {KEYSPACE}")
    print("=" * 60)

    f = TemperatureForecaster()

    print("Loading historical temperature series from Cassandra...")
    try:
        series_list = load_series_from_cassandra()
    except Exception as exc:
        print(f"ERROR: could not read from Cassandra: {exc}")
        sys.exit(1)

    total = sum(len(s) for s in series_list)
    print(f"Loaded {len(series_list)} sensor series, {total} readings total.")
    if total == 0:
        print("No data found. Seed history first:")
        print("  CASSANDRA_HOST=127.0.0.1 python infrastructure/seed_historical_readings.py")
        sys.exit(1)

    print(f"Training LSTM (epochs={EPOCHS})...")
    try:
        info = f.train(series_list, epochs=EPOCHS, verbose=1)
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    f.save()
    print("-" * 60)
    print(f"Trained on {info['windows']} windows.")
    print(f"  final loss     = {info['final_loss']:.5f}")
    print(f"  final val_loss = {info['final_val_loss']:.5f}")
    print(f"  temp range     = [{info['temp_min']:.1f}, {info['temp_max']:.1f}] degC")
    print(f"Saved model  -> {MODEL_PATH}")
    print(f"Saved scaler -> {SCALER_PATH}")
    print("The dashboard will now use the real LSTM (mode='lstm').")


if __name__ == "__main__":
    main()
