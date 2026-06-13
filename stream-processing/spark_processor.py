"""
spark_processor.py
==================
Apache Spark Structured Streaming consumer for the Weather Monitoring IoT
pipeline.

Pipeline position:
    Kafka(weather_data) -> Spark Structured Streaming -> Cassandra(weather_ks)

What this job does per micro-batch
----------------------------------
1. Read raw JSON messages from Kafka topic `weather_data`.
2. Parse them against an explicit schema.
3. Validate (drop / mark records with impossible or missing values).
4. Compute processing latency:  now_ms - producer_timestamp_ms.
5. Classify severity (NORMAL / WARNING / CRITICAL) per parameter thresholds.
6. Run AI anomaly detection (IsolationForest with statistical fallback).
7. Detect alarms (one row per crossed threshold). Each alarm is FIRST
   published to the Kafka topic `weather_alerts`, then persisted to Cassandra.
   Per batch we write to:
       - Kafka weather_alerts (alarm events stream)
       - sensor_readings   (historical time-series)
       - latest_readings   (most recent value per sensor)
       - alarms            (alarm rows)
       - performance_metrics (per-batch throughput + latency)
8. Checkpoint Kafka offsets for exactly-once-ish recovery.

A SECOND streaming query runs a WINDOWED aggregation on the same source:
tumbling 1-minute windows per city (avg/min/max temperature, avg humidity &
pressure, max wind, avg air quality, reading count) -> Cassandra
`sensor_aggregates`. This demonstrates Spark aggregation + windowing.

Run:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
      stream-processing/spark_processor.py

(Kafka / Cassandra hosts come from environment variables -> Docker friendly.)
"""

import os
import time
import uuid

# PySpark is imported lazily so the pure helper functions (classify_parameter,
# build_alarms, THRESHOLDS) can be imported and unit-tested on machines without
# a Spark install. The streaming job (main) requires pyspark at runtime.
try:
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, LongType
    )
    _SPARK_AVAILABLE = True
except Exception:  # pragma: no cover
    _SPARK_AVAILABLE = False

# AI anomaly detector (degrades to statistical fallback if sklearn missing)
import ai_model

# --------------------------------------------------------------------------
# Environment-driven configuration (works locally AND in Docker)
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_WEATHER = os.getenv("KAFKA_TOPIC", "weather_data")
TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "weather_alerts")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "weather_ks")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "stream-processing/checkpoints")
TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")

# --------------------------------------------------------------------------
# Weather thresholds (mirrors the project spec exactly)
# Each entry: parameter -> dict with warning/critical bounds.
# --------------------------------------------------------------------------
THRESHOLDS = {
    "temperature": {"warn_low": -10, "warn_high": 35, "crit_low": -20, "crit_high": 40},
    "humidity":    {"warn_high": 85, "crit_high": 95},
    "pressure":    {"warn_low": 1000, "crit_low": 980},
    "wind_speed":  {"warn_high": 10, "crit_high": 15},
    "rainfall":    {"warn_high": 10, "crit_high": 30},
    "air_quality": {"warn_high": 100, "crit_high": 150},
    "battery_level": {"warn_low": 20, "crit_low": 10},
}


def classify_parameter(param, value):
    """
    Return (severity, threshold_crossed, message_fragment) for one parameter.
    severity in {NORMAL, WARNING, CRITICAL}.
    """
    if value is None:
        return "NORMAL", None, None
    t = THRESHOLDS.get(param, {})

    # Critical checks first (highest severity wins).
    # The fragment is just the relation (no param name) so the alarm message
    # reads cleanly: "Temperature in Peja is above 40 (...)".
    if "crit_high" in t and value > t["crit_high"]:
        return "CRITICAL", t["crit_high"], f"above {t['crit_high']}"
    if "crit_low" in t and value < t["crit_low"]:
        return "CRITICAL", t["crit_low"], f"below {t['crit_low']}"
    if "warn_high" in t and value > t["warn_high"]:
        return "WARNING", t["warn_high"], f"above {t['warn_high']}"
    if "warn_low" in t and value < t["warn_low"]:
        return "WARNING", t["warn_low"], f"below {t['warn_low']}"
    return "NORMAL", None, None


def row_severity(reading):
    """Highest severity across all parameters of a reading."""
    order = {"NORMAL": 0, "WARNING": 1, "CRITICAL": 2}
    worst = "NORMAL"
    for param in THRESHOLDS:
        sev, _, _ = classify_parameter(param, reading.get(param))
        if order[sev] > order[worst]:
            worst = sev
    return worst


def build_alarms(reading):
    """Return a list of alarm dicts for every threshold crossed in a reading."""
    alarms = []
    for param in THRESHOLDS:
        value = reading.get(param)
        sev, threshold, frag = classify_parameter(param, value)
        if sev != "NORMAL":
            unit = {"temperature": "°C", "humidity": "%", "pressure": "hPa",
                    "wind_speed": " m/s", "rainfall": " mm", "air_quality": "",
                    "battery_level": "%"}.get(param, "")
            message = f"{sev}: {param.replace('_', ' ').title()} in {reading['city']} is {frag} ({value}{unit})"
            alarms.append({
                # str(uuid) -> the spark-cassandra connector parses it into the
                # uuid column; passing a raw UUID object breaks schema inference.
                "alarm_id": str(uuid.uuid4()),
                "sensor_id": reading["sensor_id"],
                "city": reading["city"],
                "timestamp": reading["timestamp"],
                "parameter": param,
                "value": float(value),
                "threshold": float(threshold),
                "severity": sev,
                "message": message,
                "status": "active",
            })
    return alarms


# --------------------------------------------------------------------------
# Kafka message schema (built lazily; needs pyspark types at runtime)
# --------------------------------------------------------------------------
def build_message_schema():
    location_schema = StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ])
    return StructType([
        StructField("event_id", StringType()),
        StructField("sensor_id", StringType()),
        StructField("city", StringType()),
        StructField("location", location_schema),
        StructField("timestamp", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("pressure", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("rainfall", DoubleType()),
        StructField("air_quality", DoubleType()),
        StructField("battery_level", DoubleType()),
        StructField("producer_timestamp_ms", LongType()),
        StructField("type", StringType()),  # 'metadata' messages are filtered out
    ])


def write_to_cassandra(df, table):
    """Append a DataFrame to a Cassandra table."""
    (df.write
       .format("org.apache.spark.sql.cassandra")
       .options(table=table, keyspace=CASSANDRA_KEYSPACE)
       .mode("append")
       .save())


def publish_alarms_to_kafka(alarms_df):
    """
    Publish each alarm to the Kafka topic `weather_alerts` (keyed by sensor_id)
    as a JSON value. Uses the spark-sql-kafka sink, which is already on the
    classpath because we read from Kafka. This runs BEFORE the Cassandra write
    so downstream consumers see alerts as soon as they are detected.
    """
    (alarms_df
        .select(
            F.col("sensor_id").cast("string").alias("key"),
            F.to_json(F.struct("*")).alias("value"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_ALERTS)
        .save())


def process_batch(batch_df, epoch_id):
    """
    foreachBatch handler. Runs once per micro-batch. We collect to the driver
    here because the per-row logic (alarms, AI) is simplest in Python; for the
    project's data volumes this is fine, and the stress test still measures the
    full path. Comments mark where you'd push logic into the cluster at scale.
    """
    batch_start = time.time()

    # Filter out metadata envelopes; keep only sensor readings
    readings = [r.asDict(recursive=True) for r in batch_df.collect()]
    readings = [r for r in readings if r.get("type") != "metadata" and r.get("sensor_id")]

    if not readings:
        return

    now_ms = int(time.time() * 1000)
    detector = ai_model.get_detector()

    valid_rows, latest_by_sensor, all_alarms = [], {}, []
    latencies = []

    for r in readings:
        # ---- Validation: required fields + plausible parsing ----
        if r.get("temperature") is None or r.get("city") is None:
            continue  # drop invalid record

        # ---- Latency ----
        prod_ms = r.get("producer_timestamp_ms") or now_ms
        latency_ms = max(0, now_ms - int(prod_ms))
        latencies.append(latency_ms)

        # ---- Severity + AI anomaly ----
        severity = row_severity(r)
        is_anomaly = detector.is_anomaly(r)

        # ---- date partition key (YYYY-MM-DD from ISO timestamp) ----
        ts = r["timestamp"]
        date_part = ts[:10] if ts else "1970-01-01"

        record = {
            "sensor_id": r["sensor_id"],
            "city": r["city"],
            "date": date_part,
            "timestamp": ts,
            "temperature": r.get("temperature"),
            "humidity": r.get("humidity"),
            "pressure": r.get("pressure"),
            "wind_speed": r.get("wind_speed"),
            "rainfall": r.get("rainfall"),
            "air_quality": r.get("air_quality"),
            "battery_level": r.get("battery_level"),
            "severity": severity,
            "is_anomaly": is_anomaly,
            "latency_ms": latency_ms,
        }
        valid_rows.append(record)
        # latest_readings keeps the newest per sensor in this batch
        latest_by_sensor[r["sensor_id"]] = record

        # ---- Alarms ----
        all_alarms.extend(build_alarms(r))

    spark = batch_df.sparkSession

    # ---- Write historical readings ----
    if valid_rows:
        hist_df = spark.createDataFrame(valid_rows)
        cass_start = time.time()
        write_to_cassandra(hist_df, "sensor_readings")
        cass_write_ms = int((time.time() - cass_start) * 1000)
    else:
        cass_write_ms = 0

    # ---- Write latest readings (one row per sensor) ----
    if latest_by_sensor:
        latest_rows = []
        for rec in latest_by_sensor.values():
            lr = dict(rec)
            lr.pop("date", None)  # latest_readings is keyed only by sensor_id
            latest_rows.append(lr)
        write_to_cassandra(spark.createDataFrame(latest_rows), "latest_readings")

    # ---- Publish alarms to Kafka (weather_alerts) THEN persist to Cassandra ----
    if all_alarms:
        alarms_df = spark.createDataFrame(all_alarms)
        publish_alarms_to_kafka(alarms_df)        # stream alerts out first
        write_to_cassandra(alarms_df, "alarms")   # then store for history/UI

    # ---- Performance metrics for this batch ----
    elapsed = time.time() - batch_start
    n = len(valid_rows)
    mps = round(n / elapsed, 2) if elapsed > 0 else 0.0
    avg_lat = round(sum(latencies) / len(latencies), 2) if latencies else 0.0
    max_lat = max(latencies) if latencies else 0
    perf = [{
        "metric_id": str(uuid.uuid4()),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "messages_processed": n,
        "messages_per_second": mps,
        "avg_latency_ms": avg_lat,
        "max_latency_ms": float(max_lat),
        "cassandra_write_time_ms": float(cass_write_ms),
        "mode": "stream",
        "notes": f"epoch={epoch_id}",
    }]
    write_to_cassandra(spark.createDataFrame(perf), "performance_metrics")

    # ---- Logging ----
    print(
        f"[batch {epoch_id}] processed={n} alarms={len(all_alarms)} "
        f"(published to {TOPIC_ALERTS}) "
        f"avg_latency={avg_lat}ms max_latency={max_lat}ms "
        f"cass_write={cass_write_ms}ms rate={mps}msg/s"
    )


def write_aggregates(batch_df, epoch_id):
    """
    foreachBatch handler for the WINDOWED aggregation stream. Each batch holds
    the (city, time-window) groups updated in this trigger; we format the window
    bounds as ISO strings and upsert into Cassandra `sensor_aggregates`
    (same (city, window_start) key -> the window row is kept current).
    """
    if batch_df.rdd.isEmpty():
        return
    out = batch_df.select(
        F.col("city"),
        F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_start"),
        F.date_format(F.col("window.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_end"),
        F.col("avg_temperature"), F.col("min_temperature"), F.col("max_temperature"),
        F.col("avg_humidity"), F.col("avg_pressure"), F.col("max_wind_speed"),
        F.col("avg_air_quality"), F.col("reading_count"),
    )
    write_to_cassandra(out, "sensor_aggregates")
    print(f"[agg batch {epoch_id}] windows updated={out.count()}")


def build_windowed_aggregates(parsed):
    """
    Spark Structured Streaming WINDOWED aggregation: tumbling 1-minute windows
    per city. Demonstrates aggregation + windowing (avg/min/max/count) on the
    live sensor stream, with a watermark to bound state.
    """
    return (
        parsed
        # keep only real readings (drop metadata envelopes / null sensors)
        .filter((F.col("type").isNull() | (F.col("type") != "metadata"))
                & F.col("sensor_id").isNotNull() & F.col("city").isNotNull())
        # event-time column from the message timestamp + watermark for state cleanup
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"), F.col("city"))
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.min("temperature").alias("min_temperature"),
            F.max("temperature").alias("max_temperature"),
            F.avg("humidity").alias("avg_humidity"),
            F.avg("pressure").alias("avg_pressure"),
            F.max("wind_speed").alias("max_wind_speed"),
            F.avg("air_quality").alias("avg_air_quality"),
            F.count(F.lit(1)).alias("reading_count"),
        )
    )


def main():
    spark = (
        SparkSession.builder
        .appName("WeatherMonitoringStreaming")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        # --- OPTIMIZATION knobs (env-tunable) ---
        # Fewer shuffle partitions for small clusters avoids tiny-task overhead.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        # Cassandra connector write tuning: parallel writes + larger batches
        # grouped by partition key -> fewer round-trips, higher write throughput.
        .config("spark.cassandra.output.concurrent.writes",
                os.getenv("CASS_CONCURRENT_WRITES", "4"))
        .config("spark.cassandra.output.batch.size.rows",
                os.getenv("CASS_BATCH_SIZE_ROWS", "200"))
        .config("spark.cassandra.output.batch.grouping.key", "partition")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print(" Weather Monitoring IoT - Spark Structured Streaming")
    print(f" Kafka: {KAFKA_BOOTSTRAP}  in={TOPIC_WEATHER}  alerts_out={TOPIC_ALERTS}")
    print(f" Cassandra: {CASSANDRA_HOST} / {CASSANDRA_KEYSPACE}")
    print(f" AI detector mode: {ai_model.get_detector().mode}")
    print("=" * 60)

    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_WEATHER)
        .option("startingOffsets", "latest")
    )
    # OPTIMIZATION (back-pressure): cap records per micro-batch so latency stays
    # bounded under load. Applied only when MAX_OFFSETS_PER_TRIGGER is set.
    max_offsets = os.getenv("MAX_OFFSETS_PER_TRIGGER")
    if max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", max_offsets)
    raw = reader.load()

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json(F.col("json"), build_message_schema()).alias("d"))
        .select("d.*")
    )

    # Query 1: per-record processing (validate, latency, severity, alarms, AI)
    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("update")
        .start()
    )

    # Query 2: windowed aggregation (tumbling 1-min per city -> sensor_aggregates)
    agg_query = (
        build_windowed_aggregates(parsed).writeStream
        .foreachBatch(write_aggregates)
        .option("checkpointLocation", CHECKPOINT_DIR + "_agg")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("update")
        .start()
    )

    print("Streaming started (processing + windowed aggregation). Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
