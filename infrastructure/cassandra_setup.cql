-- ==========================================================================
-- Weather Monitoring IoT - Cassandra schema (weather_ks)
-- ==========================================================================
-- Run with:
--   docker exec -it cassandra cqlsh -f /scripts/cassandra_setup.cql
-- or locally:
--   cqlsh -f infrastructure/cassandra_setup.cql
--
-- The schema is designed around the dashboard's query patterns:
--   * latest value per sensor          -> latest_readings (PK = sensor_id)
--   * historical time-series per sensor -> sensor_readings (PK = (sensor_id,date), timestamp DESC)
--   * recent alarms (newest first)      -> alarms (PK = city, timestamp DESC)
--   * recent performance samples        -> performance_metrics (PK = mode, timestamp DESC)
--   * sensors by city/status            -> sensor_metadata (PK = sensor_id, index on city/status)
--   * windowed aggregates per city      -> sensor_aggregates (PK = city, window_start DESC)
-- ==========================================================================

CREATE KEYSPACE IF NOT EXISTS weather_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE weather_ks;

-- --------------------------------------------------------------------------
-- 1. sensor_metadata : static description of each weather station
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_metadata (
    sensor_id                   text PRIMARY KEY,
    city                        text,
    latitude                    double,
    longitude                   double,
    manufacturer                text,
    model                       text,
    serial_number               text,
    sensor_type                 text,
    installation_date           text,
    status                      text,
    sampling_frequency_seconds  int
);
-- Secondary indexes to support "get sensors by city / status" queries.
CREATE INDEX IF NOT EXISTS idx_metadata_city   ON sensor_metadata (city);
CREATE INDEX IF NOT EXISTS idx_metadata_status ON sensor_metadata (status);

-- --------------------------------------------------------------------------
-- 2. sensor_readings : historical time-series (partitioned by sensor + day)
--    Partition key (sensor_id, date) keeps partitions bounded in size.
--    Clustering by timestamp DESC -> newest readings first.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_readings (
    sensor_id     text,
    date          text,          -- 'YYYY-MM-DD' partition bucket
    timestamp     text,
    city          text,
    temperature   double,
    humidity      double,
    pressure      double,
    wind_speed    double,
    rainfall      double,
    air_quality   double,
    battery_level double,
    severity      text,
    is_anomaly    boolean,
    latency_ms    bigint,
    PRIMARY KEY ((sensor_id, date), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

-- --------------------------------------------------------------------------
-- 3. latest_readings : most recent value per sensor (overwritten each batch)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS latest_readings (
    sensor_id     text PRIMARY KEY,
    city          text,
    timestamp     text,
    temperature   double,
    humidity      double,
    pressure      double,
    wind_speed    double,
    rainfall      double,
    air_quality   double,
    battery_level double,
    severity      text,
    is_anomaly    boolean,
    latency_ms    bigint
);

-- --------------------------------------------------------------------------
-- 4. alarms : threshold-crossing events (newest per city first)
--    Partition by city so the dashboard can list a city's recent alarms;
--    clustering by timestamp DESC then alarm_id for uniqueness.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alarms (
    city       text,
    timestamp  text,
    alarm_id   uuid,
    sensor_id  text,
    parameter  text,
    value      double,
    threshold  double,
    severity   text,
    message    text,
    status     text,
    PRIMARY KEY ((city), timestamp, alarm_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, alarm_id ASC);
-- Allow filtering alarms by severity in the dashboard.
CREATE INDEX IF NOT EXISTS idx_alarms_severity ON alarms (severity);

-- --------------------------------------------------------------------------
-- 5. performance_metrics : throughput + latency samples per batch / test
--    Partition by mode ('stream' | 'stress' | 'normal'); newest first.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS performance_metrics (
    mode                     text,
    timestamp                text,
    metric_id                uuid,
    messages_processed       bigint,
    messages_per_second      double,
    avg_latency_ms           double,
    max_latency_ms           double,
    cassandra_write_time_ms  double,
    notes                    text,
    PRIMARY KEY ((mode), timestamp, metric_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, metric_id ASC);

-- --------------------------------------------------------------------------
-- 6. sensor_aggregates : Spark Structured Streaming WINDOWED aggregations.
--    Tumbling 1-minute windows per city (avg/min/max temperature, avg humidity
--    & pressure, max wind, avg air quality, reading count). Upserted on the
--    window's (city, window_start) key as the window is updated.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_aggregates (
    city            text,
    window_start    text,
    window_end      text,
    avg_temperature double,
    min_temperature double,
    max_temperature double,
    avg_humidity    double,
    avg_pressure    double,
    max_wind_speed  double,
    avg_air_quality double,
    reading_count   bigint,
    PRIMARY KEY ((city), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);
