-- 1. Create the Keyspace (Database)
CREATE KEYSPACE IF NOT EXISTS weather_ks 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE weather_ks;

-- 3. Create the Table for Sensor Data
-- The PRIMARY KEY is (sensor_id, timestamp). 
-- 'sensor_id' is the Partition Key (groups data by sensor).
-- 'timestamp' is the Clustering Key (sorts data chronologically).
CREATE TABLE IF NOT EXISTS sensor_data (
    sensor_id text,
    timestamp timestamp,
    temperature float,
    humidity float,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);