from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder \
    .appName("WeatherMonitorStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_data") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

final_df = parsed_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

def writeToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="sensor_data", keyspace="weather_ks") \
        .mode("append") \
        .save()

query = final_df.writeStream \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .start()

print("Duke pritur për të dhëna nga Kafka për t'i kaluar në Cassandra...")
query.awaitTermination()