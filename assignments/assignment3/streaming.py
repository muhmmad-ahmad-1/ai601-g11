from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json,  to_json, struct, window, avg, count, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaTrafficStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema for incoming JSON data
traffic_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("congestion_level", StringType(), True)
])

# Read streaming data from Kafka topic "traffic_data"
traffic_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss",False) \
    .load()

# Convert Kafka messages to structured format
traffic_df = traffic_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), traffic_schema).alias("data")) \
    .select("data.*")

# DATA QUALITY CHECKS
# 1. Remove records with null sensor_id or timestamp
traffic_df = traffic_df.dropna(subset=["sensor_id", "timestamp"])

# 2. Range validation: vehicle_count >= 0 and average_speed > 0
traffic_df = traffic_df.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0))

# 3. Deduplicate records based on sensor_id and timestamp
traffic_df = traffic_df.dropDuplicates(["sensor_id", "timestamp"])

# ANALYSIS TASKS
# 1. Compute Real-Time Traffic Volume per Sensor (5-minute window)
traffic_volume = traffic_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(col("sensor_id"), window(col("timestamp"), "5 minutes").alias("time_window")) \
    .agg(count("vehicle_count").alias("total_vehicles")) \
    .select(to_json(struct(
        lit("traffic_volume").alias("type"),
        col("sensor_id"),
        col("time_window"),
        col("total_vehicles").cast("double")
    )).alias("value"))

# 2. Detect Congestion Hotspots (3+ HIGH congestion in 5-minute windows)
congestion_hotspots = traffic_df \
    .withWatermark("timestamp", "10 minutes") \
    .filter(col("congestion_level") == "HIGH") \
    .groupBy(col("sensor_id"), window(col("timestamp"), "5 minutes").alias("time_window")) \
    .agg(count("*").alias("high_congestion_count")) \
    .filter(col("high_congestion_count") >= 3) \
    .select(to_json(struct(
        lit("congestion_hotspots").alias("type"),
        col("sensor_id"),
        col("time_window"),
        col("high_congestion_count").cast("double")
    )).alias("value"))

# 3. Calculate Average Speed per Sensor (10-minute rolling window, sliding every 5 minutes)
average_speed = traffic_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(col("sensor_id"), window(col("timestamp"), "10 minutes", "5 minutes").alias("time_window")) \
    .agg(avg("average_speed").alias("avg_speed")) \
    .select(to_json(struct(
        lit("average_speed").alias("type"),
        col("sensor_id"),
        col("time_window"),
        col("avg_speed").cast("double")
    )).alias("value"))

# 4. Identify Sudden Speed Drops (2-minute window, >50% drop)
speed_drop = traffic_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(col("sensor_id"), window(col("timestamp"), "2 minutes").alias('time_window')) \
    .agg(F.max("average_speed").alias("max_speed"), F.min("average_speed").alias("min_speed")) \
    .filter((col("max_speed") > 0) & ((col("max_speed") - col("min_speed")) / col("max_speed") > 0.5))\
    .select(to_json(struct(
        lit("speed_drop").alias("type"),
        col("sensor_id"),
        col("time_window"),
        col("max_speed").cast("double"),
        col("min_speed").cast("double")
    )).alias("value"))

# 5. Find the Busiest Sensors in the Last 30 Minutes (top 3)
busiest_sensors = traffic_df \
    .withWatermark("timestamp", "30 minutes") \
    .groupBy("sensor_id") \
    .agg(count("vehicle_count").alias("total_vehicles")) \
    .orderBy(col("total_vehicles").desc()) \
    .limit(3) \
    .select(to_json(struct(
        lit("busiest_sensors").alias("type"),
        col("sensor_id"),
        col("total_vehicles").cast("double")
    )).alias("value"))

# Function to write to Kafka
def write_to_kafka(df, checkpoint_dir, output_mode="append"):
    return df.writeStream \
        .outputMode(output_mode) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "traffic_analysis") \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime="1 second") \
        .start()

# Start streaming queries to Kafka
queries = [
    write_to_kafka(traffic_volume, "/tmp/checkpoints/traffic_volume", "append"),
    write_to_kafka(congestion_hotspots, "/tmp/checkpoints/congestion_hotspots", "append"),
    write_to_kafka(average_speed, "/tmp/checkpoints/average_speed", "append"),
    write_to_kafka(speed_drop, "/tmp/checkpoints/speed_drop", "append"),
    write_to_kafka(busiest_sensors, "/tmp/checkpoints/busiest_sensors", "complete")
]

# Await termination for all queries
for query in queries:
    query.awaitTermination()