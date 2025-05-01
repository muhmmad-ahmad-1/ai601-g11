# now_trending.py
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json
from pyspark.sql.functions import desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# 1) Create SparkSession
spark = SparkSession.builder \
    .appName("NowTrendingSongs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2) Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "latest") \
    .load()

# 3) Parse the JSON 'value' from Kafka
schema = StructType([
    StructField("song_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),  # or we can store as Double
    StructField("region", StringType(), True),
    StructField("action", StringType(), True)
])

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")

# Convert timestamp double -> actual timestamp if we want event time
# But for simplicity, let's do a processing-time approach
# If you want event-time windows, do:
events_df = events_df.withColumn("event_time", (col("timestamp")).cast(TimestampType()))

# 4) Filter only "play" events
#plays_df = events_df.filter(col("action") == "play")

# Allow all of actions through, we will compute skip_ratio

# 5) Group by region + 5-minute processing time window
# We'll do a simple processing-time window using current_timestamp
# Alternatively, you can do event-time with a column if you convert 'timestamp' to a Spark timestamp
from pyspark.sql.functions import current_timestamp, expr, when, sum, to_json, struct, coalesce, lit

windowed_df = events_df \
    .withWatermark('event_time','5 minutes') \
    .groupBy(
        window("event_time", "5 minutes"),  # processing-time window
        col("region"),
        col("song_id"),
        col("action")
    ) \
    .count()

# 6) Use foreachBatch to do rank-based top N logic each micro-batch
def process_batch(batch_df, batch_id):
    """
    This function is called for each micro-batch. We treat 'batch_df' as a normal batch DataFrame.
    We'll rank by 'count' within each region & window and pick top 3 (or 5, or 100).
    """
    if batch_df.rdd.isEmpty():
        print("No data in this batch.")
        return

    play_counts_df = batch_df.filter(col('action') == 'play') \
        .groupBy("region", "window", "song_id") \
        .agg(sum(col("count")).alias("total_plays"))
    
    w = Window.partitionBy("region", "window").orderBy(desc("total_plays"))

    ranked_df = play_counts_df.withColumn("rn", row_number().over(w)) \
                              .filter(col("rn") <= 3) \
                              .drop("rn")

    ranked_df = batch_df.join(ranked_df, on=["region", "window", "song_id"], how="inner")

    ranked_df = ranked_df.groupBy("region", "window", "song_id") \
        .agg(
            sum(when(col("action") == "play", col("count"))).alias("total_plays"),
            sum(when(col("action") == "skip", col("count"))).alias("total_skips"),
            sum(col("count")).alias("total_count")  # total count includes both play + skip
        ) \
        .withColumn("total_skips", coalesce(col("total_skips"), lit(0))) \
        .withColumn("skip_ratio", expr("total_skips / total_count"))
    
    w = Window.partitionBy("region").orderBy(desc("total_plays"))

    ranked_df = ranked_df.withColumn("rank", row_number().over(w)) \
                          .filter(col("rank") <= 3) \
                          .drop("rank")

    ranked_df = ranked_df.orderBy("region", desc("total_plays"))
    
    ranked_df.show(truncate=False)

    # Write the top songs for each region + 5-min window to another topic
    kafka_df = ranked_df.selectExpr(
            "CAST(region AS STRING)", 
            "CAST(window AS STRING)", 
            "CAST(song_id AS STRING)", 
            "CAST(total_plays AS STRING)", 
            "CAST(total_skips AS STRING)", 
            "CAST(total_count AS STRING)", 
            "CAST(skip_ratio AS STRING)"
        ).withColumn("value", to_json(struct("*")))

    # Publish to Kafka topic
    kafka_df.selectExpr("value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "now_trending_results") \
        .save()

# 7) Write Stream with foreachBatch

query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
