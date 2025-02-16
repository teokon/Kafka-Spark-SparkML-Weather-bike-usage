# import logging
# from pyspark.sql import SparkSession, Window
# from pyspark.sql.functions import from_json, col, to_timestamp, avg, max, min, stddev, when, lit, expr, coalesce, \
#     window, sum as spark_sum, stddev as spark_stddev
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# from pyspark.sql.streaming import StreamingQueryException
#
# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
#
# # Create SparkSession with Kafka support
# spark = SparkSession.builder \
#     .appName("KafkaStreamJoin") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
#     .getOrCreate()
#
# # Kafka Configuration
# kafka_brokers = "150.140.142.67:9094"
# topic_1 = "station_info_1084523_2"  # Station Info
# topic_2 = "station_status_1084523_2"  # Station Status
# topic_3 = "weather_data_1084523_2"  # Weather Data
#
# # ---------------------------
# # Define Schemas Based on Message Structures
# # ---------------------------
# station_status_schema = StructType([
#     StructField("station_id", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("num_bikes_available", IntegerType(), True)
# ])
#
# station_info_schema = StructType([
#     StructField("station_id", StringType(), True),
#     StructField("capacity", IntegerType(), True),
#     StructField("metadata", StructType([
#         StructField("timestamp", StringType(), True)
#     ]), True)
# ])
#
# weather_schema = StructType([
#     StructField("temperature", FloatType(), True),
#     StructField("windspeed", FloatType(), True),
#     StructField("clouds", FloatType(), True),
#     StructField("metadata", StructType([
#         StructField("timestamp", StringType(), True)
#     ]), True)
# ])
#
# # ---------------------------
# # Read data from Kafka topics
# # ---------------------------
# kafka_stream_1 = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_brokers) \
#     .option("subscribe", topic_1) \
#     .option("startingOffsets", "latest") \
#     .load()
#
# kafka_stream_2 = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_brokers) \
#     .option("subscribe", topic_2) \
#     .option("startingOffsets", "latest") \
#     .load()
#
# kafka_stream_3 = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_brokers) \
#     .option("subscribe", topic_3) \
#     .option("startingOffsets", "latest") \
#     .load()
#
# # ---------------------------
# # Parse JSON messages using the defined schemas and rename timestamps
# # ---------------------------
# stream_1_data = (kafka_stream_1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#                  .withColumn("data", from_json(col("value"), station_info_schema))
#                  .select(
#     col("key").alias("key"),
#     col("data.station_id").alias("station_id"),
#     col("data.capacity").alias("capacity"),
#     col("data.metadata.timestamp").alias("info_timestamp")
# )
#                  .withColumn("info_timestamp", to_timestamp(col("info_timestamp")))
#                  )
#
# stream_2_data = (kafka_stream_2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#                  .withColumn("data", from_json(col("value"), station_status_schema))
#                  .select(
#     col("key").alias("key"),
#     col("data.station_id").alias("station_id"),
#     col("data.num_bikes_available").alias("num_bikes_available"),
#     col("data.timestamp").alias("status_timestamp")
# )
#                  .withColumn("status_timestamp", to_timestamp(col("status_timestamp")))
#                  )
#
# weather_data = (kafka_stream_3.selectExpr("CAST(value AS STRING)")
#                 .withColumn("data", from_json(col("value"), weather_schema))
#                 .select(
#     col("data.metadata.timestamp").alias("timestamp"),
#     col("data.temperature").alias("temperature"),
#     col("data.windspeed").alias("wind_speed"),
#     col("data.clouds").alias("cloudiness")
# )
#                 .withColumn("timestamp", to_timestamp(col("timestamp")))
#                 )
#
#
# # ---------------------------
# # Define batch processing function
# # ---------------------------
# def process_batch(batch_df, batch_id):
#     try:
#         cleaned_joined_stream = batch_df.select(
#             col("station_id"),  # Use the unqualified column name
#             col("capacity").alias("station_capacity"),
#             coalesce(col("num_bikes_available"), lit(0)).alias("num_bikes_available"),
#             col("timestamp").alias("event_timestamp"),  # Use "timestamp" from the joined DataFrame
#             col("temperature"),
#             col("wind_speed"),
#             col("cloudiness")
#         ).cache()
#         cleaned_joined_stream.printSchema()
#
#         aggregated_df = cleaned_joined_stream.groupBy(
#             window(col("event_timestamp"), "1 hour"),
#             "station_id", "station_capacity", "temperature", "wind_speed", "cloudiness"
#         ).agg(
#             spark_sum("num_bikes_available").alias("total_bikes_available_per_station"),
#             spark_sum("station_capacity").alias("total_capacity_per_station")
#         )
#         aggregated_df.printSchema()
#
#         station_utilization = aggregated_df.withColumn(
#             "station_utilization_rate",
#             (col("total_capacity_per_station") - col("total_bikes_available_per_station")) / col(
#                 "total_capacity_per_station")
#         )
#         station_utilization.printSchema()
#
#         window_spec = Window.partitionBy("window")
#         station_utilization = station_utilization.withColumn(
#             "city_total_capacity",
#             spark_sum("total_capacity_per_station").over(window_spec)
#         ).withColumn(
#             "city_total_bikes_available",
#             spark_sum("total_bikes_available_per_station").over(window_spec)
#         ).withColumn(
#             "citywide_utilization_rate",
#             (col("city_total_capacity") - col("city_total_bikes_available")) / col("city_total_capacity")
#         )
#         station_utilization.printSchema()
#
#         from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg
#         stats_df = station_utilization.groupBy("window").agg(
#             spark_min("station_utilization_rate").alias("min_utilization"),
#             spark_max("station_utilization_rate").alias("max_utilization"),
#             spark_avg("station_utilization_rate").alias("avg_utilization"),
#             spark_stddev("station_utilization_rate").alias("std_dev_docking_station_utilisation")
#         )
#
#         combined_df = station_utilization.select(
#             "window",
#             "station_id",
#             "temperature",
#             "wind_speed",
#             "cloudiness",
#             "station_utilization_rate",
#             "citywide_utilization_rate"
#         )
#         combined_with_stats = combined_df.join(stats_df, on="window", how="inner")
#         final_df = combined_with_stats.withColumn("window_start", col("window.start")) \
#             .withColumn("window_end", col("window.end")) \
#             .drop("window")
#         #
#         # logger.info(f"--- Batch ID: {batch_id} ---")
#         # combined_df.show(truncate=False)
#         # stats_df.show(truncate=False)
#         # combined_with_stats.show(truncate=False)
#         #
#         # combined_with_stats.write.mode("append").parquet("/Users/up1084523@upnet.gr")
#
#         # Αποθήκευση του τελικού DataFrame σε CSV
#         final_df.write.mode("append") \
#             .option("header", "true") \
#             .csv("/home/observer/PycharmProjects/PySparkProject/semifinal_csvs/csvs_df/")
#
#         # Εξαγωγή του συνδυασμένου DataFrame στην κονσόλα
#         logger.info(f"--- Batch ID: {batch_id} ---")
#         combined_df.show(truncate=False)
#         stats_df.show(truncate=False)
#         combined_with_stats.show(truncate=False)
#
#         combined_with_stats.write.mode("append") \
#             .parquet("/home/observer/PycharmProjects/PySparkProject/semifinal_csvs/stats_df")
#
#     except Exception as e:
#         logger.error(f"Error processing batch {batch_id}: {e}")
#
#
# # ---------------------------
# # Set watermarks for event time handling
# # ---------------------------
# stream_1_data = stream_1_data.withWatermark("info_timestamp", "1 minutes")
# stream_2_data = stream_2_data.withWatermark("status_timestamp", "1 minutes")
#
# # ---------------------------
# # Join the streams:
# # First join Station Info (stream_1) with Station Status (stream_2) on the key.
# # Then join with Weather data on event_timestamp = weather_data.timestamp.
# # ---------------------------
# joined_stream = stream_1_data.alias("stream_1").join(
#     stream_2_data.alias("stream_2"),
#     "key",
#     "inner"
# )
#
# # For joining with weather, use the info_timestamp as event_timestamp.
# joined_stream = joined_stream.withColumn("event_timestamp", col("stream_1.info_timestamp"))
#
# joined_stream_weather = joined_stream.join(
#     weather_data,
#     col("event_timestamp") == col("timestamp"),
#     "inner"
# ).select(
#     col("key"),
#     col("stream_1.station_id").alias("station_id"),
#     col("capacity"),
#     col("num_bikes_available"),
#     col("event_timestamp").alias("timestamp"),
#     col("temperature"),
#     col("wind_speed"),
#     col("cloudiness")
# )
#
# # ---------------------------
# # Start streaming query using foreachBatch for processing.
# # ---------------------------
# try:
#     query = joined_stream_weather.writeStream \
#         .foreachBatch(process_batch) \
#         .outputMode("append") \
#         .start()
#     query.awaitTermination()
# except StreamingQueryException as sqe:
#     logger.error(f"Streaming Query Exception: {sqe}")
# except Exception as e:
#     logger.error(f"An error occurred: {e}")
#

import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, avg, max, min, stddev, when, lit, expr, coalesce, window, sum as spark_sum, stddev as spark_stddev
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.streaming import StreamingQueryException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SparkSession with Kafka support
spark = SparkSession.builder \
    .appName("KafkaStreamJoin") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

# Kafka Configuration
kafka_brokers = "150.140.142.67:9094"
topic_1 = "station_info_1084523_2"      # Station Info
topic_2 = "station_status_1084523_2"      # Station Status
topic_3 = "weather_data_1084523_2"        # Weather Data

# ---------------------------
# Define Schemas Based on Message Structures
# ---------------------------
station_status_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True)
])

station_info_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("capacity", IntegerType(), True),
    StructField("metadata", StructType([
        StructField("timestamp", StringType(), True)
    ]), True)
])

weather_schema = StructType([
    StructField("temperature", FloatType(), True),
    StructField("windspeed", FloatType(), True),
    StructField("clouds", FloatType(), True),
    StructField("metadata", StructType([
        StructField("timestamp", StringType(), True)
    ]), True)
])

# ---------------------------
# Read data from Kafka topics
# ---------------------------
kafka_stream_1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", topic_1) \
    .option("startingOffsets", "latest") \
    .load()

kafka_stream_2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", topic_2) \
    .option("startingOffsets", "latest") \
    .load()

kafka_stream_3 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", topic_3) \
    .option("startingOffsets", "latest") \
    .load()

# ---------------------------
# Parse JSON messages using the defined schemas and rename timestamps
# ---------------------------
# For station info, rename the timestamp to info_timestamp.
stream_1_data = (kafka_stream_1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("data", from_json(col("value"), station_info_schema))
    .select(
        col("key").alias("key"),
        col("data.station_id").alias("station_id"),
        col("data.capacity").alias("capacity"),
        col("data.metadata.timestamp").alias("info_timestamp")
    )
    .withColumn("info_timestamp", to_timestamp(col("info_timestamp")))
)

# For station status, rename the timestamp to status_timestamp.
stream_2_data = (kafka_stream_2.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .withColumn("data", from_json(col("value"), station_status_schema))
    .select(
        col("key").alias("key"),
        col("data.station_id").alias("station_id"),
        col("data.num_bikes_available").alias("num_bikes_available"),
        col("data.timestamp").alias("status_timestamp")
    )
    .withColumn("status_timestamp", to_timestamp(col("status_timestamp")))
)

# For weather data, use the schema as defined.
weather_data = (kafka_stream_3.selectExpr("CAST(value AS STRING)")
    .withColumn("data", from_json(col("value"), weather_schema))
    .select(
        col("data.metadata.timestamp").alias("timestamp"),
        col("data.temperature").alias("temperature"),
        col("data.windspeed").alias("wind_speed"),
        col("data.clouds").alias("cloudiness")
    )
    .withColumn("timestamp", to_timestamp(col("timestamp")))
)

# For debugging, print the schema for weather_data.
weather_data.printSchema()

# ---------------------------
# Define batch processing function
# ---------------------------
def process_batch(batch_df, batch_id):
    """
    Process each micro-batch: perform aggregations and output results.
    Skip processing if the batch is empty.
    """
    try:
        # Check if the batch DataFrame is empty; if so, skip processing.
        if batch_df.rdd.isEmpty():
            logger.info(f"Batch {batch_id} is empty. Skipping processing.")
            return

        # Use unqualified column names from the joined DataFrame.
        cleaned_joined_stream = batch_df.select(
            col("station_id"),
            col("capacity").alias("station_capacity"),
            coalesce(col("num_bikes_available"), lit(0)).alias("num_bikes_available"),
            col("timestamp").alias("event_timestamp"),
            col("temperature"),
            col("wind_speed"),
            col("cloudiness")
        ).cache()
        cleaned_joined_stream.printSchema()

        aggregated_df = cleaned_joined_stream.groupBy(
            window(col("event_timestamp"), "1 hour"),
            "station_id", "station_capacity", "temperature", "wind_speed", "cloudiness"
        ).agg(
            spark_sum("num_bikes_available").alias("total_bikes_available_per_station"),
            spark_sum("station_capacity").alias("total_capacity_per_station")
        )
        aggregated_df.printSchema()

        station_utilization = aggregated_df.withColumn(
            "station_utilization_rate",
            (col("total_capacity_per_station") - col("total_bikes_available_per_station")) / col("total_capacity_per_station")
        )
        station_utilization.printSchema()

        window_spec = Window.partitionBy("window")
        station_utilization = station_utilization.withColumn(
            "city_total_capacity",
            spark_sum("total_capacity_per_station").over(window_spec)
        ).withColumn(
            "city_total_bikes_available",
            spark_sum("total_bikes_available_per_station").over(window_spec)
        ).withColumn(
            "citywide_utilization_rate",
            (col("city_total_capacity") - col("city_total_bikes_available")) / col("city_total_capacity")
        )
        station_utilization.printSchema()

        from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg
        stats_df = station_utilization.groupBy("window").agg(
            spark_min("station_utilization_rate").alias("min_utilization"),
            spark_max("station_utilization_rate").alias("max_utilization"),
            spark_avg("station_utilization_rate").alias("avg_utilization"),
            spark_stddev("station_utilization_rate").alias("std_dev_docking_station_utilisation")
        )

        combined_df = station_utilization.select(
            "window",
            "station_id",
            "temperature",
            "wind_speed",
            "cloudiness",
            "station_utilization_rate",
            "citywide_utilization_rate"
        )
        combined_with_stats = combined_df.join(stats_df, on="window", how="inner")
        final_df = combined_with_stats.withColumn("window_start", col("window.start")) \
                                       .withColumn("window_end", col("window.end")) \
                                       .drop("window")

        # Save DataFrame to CSV
        final_df.write.mode("append") \
            .option("header", "true") \
            .csv("/home/observer/PycharmProjects/PySparkProject/final/csv_files/")

        # Output DataFrame to console
        logger.info(f"--- Batch ID: {batch_id} ---")
        combined_df.show(truncate=False)
        stats_df.show(truncate=False)
        combined_with_stats.show(truncate=False)

        # combined_with_stats.write.mode("append") \
        #     .parquet("/home/observer/PycharmProjects/PySparkProject/semifinal_csvs/stats_df")

    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")

# ---------------------------
# Set watermarks for event time handling
# ---------------------------
stream_1_data = stream_1_data.withWatermark("info_timestamp", "1 minutes")
stream_2_data = stream_2_data.withWatermark("status_timestamp", "1 minutes")

# ---------------------------
# Join the streams:
# First join Station Info (stream_1) with Station Status (stream_2) on the key.
# Then join with Weather data on event_timestamp = weather_data.timestamp.
# ---------------------------
joined_stream = stream_1_data.alias("stream_1").join(
    stream_2_data.alias("stream_2"),
    "key",
    "inner"
)

# Use stream_1.info_timestamp as the event timestamp.
joined_stream = joined_stream.withColumn("event_timestamp", col("stream_1.info_timestamp"))

joined_stream_weather = joined_stream.join(
    weather_data,
    col("event_timestamp") == col("timestamp"),
    "inner"
).select(
    col("key"),
    col("stream_1.station_id").alias("station_id"),
    col("capacity"),
    col("num_bikes_available"),
    col("event_timestamp").alias("timestamp"),
    col("temperature"),
    col("wind_speed"),
    col("cloudiness")
)

# ---------------------------
# Start streaming query using foreachBatch for processing.
# ---------------------------
try:
    query = joined_stream_weather.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()
    query.awaitTermination()
except StreamingQueryException as sqe:
    logger.error(f"Streaming Query Exception: {sqe}")
except Exception as e:
    logger.error(f"An error occurred: {e}")
