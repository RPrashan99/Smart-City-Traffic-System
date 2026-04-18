from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum as _sum, avg as _avg, 
    expr, lit, to_json, struct, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, IntegerType, DoubleType
)

spark = SparkSession.builder \
    .appName("SmartCityTrafficStreaming") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "4")
spark.sparkContext.setLogLevel("WARN")

# Schema matching producer exactly
schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("vehicle_count", IntegerType(), False),
    StructField("avg_speed", DoubleType(), False)
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "traffic_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON payload
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select(
        col("data.sensor_id"),
        col("data.vehicle_count"),
        col("data.avg_speed"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    ) \
    .withWatermark("event_time", "10 minutes")

# ============================================
# 1. AGGREGATION STREAM (5-minute windows)
# ============================================
agg_df = parsed_df \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        _sum("vehicle_count").alias("total_vehicles"),
        _avg("avg_speed").alias("avg_speed_window")
    ) \
    .withColumn(
        "congestion_index",
        expr("total_vehicles / nullif(avg_speed_window, 0.0)")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "sensor_id",
        "total_vehicles",
        "avg_speed_window",
        "congestion_index"
    )

# Write aggregations to HDFS
agg_query = agg_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "hdfs://namenode:8020/data/traffic/agg_5min") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/traffic_agg_5min") \
    .trigger(processingTime='1 minute') \
    .start()

print("✅ Aggregation stream started - writing to HDFS")

# ============================================
# 2. CRITICAL ALERTS STREAM (avg_speed < 10)
# ============================================
alerts_df = parsed_df \
    .filter(col("avg_speed") < 10.0) \
    .withColumn("alert_time", current_timestamp()) \
    .withColumn("reason", lit("avg_speed_below_10_kmph")) \
    .select(
        col("alert_time"),
        col("event_time").alias("detected_at"),
        "sensor_id",
        "vehicle_count",
        "avg_speed",
        "reason"
    )

# Write alerts to HDFS
alerts_hdfs = alerts_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "hdfs://namenode:8020/data/traffic/alerts") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/traffic_alerts") \
    .trigger(processingTime='10 seconds') \
    .start()

print("✅ Alert HDFS stream started")

# Write alerts to Kafka topic (REQUIRED for the task)
alerts_kafka = alerts_df.select(
    to_json(struct("*")).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "critical-alerts") \
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/alert-kafka") \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

print("✅ Alert Kafka stream started - sending to 'critical-alerts' topic")

# ============================================
# 3. CONSOLE OUTPUT FOR DEBUGGING (optional)
# ============================================
console_agg = agg_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 10) \
    .trigger(processingTime='30 seconds') \
    .start()

print("✅ Console output started")

# Wait for all streams
spark.streams.awaitAnyTermination()