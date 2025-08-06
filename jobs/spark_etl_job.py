from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# --- Spark Session with Delta Lake and Kafka packages ---
# Packages are now provided via the spark-submit command to avoid version conflicts.
spark = SparkSession.builder \
    .appName("ActivityStreamProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Define Schema for incoming JSON data ---
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("timestamp", StringType(), True) # Read as string, then cast
])

# --- Read from Kafka ---
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_activity_data") \
    .load()

# --- ETL Logic ---
processed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_timestamp", col("timestamp").cast(TimestampType())) \
    .withColumn("processing_timestamp", current_timestamp()) \
    .drop("timestamp")

# --- Write to Delta Lake ---
query = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/delta/checkpoints") \
    .start("/tmp/delta/activity_events")

query.awaitTermination()