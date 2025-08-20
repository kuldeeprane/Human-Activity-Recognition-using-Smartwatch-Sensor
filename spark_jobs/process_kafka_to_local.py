from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- Configuration ---
KAFKA_TOPIC = "har_events"
KAFKA_BROKER = "kafka:9093"  # <-- CORRECTED THIS LINE

# Paths for the new apache/spark image
BRONZE_PATH = "/opt/spark/work-dir/data/delta_tables/bronze"
GOLD_PATH = "/opt/spark/work-dir/data/delta_tables/gold_summary"
CHECKPOINT_PATH = f"{BRONZE_PATH}/_checkpoints"

# Define the schema of the data from Kafka
schema = StructType([
    StructField("user_id", StringType()),
    StructField("activity", StringType()),
    StructField("duration_minutes", IntegerType()),
    StructField("event_time", StringType())
])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Kafka_to_Local_Delta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # 1. Read new messages from Kafka as a batch
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse the JSON from the 'value' column
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                       .select("data.*")
    
    # 2. Write the raw data to the local Bronze Delta table
    parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(availableNow=True) \
        .start(BRONZE_PATH) \
        .awaitTermination()

    print("Successfully synchronized new data from Kafka to the Bronze table.")

    # 3. Create the Gold aggregate table
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    
    gold_df = bronze_df.groupBy("user_id", "activity") \
                       .agg(sum("duration_minutes").alias("total_minutes"))
    
    # Save to Delta Lake
    gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
    print("Successfully updated the Gold summary table.")

    # Save the same summary as a single CSV file
    print(f"Saving Gold summary as a single CSV file to /opt/spark/work-dir/data/csv_summary...")
    gold_df.repartition(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("/opt/spark/work-dir/data/csv_summary")
    print("Successfully saved summary as CSV.")

    gold_df.show()
