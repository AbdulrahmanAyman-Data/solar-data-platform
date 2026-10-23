import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# MinIO S3 Credentials from environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
KAFKA_BROKER = "solar-kafka:9092"
KAFKA_TOPIC = "weather.raw"
BRONZE_URI = "s3a://bronze/realtime/weather"
CHECKPOINT_URI = "s3a://checkpoints/weather_raw_to_bronze"

# Define schema exactly matching the nasa_loader.py structure
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("site_name", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("observation_timestamp", TimestampType(), True),
    StructField("ghi_w_m2", DoubleType(), True),
    StructField("dni_w_m2", DoubleType(), True),
    StructField("dhi_w_m2", DoubleType(), True),
    StructField("wind_speed_ms", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("cloud_cover_pct", DoubleType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])

spark = SparkSession.builder \
    .appName("KafkaToBronze") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read Kafka stream
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka 'value' column
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write stream to MinIO Bronze Delta Table
query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_URI) \
    .trigger(processingTime="1 minute") \
    .start(BRONZE_URI)

query.awaitTermination()
