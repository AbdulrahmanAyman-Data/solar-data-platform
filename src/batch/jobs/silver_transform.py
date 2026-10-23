import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

BRONZE_STREAMING_PATH = "s3a://bronze/realtime/weather"
BRONZE_HISTORICAL_PATH = "s3a://bronze/historical/weather"
SILVER_PATH = "s3a://silver/weather"

FINAL_COLUMNS = [
    "event_id", "source", "ingestion_timestamp", "observation_timestamp",
    "latitude", "longitude", "location_id", "site_name",
    "temperature_c", "humidity_pct", "wind_speed_ms", "cloud_cover_pct",
    "ghi_w_m2", "dni_w_m2", "dhi_w_m2",
    "observation_date", "observation_hour", "silver_processed_timestamp",
]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SilverWeatherTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Dates/hours are derived in UTC so results never depend on the container's timezone.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_bronze_if_exists(spark, path, label):
    """Return the Delta table at `path`, or None (with a log line) if it does not exist yet."""
    try:
        df = spark.read.format("delta").load(path)
        print(f"[silver] loaded {label} Bronze from {path}")
        return df
    except Exception as exc:  # table missing on a fresh environment
        first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else repr(exc)
        print(f"[silver] skipping {label} Bronze ({path}) - not available yet: {first_line}")
        return None


def main():
    spark = create_spark_session()

    # 1. Read every Bronze source that exists
    sources = [
        df
        for df in (
            read_bronze_if_exists(spark, BRONZE_STREAMING_PATH, "streaming"),
            read_bronze_if_exists(spark, BRONZE_HISTORICAL_PATH, "historical"),
        )
        if df is not None
    ]
    if not sources:
        raise ValueError("No Bronze source exists yet - nothing to process. Is the streaming job running?")

    # 2. Union (same column names in every source, so no mapping is needed)
    combined = sources[0]
    for nxt in sources[1:]:
        combined = combined.unionByName(nxt, allowMissingColumns=True)

    # 3. Range validation - NULL is allowed through, only out-of-range non-null values are dropped
    def in_range(column, low, high):
        c = F.col(column)
        return c.isNull() | ((c >= low) & (c <= high))

    validated = combined.filter(
        in_range("latitude", -90, 90)
        & in_range("longitude", -180, 180)
        & in_range("temperature_c", -90, 60)
        & in_range("humidity_pct", 0, 100)
        & in_range("cloud_cover_pct", 0, 100)
        & in_range("ghi_w_m2", 0, 1500)
        & in_range("dni_w_m2", 0, 1500)
    )

    # 4. Full-table dedup: keep the latest-ingested row per (source, location, observation time)
    w = Window.partitionBy("source", "location_id", "observation_timestamp").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    deduped = validated.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    # 5-7. Derive date/hour, drop transport metadata, stamp processing time
    silver = (
        deduped.withColumn("observation_date", F.to_date("observation_timestamp"))
        .withColumn("observation_hour", F.hour("observation_timestamp"))
        .drop("kafka_timestamp")
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )
    silver = silver.select(*[c for c in FINAL_COLUMNS if c in silver.columns])

    # Full overwrite (intentional - see module docstring)
    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
    )

    rows = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"[silver] done - {rows} rows written to {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
