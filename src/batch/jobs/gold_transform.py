import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

SILVER_PATH = "s3a://silver/weather"
DAILY_PATH = "s3a://gold/solar_radiation_daily"
POTENTIAL_PATH = "s3a://gold/solar_potential_by_location"
PEAK_HOURS_PATH = "s3a://gold/peak_production_hours"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("GoldWeatherTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def r2(column):
    """avg / max values are rounded to 2 decimals in every Gold table."""
    return F.round(column, 2)


def write_gold(spark, df, path, name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    rows = spark.read.format("delta").load(path).count()
    print(f"[gold] {name}: {rows} rows -> {path}")


def main():
    spark = create_spark_session()
    silver = spark.read.format("delta").load(SILVER_PATH)

    # ---- 3.4.a  gold_solar_radiation_daily ---------------------------------
    daily = (
        silver.groupBy("location_id", "source", "observation_date")
        .agg(
            F.max("site_name").alias("site_name"),
            r2(F.avg("ghi_w_m2")).alias("avg_ghi_w_m2"),
            r2(F.max("ghi_w_m2")).alias("max_ghi_w_m2"),
            r2(F.avg("dni_w_m2")).alias("avg_dni_w_m2"),
            r2(F.max("dni_w_m2")).alias("max_dni_w_m2"),
            r2(F.avg("temperature_c")).alias("avg_temperature_c"),
            r2(F.max("temperature_c")).alias("max_temperature_c"),
            r2(F.avg("humidity_pct")).alias("avg_humidity_pct"),
            r2(F.avg("cloud_cover_pct")).alias("avg_cloud_cover_pct"),
            F.count(F.lit(1)).alias("observation_count"),
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
        .select(
            "location_id", "site_name", "source", "observation_date",
            "avg_ghi_w_m2", "max_ghi_w_m2", "avg_dni_w_m2", "max_dni_w_m2",
            "avg_temperature_c", "max_temperature_c", "avg_humidity_pct",
            "avg_cloud_cover_pct", "observation_count", "gold_processed_timestamp",
        )
    )
    write_gold(spark, daily, DAILY_PATH, "gold_solar_radiation_daily")

    # ---- 3.4.b  gold_solar_potential_by_location ---------------------------
    rank_within_source = Window.partitionBy("source").orderBy(F.col("avg_ghi_w_m2").desc())
    potential = (
        silver.groupBy("location_id", "source")
        .agg(
            F.max("site_name").alias("site_name"),
            r2(F.avg("ghi_w_m2")).alias("avg_ghi_w_m2"),
            r2(F.max("ghi_w_m2")).alias("max_ghi_w_m2"),
            r2(F.avg("dni_w_m2")).alias("avg_dni_w_m2"),
            r2(F.avg("cloud_cover_pct")).alias("avg_cloud_cover_pct"),
            F.countDistinct("observation_date").alias("days_observed"),
            F.count(F.lit(1)).alias("total_observations"),
        )
        .withColumn("solar_potential_rank", F.dense_rank().over(rank_within_source))
        .withColumn("gold_processed_timestamp", F.current_timestamp())
        .select(
            "location_id", "site_name", "source", "avg_ghi_w_m2", "max_ghi_w_m2",
            "avg_dni_w_m2", "avg_cloud_cover_pct", "days_observed", "total_observations",
            "solar_potential_rank", "gold_processed_timestamp",
        )
    )
    write_gold(spark, potential, POTENTIAL_PATH, "gold_solar_potential_by_location")

    # ---- 3.4.c  gold_peak_production_hours (open-meteo only) ---------------
    rank_within_location = Window.partitionBy("location_id").orderBy(F.col("avg_ghi_w_m2").desc())
    peak_hours = (
        silver.filter(F.col("source") == "open-meteo")
        .groupBy("location_id", "observation_hour")
        .agg(
            F.max("site_name").alias("site_name"),
            r2(F.avg("ghi_w_m2")).alias("avg_ghi_w_m2"),
            r2(F.avg("dni_w_m2")).alias("avg_dni_w_m2"),
            r2(F.min("ghi_w_m2")).alias("min_ghi_w_m2"),
            F.count(F.lit(1)).alias("observation_count"),
        )
        .withColumn("peak_hour_rank", F.dense_rank().over(rank_within_location))
        .withColumn("gold_processed_timestamp", F.current_timestamp())
        .select(
            "location_id", "site_name", "observation_hour", "avg_ghi_w_m2", "avg_dni_w_m2",
            "min_ghi_w_m2", "observation_count", "peak_hour_rank", "gold_processed_timestamp",
        )
    )
    write_gold(spark, peak_hours, PEAK_HOURS_PATH, "gold_peak_production_hours")

    print("[gold] all three Gold tables written successfully")
    spark.stop()


if __name__ == "__main__":
    main()
