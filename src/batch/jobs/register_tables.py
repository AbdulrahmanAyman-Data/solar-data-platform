"""
Register the existing Delta tables in the Hive Metastore (database `solar`) so Trino can query them.

Run by hand (it is NOT part of the recurring Airflow schedule):
    spark-submit ... /opt/spark-notebooks/src/batch/jobs/register_tables.py
"""
import os

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://solar-hive-metastore:9083")

DATABASE = "solar"
DATABASE_LOCATION = "s3a://warehouse/solar.db"

# table name -> Delta location
TABLES = {
    "bronze_weather": "s3a://bronze/realtime/weather",              # streaming Bronze (kafka_to_bronze.py)
    "bronze_historical_weather": "s3a://bronze/historical/weather",  # NASA POWER Bronze (when it exists)
    "silver_weather": "s3a://silver/weather",
    "gold_solar_radiation_daily": "s3a://gold/solar_radiation_daily",
    "gold_solar_potential_by_location": "s3a://gold/solar_potential_by_location",
    "gold_peak_production_hours": "s3a://gold/peak_production_hours",
    "gold_dim_governorate": "s3a://gold/dim_governorate",
    # GIS tables - uncomment once the GIS pipeline has promoted them:
    # "gold_solar_eligible_zones": "s3a://gold/solar_eligible_zones",
    # "gold_solar_candidate_sites": "s3a://gold/solar_candidate_sites",
}


def main():
    spark = (
        SparkSession.builder.appName("RegisterTables")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", METASTORE_URI)
        .config("spark.sql.warehouse.dir", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE} LOCATION '{DATABASE_LOCATION}'")

    registered, skipped = [], []
    for name, location in TABLES.items():
        try:
            spark.read.format("delta").load(location).limit(1).collect()  # is there a Delta table there?
        except Exception as exc:
            first_line = str(exc).strip().splitlines()[0] if str(exc).strip() else repr(exc)
            print(f"[register] SKIP {name}: no Delta table at {location} yet ({first_line})")
            skipped.append(name)
            continue
        spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{name} USING DELTA LOCATION '{location}'")
        print(f"[register] OK   {DATABASE}.{name} -> {location}")
        registered.append(name)

    print(f"[register] registered/confirmed: {registered}")
    print(f"[register] skipped (not created yet): {skipped}")
    spark.sql(f"SHOW TABLES IN {DATABASE}").show(50, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
