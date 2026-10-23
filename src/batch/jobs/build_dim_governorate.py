"""
Gold dimension: governorate bridge table  ->  s3a://gold/dim_governorate   (27 rows)

One row per governorate. Static, hand-maintained reference data (the same way
monitored_locations.json is hand-maintained) 

Full overwrite on every run. Not part of the recurring Airflow schedule - run it by hand
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

DIM_PATH = "s3a://gold/dim_governorate"

# location_id -> gis_slug, ONLY for governorates the GIS pipeline has processed.
# Example (fill in later, not now):  GIS_SLUGS = {"aswan": "aswan", "minya": "al_minya"}
GIS_SLUGS = {}

# (location_id, name_en, name_ar)
GOVERNORATES = [
    ("cairo", "Cairo", "\u0627\u0644\u0642\u0627\u0647\u0631\u0629"),
    ("alexandria", "Alexandria", "\u0627\u0644\u0625\u0633\u0643\u0646\u062f\u0631\u064a\u0629"),
    ("port-said", "Port Said", "\u0628\u0648\u0631\u0633\u0639\u064a\u062f"),
    ("suez", "Suez", "\u0627\u0644\u0633\u0648\u064a\u0633"),
    ("damietta", "Damietta", "\u062f\u0645\u064a\u0627\u0637"),
    ("dakahlia", "Dakahlia", "\u0627\u0644\u062f\u0642\u0647\u0644\u064a\u0629"),
    ("sharqia", "Sharqia", "\u0627\u0644\u0634\u0631\u0642\u064a\u0629"),
    ("qalyubia", "Qalyubia", "\u0627\u0644\u0642\u0644\u064a\u0648\u0628\u064a\u0629"),
    ("kafr-el-sheikh", "Kafr El Sheikh", "\u0643\u0641\u0631\u0020\u0627\u0644\u0634\u064a\u062e"),
    ("gharbia", "Gharbia", "\u0627\u0644\u063a\u0631\u0628\u064a\u0629"),
    ("monufia", "Monufia", "\u0627\u0644\u0645\u0646\u0648\u0641\u064a\u0629"),
    ("beheira", "Beheira", "\u0627\u0644\u0628\u062d\u064a\u0631\u0629"),
    ("ismailia", "Ismailia", "\u0627\u0644\u0625\u0633\u0645\u0627\u0639\u064a\u0644\u064a\u0629"),
    ("giza", "Giza", "\u0627\u0644\u062c\u064a\u0632\u0629"),
    ("beni-suef", "Beni Suef", "\u0628\u0646\u064a\u0020\u0633\u0648\u064a\u0641"),
    ("faiyum", "Faiyum", "\u0627\u0644\u0641\u064a\u0648\u0645"),
    ("minya", "Minya", "\u0627\u0644\u0645\u0646\u064a\u0627"),
    ("asyut", "Asyut", "\u0623\u0633\u064a\u0648\u0637"),
    ("sohag", "Sohag", "\u0633\u0648\u0647\u0627\u062c"),
    ("qena", "Qena", "\u0642\u0646\u0627"),
    ("luxor", "Luxor", "\u0627\u0644\u0623\u0642\u0635\u0631"),
    ("aswan", "Aswan", "\u0623\u0633\u0648\u0627\u0646"),
    ("red-sea", "Red Sea", "\u0627\u0644\u0628\u062d\u0631\u0020\u0627\u0644\u0623\u062d\u0645\u0631"),
    ("new-valley", "New Valley", "\u0627\u0644\u0648\u0627\u062f\u064a\u0020\u0627\u0644\u062c\u062f\u064a\u062f"),
    ("matrouh", "Matrouh", "\u0645\u0637\u0631\u0648\u062d"),
    ("north-sinai", "North Sinai", "\u0634\u0645\u0627\u0644\u0020\u0633\u064a\u0646\u0627\u0621"),
    ("south-sinai", "South Sinai", "\u062c\u0646\u0648\u0628\u0020\u0633\u064a\u0646\u0627\u0621"),
]


def main():
    spark = (
        SparkSession.builder.appName("BuildDimGovernorate")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # Explicit schema: gis_slug is NULL for every row today, so Spark could not infer its type.
    schema = StructType(
        [
            StructField("location_id", StringType(), False),
            StructField("gis_slug", StringType(), True),
            StructField("name_ar", StringType(), True),
            StructField("name_en", StringType(), True),
        ]
    )
    rows = [(lid, GIS_SLUGS.get(lid), ar, en) for (lid, en, ar) in GOVERNORATES]
    df = spark.createDataFrame(rows, schema).withColumn("gold_processed_timestamp", F.current_timestamp())

    assert df.count() == 27, "dim_governorate must contain exactly 27 governorates"

    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(DIM_PATH)
    print(f"[dim_governorate] {df.count()} rows -> {DIM_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
