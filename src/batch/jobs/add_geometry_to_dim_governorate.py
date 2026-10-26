from pyspark.sql.functions import current_timestamp

from common.spark_session import build_spark_session

GOLD_PATH = "s3a://gold/dim_governorate"

BOUNDARIES_PATH = (
    "/opt/spark-notebooks/gis/data/processed/"
    "governorate_boundaries.parquet"
)

# Columns that come from the boundaries dataset. They may already exist in the
# Gold table from a previous run, so they are dropped before the join to avoid
# duplicate column names (Delta rejects duplicates on write).
GEOMETRY_COLS = ["geometry_wkt", "geometry_geojson"]


def main():
    spark = build_spark_session(
        app_name="solar-dim-governorate-geometry",
        with_hive_metastore=False,
    )

    try:
        # drop() silently ignores columns that do not exist,
        # so this stays idempotent across re-runs.
        dim_df = (
            spark.read.format("delta")
            .load(GOLD_PATH)
            .drop("gold_processed_timestamp", *GEOMETRY_COLS)
        )

        # Read governorate boundaries, keeping only the columns we need.
        geom_df = spark.read.parquet(BOUNDARIES_PATH).select(
            "location_id", *GEOMETRY_COLS
        )

        before_count = dim_df.count()

        merged = (
            dim_df.join(
                geom_df,
                on="location_id",
                how="left",
            )
            .withColumn(
                "gold_processed_timestamp",
                current_timestamp(),
            )
        )

        after_count = merged.count()

        assert before_count == after_count == 27, (
            f"Governorate count changed: "
            f"{before_count} -> {after_count}"
        )

        missing = (
            merged.filter(merged["geometry_wkt"].isNull())
            .select("location_id")
            .collect()
        )

        if missing:
            print(
                "Warning: Governorates without geometry:",
                [row.location_id for row in missing],
            )

        (
            merged.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_PATH)
        )

        print(
            f"[add_geometry_to_dim_governorate] "
            f"Wrote {after_count} rows; schema updated"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()