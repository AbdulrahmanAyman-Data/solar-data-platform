"""
Promotes GIS-staged Parquet (written by gis/scripts/finalize_and_upload.py,
one file per governorate under s3a://gis-staging/eligible_zones/<slug>/)
into the gold_solar_eligible_zones Delta table.

This is the only place that writes gold_solar_eligible_zones - the GIS
container (solar-gis-processor) never touches Delta Lake directly, keeping
"every Delta write goes through the same tested Spark path" true, same as
the rest of the project's Gold layer.

Partitioned by governorate_id, using a replaceWhere overwrite scoped to
just that governorate's partition - re-running for one governorate (or
adding a new one later) never touches other governorates' existing rows.
Uses mergeSchema (rather than overwriteSchema, which Delta rejects
alongside replaceWhere) so new nullable columns added upstream to
h3_cells_<slug>.parquet - e.g. distance_to_water_km,
distance_to_urban_agri_km - don't break existing partitions that were
written before those columns existed.

Run manually via spark-submit (inside solar-spark-submit-client):

    spark-submit \
      --master spark://solar-spark-master:7077 \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      gis_zones_to_gold.py --governorate aswan
"""

import argparse

from delta.tables import DeltaTable
from common.spark_session import build_spark_session

GOLD_LOCATION = "s3a://gold/solar_eligible_zones"


def run(governorate_slug):
    spark = build_spark_session(app_name="solar-gis-zones-to-gold", with_hive_metastore=False)
    spark.sparkContext.setLogLevel("WARN")

    staging_path = f"s3a://gis-staging/eligible_zones/{governorate_slug}/h3_cells_{governorate_slug}.parquet"
    print(f"[gis_zones_to_gold] reading {staging_path}")
    df = spark.read.parquet(staging_path)
    row_count = df.count()
    print(f"[gis_zones_to_gold] {row_count} rows for governorate_id='{governorate_slug}'")

    table_exists = DeltaTable.isDeltaTable(spark, GOLD_LOCATION)

    writer = df.write.format("delta")
    if table_exists:
        print(f"[gis_zones_to_gold] table exists, overwriting only governorate_id='{governorate_slug}' partition")
        # mergeSchema (not overwriteSchema - that option is rejected together
        # with replaceWhere) lets this tolerate the table gaining new nullable
        # columns over time, e.g. distance_to_water_km/distance_to_urban_agri_km
        # were added to h3_cells_<slug>.parquet after this table was first
        # created, and older governorate partitions never got backfilled with
        # them.
        (
            writer.mode("overwrite")
            .option("replaceWhere", f"governorate_id = '{governorate_slug}'")
            .option("mergeSchema", "true")
            .save(GOLD_LOCATION)
        )
    else:
        print("[gis_zones_to_gold] table does not exist yet, creating it")
        writer.mode("overwrite").partitionBy("governorate_id").save(GOLD_LOCATION)

    print(f"[gis_zones_to_gold] wrote {row_count} rows to {GOLD_LOCATION}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Promote GIS-staged Parquet to the gold_solar_eligible_zones Delta table")
    parser.add_argument("--governorate", required=True, help="governorate slug, e.g. aswan (matches the staging path)")
    args = parser.parse_args()
    run(args.governorate)
