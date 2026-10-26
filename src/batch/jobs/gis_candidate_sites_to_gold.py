"""
Promotes GIS-staged Parquet (written by gis/scripts/finalize_candidate_sites.py,
one file per governorate under s3a://gis-staging/candidate_sites/<slug>/)
into the gold_solar_candidate_sites Delta table.

This is a SEPARATE Gold table from gold_solar_eligible_zones (written by
gis_zones_to_gold.py) - eligible_zones is the full per-cell screening
layer, candidate_sites is the buildable-unit shortlist after all hard
distance/area cutoffs in filter_candidate_sites.py. Kept as two tables on
purpose so "eligible" and "buildable" are never conflated (see
finalize_candidate_sites.py's docstring for the full reasoning).

This is the only place that writes gold_solar_candidate_sites - the GIS
container (solar-gis-processor) never touches Delta Lake directly, keeping
"every Delta write goes through the same tested Spark path" true, same as
the rest of the project's Gold layer.

Partitioned by governorate_id, using a replaceWhere overwrite scoped to
just that governorate's partition - re-running for one governorate (or
adding a new one later) never touches other governorates' existing rows.

Run manually via spark-submit (inside solar-spark-submit-client):

    spark-submit \
      --master spark://solar-spark-master:7077 \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      gis_candidate_sites_to_gold.py --governorate aswan
"""

import argparse

from delta.tables import DeltaTable
from common.spark_session import build_spark_session

GOLD_LOCATION = "s3a://gold/solar_candidate_sites"


def run(governorate_slug):
    spark = build_spark_session(app_name="solar-gis-candidate-sites-to-gold", with_hive_metastore=False)
    spark.sparkContext.setLogLevel("WARN")

    staging_path = (
        f"s3a://gis-staging/candidate_sites/{governorate_slug}/"
        f"candidate_sites_{governorate_slug}.parquet"
    )
    print(f"[gis_candidate_sites_to_gold] reading {staging_path}")
    df = spark.read.parquet(staging_path)
    row_count = df.count()
    print(f"[gis_candidate_sites_to_gold] {row_count} rows for governorate_id='{governorate_slug}'")

    table_exists = DeltaTable.isDeltaTable(spark, GOLD_LOCATION)

    writer = df.write.format("delta")
    if table_exists:
        print(f"[gis_candidate_sites_to_gold] table exists, overwriting only governorate_id='{governorate_slug}' partition")
        # mergeSchema (not overwriteSchema - Delta rejects that option
        # together with replaceWhere) so this tolerates new nullable
        # columns being added to candidate_sites_<slug>.parquet later,
        # the same schema-evolution issue hit in gis_zones_to_gold.py.
        (
            writer.mode("overwrite")
            .option("replaceWhere", f"governorate_id = '{governorate_slug}'")
            .option("mergeSchema", "true")
            .save(GOLD_LOCATION)
        )
    else:
        print("[gis_candidate_sites_to_gold] table does not exist yet, creating it")
        writer.mode("overwrite").partitionBy("governorate_id").save(GOLD_LOCATION)

    print(f"[gis_candidate_sites_to_gold] wrote {row_count} rows to {GOLD_LOCATION}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Promote GIS-staged candidate-sites Parquet to the gold_solar_candidate_sites Delta table")
    parser.add_argument("--governorate", required=True, help="governorate slug, e.g. aswan (matches the staging path)")
    args = parser.parse_args()
    run(args.governorate)
