"""
Builds the gold_dim_governorate reference table: a small (27-row), mostly-
static dimension that bridges the two naming schemes already in use across
the project's Gold layer.

Why this exists: the weather pipeline (poller.py / nasa_power_loader.py /
silver_transform.py / gold_transform.py) keys everything on `location_id`
as defined in monitored_locations.json (e.g. "minya", "red-sea",
"south-sinai"). The GIS pipeline (gis_zones_to_gold.py /
gis_candidate_sites_to_gold.py) keys everything on `governorate_id` as
produced by the GIS scripts' own slugging convention (e.g. "al_minya",
"al_bahr_al_ahmar", "janub_sina") - these are NOT the same strings for
the same governorate. Every consumer that needs to join GIS output against
weather output (Superset's candidate_sites_enriched dataset, the Power BI
model) was independently re-typing the same 9-row VALUES(...) mapping.
This table replaces every one of those copies with a single source of
truth, joined at query time - it changes nothing about how the four
existing Gold-writing jobs read or write their own tables.

Columns:
  location_id   - canonical key, matches monitored_locations.json /
                   gold_solar_potential_by_location.location_id /
                   gold_solar_radiation_daily.location_id /
                   gold_peak_production_hours.location_id (all 27 rows)
  gis_slug      - matches gold_solar_eligible_zones.governorate_id /
                   gold_solar_candidate_sites.governorate_id (only the
                   governorates the GIS pipeline has actually processed so
                   far - NULL for the rest, not a bug, just not run yet)
  name_en       - English display name, for labels/tooltips in Superset and
                   Power BI (English on purpose: avoids Arabic text being
                   garbled by ANSI ODBC drivers when read from Power BI)

This is reference data, not a derived aggregate, so unlike Silver/Gold's
weather jobs there is no upstream source to read from - the rows are
declared directly in this script, the same way monitored_locations.json is
hand-maintained. Re-run whenever a governorate name changes or a new GIS
slug is added; safe to re-run any time (full overwrite, 27 rows).

NOTE: this job overwrites the whole table with 3 data columns, so the
geometry columns are dropped. Always re-run add_geometry_to_dim_governorate.py
right after this job to add geometry_wkt / geometry_geojson back.

Run manually via spark-submit (inside solar-spark-submit-client):

    spark-submit \
      --master spark://solar-spark-master:7077 \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      build_dim_governorate.py
"""
from pyspark.sql.functions import current_timestamp

from common.spark_session import build_spark_session

GOLD_LOCATION = "s3a://gold/dim_governorate"

# (location_id, gis_slug, name_en)
# location_id: from src/ingestion/streaming_poller/config/monitored_locations.json
# gis_slug: from gis/data/processed/<slug>/ - None where GIS hasn't run yet
GOVERNORATES = [
    ("cairo", None, "Cairo"),
    ("alexandria", None, "Alexandria"),
    ("port-said", None, "Port Said"),
    ("suez", "as_suways", "Suez"),
    ("damietta", None, "Damietta"),
    ("dakahlia", None, "Dakahlia"),
    ("sharqia", None, "Sharqia"),
    ("qalyubia", None, "Qalyubia"),
    ("kafr-el-sheikh", None, "Kafr El Sheikh"),
    ("gharbia", None, "Gharbia"),
    ("monufia", None, "Monufia"),
    ("beheira", None, "Beheira"),
    ("ismailia", None, "Ismailia"),
    ("giza", None, "Giza"),
    ("beni-suef", None, "Beni Suef"),
    ("faiyum", None, "Faiyum"),
    ("minya", "al_minya", "Minya"),
    ("asyut", "asyut", "Asyut"),
    ("sohag", "suhaj", "Sohag"),
    ("qena", "qina", "Qena"),
    # al_uqsur was processed by the GIS pipeline but produced zero
    # candidate sites after the area/distance cutoffs - the slug is real,
    # it just won't ever match a row in gold_solar_candidate_sites.
    ("luxor", "al_uqsur", "Luxor"),
    ("aswan", "aswan", "Aswan"),
    ("red-sea", "al_bahr_al_ahmar", "Red Sea"),
    ("new-valley", None, "New Valley"),
    ("matrouh", None, "Matrouh"),
    ("north-sinai", None, "North Sinai"),
    ("south-sinai", "janub_sina", "South Sinai"),
]


def main() -> None:
    spark = build_spark_session(app_name="solar-build-dim-governorate", with_hive_metastore=False)
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.createDataFrame(GOVERNORATES, ["location_id", "gis_slug", "name_en"])
        .withColumn("gold_processed_timestamp", current_timestamp())
    )
    row_count = df.count()

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_LOCATION)
    )
    print(f"[build_dim_governorate] wrote {row_count} row(s) to {GOLD_LOCATION}")

    spark.stop()


if __name__ == "__main__":
    main()