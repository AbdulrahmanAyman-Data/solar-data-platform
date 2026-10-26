"""
Phase 1 - finalize and stage to MinIO.

Maps the working h3_cells_<slug>.parquet columns onto the exact
gold_solar_eligible_zones schema documented in the expansion plan
(part 2.4), adds the columns that are constant/derived at this stage
(governorate_id, is_protected_area, is_eligible, gold_processed_timestamp),
and uploads the result to MinIO as a staging Parquet file.

Also carries distance_to_water_km and distance_to_urban_agri_km
(from compute_water_distance.py / compute_urban_agri_distance.py) - these
were added after this table's original schema was written, as
descriptive attributes on every eligible cell, the same way
distance_to_road_km/distance_to_grid_km already were. This table still
represents the FULL eligible screening layer, not the buildable
shortlist: unlike filter_candidate_sites.py, nothing here is excluded
for being close to a road/grid/water/urban-agri area - that filtering
lives entirely in the separate candidate-sites table produced by
finalize_candidate_sites.py, so "eligible" (this table) and "buildable"
(that one) stay two different, non-conflated things.

is_protected_area and is_eligible are always False/True respectively for
every row here: this pipeline only ever emits cells that already passed
the eligibility filter (WDPA-excluded and non-eligible cells were
dropped upstream, never reached H3 generation), so there is nothing to
compute here for those columns.

This does NOT write to Delta Lake directly - per the project's
architecture, a small Spark job (solar-spark-submit-client) reads this
staged Parquet and writes it as the actual gold_solar_eligible_zones
Delta table, keeping "every Delta write goes through the same tested
path" true.

Usage:
    python3 finalize_and_upload.py --governorate Aswan

Requires h3_cells_<slug>.parquet to already have distance_to_water_km
(compute_water_distance.py) and distance_to_urban_agri_km
(compute_urban_agri_distance.py) in addition to the base columns from
earlier Phase 1 steps.

Output: uploads to s3a://gis-staging/eligible_zones/<slug>/h3_cells_<slug>.parquet
"""

import argparse
from datetime import datetime, timezone
import os

import boto3
import pandas as pd

PROCESSED_DIR = "/opt/gis/data/processed"
STAGING_BUCKET = "gis-staging"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")

REQUIRED_COLUMNS = ["distance_to_water_km", "distance_to_urban_agri_km"]


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def build_gold_schema_df(df, governorate_slug):
    out = pd.DataFrame()
    out["zone_id"] = df["h3_index"]
    out["governorate_id"] = governorate_slug
    out["centroid_lat"] = df["centroid_lat"]
    out["centroid_lon"] = df["centroid_lon"]
    out["elevation_m"] = df["elevation_m"]
    out["slope_deg"] = df["slope_deg"]
    out["land_cover_class"] = df["land_cover_class"]
    out["distance_to_grid_km"] = df["distance_to_grid_km"]
    out["distance_to_road_km"] = df["distance_to_road_km"]
    out["distance_to_water_km"] = df["distance_to_water_km"]
    out["distance_to_urban_agri_km"] = df["distance_to_urban_agri_km"]
    out["is_protected_area"] = False
    out["is_eligible"] = True
    out["solar_radiation_lta_ghi"] = df["ghi_kwh_m2_day"]
    out["solar_radiation_lta_dni"] = df["dni_kwh_m2_day"]
    out["site_suitability_score"] = df["site_suitability_score"]
    out["gold_processed_timestamp"] = datetime.now(timezone.utc)
    return out


def ensure_bucket(s3_client, bucket):
    existing = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    if bucket not in existing:
        s3_client.create_bucket(Bucket=bucket)
        print(f"Created bucket '{bucket}'")


def run(governorate):
    slug = slugify(governorate)
    cells_path = f"{PROCESSED_DIR}/{slug}/h3_cells_{slug}.parquet"

    print(f"Reading {cells_path}")
    df = pd.read_parquet(cells_path)
    print(f"Rows: {len(df)}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"This file is missing {missing} - run compute_water_distance.py and/or "
            "compute_urban_agri_distance.py for this governorate first, then re-run "
            "this script."
        )

    gold_df = build_gold_schema_df(df, slug)

    local_out = f"/tmp/h3_cells_{slug}_staged.parquet"
    gold_df.to_parquet(local_out, index=False)
    print(f"Wrote {local_out} ({len(gold_df)} rows, {len(gold_df.columns)} columns)")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )
    ensure_bucket(s3, STAGING_BUCKET)

    key = f"eligible_zones/{slug}/h3_cells_{slug}.parquet"
    s3.upload_file(local_out, STAGING_BUCKET, key)
    print(f"Uploaded to s3a://{STAGING_BUCKET}/{key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finalize schema and upload staging Parquet to MinIO")
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
