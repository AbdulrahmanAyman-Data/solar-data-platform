"""
Phase 1 - finalize candidate sites and stage to MinIO.

Turns candidate_sites_<slug>.parquet (the buildable-unit shortlist
produced by filter_candidate_sites.py) into a self-contained, gold-ready
table: each site's H3 cells are dissolved into one polygon (same
approach as export_candidate_sites_polygons.py), encoded as both a WKT
string and a GeoJSON string so downstream consumers (Trino ST_GeomFromText,
a deck.gl map in Superset, etc.) don't need a spatial-aware Parquet
reader, and the per-site attributes are selected/renamed into the agreed
schema below.

This is a SEPARATE staging table from finalize_and_upload.py (which
stages the full h3_cells screening layer into gold_solar_eligible_zones).
The two represent different things on purpose: every individually-
eligible cell vs. the buildable-unit shortlist after all hard distance/
area cutoffs - keeping them as separate tables avoids conflating
"eligible" with "buildable".

Schema (one row per candidate site):
    site_id, governorate_id, geometry_wkt, geometry_geojson, area_km2,
    cell_count, centroid_lat, centroid_lon,
    distance_to_nearest_road_km, distance_to_nearest_grid_km,
    distance_to_nearest_water_km, distance_to_nearest_urban_km,
    mean_slope_deg, min_slope_deg, max_slope_deg,
    mean_elevation_m, min_elevation_m, max_elevation_m,
    avg_ghi_kwh_m2_day, avg_dni_kwh_m2_day, site_suitability_score,
    gold_processed_timestamp

Note: the "distance_to_nearest_*" columns are the MINIMUM of that
distance across the site's own H3 cells (the site's closest approach to
that feature) - not the average across the site, which is what
candidate_sites_<slug>.parquet's avg_distance_to_*_km columns give.
"Nearest" is the more honest number for a site-level proximity claim.

Like finalize_and_upload.py, this does NOT write to Delta Lake directly
- a small Spark job reads this staged Parquet and writes the actual
Delta table, keeping "every Delta write goes through the same tested
path" true.

Usage:
    python3 finalize_candidate_sites.py --governorate Aswan

Output: uploads to
    s3a://gis-staging/candidate_sites/<slug>/candidate_sites_<slug>.parquet
"""

import argparse
import json
import os
from datetime import datetime, timezone

import boto3
import h3
import pandas as pd
from shapely.geometry import Polygon, mapping
from shapely.ops import unary_union

PROCESSED_DIR = "/opt/gis/data/processed"
STAGING_BUCKET = "gis-staging"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://solar-minio:9000")
MINIO_ROOT_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123")

REQUIRED_COLUMNS = [
    "h3_cells",
    "min_distance_to_road_km",
    "min_distance_to_grid_km",
    "min_distance_to_water_km",
    "min_distance_to_urban_agri_km",
    "mean_slope_deg",
    "min_slope_deg",
    "max_slope_deg",
    "mean_elevation_m",
    "min_elevation_m",
    "max_elevation_m",
    "avg_ghi_kwh_m2_day",
    "avg_dni_kwh_m2_day",
]


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def cells_to_polygon(cell_ids):
    """Dissolve a list of H3 cell boundaries into one (multi)polygon."""
    polys = []
    for cell in cell_ids:
        boundary = h3.cell_to_boundary(cell)  # list of (lat, lon) pairs
        polys.append(Polygon([(lon, lat) for lat, lon in boundary]))
    merged = unary_union(polys)
    if not merged.is_valid:
        merged = merged.buffer(0)
    return merged


def build_gold_schema_df(df, governorate_slug):
    print("Building one dissolved polygon per site from its H3 cells")
    geometries = [cells_to_polygon(cells) for cells in df["h3_cells"]]

    out = pd.DataFrame()
    out["site_id"] = df["site_id"]
    out["governorate_id"] = governorate_slug
    out["geometry_wkt"] = [g.wkt for g in geometries]
    out["geometry_geojson"] = [json.dumps(mapping(g)) for g in geometries]
    out["area_km2"] = df["area_km2"]
    out["cell_count"] = df["cell_count"]
    out["centroid_lat"] = df["centroid_lat"]
    out["centroid_lon"] = df["centroid_lon"]
    out["distance_to_nearest_road_km"] = df["min_distance_to_road_km"]
    out["distance_to_nearest_grid_km"] = df["min_distance_to_grid_km"]
    out["distance_to_nearest_water_km"] = df["min_distance_to_water_km"]
    out["distance_to_nearest_urban_km"] = df["min_distance_to_urban_agri_km"]
    out["mean_slope_deg"] = df["mean_slope_deg"]
    out["min_slope_deg"] = df["min_slope_deg"]
    out["max_slope_deg"] = df["max_slope_deg"]
    out["mean_elevation_m"] = df["mean_elevation_m"]
    out["min_elevation_m"] = df["min_elevation_m"]
    out["max_elevation_m"] = df["max_elevation_m"]
    out["avg_ghi_kwh_m2_day"] = df["avg_ghi_kwh_m2_day"]
    out["avg_dni_kwh_m2_day"] = df["avg_dni_kwh_m2_day"]
    out["site_suitability_score"] = df["avg_site_suitability_score"]
    out["gold_processed_timestamp"] = datetime.now(timezone.utc)
    return out


def ensure_bucket(s3_client, bucket):
    existing = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
    if bucket not in existing:
        s3_client.create_bucket(Bucket=bucket)
        print(f"Created bucket '{bucket}'")


def run(governorate):
    slug = slugify(governorate)
    sites_path = f"{PROCESSED_DIR}/{slug}/candidate_sites_{slug}.parquet"

    print(f"Reading {sites_path}")
    df = pd.read_parquet(sites_path)
    print(f"Rows: {len(df)}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"This file is missing {missing} - it was likely produced by an "
            "older version of filter_candidate_sites.py. Re-run "
            "filter_candidate_sites.py with the current version of the script "
            "to regenerate candidate_sites_<slug>.parquet with these columns, "
            "then re-run this script."
        )

    gold_df = build_gold_schema_df(df, slug)

    local_out = f"/tmp/candidate_sites_{slug}_staged.parquet"
    gold_df.to_parquet(local_out, index=False)
    print(f"Wrote {local_out} ({len(gold_df)} rows, {len(gold_df.columns)} columns)")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )
    ensure_bucket(s3, STAGING_BUCKET)

    key = f"candidate_sites/{slug}/candidate_sites_{slug}.parquet"
    s3.upload_file(local_out, STAGING_BUCKET, key)
    print(f"Uploaded to s3a://{STAGING_BUCKET}/{key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Finalize candidate sites schema and upload staging Parquet to MinIO"
    )
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
