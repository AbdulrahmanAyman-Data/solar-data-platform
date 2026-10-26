"""
Converts the candidate site shortlist (candidate_sites_<slug>.parquet,
one row per buildable unit) into a point GeoPackage, for visual
inspection in ArcGIS Pro / QGIS. This is the buildable-unit counterpart
to export_for_visual_check.py, which exports the full H3 cell screening
layer instead - use that one to sanity-check the raw eligible area, and
this one to sanity-check the actual candidate sites (post area-based
splitting/merging) produced by filter_candidate_sites.py.

Usage:
    python3 export_candidate_sites_for_visual_check.py --governorate Aswan

Output: /opt/gis/data/processed/<slug>/candidate_sites_<slug>_points.gpkg
    (point layer, one point per candidate site unit, all attributes
    preserved - symbolize by avg_site_suitability_score or area_km2 in
    ArcGIS/QGIS)
"""

import argparse

import geopandas as gpd
import pandas as pd

PROCESSED_DIR = "/opt/gis/data/processed"


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def run(governorate):
    slug = slugify(governorate)
    d = f"{PROCESSED_DIR}/{slug}"
    sites_path = f"{d}/candidate_sites_{slug}.parquet"
    out_path = f"{d}/candidate_sites_{slug}_points.gpkg"

    print(f"Reading {sites_path}")
    df = pd.read_parquet(sites_path)
    print(f"Rows: {len(df)}")

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["centroid_lon"], df["centroid_lat"]),
        crs="EPSG:4326",
    )
    gdf.to_file(out_path, driver="GPKG", layer=f"candidate_sites_{slug}")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export candidate site centroids as a point GPKG for visual QC"
    )
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
