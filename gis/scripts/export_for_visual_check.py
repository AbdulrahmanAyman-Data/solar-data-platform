"""
Converts the H3 cell centroids + all computed attributes into a point
GeoPackage, for visual inspection in ArcGIS Pro / QGIS (the plan's step
10 sanity check) — h3_cells_<slug>.parquet has centroid_lat/lon as plain
numeric columns, not an actual geometry, so most GIS software won't
auto-recognize it as spatial without this conversion.

Usage:
    python3 export_for_visual_check.py --governorate Aswan

Output: /opt/gis/data/processed/<slug>/h3_cells_<slug>_points.gpkg
    (point layer, one point per H3 cell, all attributes preserved -
    symbolize by site_suitability_score in ArcGIS/QGIS)
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
    cells_path = f"{d}/h3_cells_{slug}.parquet"
    out_path = f"{d}/h3_cells_{slug}_points.gpkg"

    print(f"Reading {cells_path}")
    df = pd.read_parquet(cells_path)
    print(f"Rows: {len(df)}")

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["centroid_lon"], df["centroid_lat"]),
        crs="EPSG:4326",
    )
    gdf.to_file(out_path, driver="GPKG", layer=f"h3_cells_{slug}")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export H3 cell centroids as a point GPKG for visual QC")
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
