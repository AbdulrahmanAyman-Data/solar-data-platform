"""
Phase 1 - distance to water bodies.

Uses the JRC Global Surface Water "occurrence" raster that was already
clipped in clip_rasters.py (jrc_water_<slug>.tif) but, until now, never
actually consumed downstream. Thresholds it into a binary permanent-
water mask (occurrence >= WATER_OCCURRENCE_MIN_PCT, so seasonal/transient
wet pixels don't get treated as a hard water body), vectorizes that mask,
and computes each H3 cell's distance to the nearest water polygon (e.g.
the Nile).

This intentionally follows the same GDAL-CLI-vectorize + sjoin_nearest
pattern as compute_distances.py (roads/power lines) for consistency, and
for the same reason: these rasters are too large to safely load whole
into memory (same issue that caused the OOM kill fixed in clip_rasters.py).

Usage:
    python3 compute_water_distance.py --governorate Aswan

Output: overwrites h3_cells_<slug>.parquet, adding distance_to_water_km.
"""

import argparse
import os
import subprocess

import geopandas as gpd
import pandas as pd

RAW_DIR = "/opt/gis/data/raw"
PROCESSED_DIR = "/opt/gis/data/processed"
UTM36N = "EPSG:32636"  # covers Aswan's longitude range (30-36E)

# JRC occurrence is 0-100 (% of time a pixel was observed as water). A
# pixel classified as water only occasionally (e.g. seasonal flooding)
# shouldn't force a 1km exclusion buffer the same way the Nile itself
# does, so only "occurs at least half the time" counts here.
WATER_OCCURRENCE_MIN_PCT = 50


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def threshold_water_mask(jrc_path, mask_path):
    """Binary mask: 1 where occurrence >= WATER_OCCURRENCE_MIN_PCT, else 0."""
    cmd = [
        "gdal_calc.py",
        "-A", jrc_path,
        f"--calc=1*(A>={WATER_OCCURRENCE_MIN_PCT})",
        f"--outfile={mask_path}",
        "--type=Byte",
        "--NoDataValue=0",
        "--overwrite",
    ]
    subprocess.run(cmd, check=True)


def polygonize_water(mask_path, out_gpkg_path):
    # Same reasoning as generate_h3_cells.py's polygonize(): gdal_polygonize.py
    # appends rather than overwrites, so a stale file from a previous run
    # would silently accumulate polygons across reruns.
    if os.path.exists(out_gpkg_path):
        os.remove(out_gpkg_path)

    cmd = [
        "gdal_polygonize.py",
        mask_path,
        "-f", "GPKG",
        out_gpkg_path,
        "water_poly",
        "DN",
    ]
    subprocess.run(cmd, check=True)


def nearest_distance_km(points_gdf, polys_gdf):
    """For each point, distance in km to the nearest water polygon, using a
    metric CRS. A point that falls inside a water polygon gets distance 0."""
    if polys_gdf.empty:
        return pd.Series([None] * len(points_gdf), index=points_gdf.index)
    points_m = points_gdf.to_crs(UTM36N)
    polys_m = polys_gdf.to_crs(UTM36N)
    joined = gpd.sjoin_nearest(points_m, polys_m, distance_col="dist_m", how="left")
    # sjoin_nearest can produce duplicate rows if multiple nearest polygons
    # tie; keep the closest per original point
    joined = joined.groupby(joined.index).first()
    return joined["dist_m"] / 1000.0


def run(governorate):
    slug = slugify(governorate)
    d = f"{PROCESSED_DIR}/{slug}"

    jrc_path = f"{d}/jrc_water_{slug}.tif"
    mask_path = f"{d}/water_mask_{slug}.tif"
    water_gpkg_path = f"{d}/water_polygons_{slug}.gpkg"

    print(f"Thresholding {jrc_path} at occurrence >= {WATER_OCCURRENCE_MIN_PCT}%")
    threshold_water_mask(jrc_path, mask_path)

    print("Vectorizing water mask")
    polygonize_water(mask_path, water_gpkg_path)

    water_gdf = gpd.read_file(water_gpkg_path)
    water_gdf = water_gdf[water_gdf["DN"] == 1]
    print(f"Water polygons (occurrence >= {WATER_OCCURRENCE_MIN_PCT}%): {len(water_gdf)} features")

    cells_path = f"{d}/h3_cells_{slug}.parquet"
    df = pd.read_parquet(cells_path)
    print(f"Cells: {len(df)}")

    points_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["centroid_lon"], df["centroid_lat"]),
        crs="EPSG:4326",
    )

    print("Computing distance to nearest water body")
    df["distance_to_water_km"] = nearest_distance_km(points_gdf, water_gdf).values

    df.to_parquet(cells_path, index=False)
    print(f"Wrote {cells_path}")
    print(df["distance_to_water_km"].describe())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute per-cell distance to water bodies")
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
