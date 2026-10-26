"""
Phase 1 - distance to built-up/agricultural areas.

Builds a binary mask from the already-clipped ESA WorldCover raster
(worldcover_<slug>.tif) flagging built-up (class 50) and cropland
(class 40) pixels, vectorizes that mask, and computes each H3 cell's
distance to the nearest such area. filter_eligible.py already excludes
cells that ARE built-up/cropland at the pixel level, but that alone
gives no setback distance - a cell right on the edge of a village or
field still counts as eligible. This adds the actual buffer requirement.

Follows the same GDAL-CLI-vectorize + sjoin_nearest pattern as
compute_distances.py and compute_water_distance.py, for consistency and
to avoid loading these rasters fully into memory.

Usage:
    python3 compute_urban_agri_distance.py --governorate Aswan

Output: overwrites h3_cells_<slug>.parquet, adding
distance_to_urban_agri_km.
"""

import argparse
import os
import subprocess

import geopandas as gpd
import pandas as pd

RAW_DIR = "/opt/gis/data/raw"
PROCESSED_DIR = "/opt/gis/data/processed"
UTM36N = "EPSG:32636"  # covers Aswan's longitude range (30-36E)

# ESA WorldCover classes treated as "urban/agricultural" for this buffer:
# 40 = cropland, 50 = built-up. Same classes filter_eligible.py already
# excludes at the pixel level - this just adds a distance requirement
# around them rather than only excluding the pixels themselves.
URBAN_AGRI_CLASSES = [40, 50]


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def build_urban_agri_mask(worldcover_path, mask_path):
    """Binary mask: 1 where WorldCover class is in URBAN_AGRI_CLASSES."""
    calc_expr = "+".join(f"1*(A=={c})" for c in URBAN_AGRI_CLASSES)
    cmd = [
        "gdal_calc.py",
        "-A", worldcover_path,
        f"--calc={calc_expr}",
        f"--outfile={mask_path}",
        "--type=Byte",
        "--NoDataValue=0",
        "--overwrite",
    ]
    subprocess.run(cmd, check=True)


def polygonize_mask(mask_path, out_gpkg_path):
    # gdal_polygonize.py appends rather than overwrites, so a stale file
    # from a previous run would silently accumulate polygons across reruns
    # (same reasoning as generate_h3_cells.py / compute_water_distance.py).
    if os.path.exists(out_gpkg_path):
        os.remove(out_gpkg_path)

    cmd = [
        "gdal_polygonize.py",
        mask_path,
        "-f", "GPKG",
        out_gpkg_path,
        "urban_agri_poly",
        "DN",
    ]
    subprocess.run(cmd, check=True)


def nearest_distance_km(points_gdf, polys_gdf):
    """For each point, distance in km to the nearest urban/agri polygon,
    using a metric CRS. A point inside such a polygon gets distance 0."""
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

    worldcover_path = f"{d}/worldcover_{slug}.tif"
    mask_path = f"{d}/urban_agri_mask_{slug}.tif"
    urban_agri_gpkg_path = f"{d}/urban_agri_polygons_{slug}.gpkg"

    print(f"Building urban/agricultural mask from {worldcover_path} (classes {URBAN_AGRI_CLASSES})")
    build_urban_agri_mask(worldcover_path, mask_path)

    print("Vectorizing urban/agricultural mask")
    polygonize_mask(mask_path, urban_agri_gpkg_path)

    urban_agri_gdf = gpd.read_file(urban_agri_gpkg_path)
    urban_agri_gdf = urban_agri_gdf[urban_agri_gdf["DN"] == 1]
    print(f"Urban/agricultural polygons: {len(urban_agri_gdf)} features")

    cells_path = f"{d}/h3_cells_{slug}.parquet"
    df = pd.read_parquet(cells_path)
    print(f"Cells: {len(df)}")

    points_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["centroid_lon"], df["centroid_lat"]),
        crs="EPSG:4326",
    )

    print("Computing distance to nearest urban/agricultural area")
    df["distance_to_urban_agri_km"] = nearest_distance_km(points_gdf, urban_agri_gdf).values

    df.to_parquet(cells_path, index=False)
    print(f"Wrote {cells_path}")
    print(df["distance_to_urban_agri_km"].describe())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute per-cell distance to built-up/agricultural areas"
    )
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
