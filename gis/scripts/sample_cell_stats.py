"""
Phase 1 - per-cell statistics (centroid sampling).

For each H3 cell centroid, samples the elevation (filled DEM) and slope
(the WorldCover-grid-aligned raster, matching what was actually used to
determine eligibility - not the raw unaligned slope raster) at that
point. Uses centroid sampling rather than a full zonal mean: with the
'overlap' H3 containment mode, some included cells only partially
overlap the true eligible area, so a centroid sample can still land
outside it (e.g. show slope > the 5deg threshold) even on the aligned
grid - a known, accepted approximation for this phase. Revisit with
rasterstats zonal_stats or 'full' containment mode later if this
matters more.

Usage:
    python3 sample_cell_stats.py --governorate Aswan

Output: overwrites h3_cells_<slug>.parquet, adding elevation_m and
slope_deg columns.
"""

import argparse

import pandas as pd
import rasterio

PROCESSED_DIR = "/opt/gis/data/processed"


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def sample_raster_at_points(raster_path, lats, lons):
    """Sample a raster's value at a list of (lat, lon) points."""
    with rasterio.open(raster_path) as src:
        coords = list(zip(lons, lats))  # rasterio expects (x, y) = (lon, lat)
        values = [v[0] for v in src.sample(coords)]
        nodata = src.nodata
    if nodata is not None:
        values = [None if v == nodata else v for v in values]
    return values


def run(governorate):
    slug = slugify(governorate)
    d = f"{PROCESSED_DIR}/{slug}"

    cells_path = f"{d}/h3_cells_{slug}.parquet"
    dem_path = f"{d}/dem_filled_{slug}.tif"
    slope_path = f"{d}/slope_aligned_{slug}.tif"
    worldcover_path = f"{d}/worldcover_{slug}.tif"

    print(f"Reading {cells_path}")
    df = pd.read_parquet(cells_path)
    print(f"Cells: {len(df)}")

    print(f"Sampling elevation from {dem_path}")
    df["elevation_m"] = sample_raster_at_points(dem_path, df["centroid_lat"], df["centroid_lon"])

    print(f"Sampling slope from {slope_path}")
    df["slope_deg"] = sample_raster_at_points(slope_path, df["centroid_lat"], df["centroid_lon"])

    print(f"Sampling land cover class from {worldcover_path}")
    df["land_cover_class"] = sample_raster_at_points(worldcover_path, df["centroid_lat"], df["centroid_lon"])

    df.to_parquet(cells_path, index=False)
    print(f"Wrote {cells_path}")
    print(df[["elevation_m", "slope_deg", "land_cover_class"]].describe())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample per-cell elevation and slope at H3 centroids")
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
