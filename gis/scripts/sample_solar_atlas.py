"""
Phase 1 - solar radiation potential per cell.

Samples Global Solar Atlas GHI (Global Horizontal Irradiance) and DNI
(Direct Normal Irradiance) at each H3 cell's centroid. These rasters
cover all of Egypt (downloaded once in Phase 0), so the same files are
used regardless of governorate - no clipping needed, just point sampling.

Usage:
    python3 sample_solar_atlas.py --governorate Aswan

Output: overwrites h3_cells_<slug>.parquet, adding ghi_kwh_m2_day and
dni_kwh_m2_day columns.
"""

import argparse

import pandas as pd
import rasterio

RAW_DIR = "/opt/gis/data/raw"
PROCESSED_DIR = "/opt/gis/data/processed"
GHI_PATH = f"{RAW_DIR}/global_solar_atlas/GHI.tif"
DNI_PATH = f"{RAW_DIR}/global_solar_atlas/DNI.tif"


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def sample_raster_at_points(raster_path, lats, lons):
    with rasterio.open(raster_path) as src:
        coords = list(zip(lons, lats))
        values = [v[0] for v in src.sample(coords)]
        nodata = src.nodata
    if nodata is not None:
        values = [None if v == nodata else v for v in values]
    return values


def run(governorate):
    slug = slugify(governorate)
    cells_path = f"{PROCESSED_DIR}/{slug}/h3_cells_{slug}.parquet"

    print(f"Reading {cells_path}")
    df = pd.read_parquet(cells_path)
    print(f"Cells: {len(df)}")

    print(f"Sampling GHI from {GHI_PATH}")
    df["ghi_kwh_m2_day"] = sample_raster_at_points(GHI_PATH, df["centroid_lat"], df["centroid_lon"])

    print(f"Sampling DNI from {DNI_PATH}")
    df["dni_kwh_m2_day"] = sample_raster_at_points(DNI_PATH, df["centroid_lat"], df["centroid_lon"])

    df.to_parquet(cells_path, index=False)
    print(f"Wrote {cells_path}")
    print(df[["ghi_kwh_m2_day", "dni_kwh_m2_day"]].describe())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample Global Solar Atlas GHI/DNI at H3 centroids")
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
