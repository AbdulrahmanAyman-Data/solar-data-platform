"""
GIS raster clip pipeline - mosaics adjacent raster tiles (DEM, ESA
WorldCover, JRC Global Surface Water) and clips them to an Egypt
governorate boundary (or the whole country) from the admin boundaries
shapefile.

Usage:
    python3 clip_rasters.py --governorate Aswan
    python3 clip_rasters.py --governorate "Al Bahr al Ahmar"
    python3 clip_rasters.py --governorate ALL   # whole-country boundary (adm0)

If a governorate's boundary polygon has a topology defect (e.g.
self-intersecting ring - hit this for "As Suways"/Suez, gdalwarp refuses
it outright with "Cutline polygon is invalid"), run
fix_admin_boundaries.py once first; this script then picks up the
repaired EGY_adm1_fixed.shp/EGY_adm0_fixed.shp automatically.

Output: /opt/gis/data/processed/<slug>/{dem,worldcover,jrc_water}_<slug>.tif
"""

import argparse
import glob
import os
import re
import subprocess

import geopandas as gpd

RAW_DIR = "/opt/gis/data/raw"
PROCESSED_DIR = "/opt/gis/data/processed"

DEM_TILE_RE = re.compile(r"_(N|S)(\d+)_00_(E|W)(\d+)_00_DEM\.tif$")

_WARNED_UNFIXED = set()


def admin_boundary_path(level):
    """
    Resolve the admin boundary shapefile to use for cutlines/bounds:
    EGY_adm{level}_fixed.shp if fix_admin_boundaries.py has been run
    (repairs self-intersecting rings that make gdalwarp reject a polygon
    as an invalid cutline - this hit "As Suways" specifically), otherwise
    the original EGY_adm{level}.shp with a one-time warning. Governorates
    whose boundary was already valid work identically either way.
    """
    fixed = f"{RAW_DIR}/EGY_adm/EGY_adm{level}_fixed.shp"
    original = f"{RAW_DIR}/EGY_adm/EGY_adm{level}.shp"
    if os.path.exists(fixed):
        return fixed
    if level not in _WARNED_UNFIXED:
        print(
            f"NOTE: {fixed} not found - using {original} as-is. If a governorate's "
            "cutline fails with 'Ring Self-intersection' / 'Cutline polygon is "
            "invalid', run fix_admin_boundaries.py once first."
        )
        _WARNED_UNFIXED.add(level)
    return original


def slugify(name):
    """Turn a governorate name into a filesystem-safe slug."""
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def load_bounds(governorate):
    """
    Load the target boundary's bounding box (used only for tile matching).
    The actual clip happens later via gdalwarp -cutline, reading the
    shapefile directly, so no geometry object is needed here.

    governorate: exact NAME_1 value from EGY_adm1.shp, or "ALL" for the
    whole-country boundary (EGY_adm0.shp).
    """
    if governorate.upper() == "ALL":
        adm = gpd.read_file(admin_boundary_path(0))
        row = adm.iloc[[0]]
    else:
        adm1 = gpd.read_file(admin_boundary_path(1))
        row = adm1[adm1["NAME_1"] == governorate]
        if row.empty:
            available = sorted(adm1["NAME_1"].tolist())
            raise ValueError(
                f"Governorate '{governorate}' not found in NAME_1. "
                f"Available values: {available}"
            )
    return row.total_bounds  # [minx, miny, maxx, maxy]


def mosaic_and_clip(tile_paths, out_path, governorate, dstnodata=None):
    """
    Mosaic a list of raster tiles and clip to the governorate boundary
    using gdalwarp. This streams/blocks internally instead of loading
    full arrays into RAM, which is what caused the OOM kill (exit 137)
    with the rasterio.merge approach on larger governorates.

    dstnodata: explicit NoData value for the output. Needed for sources
    (like the Copernicus DEM tiles) that carry no NoData tag at all, so
    gdalwarp would otherwise silently fill cutline-masked areas (outside
    the governorate's true, non-rectangular boundary) with a real-looking
    value (0) instead of flagging them as NoData.
    """
    if not tile_paths:
        raise ValueError("No matching tiles found for this layer")

    cmd = [
        "gdalwarp",
        "-overwrite",
        "-co", "COMPRESS=LZW",
        "-co", "TILED=YES",
        "-crop_to_cutline",
    ]

    if dstnodata is not None:
        cmd += ["-dstnodata", str(dstnodata)]

    if governorate.upper() == "ALL":
        cmd += ["-cutline", admin_boundary_path(0)]
    else:
        # Escape single quotes for the SQL WHERE clause (e.g. Al Isma`iliyah
        # uses a backtick, not an apostrophe, but escape defensively anyway)
        safe_name = governorate.replace("'", "''")
        cmd += [
            "-cutline", admin_boundary_path(1),
            "-cwhere", f"NAME_1='{safe_name}'",
        ]

    cmd += tile_paths + [out_path]

    subprocess.run(cmd, check=True)
    print(f"Wrote {out_path}")


def find_dem_tiles(bounds):
    """Match 1x1 degree DEM tiles overlapping the bounding box."""
    minx, miny, maxx, maxy = bounds
    tiles = glob.glob(f"{RAW_DIR}/dem/*/Copernicus_DSM_COG_10_N*_00_E*_00_DEM.tif")
    matching = []
    for t in tiles:
        m = DEM_TILE_RE.search(os.path.basename(t))
        if not m:
            continue
        lat = int(m.group(2)) * (1 if m.group(1) == "N" else -1)
        lon = int(m.group(4)) * (1 if m.group(3) == "E" else -1)
        if lat <= maxy and lat + 1 >= miny and lon <= maxx and lon + 1 >= minx:
            matching.append(t)
    return matching


def find_worldcover_tiles(bounds):
    """Match 3x3 degree ESA WorldCover tiles overlapping the bounding box."""
    minx, miny, maxx, maxy = bounds
    tiles = glob.glob(
        f"{RAW_DIR}/worldcover/ESA_WorldCover_10m_2021_v200_N*E*_Map.tif"
    )
    matching = []
    for t in tiles:
        base = os.path.basename(t)
        tag = base.split("_v200_")[1].split("_Map")[0]  # e.g. N21E024
        lat = int(tag[1:3])
        lon = int(tag[4:7])
        if lat <= maxy and lat + 3 >= miny and lon <= maxx and lon + 3 >= minx:
            matching.append(t)
    return matching


def find_jrc_tiles(bounds):
    """Match 10x10 degree JRC Global Surface Water tiles overlapping the bbox."""
    minx, miny, maxx, maxy = bounds
    tiles = glob.glob(f"{RAW_DIR}/jrc_water/occurrence_*.tif")
    matching = []
    for t in tiles:
        base = os.path.basename(t)
        parts = base.replace("occurrence_", "").split("_")
        lon = int(parts[0].replace("E", ""))
        lat = int(parts[1].replace("N", ""))
        # Tile is named by its NW corner: covers [lon, lon+10] x [lat-10, lat]
        if lon <= maxx and lon + 10 >= minx and lat >= miny and lat - 10 <= maxy:
            matching.append(t)
    return matching


def run(governorate):
    slug = slugify(governorate)
    out_dir = f"{PROCESSED_DIR}/{slug}"
    os.makedirs(out_dir, exist_ok=True)

    bounds = load_bounds(governorate)
    print(f"Target: {governorate}  bounds: {bounds}")

    dem_tiles = find_dem_tiles(bounds)
    print(f"DEM tiles matched: {len(dem_tiles)}")
    mosaic_and_clip(dem_tiles, f"{out_dir}/dem_{slug}.tif", governorate, dstnodata=-9999)

    wc_tiles = find_worldcover_tiles(bounds)
    print(f"WorldCover tiles matched: {len(wc_tiles)}")
    mosaic_and_clip(wc_tiles, f"{out_dir}/worldcover_{slug}.tif", governorate)

    jrc_tiles = find_jrc_tiles(bounds)
    print(f"JRC tiles matched: {len(jrc_tiles)}")
    mosaic_and_clip(jrc_tiles, f"{out_dir}/jrc_water_{slug}.tif", governorate)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clip GIS rasters to an Egypt boundary")
    parser.add_argument(
        "--governorate",
        default="Aswan",
        help='Exact NAME_1 value from EGY_adm1.shp, or "ALL" for the whole country',
    )
    args = parser.parse_args()
    run(args.governorate)
