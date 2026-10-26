"""
Phase 1 - raster filtering.

Builds a binary eligibility mask that excludes:
  - WorldCover classes: 40 (cropland), 50 (built-up), 80 (water bodies),
    90 (herbaceous wetland), 95 (mangroves)
  - Slope above a configurable threshold (computed from the DEM)
  - Pixels inside any WDPA protected area polygon

All steps use GDAL command-line utilities (gdaldem, gdalwarp,
gdal_rasterize, gdal_calc.py), which process rasters in blocks rather
than loading full arrays into memory - the same fix applied in
clip_rasters.py, needed here for the same reason (these rasters are too
large to safely load whole into numpy).

Usage:
    python3 filter_eligible.py --governorate Aswan --slope-max 5

Output: /opt/gis/data/processed/<slug>/eligible_mask_<slug>.tif
    (Byte raster: 1 = eligible, 0 = excluded, aligned to the WorldCover
    grid for that governorate)
"""

import argparse
import os
import subprocess

import rasterio

RAW_DIR = "/opt/gis/data/raw"
PROCESSED_DIR = "/opt/gis/data/processed"

EXCLUDED_WORLDCOVER_CLASSES = [40, 50, 80, 90, 95]
SLOPE_NODATA = -9999  # gdaldem slope's default output NoData value


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def get_grid_params(raster_path):
    """Read only the metadata (not pixel data) of a raster: resolution,
    bounds, and size - used to align other rasters onto this grid."""
    with rasterio.open(raster_path) as src:
        return {
            "xres": src.res[0],
            "yres": src.res[1],
            "bounds": src.bounds,
            "width": src.width,
            "height": src.height,
        }


def run_gdal(cmd):
    subprocess.run(cmd, check=True)


def fill_dem_gaps(dem_path, filled_path, max_distance=1000):
    """
    Fill small NoData voids in the DEM via interpolation from surrounding
    valid pixels. Copernicus DEM voids over desert terrain are typically
    small scattered radar-processing gaps rather than genuinely complex
    unmappable terrain, so filling them is standard practice before slope
    computation - leaving them as NoData instead caused a much larger
    effective eligible-area loss than the raw void fraction, because
    bilinear-resampling the slope raster from 30m to WorldCover's 10m grid
    erodes a buffer around every void edge, and Aswan's voids are many
    small scattered patches rather than one contiguous gap.
    """
    cmd = [
        "gdal_fillnodata.py",
        "-md", str(max_distance),
        "-si", "1",
        dem_path,
        filled_path,
    ]
    subprocess.run(cmd, check=True)


def compute_slope(dem_path, slope_out_path):
    """
    Slope in degrees, computed from the DEM via gdaldem (streams in blocks).

    -s 111120: the DEM is in a geographic CRS (degrees), not a projected
    metric one, so gdaldem needs an explicit horizontal-to-vertical unit
    ratio (meters per degree, ~111120 at the equator) to compute a correct
    rise-over-run. Without it, gdaldem treats a pixel-to-pixel horizontal
    distance of ~0.0000278 degrees as if it were 0.0000278 meters, making
    slope saturate near 90 degrees for almost any real elevation variation.
    """
    run_gdal(["gdaldem", "slope", dem_path, slope_out_path, "-compute_edges", "-s", "111120"])


def align_to_grid(src_path, out_path, grid, resample="bilinear"):
    """Warp src_path onto the exact resolution/extent of a reference grid."""
    b = grid["bounds"]
    run_gdal(
        [
            "gdalwarp",
            "-overwrite",
            "-tr", str(grid["xres"]), str(grid["yres"]),
            "-te", str(b.left), str(b.bottom), str(b.right), str(b.top),
            "-r", resample,
            "-co", "COMPRESS=LZW",
            "-co", "BIGTIFF=YES",
            src_path,
            out_path,
        ]
    )


def rasterize_wdpa(governorate, out_path, grid):
    """Burn 1 inside any WDPA protected-area polygon, onto the given grid."""
    b = grid["bounds"]
    cmd = [
        "gdal_rasterize",
        "-burn", "1",
        "-init", "0",
        "-ot", "Byte",
        "-tr", str(grid["xres"]), str(grid["yres"]),
        "-te", str(b.left), str(b.bottom), str(b.right), str(b.top),
        "-co", "COMPRESS=LZW",
        f"{RAW_DIR}/wdpa/WDPA_Egypt_Polygons.shp",
        out_path,
    ]
    run_gdal(cmd)


def combine_mask(worldcover_path, slope_aligned_path, protected_path, out_path, slope_max):
    """
    Eligible = WorldCover class not in the excluded list
               AND slope <= slope_max
               AND not inside a protected area.
    Runs via gdal_calc.py, which processes in blocks (no full-array load).
    """
    wc_excl = "".join(f"*(A!={c})" for c in EXCLUDED_WORLDCOVER_CLASSES)
    # A!=0 excludes WorldCover NoData / outside-boundary pixels explicitly.
    # B!=SLOPE_NODATA excludes slope's NoData sentinel explicitly.
    # gdal_calc.py evaluates the formula on raw pixel values regardless of
    # --hideNoData - it does not auto-exclude NoData inputs from the
    # arithmetic, so every sentinel must be excluded in the expression itself
    # (confirmed: -9999<=5 evaluates as numerically true, wrongly marking
    # real DEM data-void areas as flat/eligible).
    calc_expr = f"(1*(A!=0){wc_excl})*(B!={SLOPE_NODATA})*(B<={slope_max})*(C==0)"

    cmd = [
        "gdal_calc.py",
        "-A", worldcover_path,
        "-B", slope_aligned_path,
        "-C", protected_path,
        f"--calc={calc_expr}",
        f"--outfile={out_path}",
        "--type=Byte",
        "--NoDataValue=0",
        "--overwrite",
    ]
    run_gdal(cmd)


def run(governorate, slope_max):
    slug = slugify(governorate)
    d = f"{PROCESSED_DIR}/{slug}"

    worldcover_path = f"{d}/worldcover_{slug}.tif"
    dem_path = f"{d}/dem_{slug}.tif"
    dem_filled_path = f"{d}/dem_filled_{slug}.tif"
    slope_path = f"{d}/slope_{slug}.tif"
    slope_aligned_path = f"{d}/slope_aligned_{slug}.tif"
    protected_path = f"{d}/protected_mask_{slug}.tif"
    eligible_path = f"{d}/eligible_mask_{slug}.tif"

    print(f"Filling DEM voids in {dem_path}")
    fill_dem_gaps(dem_path, dem_filled_path)

    print(f"Computing slope from {dem_filled_path}")
    compute_slope(dem_filled_path, slope_path)

    print("Reading WorldCover grid parameters (metadata only)")
    grid = get_grid_params(worldcover_path)
    print(f"Grid: {grid['width']}x{grid['height']}  res={grid['xres']},{grid['yres']}")

    print("Aligning slope raster to WorldCover grid")
    align_to_grid(slope_path, slope_aligned_path, grid, resample="bilinear")

    print("Rasterizing WDPA protected areas onto WorldCover grid")
    rasterize_wdpa(governorate, protected_path, grid)

    print(f"Combining masks (slope <= {slope_max} deg, excluding classes {EXCLUDED_WORLDCOVER_CLASSES})")
    combine_mask(worldcover_path, slope_aligned_path, protected_path, eligible_path, slope_max)

    print(f"Wrote {eligible_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build a solar site eligibility mask")
    parser.add_argument("--governorate", default="Aswan")
    parser.add_argument("--slope-max", type=float, default=5.0)
    args = parser.parse_args()
    run(args.governorate, args.slope_max)
