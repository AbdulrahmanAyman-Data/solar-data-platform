"""
Phase 0 - one-time repair of the Egypt admin boundaries shapefile.

clip_rasters.py uses EGY_adm1.shp/EGY_adm0.shp polygons directly as
gdalwarp cutlines. GADM-derived shapefiles occasionally contain rings
with self-intersections (a real topology defect in the source data, not
a bounding-box or projection issue) - gdalwarp refuses to use such a
polygon as a cutline at all ("Cutline polygon is invalid"), which is
exactly what happened for As Suways (Suez): its NAME_1 polygon has a
self-intersecting ring, so every clip against it fails outright, no
matter how the raster/bbox looks.

This repairs the geometries once, up front, via ogr2ogr's -makevalid
(GEOS MakeValid under the hood - splits/re-closes self-intersecting
rings into a valid geometry covering the same area) and writes fixed
copies alongside the originals. clip_rasters.py then reads the fixed
files if they exist, falling back to the originals with a clear warning
if this script hasn't been run yet - so a governorate with an already-
valid boundary is completely unaffected either way.

Requires GDAL >= 3.4 (for ogr2ogr -makevalid). If your GDAL build is
older, this will raise a clear "unrecognized argument" error - upgrade
GDAL, or ask for an alternative fix (buffer(0) via GeoPandas) instead.

Usage:
    python3 fix_admin_boundaries.py
"""

import os
import subprocess

RAW_DIR = "/opt/gis/data/raw"

SOURCES = [
    (f"{RAW_DIR}/EGY_adm/EGY_adm1.shp", f"{RAW_DIR}/EGY_adm/EGY_adm1_fixed.shp"),
    (f"{RAW_DIR}/EGY_adm/EGY_adm0.shp", f"{RAW_DIR}/EGY_adm/EGY_adm0_fixed.shp"),
]


def make_valid(src_path, dst_path):
    if not os.path.exists(src_path):
        print(f"SKIP: {src_path} not found")
        return

    # ogr2ogr writes a whole set of sidecar files (.shx, .dbf, .prj) for a
    # shapefile - remove any stale set first so a rerun doesn't leave mixed
    # old/new sidecar files behind.
    base = dst_path.rsplit(".", 1)[0]
    for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
        stale = base + ext
        if os.path.exists(stale):
            os.remove(stale)

    cmd = [
        "ogr2ogr",
        "-f", "ESRI Shapefile",
        "-makevalid",
        dst_path,
        src_path,
    ]
    print(f"Repairing {src_path} -> {dst_path}")
    subprocess.run(cmd, check=True)
    print(f"Wrote {dst_path}")


def main():
    for src_path, dst_path in SOURCES:
        make_valid(src_path, dst_path)
    print("Done. clip_rasters.py will now use the _fixed shapefiles automatically.")


if __name__ == "__main__":
    main()
