"""
Exports the candidate site shortlist as actual site-boundary polygons
(not just centroid points), for visual inspection in QGIS/ArcGIS. Each
site's boundary is built by dissolving the H3 hexagon boundaries of the
cells that make up that site into a single (multi)polygon.

This is the polygon counterpart to
export_candidate_sites_for_visual_check.py (which only exports
centroids) - use this one when the actual site shape/extent matters,
e.g. to confirm a "site" isn't secretly a thin, oddly-shaped sliver
after the split/merge step in filter_candidate_sites.py.

Requires the h3_cells column, so this only works against a
candidate_sites_<slug>.parquet produced with the current version of
filter_candidate_sites.py (the one that stores each site's H3 cell IDs,
not an older run predating that change).

Usage:
    python3 export_candidate_sites_polygons.py --governorate Aswan

Output: /opt/gis/data/processed/<slug>/candidate_sites_<slug>_polygons.gpkg
    (polygon layer, one feature per candidate site unit, all attributes
    preserved except h3_cells itself - symbolize by
    avg_site_suitability_score or area_km2 in ArcGIS/QGIS)
"""

import argparse

import geopandas as gpd
import h3
import pandas as pd
from shapely.geometry import Polygon
from shapely.ops import unary_union

PROCESSED_DIR = "/opt/gis/data/processed"


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


def run(governorate):
    slug = slugify(governorate)
    d = f"{PROCESSED_DIR}/{slug}"
    sites_path = f"{d}/candidate_sites_{slug}.parquet"
    out_path = f"{d}/candidate_sites_{slug}_polygons.gpkg"

    print(f"Reading {sites_path}")
    df = pd.read_parquet(sites_path)
    print(f"Rows: {len(df)}")

    if "h3_cells" not in df.columns:
        raise ValueError(
            "This file has no 'h3_cells' column - it was likely produced by an "
            "older version of filter_candidate_sites.py. Re-run "
            "filter_candidate_sites.py with the current version of the script "
            "to regenerate candidate_sites_<slug>.parquet with cell membership "
            "included, then re-run this export."
        )

    print("Building one dissolved polygon per site from its H3 cells")
    geometries = [cells_to_polygon(cells) for cells in df["h3_cells"]]

    gdf = gpd.GeoDataFrame(
        df.drop(columns=["h3_cells"]),
        geometry=geometries,
        crs="EPSG:4326",
    )
    gdf.to_file(out_path, driver="GPKG", layer=f"candidate_sites_{slug}")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export candidate sites as dissolved H3-hex boundary polygons for visual QC"
    )
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
