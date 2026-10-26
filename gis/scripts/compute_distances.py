"""
Phase 1 - distance to grid/roads.

Extracts roads (highway=*) and power lines (power=* via the other_tags
hstore, since 'power' isn't a directly exposed OSM driver column) from
the Egypt OSM PBF, clipped to a governorate's bounding box, then computes
each H3 cell's distance to the nearest road and nearest power line.

Usage:
    python3 compute_distances.py --governorate Aswan

Output: overwrites h3_cells_<slug>.parquet, adding distance_to_road_km
and distance_to_grid_km columns.
"""

import argparse
import subprocess

import geopandas as gpd
import pandas as pd

RAW_DIR = "/opt/gis/data/raw"
PROCESSED_DIR = "/opt/gis/data/processed"
UTM36N = "EPSG:32636"  # covers Aswan's longitude range (30-36E)
OSM_PBF = f"{RAW_DIR}/osm/egypt-latest.osm.pbf"


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def get_bounds(governorate):
    adm1 = gpd.read_file(f"{RAW_DIR}/EGY_adm/EGY_adm1.shp")
    row = adm1[adm1["NAME_1"] == governorate]
    return row.total_bounds  # [minx, miny, maxx, maxy]


def extract_osm_layer(where_clause, bounds, out_path):
    minx, miny, maxx, maxy = bounds
    cmd = [
        "ogr2ogr",
        "-f", "GPKG",
        "-overwrite",
        out_path,
        OSM_PBF,
        "lines",
        "-where", where_clause,
        "-spat", str(minx), str(miny), str(maxx), str(maxy),
    ]
    subprocess.run(cmd, check=True)


def nearest_distance_km(points_gdf, lines_gdf):
    """For each point, distance in km to the nearest line, using a metric CRS."""
    if lines_gdf.empty:
        return pd.Series([None] * len(points_gdf), index=points_gdf.index)
    points_m = points_gdf.to_crs(UTM36N)
    lines_m = lines_gdf.to_crs(UTM36N)
    joined = gpd.sjoin_nearest(points_m, lines_m, distance_col="dist_m", how="left")
    # sjoin_nearest can produce duplicate rows if multiple nearest lines tie;
    # keep the closest per original point
    joined = joined.groupby(joined.index).first()
    return joined["dist_m"] / 1000.0


def run(governorate):
    slug = slugify(governorate)
    d = f"{PROCESSED_DIR}/{slug}"

    bounds = get_bounds(governorate)
    print(f"Bounds: {bounds}")

    roads_path = f"{d}/roads_{slug}.gpkg"
    power_path = f"{d}/power_{slug}.gpkg"

    print("Extracting roads (highway=*) from OSM")
    extract_osm_layer("highway IS NOT NULL", bounds, roads_path)

    print("Extracting power lines (power=*) from OSM")
    extract_osm_layer("other_tags LIKE '%\"power\"=>%'", bounds, power_path)

    roads_gdf = gpd.read_file(roads_path)
    power_gdf = gpd.read_file(power_path)
    print(f"Roads: {len(roads_gdf)} features, Power lines: {len(power_gdf)} features")

    cells_path = f"{d}/h3_cells_{slug}.parquet"
    df = pd.read_parquet(cells_path)
    print(f"Cells: {len(df)}")

    points_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["centroid_lon"], df["centroid_lat"]),
        crs="EPSG:4326",
    )

    print("Computing distance to nearest road")
    df["distance_to_road_km"] = nearest_distance_km(points_gdf, roads_gdf).values

    print("Computing distance to nearest power line")
    df["distance_to_grid_km"] = nearest_distance_km(points_gdf, power_gdf).values

    df.to_parquet(cells_path, index=False)
    print(f"Wrote {cells_path}")
    print(df[["distance_to_road_km", "distance_to_grid_km"]].describe())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute per-cell distance to roads/power lines")
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
