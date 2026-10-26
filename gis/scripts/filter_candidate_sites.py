"""
Phase 1/2 - candidate site shortlisting.

The eligibility mask + H3 cell generation produces a broad screening
layer (tens of thousands of individually-eligible cells). This script
narrows that down to a short list of actual buildable candidate site
units, in two stages:

  1. Apply hard cutoffs, then cluster the surviving cells into
     contiguous zones. Drop any zone whose TOTAL area is below
     min_area_km2 - this is the "is this a viable area at all" filter,
     applied to the whole zone, not individual cells. The cutoffs are:
       - a stricter slope ceiling than the base eligibility threshold
       - distance to road/grid must stay under max_road_km/max_grid_km
         (too far from infrastructure isn't worth building), but also
         at or above min_road_km/min_grid_km (a required safety/legal
         setback - a site directly on top of a road or power line isn't
         buildable even though it looks "close to infrastructure" in
         the scoring sense)
       - distance to the nearest water body must be at or above
         min_water_km (keeps sites clear of the Nile/other permanent
         water, per distance_to_water_km from compute_water_distance.py -
         this is a floor only, no upper bound: farther from water is
         always fine here)
       - distance to the nearest built-up or agricultural area must be
         at or above min_urban_agri_km (per distance_to_urban_agri_km
         from compute_urban_agri_distance.py - filter_eligible.py
         already excludes cells that ARE built-up/cropland at the pixel
         level, this adds the actual setback distance around them;
         floor only, no upper bound)
  2. For each surviving (viable) zone, decide whether it needs splitting
     at all:
       - If its total area is at or below split_threshold_km2, it is
         kept as a single buildable unit as-is - splitting a modestly
         sized zone into fragments doesn't add value.
       - If it exceeds split_threshold_km2, it is cut into
         ~target_unit_km2 pieces using H3's own hierarchy (grouping by
         ancestor cell at an increasingly fine resolution). Because that
         ancestor-based partition isn't perfectly balanced, it can leave
         behind small leftover fragments - each such fragment (area below
         min_area_km2, the same threshold used for whole-zone viability)
         is merged into its nearest sibling fragment (by centroid
         distance) from the same original zone, rather than being kept as
         an undersized standalone site or dropped.

Unlike compute_suitability_score.py (a continuous weighted score), this
applies hard pass/fail cutoffs - a cell either qualifies for the
shortlist or it doesn't. Both outputs are kept: the full scored cell
table stays useful for broader analysis, this shortlist is for "where
would we actually consider building".

Usage:
    python3 filter_candidate_sites.py --governorate Aswan \
        --max-road-km 10 --min-road-km 0.5 \
        --max-grid-km 10 --min-grid-km 0.1 \
        --min-water-km 1.0 --min-urban-agri-km 5.0 --max-slope-deg 2 \
        --min-area-km2 2 --split-threshold-km2 10 --target-unit-km2 7

Requires h3_cells_<slug>.parquet to already have distance_to_water_km
(run compute_water_distance.py first) and distance_to_urban_agri_km (run
compute_urban_agri_distance.py first), in addition to the
distance_to_road_km/distance_to_grid_km columns from compute_distances.py.

Output: /opt/gis/data/processed/<slug>/candidate_sites_<slug>.parquet
    (one row per buildable unit: site_id, h3_cells (the list of H3 cell
    IDs making up that unit - needed to reconstruct its actual boundary
    polygon, see export_candidate_sites_polygons.py / finalize_candidate_sites.py),
    cell_count, area_km2, centroid_lat/lon, mean/min/max slope_deg,
    mean/min/max elevation_m, avg + min distance to road/grid/water/
    urban-agri, avg GHI/DNI, avg_site_suitability_score)
"""

import argparse
from collections import defaultdict

import h3
import pandas as pd

PROCESSED_DIR = "/opt/gis/data/processed"


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


class UnionFind:
    """Minimal union-find for grouping adjacent cells into clusters."""

    def __init__(self, items):
        self.parent = {item: item for item in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def cluster_adjacent_cells(cell_ids):
    """Group H3 cells into contiguous clusters using H3 grid adjacency."""
    cell_set = set(cell_ids)
    uf = UnionFind(cell_set)
    for cell in cell_set:
        for neighbor in h3.grid_disk(cell, 1):
            if neighbor != cell and neighbor in cell_set:
                uf.union(cell, neighbor)

    clusters = {}
    for cell in cell_set:
        root = uf.find(cell)
        clusters.setdefault(root, []).append(cell)
    return list(clusters.values())


def cluster_area_km2(cells):
    return sum(h3.cell_area(c, unit="km^2") for c in cells)


def cluster_centroid(cells):
    lats, lons = zip(*(h3.cell_to_latlng(c) for c in cells))
    return sum(lats) / len(lats), sum(lons) / len(lons)


def subdivide_cluster(cells, target_area_km2, res=6):
    """
    Recursively group cells by their H3 ancestor at an increasingly finer
    resolution (starting at 6) until each resulting group's area is at or
    below target_area_km2. Terminates at resolution 8 (the cells' native
    resolution, each ~0.74 km² - always under any reasonable target) in
    the worst case.

    This aims for ~target_area_km2 per piece; it does not guarantee a
    hard cap or balanced piece sizes - some leaf groups can come out much
    smaller than the target, which is why the caller runs
    merge_small_fragments() afterwards.
    """
    area = cluster_area_km2(cells)
    if area <= target_area_km2 or res > 8:
        return [cells]

    groups = defaultdict(list)
    for cell in cells:
        groups[h3.cell_to_parent(cell, res)].append(cell)

    result = []
    for group_cells in groups.values():
        result.extend(subdivide_cluster(group_cells, target_area_km2, res + 1))
    return result


def merge_small_fragments(fragments, min_fragment_km2):
    """
    Merge any fragment smaller than min_fragment_km2 into whichever other
    fragment (from the same original zone) has the nearest centroid,
    instead of leaving it as an undersized standalone site or dropping it.
    Repeats until no fragment remains below the threshold or only one
    fragment is left.
    """
    fragments = [list(f) for f in fragments]
    if len(fragments) <= 1:
        return fragments

    while len(fragments) > 1:
        areas = [cluster_area_km2(f) for f in fragments]
        small_idx = next((i for i, a in enumerate(areas) if a < min_fragment_km2), None)
        if small_idx is None:
            break

        small_centroid = cluster_centroid(fragments[small_idx])
        nearest_idx = min(
            (i for i in range(len(fragments)) if i != small_idx),
            key=lambda i: (
                (cluster_centroid(fragments[i])[0] - small_centroid[0]) ** 2
                + (cluster_centroid(fragments[i])[1] - small_centroid[1]) ** 2
            ),
        )
        fragments[nearest_idx] = fragments[nearest_idx] + fragments[small_idx]
        del fragments[small_idx]

    return fragments


def process_viable_zone(cells, split_threshold_km2, target_unit_km2, min_fragment_km2):
    """
    Turn one viable (already area-qualified) contiguous zone into one or
    more buildable site units.
    """
    if cluster_area_km2(cells) <= split_threshold_km2:
        return [cells]

    fragments = subdivide_cluster(cells, target_unit_km2)
    return merge_small_fragments(fragments, min_fragment_km2)


CANDIDATE_SITES_COLUMNS = [
    "site_id",
    "h3_cells",
    "cell_count",
    "area_km2",
    "centroid_lat",
    "centroid_lon",
    "mean_slope_deg",
    "min_slope_deg",
    "max_slope_deg",
    "mean_elevation_m",
    "min_elevation_m",
    "max_elevation_m",
    "avg_distance_to_road_km",
    "min_distance_to_road_km",
    "avg_distance_to_grid_km",
    "min_distance_to_grid_km",
    "avg_distance_to_water_km",
    "min_distance_to_water_km",
    "avg_distance_to_urban_agri_km",
    "min_distance_to_urban_agri_km",
    "avg_ghi_kwh_m2_day",
    "avg_dni_kwh_m2_day",
    "avg_site_suitability_score",
]


def write_empty_candidate_sites(out_path, reason):
    """
    Writes a zero-row candidate_sites_<slug>.parquet with the correct
    schema instead of leaving no file at all. A governorate can
    legitimately produce zero buildable candidates (e.g. Luxor - small,
    mostly narrow floodplain, easily filtered out entirely by the
    distance cutoffs) - that is a real, valid outcome, not a failure, and
    downstream steps like finalize_candidate_sites.py should be able to
    process "zero candidates" cleanly rather than crash on a missing file.
    """
    print(f"{reason} Writing an empty {out_path} with the correct columns.")
    empty_df = pd.DataFrame(columns=CANDIDATE_SITES_COLUMNS)
    empty_df.to_parquet(out_path, index=False)
    print(f"Wrote {out_path} (0 rows)")


def run(
    governorate,
    max_road_km,
    min_road_km,
    max_grid_km,
    min_grid_km,
    min_water_km,
    min_urban_agri_km,
    max_slope_deg,
    min_area_km2,
    split_threshold_km2,
    target_unit_km2,
):
    slug = slugify(governorate)
    cells_path = f"{PROCESSED_DIR}/{slug}/h3_cells_{slug}.parquet"
    out_path = f"{PROCESSED_DIR}/{slug}/candidate_sites_{slug}.parquet"

    print(f"Reading {cells_path}")
    df = pd.read_parquet(cells_path)
    print(f"Total cells: {len(df)}")

    required_distance_cols = ["distance_to_water_km", "distance_to_urban_agri_km"]
    missing = [c for c in required_distance_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"This file is missing {missing} - run compute_water_distance.py and/or "
            "compute_urban_agri_distance.py for this governorate first, then re-run "
            "this script."
        )

    survivors = df[
        (df["distance_to_road_km"] <= max_road_km)
        & (df["distance_to_road_km"] >= min_road_km)
        & (df["distance_to_grid_km"] <= max_grid_km)
        & (df["distance_to_grid_km"] >= min_grid_km)
        & (df["distance_to_water_km"] >= min_water_km)
        & (df["distance_to_urban_agri_km"] >= min_urban_agri_km)
        & (df["slope_deg"] <= max_slope_deg)
    ].copy()
    print(
        f"Cells passing hard cutoffs ({min_road_km}km<=road<={max_road_km}km, "
        f"{min_grid_km}km<=grid<={max_grid_km}km, water>={min_water_km}km, "
        f"urban/agri>={min_urban_agri_km}km, slope<={max_slope_deg}deg): {len(survivors)}"
    )

    if survivors.empty:
        write_empty_candidate_sites(
            out_path, "No cells survive these cutoffs."
        )
        return

    raw_clusters = cluster_adjacent_cells(survivors["h3_index"].tolist())
    print(f"Contiguous clusters formed: {len(raw_clusters)}")

    # Stage 1: only clusters whose TOTAL contiguous area is a viable zone
    # (>= min_area_km2) survive - drops small isolated patches entirely.
    viable_clusters = [c for c in raw_clusters if cluster_area_km2(c) >= min_area_km2]
    print(f"Clusters meeting the {min_area_km2} km² viability minimum: {len(viable_clusters)}")

    if not viable_clusters:
        write_empty_candidate_sites(
            out_path, "No clusters meet the minimum area."
        )
        return

    # Stage 2: for each viable zone, split only if it exceeds
    # split_threshold_km2, aiming for ~target_unit_km2 pieces, then merge
    # away any undersized leftover fragment into its nearest sibling.
    units = []
    for cluster_cells in viable_clusters:
        units.extend(
            process_viable_zone(cluster_cells, split_threshold_km2, target_unit_km2, min_area_km2)
        )
    print(
        f"Buildable units after splitting zones over {split_threshold_km2} km² "
        f"into ~{target_unit_km2} km² pieces (fragments merged into nearest sibling): {len(units)}"
    )

    survivors_by_id = survivors.set_index("h3_index")

    rows = []
    for i, unit_cells in enumerate(units):
        area_km2 = cluster_area_km2(unit_cells)
        sub = survivors_by_id.loc[unit_cells]
        rows.append(
            {
                "site_id": f"{slug}_site_{i:04d}",
                "h3_cells": list(unit_cells),
                "cell_count": len(unit_cells),
                "area_km2": area_km2,
                "centroid_lat": sub["centroid_lat"].mean(),
                "centroid_lon": sub["centroid_lon"].mean(),
                "mean_slope_deg": sub["slope_deg"].mean(),
                "min_slope_deg": sub["slope_deg"].min(),
                "max_slope_deg": sub["slope_deg"].max(),
                "mean_elevation_m": sub["elevation_m"].mean(),
                "min_elevation_m": sub["elevation_m"].min(),
                "max_elevation_m": sub["elevation_m"].max(),
                "avg_distance_to_road_km": sub["distance_to_road_km"].mean(),
                "min_distance_to_road_km": sub["distance_to_road_km"].min(),
                "avg_distance_to_grid_km": sub["distance_to_grid_km"].mean(),
                "min_distance_to_grid_km": sub["distance_to_grid_km"].min(),
                "avg_distance_to_water_km": sub["distance_to_water_km"].mean(),
                "min_distance_to_water_km": sub["distance_to_water_km"].min(),
                "avg_distance_to_urban_agri_km": sub["distance_to_urban_agri_km"].mean(),
                "min_distance_to_urban_agri_km": sub["distance_to_urban_agri_km"].min(),
                "avg_ghi_kwh_m2_day": sub["ghi_kwh_m2_day"].mean(),
                "avg_dni_kwh_m2_day": sub["dni_kwh_m2_day"].mean(),
                "avg_site_suitability_score": sub["site_suitability_score"].mean(),
            }
        )

    sites_df = pd.DataFrame(rows).sort_values("avg_site_suitability_score", ascending=False)
    print(f"Final candidate site units: {len(sites_df)}")

    sites_df.to_parquet(out_path, index=False)
    print(f"Wrote {out_path}")
    print(sites_df[["site_id", "area_km2", "avg_distance_to_road_km", "avg_distance_to_grid_km", "avg_distance_to_water_km", "avg_distance_to_urban_agri_km", "avg_site_suitability_score"]].to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shortlist candidate solar sites from H3 cells")
    parser.add_argument("--governorate", default="Aswan")
    parser.add_argument(
        "--max-road-km", type=float, default=10.0,
        help="Sites farther than this from the nearest road are dropped (too remote to build).",
    )
    parser.add_argument(
        "--min-road-km", type=float, default=0.5,
        help="Required safety/legal setback from the nearest road, in km (default 500m).",
    )
    parser.add_argument(
        "--max-grid-km", type=float, default=10.0,
        help="Sites farther than this from the nearest power line are dropped (too remote to build).",
    )
    parser.add_argument(
        "--min-grid-km", type=float, default=0.1,
        help="Required safety/legal setback from the nearest power line, in km (default 100m).",
    )
    parser.add_argument(
        "--min-water-km", type=float, default=1.0,
        help="Required setback from the nearest water body (e.g. the Nile), in km. No upper bound.",
    )
    parser.add_argument(
        "--min-urban-agri-km", type=float, default=5.0,
        help="Required setback from the nearest built-up or agricultural area, in km. No upper bound.",
    )
    parser.add_argument("--max-slope-deg", type=float, default=2.0)
    parser.add_argument(
        "--min-area-km2",
        type=float,
        default=2.0,
        help="Minimum total area for a contiguous zone to be considered viable at all, "
        "and the same threshold used to detect an undersized fragment after splitting.",
    )
    parser.add_argument(
        "--split-threshold-km2",
        type=float,
        default=10.0,
        help="A viable zone is only split into smaller units if its total area exceeds this.",
    )
    parser.add_argument(
        "--target-unit-km2",
        type=float,
        default=7.0,
        help="Target area per piece when a zone is split.",
    )
    args = parser.parse_args()
    run(
        args.governorate,
        args.max_road_km,
        args.min_road_km,
        args.max_grid_km,
        args.min_grid_km,
        args.min_water_km,
        args.min_urban_agri_km,
        args.max_slope_deg,
        args.min_area_km2,
        args.split_threshold_km2,
        args.target_unit_km2,
    )
