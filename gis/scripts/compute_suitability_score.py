"""
Phase 1 - site suitability score.

Computes site_suitability_score (0-100, higher is better) from three
land-only factors, kept deliberately separate from solar radiation
potential (GHI/DNI) per the plan's design decision to avoid hiding any
one factor inside a single blended number.

Weights (chosen by the user, informed by AHP-based solar-siting studies
where distance to grid/roads dominates once terrain is pre-filtered):
    - slope:              40%
    - distance to grid:   30%
    - distance to roads:  30%

Each factor is normalized to 0-100 with a fixed cap, so scores stay
comparable across governorates rather than being rescaled to each
governorate's own min/max range:
    - slope:  0 deg -> 100, SLOPE_CAP_DEG -> 0 (linear)
    - grid/road distance: 0 km -> 100, *_CAP_KM -> 0 (linear)
Values beyond the cap are clipped to a score of 0.

Usage:
    python3 compute_suitability_score.py --governorate Aswan

Output: overwrites h3_cells_<slug>.parquet, adding site_suitability_score.
"""

import argparse

import pandas as pd

PROCESSED_DIR = "/opt/gis/data/processed"

WEIGHT_SLOPE = 0.40
WEIGHT_GRID = 0.30
WEIGHT_ROAD = 0.30

SLOPE_CAP_DEG = 5.0
GRID_DIST_CAP_KM = 50.0
ROAD_DIST_CAP_KM = 50.0


def slugify(name):
    return name.lower().replace(" ", "_").replace("'", "").replace("`", "")


def normalize_lower_is_better(series, cap):
    score = 100 * (1 - series / cap)
    return score.clip(lower=0, upper=100)


def run(governorate):
    slug = slugify(governorate)
    cells_path = f"{PROCESSED_DIR}/{slug}/h3_cells_{slug}.parquet"

    print(f"Reading {cells_path}")
    df = pd.read_parquet(cells_path)
    print(f"Cells: {len(df)}")

    slope_score = normalize_lower_is_better(df["slope_deg"], SLOPE_CAP_DEG)
    grid_score = normalize_lower_is_better(df["distance_to_grid_km"], GRID_DIST_CAP_KM)
    road_score = normalize_lower_is_better(df["distance_to_road_km"], ROAD_DIST_CAP_KM)

    df["site_suitability_score"] = (
        WEIGHT_SLOPE * slope_score + WEIGHT_GRID * grid_score + WEIGHT_ROAD * road_score
    )

    df.to_parquet(cells_path, index=False)
    print(f"Wrote {cells_path}")
    print(df["site_suitability_score"].describe())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute site_suitability_score per H3 cell")
    parser.add_argument("--governorate", default="Aswan")
    args = parser.parse_args()
    run(args.governorate)
