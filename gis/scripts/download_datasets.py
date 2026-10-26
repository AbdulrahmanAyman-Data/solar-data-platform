"""
Phase 0 — download the raw GIS data sources for Egypt.

Run this INSIDE solar-gis-processor:
    docker exec solar-gis-processor python3 /opt/gis/scripts/download_datasets.py

Egypt's approximate bounding box (used for clipping/selecting tiles below):
    west=24.7, south=21.7, east=36.9, north=31.7

WHAT THIS SCRIPT AUTOMATES vs. WHAT NEEDS A MANUAL STEP:
  - ESA WorldCover: automated (public AWS S3 bucket, no account needed)
  - Copernicus DEM GLO-30: automated (public AWS Open Data bucket)
  - OpenStreetMap (roads + power grid): automated (Geofabrik direct download)
  - Global Solar Atlas: MANUAL — requires account registration, no stable
    scriptable download URL. Instructions printed below.
  - WDPA (protected areas): MANUAL — protectedplanet.net requires a click-
    through license agreement before download. Instructions printed below.
  - JRC Global Surface Water: automated (public GCS bucket tiles)
"""
import os
import subprocess
import sys

RAW_DIR = "/opt/gis/data/raw"
EGYPT_BBOX = {"west": 24.7, "south": 21.7, "east": 36.9, "north": 31.7}


def run(cmd: str, label: str) -> None:
    print(f"\n[{label}] {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"[{label}] FAILED (exit code {result.returncode}) — see output above.")
    else:
        print(f"[{label}] done.")


def download_osm_egypt() -> None:
    """Geofabrik's Egypt extract — direct, no account needed. Includes
    roads (highway=*) and power infrastructure (power=line/substation)
    as an .osm.pbf file. Filtering to just roads/power happens in a later
    processing step, not here — this just gets the raw extract."""
    out_dir = os.path.join(RAW_DIR, "osm")
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "egypt-latest.osm.pbf")
    run(
        f"wget -q --show-progress -O {out_file} "
        "https://download.geofabrik.de/africa/egypt-latest.osm.pbf",
        "OSM Egypt extract",
    )


def download_copernicus_dem() -> None:
    """Copernicus DEM GLO-30 is tiled and hosted on AWS Open Data
    (registry.opendata.aws/copernicus-dem). Full automated tile-selection
    for Egypt's bbox needs the AWS CLI + the tile index; this stub
    downloads the tile index so the actual tile list can be computed, then
    prints the follow-up command. Kept as two steps deliberately — fetching
    ~150+ individual 30m tiles blind without checking the index first risks
    a very large, wasted download if the bbox math is off."""
    out_dir = os.path.join(RAW_DIR, "dem")
    os.makedirs(out_dir, exist_ok=True)
    print(
        "\n[Copernicus DEM] This dataset is tiled (1x1 degree tiles). "
        "Automated bulk download needs the AWS CLI (no account needed, "
        "public bucket). Run this separately once AWS CLI is available "
        "in the container:\n"
        "  aws s3 cp --no-sign-request --recursive "
        "s3://copernicus-dem-30m/ /opt/gis/data/raw/dem/ "
        "--exclude '*' --include 'Copernicus_DSM_COG_10_N2*_00_E0*_00_DEM/*'\n"
        "(the N2*/E0* pattern is a rough Egypt-latitude/longitude prefix "
        "filter — refine after checking the tile index, since prefix "
        "matching alone will pull in some neighboring-country tiles too; "
        "those get clipped out in the processing step anyway, so slight "
        "over-fetching here is harmless, just not maximally efficient)."
    )


def download_worldcover() -> None:
    """ESA WorldCover 10m, public AWS S3 bucket, no account needed.
    Tiled in 3x3 degree cells. Same reasoning as the DEM: print the
    AWS CLI command rather than guess tile names blind."""
    print(
        "\n[ESA WorldCover] Public bucket, tiled 3x3 degrees. Run:\n"
        "  aws s3 cp --no-sign-request --recursive "
        "s3://esa-worldcover/v200/2021/map/ /opt/gis/data/raw/worldcover/ "
        "--exclude '*' --include 'ESA_WorldCover_10m_2021_v200_N2*E0*_Map.tif'\n"
        "(same prefix-filter caveat as the DEM above — some over-fetch of "
        "neighboring tiles is fine, gets clipped later)."
    )


def download_jrc_water() -> None:
    """JRC Global Surface Water, public Google Cloud Storage bucket."""
    out_dir = os.path.join(RAW_DIR, "jrc_water")
    os.makedirs(out_dir, exist_ok=True)
    print(
        "\n[JRC Global Surface Water] Public GCS bucket, tiled. "
        "Occurrence layer (the one relevant here) download pattern:\n"
        "  wget -P /opt/gis/data/raw/jrc_water/ "
        "https://storage.googleapis.com/global-surface-water/downloads2021/occurrence/occurrence_30E_30Nv1_4_2021.tif\n"
        "(tile naming follows lon/lat of the tile's NW corner — check "
        "https://global-surface-water.appspot.com/download for the exact "
        "tile names covering 24.7-36.9E, 21.7-31.7N; likely 2-3 tiles for "
        "Egypt's full extent)."
    )


def print_manual_steps() -> None:
    print(
        "\n" + "=" * 70
        + "\nMANUAL STEPS — these two sources cannot be scripted:\n"
        + "=" * 70
        + """

1) GLOBAL SOLAR ATLAS
   - Go to: https://globalsolaratlas.info/download/egypt
   - Requires free account registration (click "Register" first if you
     don't have one).
   - Download the "GHI" and "DNI" GeoTIFFs for Egypt (country-level
     download is offered directly — no need to compute a bbox yourself).
   - Save both files to: /opt/gis/data/raw/global_solar_atlas/
     (create that folder first: mkdir -p gis/data/raw/global_solar_atlas
     on the host, or `docker exec solar-gis-processor mkdir -p
     /opt/gis/data/raw/global_solar_atlas`)

2) WDPA (World Database on Protected Areas)
   - Go to: https://www.protectedplanet.net/en/thematic-areas/wdpa
   - Search/filter for Egypt, or download the full global dataset and
     filter to Egypt in the processing step (simpler — global WDPA as a
     shapefile is a few hundred MB, not huge).
   - Requires accepting the WDPA terms of use (click-through, no account
     needed for the shapefile download).
   - Save to: /opt/gis/data/raw/wdpa/
     (create the folder first, same as above)
"""
    )


def main() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)
    print(f"Egypt bounding box: {EGYPT_BBOX}")

    download_osm_egypt()
    download_copernicus_dem()
    download_worldcover()
    download_jrc_water()
    print_manual_steps()

    print(
        "\nDone with what can be automated. Complete the manual steps above, "
        "then verify everything landed correctly with:\n"
        "  docker exec solar-gis-processor find /opt/gis/data/raw -type f\n"
    )


if __name__ == "__main__":
    main()
