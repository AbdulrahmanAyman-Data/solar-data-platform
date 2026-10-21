"""
NASA POWER historical ingestion (hourly, last N years)
-> MinIO  s3://bronze/historical/weather  (Delta)

Schema = Bronze_historical_weather (Nasa Power).
Locations come from the same config/monitored_locations.json used by the streaming poller.

Run from the project root:
    pip install requests pandas pyarrow deltalake tenacity
    python historical_poller/nasa_power_historical.py
Incremental: first run loads the last N years; later runs load only what is new
per location (from the last loaded day up to today - NASA_END_LAG_DAYS).
Use --full-refresh to reload the whole window. Re-running is safe: rows merge on event_id.
"""
import argparse
Re-running is safe: rows are merged on event_id.
"""
import json
import logging
import os
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import requests
from deltalake import DeltaTable, write_deltalake
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()  # بيقرا ملف .env ويحط المتغيرات في الـ Environment أوتوماتيك
#Configuration
NASA_URL = os.getenv("NASA_POWER_URL", "https://power.larc.nasa.gov/api/temporal/hourly/point")
LOCATIONS_FILE = os.getenv("MONITORED_LOCATIONS_FILE", "config/monitored_locations.json")
YEARS_BACK = int(os.getenv("HISTORICAL_YEARS", "5"))
END_LAG_DAYS = int(os.getenv("NASA_END_LAG_DAYS", "7"))  # NASA API data is delayed by ~7 days, so we don't try to load the last week
SOURCE = "nasa-power"

#http://localhost:9000 | http://solar-minio:9000
#  Configuration
NASA_URL = os.getenv("NASA_POWER_URL", "https://power.larc.nasa.gov/api/temporal/hourly/point")
LOCATIONS_FILE = os.getenv("MONITORED_LOCATIONS_FILE", "config/monitored_locations.json")
YEARS_BACK = int(os.getenv("HISTORICAL_YEARS", "5"))
END_LAG_DAYS = int(os.getenv("NASA_END_LAG_DAYS", "7"))  
SOURCE = "nasa-power"

# http://localhost:9000 |  http://solar-minio:9000
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
TABLE_URI = os.getenv("BRONZE_HISTORICAL_URI", "s3://bronze/historical/weather")
STORAGE_OPTS = {
    "AWS_ACCESS_KEY_ID": os.environ["MINIO_ROOT_USER"],
    "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_ROOT_PASSWORD"],
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

#Bronze
# Bronze 
PARAM_MAP = {
    "ALLSKY_SFC_SW_DWN": "ghi_w_m2",
    "ALLSKY_SFC_SW_DNI": "dni_w_m2",
    "ALLSKY_SFC_SW_DIFF": "dhi_w_m2",
    "WS10M": "wind_speed_ms",
    "T2M": "temperature_c",
    "RH2M": "humidity_pct",
    "CLOUD_AMT": "cloud_cover_pct",
}
FILL_VALUE = -999

#schema

SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("source", pa.string()),
        ("location_id", pa.string()),
        ("location_id", pa.int32()),
        ("site_name", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("observation_timestamp", pa.timestamp("us", tz="UTC")),
        ("ghi_w_m2", pa.float64()),
        ("dni_w_m2", pa.float64()),
        ("dhi_w_m2", pa.float64()),
        ("wind_speed_ms", pa.float64()),
        ("temperature_c", pa.float64()),
        ("humidity_pct", pa.float64()),
        ("cloud_cover_pct", pa.float64()),
    ]
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("nasa_historical")


#Helpers
def load_locations() -> list[dict]:
    with Path(LOCATIONS_FILE).open("r", encoding="utf-8") as f:
        locations = json.load(f)
    if not isinstance(locations, list) or not locations:
        raise ValueError("Locations file must contain a non-empty JSON list")
    required = {"location_id", "site_name", "latitude", "longitude"}
    for loc in locations:
        missing = required - loc.keys()
        if missing:
            raise ValueError(f"Location {loc} is missing fields: {sorted(missing)}")
    logger.info("Loaded %s monitored locations", len(locations))
    return locations


def date_chunks(start: date, end: date, days: int = 365):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=days - 1), end)
        yield cur, nxt
        cur = nxt + timedelta(days=1)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return False


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
def fetch_parameters(location: dict, start: date, end: date) -> dict:
    params = {
        "start": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "community": "RE",
        "parameters": ",".join(PARAM_MAP),
        "format": "JSON",
        "time-standard": "UTC",
    }
    response = requests.get(NASA_URL, params=params, timeout=120)
    if response.status_code == 422:
        raise RuntimeError(f"NASA rejected the request (check PARAM_MAP): {response.text[:500]}")
    response.raise_for_status()
    return response.json()["properties"]["parameter"]


def build_table(location: dict, parameters: dict) -> pa.Table:
    df = pd.DataFrame(parameters)  # index = "YYYYMMDDHH"
    df = df.replace(FILL_VALUE, float("nan")).astype("float64").rename(columns=PARAM_MAP)
    df.index = pd.to_datetime(df.index, format="%Y%m%d%H", utc=True)
    df = df.rename_axis("observation_timestamp").reset_index()

    location_id = str(location["location_id"])
    location_id = int(location["location_id"])
    df["observation_timestamp"] = df["observation_timestamp"].astype("datetime64[us, UTC]")
    df["event_id"] = [
        str(uuid.uuid5(uuid.NAMESPACE_URL, f"{SOURCE}:{location_id}:{ts.strftime('%Y%m%d%H')}"))
        for ts in df["observation_timestamp"]
    ]
    df["source"] = SOURCE
    df["location_id"] = location_id
    df["site_name"] = location["site_name"]
    df["latitude"] = float(location["latitude"])
    df["longitude"] = float(location["longitude"])

    df = df[[f.name for f in SCHEMA]]
    return pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)


def save(table: pa.Table) -> None:
    if DeltaTable.is_deltatable(TABLE_URI, storage_options=STORAGE_OPTS):
        dt = DeltaTable(TABLE_URI, storage_options=STORAGE_OPTS)
        (
            dt.merge(
                source=table,
                predicate="t.event_id = s.event_id",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    else:
        write_deltalake(TABLE_URI, table, storage_options=STORAGE_OPTS)


def get_last_loaded() -> dict[str, date]:
    """آخر يوم اتحمّل لكل موقع من جدول Delta (فاضي لو الجدول لسه مش موجود)."""
    if not DeltaTable.is_deltatable(TABLE_URI, storage_options=STORAGE_OPTS):
        return {}
    dt = DeltaTable(TABLE_URI, storage_options=STORAGE_OPTS)
    df = dt.to_pyarrow_table(columns=["location_id", "observation_timestamp"]).to_pandas()
    if df.empty:
        return {}
    last = df.groupby("location_id")["observation_timestamp"].max()
    return {str(loc_id): ts.date() for loc_id, ts in last.items()}


#Main
def main(full_refresh: bool = False) -> None:
    locations = load_locations()
    end = date.today() - timedelta(days=END_LAG_DAYS)
    window_start = end - timedelta(days=365 * YEARS_BACK) + timedelta(days=1)
    last_loaded = {} if full_refresh else get_last_loaded()

    for location in locations:
        location_id = str(location["location_id"])

        
        start = max(last_loaded[location_id], window_start) if location_id in last_loaded else window_start

        if start > end:
            logger.info("%s | up to date (last loaded %s)", location["site_name"], last_loaded.get(location_id))
            continue

        logger.info("%s | fetching %s -> %s", location["site_name"], start, end)
#Main
def main() -> None:
    locations = load_locations()
    end = date.today() - timedelta(days=END_LAG_DAYS)
    start = end.replace(year=end.year - YEARS_BACK) + timedelta(days=1)
    logger.info("Fetching %s -> %s for %d locations -> %s", start, end, len(locations), TABLE_URI)

    for location in locations:
        for chunk_start, chunk_end in date_chunks(start, end):
            parameters = fetch_parameters(location, chunk_start, chunk_end)
            table = build_table(location, parameters)
            save(table)
            logger.info(
                "%s | %s -> %s | %d rows saved",
                location["site_name"], chunk_start, chunk_end, table.num_rows,
            )
            time.sleep(2)  # to avoid hitting NASA too fast (429)
            time.sleep(2)  

    logger.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--full-refresh", action="store_true", help="reload the whole N-year window")
    main(full_refresh=parser.parse_args().full_refresh)
    main()
