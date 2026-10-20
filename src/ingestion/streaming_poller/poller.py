import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Producer
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather.raw")
OPEN_METEO_BASE_URL = os.getenv(
    "OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast"
)


DEFAULT_LOCATIONS_FILE = Path(__file__).resolve().parent / "config" / "monitored_locations.json"
LOCATIONS_FILE = os.getenv("MONITORED_LOCATIONS_FILE", str(DEFAULT_LOCATIONS_FILE))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "300"))
SOURCE = "open-meteo"


CURRENT_VARS = ",".join(
    [
        "shortwave_radiation",        # -> ghi_w_m2
        "direct_normal_irradiance",   # -> dni_w_m2
        "diffuse_radiation",          # -> dhi_w_m2
        "wind_speed_10m",             # -> wind_speed_ms (بعد wind_speed_unit=ms)
        "temperature_2m",             # -> temperature_c
        "relative_humidity_2m",       # -> humidity_pct
        "cloud_cover",                # -> cloud_cover_pct
    ]
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

running = True
last_observation: dict[int, str] = {}  


def stop_handler(signum, frame):
    global running
    logger.info("Shutdown signal received")
    running = False


signal.signal(signal.SIGTERM, stop_handler)
signal.signal(signal.SIGINT, stop_handler)


def load_locations():
    path = Path(LOCATIONS_FILE)
    with path.open("r", encoding="utf-8") as file:
        locations = json.load(file)

    if not isinstance(locations, list) or not locations:
        raise ValueError("Locations file must contain a non-empty JSON list")

    required = {"location_id", "site_name", "latitude", "longitude"}
    for loc in locations:
        missing = required - loc.keys()
        if missing:
            raise ValueError(f"Location {loc} is missing fields: {sorted(missing)}")

    logger.info("Loaded %s monitored locations from %s", len(locations), path)
    return locations


def _is_retryable(exc: BaseException) -> bool:
    """نعيد المحاولة على أخطاء الشبكة و 429 و 5xx فقط، مش 4xx (إحداثيات غلط مثلًا)."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        code = exc.response.status_code
        return code == 429 or code >= 500
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception(_is_retryable),
    reraise=True,
)
def fetch_weather(location):
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "current": CURRENT_VARS,
        "wind_speed_unit": "ms",  # m/s بدل km/h
        "timezone": "UTC",
    }

    response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=20)
    response.raise_for_status()

    current = response.json().get("current")
    if not current:
        raise ValueError(f"No current weather data returned for {location['location_id']}")

    location_id = int(location["location_id"])
   
    observation_ts = f"{current['time']}:00Z"

    return {
        #Bronze_weather 
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{SOURCE}:{location_id}:{observation_ts}")),
        "source": SOURCE,
        "location_id": location_id,
        "site_name": location["site_name"],
        "latitude": float(location["latitude"]),
        "longitude": float(location["longitude"]),
        "observation_timestamp": observation_ts,
        "ghi_w_m2": current.get("shortwave_radiation"),
        "dni_w_m2": current.get("direct_normal_irradiance"),
        "dhi_w_m2": current.get("diffuse_radiation"),
        "wind_speed_ms": current.get("wind_speed_10m"),
        "temperature_c": current.get("temperature_2m"),
        "humidity_pct": current.get("relative_humidity_2m"),
        "cloud_cover_pct": current.get("cloud_cover"),
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        # kafka_timestamp مش بيتبعت من هنا: Spark بيقراه من عمود timestamp بتاع Kafka
    }


def on_delivery(err, message):
    if err:
        logger.error("Kafka delivery failed: topic=%s error=%s", message.topic(), err)
    else:
        logger.debug(
            "Delivered topic=%s partition=%s offset=%s key=%s",
            message.topic(),
            message.partition(),
            message.offset(),
            message.key().decode("utf-8") if message.key() else None,
        )


def main():
    locations = load_locations()

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "solar-weather-poller",
            "acks": "all",
        }
    )

    logger.info(
        "Starting weather poller: bootstrap=%s topic=%s interval=%ss",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        POLL_INTERVAL_SECONDS,
    )

    try:
        while running:
            cycle_started = time.monotonic()
            sent = skipped = failed = 0

            for location in locations:
                if not running:
                    break

                location_id = location.get("location_id", "unknown")

                try:
                    weather = fetch_weather(location)

                    
                    if last_observation.get(weather["location_id"]) == weather["observation_timestamp"]:
                        skipped += 1
                        continue
                    last_observation[weather["location_id"]] = weather["observation_timestamp"]

                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=str(weather["location_id"]).encode("utf-8"),
                        value=json.dumps(weather, ensure_ascii=False).encode("utf-8"),
                        callback=on_delivery,
                    )
                    producer.poll(0)
                    sent += 1

                except Exception:
                    failed += 1
                    logger.exception("Failed to process location %s", location_id)

            remaining = producer.flush(timeout=30)
            if remaining:
                logger.warning("%s Kafka message(s) still pending after flush", remaining)

            logger.info(
                "Polling cycle completed: queued=%s skipped(no new obs)=%s failed=%s duration=%.1fs",
                sent,
                skipped,
                failed,
                time.monotonic() - cycle_started,
            )

            for _ in range(POLL_INTERVAL_SECONDS):
                if not running:
                    break
                time.sleep(1)

    finally:
        producer.flush(timeout=30)
        logger.info("Weather poller stopped")


if __name__ == "__main__":
    main()