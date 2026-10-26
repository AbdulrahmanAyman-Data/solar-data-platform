"""
DAG: nasa_power_batch_dag  (NASA POWER historical -> bronze/historical/weather)

  load_nasa_power_incremental

* Runs once a day at 03:00 UTC, catchup off, one active run at a time.
* The loader is incremental: it reads the last loaded day per location from the Delta table
  and fetches only what is new, then merges on event_id. So retries and re-runs are safe.
* Same pattern as solar_medallion_batch: the Airflow worker does `docker exec` into the
  long-lived `solar-spark-submit-client` container, which has the project mounted at
  /opt/spark-notebooks. The docker CLI comes from `solar-docker-cli-init` (/opt/docker-cli).

Requirements inside that container (see the checklist in the chat):
  * python packages: requests, pandas, pyarrow, deltalake, tenacity
  * env vars: MINIO_ROOT_USER, MINIO_ROOT_PASSWORD
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DOCKER = "/opt/docker-cli/docker"
RUN_CONTAINER = "solar-spark-submit-client"
PROJECT_DIR = "/opt/spark-notebooks"
LOADER_SCRIPT = f"{PROJECT_DIR}/src/ingestion/nasa_loader.py"
LOCATIONS_FILE = f"{PROJECT_DIR}/src/ingestion/streaming_poller/config/monitored_locations.json"

# Non-secret settings only. MINIO_ROOT_USER / MINIO_ROOT_PASSWORD must already exist
# in the container's own environment (set in docker-compose.yml), so no password sits in this file.
LOADER_ENV = {
    "MINIO_ENDPOINT": "http://solar-minio:9000",  # inside the Docker network
    "MONITORED_LOCATIONS_FILE": LOCATIONS_FILE,
    "BRONZE_HISTORICAL_URI": "s3://bronze/historical/weather",
    "HISTORICAL_YEARS": "5",
}


def run_loader_command() -> str:
    """Build the `docker exec ... python3 nasa_loader.py` command line."""
    env_flags = []
    for key, value in LOADER_ENV.items():
        env_flags += ["-e", f"{key}={value}"]
    return " ".join([DOCKER, "exec", *env_flags, RUN_CONTAINER, "python3", LOADER_SCRIPT])


default_args = {
    "owner": "solar-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),  # NASA API hiccups usually clear up quickly
}

with DAG(
    dag_id="nasa_power_batch_dag",
    description="Daily incremental load of NASA POWER hourly data into bronze/historical/weather",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["solar", "weather", "nasa-power", "batch", "bronze"],
) as dag:

    load_nasa_power_incremental = BashOperator(
        task_id="load_nasa_power_incremental",
        bash_command=run_loader_command(),
        execution_timeout=timedelta(hours=2),
    )