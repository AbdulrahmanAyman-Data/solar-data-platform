# Solar Data Platform: Ingestion Layer Guide

A plain-English summary of what we built, how the pieces connect, and how to run and check each one.

---

## 1. The big picture

We are building the **ingestion layer** of the Solar Energy platform. Two weather sources feed the **Bronze** layer of the data lake (MinIO):

| Source | Type | Path | Final destination |
|---|---|---|---|
| **NASA POWER** | Historical (batch, last 5 years) | Python script → MinIO | `bronze/historical/weather` |
| **Open-Meteo** | Near real-time (every poll interval) | Python poller → Kafka → *(Spark job, next step)* → MinIO | `bronze/streaming/weather` |

```
NASA POWER API ──► nasa_loader.py ───────────────────────► MinIO  bronze/historical/weather  (Delta)

Open-Meteo API ──► poller.py ──► Kafka (weather.raw) ──► Spark Streaming ──► MinIO  bronze/streaming/weather
                                                          (NOT written yet)
```

**Status:** everything on the left side of the Spark step is done. The Spark job is the next task.

---

## 2. What we did, step by step

1. **Docker Compose stack**: started the platform (MinIO, Kafka, Spark, Airflow, Trino, Grafana, Prometheus, Superset, etc.).
2. **`.env` file**: created from `.env.example` with our own passwords and a random Superset secret key.
3. **Column schema table**: designed the first weather table (radiation, temperature, wind, humidity, cloud cover, precipitation).
4. **Streaming poller** (`poller.py`): reads current weather from Open-Meteo and publishes JSON messages to Kafka.
5. **Historical loader** (`nasa_loader.py`): downloads hourly NASA POWER data and writes it to MinIO as a Delta table.
6. **Incremental loading**: the loader remembers the last loaded day per location, so re-runs fetch only new data.
7. **Shared locations config**: one `monitored_locations.json` (27 sites) used by both scripts.
8. **Git branch**: moved the work into `feature/ingestion-pipeline`.

---

## 3. Project layout

```
solar-data-platform/
├── .env                          ← real passwords (NEVER commit; must be in .gitignore)
├── .env.example                  ← template with fake values (safe to commit)
├── docker-compose.yml
└── src/ingestion/
    ├── nasa_loader.py            ← historical loader (NASA POWER → MinIO)
    └── streaming_poller/
        ├── poller.py             ← real-time poller (Open-Meteo → Kafka)
        ├── requirements.txt      ← requests, confluent-kafka, tenacity
        ├── Dockerfile
        └── config/
            └── monitored_locations.json   ← 27 locations, shared by both scripts
```

---

## 4. The two Bronze schemas

Both tables use the same column names so they can be unioned in Silver.

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | Deterministic UUID from `source + location_id + observation time` |
| `source` | string | `open-meteo` or `nasa-power` |
| `location_id` | string | e.g. `cairo` (see note below) |
| `site_name` | string | |
| `latitude`, `longitude` | double | |
| `observation_timestamp` | timestamp (UTC) | |
| `ghi_w_m2` | double | Global horizontal irradiance |
| `dni_w_m2` | double | Direct normal irradiance |
| `dhi_w_m2` | double | Diffuse horizontal irradiance |
| `wind_speed_ms` | double | m/s |
| `temperature_c` | double | °C |
| `humidity_pct` | double | % |
| `cloud_cover_pct` | double | % |
| `ingestion_timestamp` | timestamp | Streaming table only (set by the poller) |
| `kafka_timestamp` | timestamp | Streaming table only (added by Spark from the Kafka message) |

> **Note:** the architecture diagram says `location_id` is `int`, but our real IDs are text (`cairo`, ...). We use **string**. Update the diagram to match.

---

## 5. How each script works

### 5.1 `poller.py` (real-time → Kafka)

- Loads locations from `monitored_locations.json`.
- Every `POLL_INTERVAL_SECONDS`, calls Open-Meteo for each location (wind in m/s, time in UTC).
- Skips a reading if it is the same observation as last time (Open-Meteo updates only every 15 minutes).
- Publishes one JSON message per new observation to Kafka topic `weather.raw`, keyed by `location_id`.
- Retries on network errors, HTTP 429 and 5xx only (not on bad-request errors).
- Runs inside Docker as the `solar-streaming-poller` service.

### 5.2 `nasa_loader.py` (historical → MinIO)

- Calls the NASA POWER hourly API in chunks of up to 365 days per location.
- Replaces NASA's `-999` "missing" marker with null and renames columns to our schema.
- Writes to the Delta table `s3://bronze/historical/weather` using a **merge on `event_id`**, so re-runs never create duplicates.
- **Incremental:** reads the latest `observation_timestamp` per location and fetches only from that day onward. A new location automatically gets the full 5 years.
- Ends 7 days before today because NASA publishes with a delay.
- Runs from your machine (not in Docker).

Useful options:

| Setting | Meaning |
|---|---|
| `--full-refresh` | Reload the whole window (merge updates existing rows) |
| `HISTORICAL_YEARS` | How many years to load (default 5) |
| `BRONZE_HISTORICAL_URI` | Target table (default `s3://bronze/historical/weather`) |
| `MONITORED_LOCATIONS_FILE` | Path to the locations JSON |

---

## 6. Locations file format

`src/ingestion/streaming_poller/config/monitored_locations.json`:

```json
[
  { "location_id": "cairo", "site_name": "Cairo", "latitude": 30.04, "longitude": 31.23 }
]
```

All four fields are required. If you edit it: restart the poller (`docker compose restart solar-streaming-poller`) and re-run the loader (it only fetches data for what is missing).

---

## 7. Setup and run (Windows / PowerShell)

### 7.1 Environment file

```powershell
Copy-Item .env.example .env
```

Edit `.env` with unique passwords, then:

```powershell
# generate random values
python -c "import secrets; print(secrets.token_urlsafe(12))"   # passwords
python -c "import secrets; print(secrets.token_urlsafe(48))"   # SUPERSET_SECRET_KEY

# make sure .env is ignored by git
Add-Content .gitignore "`n.env"
git check-ignore -v .env          # must print a line
docker compose config --quiet     # validates the config (no output = OK)
```

### 7.2 Start the stack

First time, or after changing passwords in `.env` (this **deletes all volumes**):

```powershell
docker compose down -v
docker compose up -d --build
docker compose ps -a
```

Expected: services `Up` / `healthy`; the `*-init` services show `Exited (0)` (normal). `solar-spark-streaming` keeps restarting until its job file exists.

### 7.3 Python environment (for the historical loader)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install requests pandas pyarrow deltalake tenacity
```

Load `.env` into the current session:

```powershell
Get-Content .env | Where-Object { $_ -match '^\s*[^#\s].*=' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item "env:$($k.Trim())" $v.Trim() }
```

### 7.4 Run the historical loader

**Test first** on a separate table with one year of data:

```powershell
$env:MONITORED_LOCATIONS_FILE="src/ingestion/streaming_poller/config/monitored_locations.json"
$env:HISTORICAL_YEARS="1"
$env:BRONZE_HISTORICAL_URI="s3://bronze/test/weather"
python src/ingestion/nasa_loader.py
```

**Real run** (5 years): clear the test variables first.

```powershell
Remove-Item env:HISTORICAL_YEARS, env:BRONZE_HISTORICAL_URI
python src/ingestion/nasa_loader.py
```

> Why a test table? The loader is incremental: if a 1-year test wrote to the real table, the next run would continue from there and never fetch the older 4 years.

### 7.5 Run the real-time poller

```powershell
docker compose exec solar-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server solar-kafka:9092 --create --if-not-exists --topic weather.raw --partitions 1 --replication-factor 1
docker compose up -d --build --force-recreate solar-streaming-poller
docker compose logs -f --tail 40 solar-streaming-poller
```

---

## 8. How to check that it works

**Historical data in MinIO**

- Open `http://localhost:9001`, sign in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`.
- Browse to `bronze` → `historical/weather` (or `test/weather`). You should see `_delta_log/` and `.parquet` files.
- Or read it with Python:

```powershell
python -c "import os; from deltalake import DeltaTable; dt=DeltaTable('s3://bronze/test/weather', storage_options={'AWS_ACCESS_KEY_ID':os.environ['MINIO_ROOT_USER'],'AWS_SECRET_ACCESS_KEY':os.environ['MINIO_ROOT_PASSWORD'],'AWS_ENDPOINT_URL':'http://localhost:9000','AWS_ALLOW_HTTP':'true','AWS_REGION':'us-east-1'}); df=dt.to_pandas(); print(df.shape); print(df.head()); print(df.isna().sum())"
```

  Roughly 8,760 rows per location per year. A column that is entirely null means that NASA parameter was not returned.

- Run the loader a second time: it should fetch only the last day or print `up to date`.

**Real-time messages in Kafka**

- Poller log should show `Polling cycle completed: queued=27 ... failed=0`. Later cycles show many `skipped` (normal, new data every 15 min).
- Kafka UI: `http://localhost:8084` → Topics → `weather.raw` → Messages.
- Or from the terminal:

```powershell
docker compose exec solar-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server solar-kafka:9092 --topic weather.raw --from-beginning --max-messages 3
```

Check: `location_id` is text, radiation values are numbers (0 at night is normal), and `observation_timestamp` ends with `Z`.

---

## 9. Troubleshooting

| Problem | Likely cause / fix |
|---|---|
| `KeyError: 'MINIO_ROOT_USER'` | `.env` not loaded into the session (see 7.3) |
| `Connection refused` on port 9000 | MinIO not running: `docker compose up -d solar-minio` |
| `InvalidAccessKeyId` / `SignatureDoesNotMatch` | Password differs from the one MinIO was first started with: `docker compose down -v` then `up -d` |
| `NoSuchBucket` | Run `docker compose up solar-minio-init` |
| `invalid literal for int() ... 'cairo'` | `location_id` must be treated as a string in the scripts |
| `NASA rejected the request` (422) | A parameter is not available hourly (usually `CLOUD_AMT`): remove it from `PARAM_MAP` and the schema |
| `FileNotFoundError` for the JSON | Wrong path or missing Docker volume mount for `config/` |
| `ModuleNotFoundError: tenacity` | Add it to `requirements.txt` and rebuild with `--build` |
| Poller cannot reach Kafka | Inside Docker use `solar-kafka:9092` (not `localhost`) |
| `python` not found / `.venv\Scripts\Activate.ps1` missing | Install Python and recreate the venv: `python -m venv .venv` |

---

## 10. Security and Git checklist

- Never commit `.env`. Confirm `git check-ignore -v .env` prints a line and `git ls-files .env` prints nothing.
- Also ignore `.venv/`.
- Never paste the output of plain `docker compose config` (it prints passwords). Use `--quiet`.
- Work is on branch `feature/ingestion-pipeline`:

```powershell
git add src/ingestion docker-compose.yml .gitignore
git commit -m "feat(ingestion): Open-Meteo poller and incremental NASA POWER loader"
git push -u origin feature/ingestion-pipeline
```

---

## 11. Known limits and tips

- **Open-Meteo rate limit:** the free tier allows about 10,000 requests/day. 27 locations every 5 minutes is ~7,800/day, close to the limit. Open-Meteo updates every 15 minutes anyway, so set `POLL_INTERVAL_SECONDS=900` in `.env` (about 2,600 requests/day) and recreate the poller.
- **NASA data delay:** the newest few days are not available yet, so the loader stops 7 days before today.
- **Incremental gaps:** if a run failed midway for a location, the next run continues from the last saved point and will not backfill older missing days. Use `--full-refresh` to repair.
- **Units:** NASA hourly radiation is numerically the hourly average in W/m², comparable to Open-Meteo values. All timestamps are UTC.

---

## 12. Next steps

1. **Spark Structured Streaming job** (`Kafka → bronze/streaming/weather`): read `weather.raw`, parse the JSON with a schema using `location_id` as string, add `kafka_timestamp` from the Kafka message timestamp, write Delta to MinIO with a checkpoint in the `checkpoints` bucket (watermark 30 min, trigger 1 min).
2. **Silver layer**: union both Bronze tables and de-duplicate on `event_id` / `location_id + observation_timestamp`.
3. **Gold layer + Trino/Hive + Superset/Power BI** dashboards.
4. **Airflow DAGs**: schedule the NASA loader daily (`nasa_power_batch_dag`) and the Medallion batch (`medallion_batch_dag`).
5. Update the architecture diagram: `location_id` type, and the real script names.
