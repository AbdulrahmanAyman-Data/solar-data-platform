# Running the Solar Data Platform with Docker Compose

## 1. What will you run?

Docker Compose starts a group of connected containers:

| Area | Services | What they do |
|---|---|---|
| Storage | MinIO | Stores data files through an S3-compatible API |
| Messaging | Kafka, Kafka UI, Kafka Exporter | Receives messages, lets you browse them, and exposes Kafka metrics |
| Processing | Spark Master, Worker, Jupyter | Runs Spark and provides an interactive environment |
| Data catalog | Metastore PostgreSQL, Hive Metastore | Stores table metadata for Spark and Trino |
| Query engine | Trino | Queries data stored in MinIO |
| Monitoring | Prometheus, Grafana | Collects metrics and displays dashboards |
| Scheduling | PostgreSQL, Redis, Airflow | Schedules data jobs after DAGs are added |
| Business intelligence | PostgreSQL, Superset | Builds and displays BI dashboards |
| GIS tools | GIS Processor | Provides a Jupyter environment for GIS work |

Compose creates an internal network named `pipeline-net` and persistent storage volumes for services. Services use each other's Compose service names to communicate on the network.

## 2. Requirements

1. A Windows, Linux, or macOS computer.
2. Docker Desktop on Windows/macOS, or Docker Engine with the Docker Compose plugin on Linux.
3. Docker Desktop must be running Linux containers.
4. An internet connection for the first startup, because Docker must download images and some packages.
5. Enough free disk space. Spark, Airflow, and Superset images are relatively large. At least 10 GB of memory is helpful; more memory and disk space are recommended when running many services together.

Open PowerShell in the project folder. You should see `docker-compose.yml` and `.env.example`:

```powershell
Get-Location
Get-ChildItem
```

If you have not downloaded the project yet, clone it from GitHub and enter its folder:

```powershell
git clone <repository-url>
cd solar_data_platform
```

Replace `<repository-url>` with the actual repository URL. Do not type the angle brackets.

## 3. Create the `.env` file

1. Make sure PowerShell is open in the same folder as `docker-compose.yml` and `.env.example`.
2. Copy the template to a new local file named `.env`:

```powershell
Copy-Item .env.example .env
```

3. Open the new file in Notepad:

```powershell
notepad .env
```

4. Replace the example values with your own values. Keep the format `NAME=value`: do not remove the variable name, and do not add spaces around `=`. For example:

```dotenv
MINIO_ROOT_USER=solaradmin
MINIO_ROOT_PASSWORD=use-a-unique-password-here
POLL_INTERVAL_SECONDS=300
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=use-another-unique-password-here
SUPERSET_SECRET_KEY=paste-a-long-random-key-here
SUPERSET_ADMIN_PASSWORD=use-a-third-unique-password-here
MAPBOX_API_KEY=
```

The example passwords above are placeholders. Replace each one with a different, strong password before starting the services. For a local learning setup, use passwords that are easy for you to retrieve but are not used for any personal or production account. Avoid spaces in values. Leave `MAPBOX_API_KEY` blank if you do not use Mapbox.

Here is what each variable controls:

- `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`: MinIO login.
- `POLL_INTERVAL_SECONDS`: How often the Open-Meteo poller requests data, in seconds. `300` means every five minutes. It does not affect the poller until the application code is added.
- `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD`: Grafana login.
- `SUPERSET_SECRET_KEY`: A long, cryptographically random private key used by Superset to sign sessions and encrypt sensitive values in its metadata database. Do not type a phrase yourself or keep the example placeholder. Generate a unique key for this Superset installation, save it in `.env`, and keep the same key when restarting the project. If you change it after Superset has stored data, use Superset's documented key-rotation and re-encryption process; changing it casually can make previously encrypted values unreadable.
- `SUPERSET_ADMIN_PASSWORD`: Superset administrator password.
- `MAPBOX_API_KEY`: Optional; used for Mapbox maps. Leave it empty if you do not have a key.

Generate a random Superset key in PowerShell with:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Copy the printed value and paste it after `SUPERSET_SECRET_KEY=` in Notepad, replacing `paste-a-long-random-key-here`. Save the file and close Notepad. If the `python` command is not available, install Python or use another trusted cryptographically secure random-key generator to create a long random key. The official Superset guide also shows `openssl rand -base64 42` as a way to generate one.

5. Confirm that `.env` exists next to `docker-compose.yml`:

```powershell
Test-Path .env
```

The result should be `True`. Then check the Compose configuration:

```powershell
docker compose config --quiet
```

If this command returns without an error, Compose can read the project configuration. Do not display `.env` in screenshots, send it in chat, or commit it to GitHub. The repository's `.gitignore` excludes `.env`; only `.env.example` should be committed.

## 4. Validate the configuration

From the project folder, run:

```powershell
docker --version
docker compose version
docker compose config --quiet
```

- The first two commands display the Docker and Compose versions.
- If `docker compose config --quiet` exits without an error, the Compose file is syntactically valid.
- If Docker reports that it cannot connect to the daemon, start Docker Desktop and wait until its status is Running.
- If a variable is missing, make sure `.env` exists next to `docker-compose.yml`.

## 5. Understand the basic commands

- `docker compose up -d SERVICE`: Creates and starts a service in the background. Compose also starts its declared dependencies.
- `docker compose ps`: Displays service status.
- `docker compose logs -f --tail 100 SERVICE`: Shows the latest 100 log lines and follows new output. Press `Ctrl+C` to stop following logs; this does not stop the container.
- `docker compose stop SERVICE`: Stops a service without deleting its container or data.
- `docker compose down`: Stops and removes containers and the network, but keeps persistent data volumes.

The first startup may take a while because Docker downloads images and builds the Spark and GIS images. Later starts are usually faster because Docker reuses downloaded image layers.

## 6. Start the services step by step

Run these commands from the project root and follow the order below. After each group, check `docker compose ps` and the service logs.

### Stage 1: MinIO and data buckets

```powershell
docker compose up -d solar-minio
docker compose ps solar-minio
docker compose logs --tail 80 solar-minio
docker compose up solar-minio-init
```

Wait for `solar-minio` to show `Up` or `healthy`. The one-time `solar-minio-init` service creates the `bronze`, `silver`, `gold`, `checkpoints`, and `warehouse` buckets. It is normal for this initialization service to exit after it finishes.

Open the MinIO Console at <http://localhost:9001>. Sign in with `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` from `.env`.

### Stage 2: Kafka and message browsing tools

```powershell
docker compose up -d solar-kafka
docker compose ps solar-kafka
docker compose logs --tail 100 solar-kafka
docker compose up -d solar-kafka-ui solar-kafka-exporter
```

Wait for Kafka to start. Then open Kafka UI at <http://localhost:8084>. It should show a cluster named `local`, connected to Kafka at `solar-kafka:9092` inside the Docker network.

Kafka Exporter does not provide a user interface. It reads Kafka metrics on internal port 9308 so Prometheus can collect them later.

### Stage 3: Spark

Start the Master and wait for it to become available, then start the Worker and the related tools:

```powershell
docker compose up -d solar-spark-master
docker compose ps solar-spark-master
docker compose logs --tail 100 solar-spark-master
docker compose up -d solar-spark-worker solar-jupyter solar-spark-submit-client
```

Compose builds the `local-pyspark` image from `docker/spark/Dockerfile.spark` if it does not already exist. Open:

- Spark Master UI: <http://localhost:8081>. After the Worker starts, check that one worker is active.
- Spark Worker UI: <http://localhost:8082>.
- JupyterLab: <http://localhost:8888>.

`solar-spark-submit-client` is a helper container that Airflow uses to submit Spark commands. It does not have a web interface.

**Check that Jupyter and Spark work:** Open a Terminal in JupyterLab and run:

```bash
python3 -c "from pyspark.sql import SparkSession; s=SparkSession.builder.appName('smoke-check').getOrCreate(); print(s.version); s.stop()"
```

If it prints the Spark version and exits, Spark started successfully in local mode. The first startup may take time while packages download. This check does not confirm connections to MinIO or Hive; those are checked in later stages.

### Stage 4: Hive Metastore

```powershell
docker compose up -d solar-metastore-db
docker compose ps solar-metastore-db
docker compose logs --tail 80 solar-metastore-db
docker compose up -d solar-hive-metastore
docker compose ps solar-hive-metastore
docker compose logs --tail 120 solar-hive-metastore
```

Wait until PostgreSQL is healthy, then wait for Hive Metastore to start. This service usually has no web page. Compose checks its Thrift connection on port 9083. Check `healthy` in `docker compose ps` or inspect its logs.

### Stage 5: Trino

```powershell
docker compose up -d solar-trino
docker compose ps solar-trino
docker compose logs --tail 120 solar-trino
```

Trino depends on Hive Metastore and MinIO, so start it only after Stages 1 and 4 succeed. Open the Trino UI at <http://localhost:8085>. The service information endpoint is <http://localhost:8085/v1/info>.

### Stage 6: Airflow and its database

Start the database and Redis first:

```powershell
docker compose up -d solar-postgres-airflow solar-redis
docker compose ps solar-postgres-airflow solar-redis
docker compose logs --tail 80 solar-postgres-airflow
docker compose logs --tail 80 solar-redis
```

After the database and Redis are ready, run the one-time initialization services:

```powershell
docker compose up solar-airflow-init solar-docker-cli-init
```

Wait for both commands to finish successfully. `solar-airflow-init` prepares the Airflow database. `solar-docker-cli-init` provides the Docker command-line tool that the Airflow Worker needs. Then start the Airflow services:

```powershell
docker compose up -d solar-airflow-apiserver solar-airflow-scheduler solar-airflow-worker solar-airflow-triggerer solar-airflow-dag-processor
docker compose ps
```

Open Airflow at <http://localhost:8083>. Local account credentials are configured in `config/airflow/simple_auth_manager_passwords.json` and the Compose settings. No DAGs may appear in this structure-only repository because the DAG source files are excluded.

### Stage 7: Prometheus and Grafana

```powershell
docker compose up -d solar-prometheus
docker compose ps solar-prometheus
docker compose logs --tail 80 solar-prometheus
docker compose up -d solar-grafana
docker compose ps solar-grafana
docker compose logs --tail 80 solar-grafana
```

Open:

- Prometheus: <http://localhost:9090/targets>. Check that the listed targets show `UP`.
- Grafana: <http://localhost:3000>. Sign in using `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` from `.env`.

Prometheus and Grafana settings are provisioned from `config/prometheus` and `config/grafana`. Grafana may take about half a minute to load the provisioned dashboard files.

#### How metrics get from Kafka, Spark, and Trino into Grafana

Prometheus does not collect metrics directly from every application in the same way. This project uses three different collection methods:

| Source | How metrics are exposed | What you need to start |
|---|---|---|
| Kafka | `solar-kafka-exporter` is a separate container. It reads Kafka and exposes metrics on port 9308. | Start Kafka first, then `solar-kafka-exporter`. Stage 2 already starts it. |
| Spark | The Spark Master and Worker expose Prometheus endpoints using `config/spark/metrics.properties`, mounted by Compose. | Start `solar-spark-master` and `solar-spark-worker`. Their metrics endpoints are enabled by the mounted configuration. |
| Trino | The JMX Prometheus Java agent runs inside the Trino process. Its settings and Java agent are mounted from `config/trino`. There is no separate Trino exporter container. | Start `solar-trino`. The agent exposes metrics on port 9401 inside the Docker network. |
| Prometheus | Scrapes the Kafka Exporter, Spark Master, Spark Worker, and Trino endpoints every 15 seconds. | Start `solar-prometheus` after the source services are running. Its scrape targets are in `config/prometheus/prometheus.yml`. |
| Grafana | Reads the metrics from Prometheus and loads the provisioned dashboard. | Start `solar-grafana` after Prometheus is healthy. Its data source and dashboard files are in `config/grafana`. |

The collectors do not all start automatically with their source applications: Kafka Exporter is its own service and must be running. Spark's metrics endpoint and Trino's JMX agent are part of their respective services and start with them. Prometheus and Grafana are separate services and must also be started. If you followed the earlier stages, these components are already started; Compose also starts Kafka Exporter when Prometheus is started because Prometheus declares it as a dependency.

To check the collector and source services:

```powershell
docker compose ps solar-kafka solar-kafka-exporter solar-spark-master solar-spark-worker solar-trino solar-prometheus solar-grafana
```

Wait at least 15 seconds after starting Prometheus, then open <http://localhost:9090/targets>. The jobs `kafka`, `spark-master`, `spark-worker`, and `trino` should show `UP`. `UP` means Prometheus can scrape the endpoint; it does not mean that every application-specific metric has a value.

You can check metrics directly in the Prometheus expression box by entering these queries one at a time:

```promql
up{job="kafka"}
up{job="spark-master"}
up{job="spark-worker"}
up{job="trino"}
```

When a target is up, these example metric queries should return data if their source is running and exporting the metric:

```promql
kafka_brokers
metrics_master_aliveWorkers_Value
trino_execution_querymanager_runningqueries
```

Prometheus currently scrapes the Spark Master and Worker daemons. It does not automatically discover every Spark application UI, because each application can use a different address and port.

**About Kafka consumer lag:** Kafka Exporter reports lag for consumer groups with committed Kafka offsets. Spark Structured Streaming tracks offsets in its checkpoint, so its processing delay may not appear as a Kafka consumer-group lag metric. An empty lag panel can therefore mean that no matching committed-offset consumer group exists; it does not by itself prove that Kafka or Prometheus is broken. Check that the Kafka target is `UP` and that `kafka_consumergroup_lag` returns series before diagnosing the panel.

### Stage 8: Superset

Start the Superset database and wait until it is healthy:

```powershell
docker compose up -d solar-postgres-superset
docker compose ps solar-postgres-superset
docker compose logs --tail 80 solar-postgres-superset
```

On a new database, run initialization once and wait for it to finish:

```powershell
docker compose up solar-superset-init
```

Then start Superset after Trino is ready:

```powershell
docker compose up -d solar-superset
docker compose ps solar-superset
docker compose logs --tail 120 solar-superset
```

Open <http://localhost:8088>. The initialization service creates the `superset` user. Its password is the value of `SUPERSET_ADMIN_PASSWORD` in `.env` (or the default if you did not change it). Superset may install some libraries the first time it starts, so this step can take a while.

### Stage 9: GIS Processor (optional)

```powershell
docker compose up -d solar-gis-processor
docker compose ps solar-gis-processor
docker compose logs --tail 100 solar-gis-processor
```

Compose builds the GIS image from `docker/gis/Dockerfile.gis`. Open its Jupyter interface at <http://localhost:8889>. The GIS project data is not included in this structure-only repository and must be recreated by the team.

## 7. Start all infrastructure services together

After completing the one-time initialization steps, you can start the services together:

```powershell
docker compose up -d
```

In this structure-only repository, do not expect `solar-streaming-poller` or `solar-spark-streaming` to run successfully. Their application source files are not included. The poller may repeatedly restart, and the Spark streaming service may fail because it cannot find its job file. This does not mean MinIO, Kafka, Spark Master, or Grafana are broken. After the team recreates the code, rebuild the poller image and recreate the affected services:

```powershell
docker compose up -d --build --force-recreate solar-streaming-poller solar-spark-streaming
```

Do not run this command until the application code exists.

## 8. Quick verification checklist

Show all services, including ones that have exited:

```powershell
docker compose ps -a
```

Follow the logs for a service that did not start:

```powershell
docker compose logs -f --tail 100 solar-trino
```

Replace `solar-trino` with the service name shown in `docker compose ps -a`. You can also open these local pages in your browser:

| Service | Local address |
|---|---|
| MinIO Console | <http://localhost:9001> |
| Spark Master | <http://localhost:8081> |
| Spark Worker | <http://localhost:8082> |
| JupyterLab | <http://localhost:8888> |
| Kafka UI | <http://localhost:8084> |
| Airflow | <http://localhost:8083> |
| Trino | <http://localhost:8085> |
| Prometheus | <http://localhost:9090> |
| Grafana | <http://localhost:3000> |
| Superset | <http://localhost:8088> |
| GIS Jupyter | <http://localhost:8889> |

`Up` means a container is running. `healthy` means the health check defined for it succeeded. Services without a health check may show only `Up`; inspect their logs and open their web interface to verify them.

## 9. Common problems and solutions

### Docker is not running

Start Docker Desktop, wait until its engine is running, and retry:

```powershell
docker info
docker compose ps
```

### A port is already in use

You may see `port is already allocated`. Stop the program using that port, or change the left-hand port in `docker-compose.yml`. For example, in `"8083:8080"`, `8083` is the port on your computer and `8080` is the port inside the container. After changing it, recreate the affected service.

### A container is `Restarting` or `Exited`

Read the logs before deleting anything:

```powershell
docker compose logs --tail 150 SERVICE_NAME
```

Replace `SERVICE_NAME` with the name shown by `docker compose ps -a`. If the service requires source code excluded from this structure-only repository, see the warning at the beginning of this guide.

### A service says its database or another dependency is not ready

Start the dependency first and wait for its status to become `healthy`, then check the dependent service again:

```powershell
docker compose ps
```

Compose starts declared dependencies in order, but some applications need more time after opening their port to complete initialization.

### The first startup is slow

This is normal while Docker downloads images or installs libraries. Check the logs for progress. Avoid stopping the download while it is making progress. Do not use `--build` on every run; use it when a Dockerfile or a build requirement changes.

### Login credentials do not work

Check `.env`, then recreate the service that reads the changed variables. For example:

```powershell
docker compose up -d --force-recreate solar-grafana
```

For Superset, the admin account is created during `solar-superset-init` on a new database. Changing the password in `.env` does not automatically change an existing Superset user.

### A file or folder is missing during build

Check that the files required by the service exist under the `build.context` path in `docker-compose.yml`. If the service needs application code, the team must add that code to the new repository first.

## 10. Stop and restart the project

Stop the containers while keeping persistent data:

```powershell
docker compose down
```

Start them again later:

```powershell
docker compose up -d
```

Stop one service only:

```powershell
docker compose stop solar-grafana
```

Start it again:

```powershell
docker compose start solar-grafana
```

> **Warning:** The following command deletes persistent volumes, including databases, Kafka messages, and data stored by services. Do not use it just to restart the project.
>
> ```powershell
> docker compose down -v
> ```

## 11. After the team recreates the application code

When the application source files are added, validate Compose and rebuild the poller image, which is built from the application directory:

```powershell
docker compose config --quiet
docker compose up -d --build
```

Then inspect the application service logs:

```powershell
docker compose logs -f --tail 100 solar-streaming-poller
docker compose logs -f --tail 100 solar-spark-streaming
```

Open Airflow and confirm that the DAGs appear. Open Grafana and confirm that metrics are available. A successful container build alone does not prove that the full data pipeline works. Also verify that Kafka receives messages, Bronze data is written, and the data can be queried in Trino or Superset.
