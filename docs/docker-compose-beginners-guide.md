# Running the Solar Data Platform with Docker Compose

This guide is for beginners. Follow the steps in order, and verify each stage before moving to the next one.

> **Important:** The intended GitHub repository contains only the project structure, Docker files, and configuration. Application code such as files under `src`, Airflow DAGs, and GIS scripts is intentionally excluded. You can start and verify the infrastructure services using this guide, but application services such as the streaming poller and Spark streaming job cannot perform their project work until the team recreates and adds the application code.

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
5. Enough free disk space. Spark, Airflow, and Superset images are relatively large. At least 8 GB of memory is helpful; more memory and disk space are recommended when running many services together.

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

`.env.example` is a safe template that can be committed to the repository. The local `.env` file contains values Docker Compose reads and must not be uploaded to GitHub.

Create your local copy:

```powershell
Copy-Item .env.example .env
```

Open `.env` in a text editor and update the passwords and values before starting the services. The template contains these variables:

- `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`: MinIO login.
- `POLL_INTERVAL_SECONDS`: Open-Meteo polling interval. It does not affect the poller until the application code is added.
- `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD`: Grafana login.
- `SUPERSET_SECRET_KEY`: A long, random secret key for Superset.
- `SUPERSET_ADMIN_PASSWORD`: Superset administrator password.
- `MAPBOX_API_KEY`: Optional; used for Mapbox maps.

You can generate a random Superset key in PowerShell with:

```powershell
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Copy the output into `SUPERSET_SECRET_KEY`. If Python is not installed on your computer, enter a long random value manually. Do not share or commit `.env`.

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
