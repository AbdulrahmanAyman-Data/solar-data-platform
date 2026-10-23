"""
DAG: solar_medallion_batch  (weather Silver -> Gold)

  run_silver_transform >> run_gold_transform

* Every 30 minutes, catchup off, one active run at a time.
* Both tasks run in ONE DAG (not two schedules) so Gold always aggregates the exact Silver
  output that was just produced in the same run.
* Each task submits its Spark job by `docker exec` into the long-lived `solar-spark-submit-client`
  container (which has the Spark/Delta tooling and the project mounted at /opt/spark-notebooks).
  The docker CLI binary is provided to the Airflow worker by `solar-docker-cli-init`
  (volume mounted at /opt/docker-cli) and the host docker socket is mounted in the worker.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DOCKER = "/opt/docker-cli/docker"
SUBMIT_CONTAINER = "solar-spark-submit-client"
JOBS_DIR = "/opt/spark-notebooks/src/batch/jobs"
PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"


def spark_submit(script_name: str, app_name: str) -> str:
    """Build the `docker exec ... spark-submit ...` command line for one job."""
    return " ".join(
        [
            DOCKER, "exec", "-u", "root", SUBMIT_CONTAINER,
            "/opt/spark/bin/spark-submit",
            "--master", "spark://solar-spark-master:7077",
            "--name", app_name,
            "--packages", PACKAGES,
            "--conf", "spark.jars.ivy=/opt/spark-local/ivy2",
            "--conf", "spark.cores.max=4",
            "--conf", "spark.executor.memory=512m",
            "--conf", "spark.sql.shuffle.partitions=8",
            "--conf", "spark.driver.host=solar-spark-submit-client",
            "--conf", "spark.driver.bindAddress=0.0.0.0",
            f"{JOBS_DIR}/{script_name}",
        ]
    )


default_args = {
    "owner": "solar-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="solar_medallion_batch",
    description="Weather medallion batch: Silver transform -> Gold transform (every 30 minutes)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
    max_active_runs=1,
    tags=["solar", "weather", "medallion", "batch"],
) as dag:

    run_silver_transform = BashOperator(
        task_id="run_silver_transform",
        bash_command=spark_submit("silver_transform.py", "solar_silver_transform"),
        execution_timeout=timedelta(minutes=15),
    )

    run_gold_transform = BashOperator(
        task_id="run_gold_transform",
        bash_command=spark_submit("gold_transform.py", "solar_gold_transform"),
        execution_timeout=timedelta(minutes=15),
    )

    run_silver_transform >> run_gold_transform
