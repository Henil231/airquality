"""Batch lane DAG: ingest readings, then roll up monthly trends."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
PG_DSN = "postgresql://air:air@db:5432/air"
PG_JDBC = "jdbc:postgresql://db:5432/air?user=air&password=air"
PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3"

with DAG(
    dag_id="air_quality_batch",
    description="Ingest readings and build monthly trends",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["airquality"],
) as dag:
    ingest = BashOperator(
        task_id="ingest_readings",
        bash_command=f"python {PROJECT}/fetchers/fetch.py --sink postgres --pg '{PG_DSN}'",
    )
    trends = BashOperator(
        task_id="build_trends",
        bash_command=(
            f"spark-submit --packages {PACKAGES} "
            f"{PROJECT}/spark/batch_trends.py --pg '{PG_JDBC}'"
        ),
    )
    ingest >> trends
