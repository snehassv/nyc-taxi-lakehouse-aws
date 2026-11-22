"""NYC taxi lakehouse: raw files -> S3 -> Glue (PySpark + Iceberg) -> Athena benchmark.

Run it from the Airflow UI (http://localhost:8080) or:
    docker compose exec airflow airflow dags trigger nyc_taxi_lakehouse

Settings come from environment variables in ../.env (written by `make env`).
"""
import os

import pendulum
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import dag, task

YEARS = os.environ.get("TAXI_YEARS", "2011-2025")
MONTH = int(os.environ.get("TAXI_MONTH", "1"))


@dag(
    dag_id="nyc_taxi_lakehouse",
    schedule=None,  # run by hand; TLC data only changes monthly
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["portfolio", "aws", "iceberg"],
    params={"reset": "false"},
)
def nyc_taxi_lakehouse():
    @task
    def upload_raw_files() -> int:
        """Download one month per year from TLC and upload to s3://<bucket>/raw/yellow/."""
        from pipeline import raw

        manifest = raw.run(os.environ["TAXI_BUCKET"], raw.parse_years(YEARS), MONTH)
        return sum(m["rows"] for m in manifest)

    load = GlueJobOperator(
        task_id="glue_load_iceberg",
        job_name=os.environ.get("TAXI_GLUE_JOB", "nyc-taxi-load-trips"),
        script_args={"--reset": "{{ params.reset }}"},
        wait_for_completion=True,
        verbose=False,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"),
    )

    @task
    def write_load_report():
        from pipeline import load_report
        import sys

        sys.argv = ["load_report", "--bucket", os.environ["TAXI_BUCKET"]]
        load_report.main()

    @task
    def athena_benchmark():
        from pipeline import athena_bench
        import sys

        sys.argv = ["athena_bench",
                    "--workgroup", os.environ["TAXI_ATHENA_WORKGROUP"],
                    "--database", os.environ["TAXI_DATABASE"]]
        athena_bench.main()

    upload_raw_files() >> load >> write_load_report() >> athena_benchmark()


dag_object = nyc_taxi_lakehouse()
