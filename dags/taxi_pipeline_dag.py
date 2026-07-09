from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.load_to_snowflake import main as load_to_snowflake
from ingestion.validate_files import validate_source_files
from dags.dbt_runner import dbt_run, dbt_test


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="firmable_nyc_taxi_platform",
    description="End-to-end NYC Taxi pipeline: file validation, Snowflake ingestion, dbt transformations and dbt tests.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["firmable", "nyc-taxi", "snowflake", "dbt"],
) as dag:

    validate_files = PythonOperator(
        task_id="validate_source_files",
        python_callable=validate_source_files,
    )

    load_raw_data = PythonOperator(
        task_id="load_raw_data_to_snowflake",
        python_callable=load_to_snowflake,
    )

    run_dbt_models = PythonOperator(
        task_id="run_dbt_models",
        python_callable=dbt_run,
    )

    run_dbt_tests = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=dbt_test,
    )

    validate_files >> load_raw_data >> run_dbt_models >> run_dbt_tests