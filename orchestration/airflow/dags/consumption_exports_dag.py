"""Airflow DAG for P2 consumption exports."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_consumption_exports_task() -> dict[str, str | int]:
    from consumption.run_exports import run_consumption_exports

    return run_consumption_exports(Path("/opt/project/consumption/outputs"))


with DAG(
    dag_id="cybersecintel_consumption_exports",
    description="Export exploitation assets into analyst-facing CSV, JSON, and HTML files.",
    start_date=datetime(2026, 5, 9),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["bdm", "p2", "consumption"],
) as dag:
    export_consumption_outputs = PythonOperator(
        task_id="export_consumption_outputs",
        python_callable=run_consumption_exports_task,
    )
