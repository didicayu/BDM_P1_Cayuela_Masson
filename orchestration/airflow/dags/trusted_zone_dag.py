"""Airflow DAG for the P2 Trusted Zone."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_trusted_zone_task() -> dict[str, dict[str, int]]:
    from trusted.run_trusted import run_trusted_zone

    return run_trusted_zone()


with DAG(
    dag_id="cybersecintel_trusted_zone",
    description="Clean P1 Delta assets into separate Trusted Zone Delta tables.",
    start_date=datetime(2026, 5, 9),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["bdm", "p2", "trusted"],
) as dag:
    materialize_trusted_zone = PythonOperator(
        task_id="materialize_trusted_zone",
        python_callable=run_trusted_zone_task,
    )
