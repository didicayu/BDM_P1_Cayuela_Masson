"""Airflow DAG for bounded warm-stream aggregate materialization."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def compute_warm_stream_aggregates_task() -> dict[str, int | str]:
    from ingestion.stream.warm_aggregates import materialize_warm_stream_aggregates

    return materialize_warm_stream_aggregates(strict_kafka=False)


def validate_warm_stream_aggregates_task() -> dict[str, int | str]:
    from ingestion.stream.warm_aggregates import validate_warm_stream_aggregates

    return validate_warm_stream_aggregates()


with DAG(
    dag_id="cybersecintel_warm_stream_aggregates",
    description="Compute bounded warm IDS alert aggregates into landing and Delta.",
    start_date=datetime(2026, 5, 9),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["bdm", "p2", "warm"],
) as dag:
    compute_warm_aggregates = PythonOperator(
        task_id="compute_warm_aggregates",
        python_callable=compute_warm_stream_aggregates_task,
    )
    validate_warm_aggregates = PythonOperator(
        task_id="validate_warm_aggregates",
        python_callable=validate_warm_stream_aggregates_task,
    )

    compute_warm_aggregates >> validate_warm_aggregates
