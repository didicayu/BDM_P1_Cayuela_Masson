"""Airflow parent DAG for the complete CyberSecIntel pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def _trigger(task_id: str, dag_id: str) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        poke_interval=15,
    )


with DAG(
    dag_id="cybersecintel_end_to_end",
    description="Run ingestion, warm aggregation, Trusted, Exploitation, and Consumption in dependency order.",
    start_date=datetime(2026, 6, 7),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["bdm", "p2", "end-to-end"],
) as dag:
    trigger_api_ingestion = _trigger("trigger_api_ingestion", "cybersecintel_api_expansion_ingestion")
    trigger_warm_aggregates = _trigger("trigger_warm_aggregates", "cybersecintel_warm_stream_aggregates")
    trigger_trusted_zone = _trigger("trigger_trusted_zone", "cybersecintel_trusted_zone")
    trigger_exploitation_zone = _trigger("trigger_exploitation_zone", "cybersecintel_exploitation_zone")
    trigger_consumption = _trigger("trigger_consumption", "cybersecintel_consumption_exports")

    trigger_api_ingestion >> trigger_warm_aggregates >> trigger_trusted_zone >> trigger_exploitation_zone >> trigger_consumption
