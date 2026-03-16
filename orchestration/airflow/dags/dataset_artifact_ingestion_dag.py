"""Airflow DAG for automatic remote dataset artifact ingestion."""

from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_BASE = Path("/opt/project")
DATA_BASE = PROJECT_BASE / "data"
CONFIG_PATH = PROJECT_BASE / "config" / "pcap_sources.json"


def setup_landing_task() -> list[str]:
    from landing.setup_landing import setup_landing

    paths = setup_landing(DATA_BASE)
    return [str(path) for path in paths]


def ingest_ctu_remote_artifacts_task() -> dict[str, str | int]:
    from ingestion.datasets.remote_pcap_ingest import run

    return run(
        base_dir=DATA_BASE,
        profile=os.getenv("PCAP_SOURCE_PROFILE", "subset"),
        config_path=CONFIG_PATH,
        timeout_seconds=60,
        retries=3,
        force=False,
    )


with DAG(
    dag_id="cybersecintel_dataset_artifact_ingestion",
    description="Automatic remote CTU artifact discovery and landing ingestion.",
    start_date=datetime(2026, 3, 16),
    schedule="45 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["bdm", "dataset", "ingestion", "landing"],
) as dag:
    setup_landing = PythonOperator(
        task_id="setup_landing",
        python_callable=setup_landing_task,
    )
    ingest_ctu_remote_artifacts = PythonOperator(
        task_id="ingest_ctu_remote_artifacts",
        python_callable=ingest_ctu_remote_artifacts_task,
    )

    setup_landing >> ingest_ctu_remote_artifacts
