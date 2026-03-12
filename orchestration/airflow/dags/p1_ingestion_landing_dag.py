"""Airflow DAG for P1 ingestion + landing workflow."""

from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_BASE = Path("/opt/project")
DATA_BASE = PROJECT_BASE / "data"
RAW_DOWNLOADS = DATA_BASE / "raw_downloads"


def setup_landing_task() -> list[str]:
    from landing.setup_landing import setup_landing

    paths = setup_landing(DATA_BASE)
    return [str(path) for path in paths]


def import_cic_csv_task() -> dict[str, str | int]:
    from ingestion.imports.dataset_import import run

    if not RAW_DOWNLOADS.exists():
        return {"imported_files": 0, "reason": "raw_downloads_missing"}

    try:
        result = run(
            dataset="cic_ids2017_csv",
            source_path=RAW_DOWNLOADS,
            base_dir=DATA_BASE,
            copy_mode=os.getenv("DATASET_IMPORT_MODE", "move"),
        )
    except FileNotFoundError:
        return {"imported_files": 0, "reason": "no_csv_or_zip_files_found"}
    return result


def import_cic_pcap_task() -> dict[str, str | int]:
    from ingestion.imports.dataset_import import run

    if not RAW_DOWNLOADS.exists():
        return {"imported_files": 0, "reason": "raw_downloads_missing"}

    try:
        result = run(
            dataset="cic_ids2017_pcap",
            source_path=RAW_DOWNLOADS,
            base_dir=DATA_BASE,
            copy_mode=os.getenv("DATASET_IMPORT_MODE", "move"),
        )
    except FileNotFoundError:
        return {"imported_files": 0, "reason": "no_pcap_files_found"}
    return result


with DAG(
    dag_id="cybersecintel_p1_ingestion_landing",
    description="P1 dataset-import and landing orchestration for CyberSecIntel.",
    start_date=datetime(2026, 3, 1),
    schedule="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bdm", "p1", "ingestion", "landing"],
) as dag:
    setup_landing = PythonOperator(
        task_id="setup_landing",
        python_callable=setup_landing_task,
    )
    import_cic_csv = PythonOperator(
        task_id="import_cic_ids2017_csv_from_raw_downloads",
        python_callable=import_cic_csv_task,
    )
    import_cic_pcap = PythonOperator(
        task_id="import_cic_ids2017_pcap_from_raw_downloads",
        python_callable=import_cic_pcap_task,
    )

    setup_landing >> [
        import_cic_csv,
        import_cic_pcap,
    ]
