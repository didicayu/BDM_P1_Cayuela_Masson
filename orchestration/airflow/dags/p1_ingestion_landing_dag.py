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


def ingest_kev_task() -> dict[str, str | int]:
    from ingestion.batch.kev_ingest import run

    return run(base_dir=DATA_BASE, timeout_seconds=30, retries=3)


def ingest_nvd_task() -> dict[str, str | int]:
    from ingestion.batch.nvd_ingest import run

    return run(
        base_dir=DATA_BASE,
        window_hours=24,
        results_per_page=100,
        max_pages=1,
        timeout_seconds=45,
        retries=3,
        sleep_seconds=1.0,
    )


def ingest_urlhaus_task() -> dict[str, str | int]:
    from ingestion.batch.urlhaus_ingest import run

    return run(base_dir=DATA_BASE, timeout_seconds=30, retries=3)


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


def generate_synthetic_stream_task() -> str:
    from ingestion.stream.synthetic_ids_stream import run

    output = run(base_dir=DATA_BASE, events=100, interval_ms=0)
    return str(output)


with DAG(
    dag_id="cybersecintel_p1_ingestion_landing",
    description="P1 ingestion + landing orchestration for CyberSecIntel.",
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
    ingest_kev = PythonOperator(
        task_id="ingest_kev",
        python_callable=ingest_kev_task,
    )
    ingest_nvd = PythonOperator(
        task_id="ingest_nvd",
        python_callable=ingest_nvd_task,
    )
    ingest_urlhaus = PythonOperator(
        task_id="ingest_urlhaus",
        python_callable=ingest_urlhaus_task,
    )
    import_cic_csv = PythonOperator(
        task_id="import_cic_ids2017_csv_from_raw_downloads",
        python_callable=import_cic_csv_task,
    )
    import_cic_pcap = PythonOperator(
        task_id="import_cic_ids2017_pcap_from_raw_downloads",
        python_callable=import_cic_pcap_task,
    )
    synthetic_stream = PythonOperator(
        task_id="generate_synthetic_stream_events",
        python_callable=generate_synthetic_stream_task,
    )

    setup_landing >> [
        ingest_kev,
        ingest_nvd,
        ingest_urlhaus,
        import_cic_csv,
        import_cic_pcap,
        synthetic_stream,
    ]
