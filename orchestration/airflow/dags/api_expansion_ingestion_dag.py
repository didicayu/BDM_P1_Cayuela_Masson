"""Airflow DAG for public API/feed ingestion and stream scaffolding."""

from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_BASE = Path("/opt/project")
DATA_BASE = PROJECT_BASE / "data"


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


def ingest_epss_task() -> dict[str, str | int]:
    from ingestion.batch.epss_ingest import run

    return run(
        base_dir=DATA_BASE,
        results_per_page=int(os.getenv("EPSS_RESULTS_PER_PAGE", "1000")),
        max_pages=int(os.getenv("EPSS_MAX_PAGES", "10")),
        timeout_seconds=30,
        retries=3,
    )


def ingest_circl_from_epss_task() -> dict[str, str | int]:
    from ingestion.batch.circl_vulnlookup_ingest import run

    return run(
        base_dir=DATA_BASE,
        top_n_cves=int(os.getenv("CIRCL_TOP_N_CVES", "200")),
        epss_max_pages=int(os.getenv("EPSS_MAX_PAGES", "10")),
        request_sleep_seconds=0.2,
        timeout_seconds=30,
        retries=3,
    )


def ingest_threatfox_task() -> dict[str, str | int]:
    from ingestion.batch.threatfox_ingest import run

    return run(
        base_dir=DATA_BASE,
        days=int(os.getenv("THREATFOX_DAYS", "1")),
        timeout_seconds=30,
        retries=3,
    )


def ingest_shodan_seeded_task() -> dict[str, str | int]:
    from ingestion.batch.shodan_seeded_ingest import run

    return run(
        base_dir=DATA_BASE,
        max_indicators_per_run=int(os.getenv("SHODAN_MAX_INDICATORS_PER_RUN", "50")),
        request_sleep_seconds=float(os.getenv("SHODAN_REQUEST_SLEEP_SECONDS", "1.0")),
        timeout_seconds=30,
        retries=2,
    )


def generate_synthetic_stream_task() -> str:
    from ingestion.stream.synthetic_ids_stream import run

    output = run(base_dir=DATA_BASE, events=100, interval_ms=0)
    return str(output)


with DAG(
    dag_id="cybersecintel_api_expansion_ingestion",
    description="Public API/feed ingestion for KEV/NVD/URLhaus/EPSS/CIRCL/ThreatFox/Shodan plus stream scaffold.",
    start_date=datetime(2026, 3, 12),
    schedule="15 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bdm", "api", "ingestion", "landing"],
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
    ingest_epss = PythonOperator(
        task_id="ingest_epss",
        python_callable=ingest_epss_task,
    )
    ingest_circl = PythonOperator(
        task_id="ingest_circl_from_epss",
        python_callable=ingest_circl_from_epss_task,
    )
    ingest_threatfox = PythonOperator(
        task_id="ingest_threatfox",
        python_callable=ingest_threatfox_task,
    )
    ingest_shodan = PythonOperator(
        task_id="ingest_shodan_seeded",
        python_callable=ingest_shodan_seeded_task,
    )
    synthetic_stream = PythonOperator(
        task_id="generate_synthetic_stream_events",
        python_callable=generate_synthetic_stream_task,
    )

    setup_landing >> [
        ingest_kev,
        ingest_nvd,
        ingest_urlhaus,
        ingest_epss,
        ingest_threatfox,
        synthetic_stream,
    ]
    ingest_epss >> ingest_circl
    ingest_threatfox >> ingest_shodan
