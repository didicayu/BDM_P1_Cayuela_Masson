"""Airflow DAG for replaying landed CTU PCAP artifacts with Suricata."""

from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator

PROJECT_BASE = Path("/opt/project")
DATA_BASE = PROJECT_BASE / "data"


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int | None = None) -> int | None:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    return int(value)


def _csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    return [value.strip() for value in raw.split(",") if value.strip()]


def ensure_replay_enabled_task() -> str:
    if not _bool_env("PCAP_REPLAY_ENABLED", False):
        raise AirflowSkipException("Set PCAP_REPLAY_ENABLED=true to run the PCAP replay DAG.")
    return "enabled"


def build_pcap_catalog_task() -> dict[str, int]:
    from ingestion.replay.suricata_replay import build_pcap_catalog

    return build_pcap_catalog(
        base_dir=DATA_BASE,
        ingest_date=os.getenv("PCAP_REPLAY_INGEST_DATE", "").strip() or None,
        write_delta=True,
        strict_delta=True,
    )


def replay_selected_pcaps_task() -> dict[str, int | str]:
    from ingestion.replay.suricata_replay import (
        DEFAULT_EVENT_TYPES,
        DEFAULT_IDS_ALERTS_TOPIC,
        DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
        DEFAULT_SURICATA_EVENTS_TOPIC,
        replay_selected_pcaps,
    )

    return replay_selected_pcaps(
        base_dir=DATA_BASE,
        max_artifacts=_int_env("PCAP_REPLAY_MAX_ARTIFACTS", 1) or 1,
        event_types=_csv_env("PCAP_REPLAY_EVENT_TYPES", ",".join(DEFAULT_EVENT_TYPES)),
        allow_decompress=_bool_env("PCAP_REPLAY_ALLOW_DECOMPRESS", False),
        max_bytes=_int_env("PCAP_REPLAY_MAX_BYTES", None),
        suricata_bin=os.getenv("PCAP_REPLAY_SURICATA_BIN", "suricata"),
        write_catalog=False,
        write_delta=True,
        strict_delta=True,
        ingest_date_filter=os.getenv("PCAP_REPLAY_INGEST_DATE", "").strip() or None,
        kafka_enabled=_bool_env("PCAP_REPLAY_KAFKA_ENABLED", False),
        kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
        ),
        kafka_suricata_topic=os.getenv(
            "PCAP_REPLAY_KAFKA_SURICATA_TOPIC",
            DEFAULT_SURICATA_EVENTS_TOPIC,
        ),
        kafka_ids_alert_topic=os.getenv(
            "PCAP_REPLAY_KAFKA_IDS_ALERT_TOPIC",
            DEFAULT_IDS_ALERTS_TOPIC,
        ),
        kafka_strict=_bool_env("PCAP_REPLAY_KAFKA_STRICT", True),
    )


def persist_replay_outputs_task(**context) -> dict[str, int | str]:
    result = context["ti"].xcom_pull(task_ids="replay_selected_pcaps") or {}
    if int(result.get("failed_replays", 0)):
        raise AirflowException(f"PCAP replay failed: {result}")
    return result


with DAG(
    dag_id="cybersecintel_pcap_replay",
    description="Build CTU PCAP catalog and replay selected artifacts through Suricata offline.",
    start_date=datetime(2026, 4, 11),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "cybersecintel",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bdm", "pcap", "suricata", "replay"],
) as dag:
    ensure_replay_enabled = PythonOperator(
        task_id="ensure_replay_enabled",
        python_callable=ensure_replay_enabled_task,
    )
    build_pcap_catalog = PythonOperator(
        task_id="build_pcap_catalog",
        python_callable=build_pcap_catalog_task,
    )
    replay_selected_pcaps = PythonOperator(
        task_id="replay_selected_pcaps",
        python_callable=replay_selected_pcaps_task,
    )
    persist_replay_outputs = PythonOperator(
        task_id="persist_replay_outputs",
        python_callable=persist_replay_outputs_task,
    )

    ensure_replay_enabled >> build_pcap_catalog >> replay_selected_pcaps >> persist_replay_outputs
