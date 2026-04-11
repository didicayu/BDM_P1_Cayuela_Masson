"""Replay landed CTU PCAP artifacts with Suricata offline mode.

The raw PCAP objects remain immutable in bronze. This module stages selected
artifacts temporarily, runs Suricata with ``-r``, lands EVE JSON output, and
materializes normalized event records into Delta tables.
"""

from __future__ import annotations

import argparse
import bz2
from collections import Counter
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any
import uuid

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.landing_utils import (
    ensure_dir,
    ingest_date_str,
    partition_now,
    sha256_file,
    utc_now,
)
from ingestion.common.storage import LandingStorage

PCAP_SOURCE_ID = "ctu13_remote_pcap"
REPLAY_SOURCE_ID = "ctu13_pcap_replay"
SURICATA_SOURCE = "suricata"
DEFAULT_EVENT_TYPES = ("alert", "flow", "dns", "http", "tls")
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
DEFAULT_SURICATA_EVENTS_TOPIC = "suricata.events"
DEFAULT_IDS_ALERTS_TOPIC = "ids.alerts"


@dataclass(frozen=True)
class SuricataExecution:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    eve_path: Path


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


def _csv_values(raw: str | None, default: tuple[str, ...]) -> list[str]:
    if not raw:
        return list(default)
    values = [value.strip() for value in raw.split(",") if value.strip()]
    return values or list(default)


def _extract_partition(relative_path: str, name: str) -> str:
    match = re.search(rf"(?:^|/){re.escape(name)}=([^/]+)", relative_path)
    return match.group(1) if match else ""


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_int_default(value: Any, default: int = 0) -> int:
    parsed = _safe_int(value)
    return default if parsed is None else parsed


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value)


def _artifact_format(artifact_name: str) -> str:
    if artifact_name.endswith(".pcap.bz2"):
        return "pcap_bz2"
    if artifact_name.endswith(".pcap"):
        return "pcap"
    return Path(artifact_name).suffix.lstrip(".") or "unknown"


def _sort_date_value(value: str) -> int:
    clean = value.replace("-", "")
    return int(clean) if clean.isdigit() else 0


def artifact_record_from_metadata(
    metadata: dict[str, Any],
    *,
    cataloged_at_utc: str | None = None,
) -> dict[str, Any] | None:
    artifact_name = _safe_str(metadata.get("artifact_name"))
    relative_path = _safe_str(metadata.get("relative_landing_path"))
    sha256 = _safe_str(metadata.get("sha256"))
    if not artifact_name or not relative_path or not sha256:
        return None

    ingest_date = _safe_str(metadata.get("ingest_date")) or _extract_partition(
        relative_path,
        "ingest_date",
    )
    scenario_number = _safe_int(metadata.get("scenario_number"))
    is_compressed = artifact_name.endswith(".bz2")

    return {
        "source_id": _safe_str(metadata.get("source_id")) or PCAP_SOURCE_ID,
        "scenario_number": scenario_number,
        "scenario_directory": _safe_str(metadata.get("scenario_directory")),
        "scenario_url": _safe_str(metadata.get("scenario_url") or metadata.get("page_url")),
        "artifact_name": artifact_name,
        "artifact_url": _safe_str(metadata.get("artifact_url")),
        "artifact_format": _artifact_format(artifact_name),
        "compression": "bz2" if is_compressed else "",
        "is_compressed": is_compressed,
        "profile": _safe_str(metadata.get("profile")),
        "selection_rule": _safe_str(metadata.get("selection_rule")),
        "retrieved_at_utc": _safe_str(metadata.get("retrieved_at_utc")),
        "cataloged_at_utc": cataloged_at_utc or utc_now().isoformat(),
        "content_type": _safe_str(metadata.get("content_type")),
        "size_bytes": _safe_int(metadata.get("size_bytes")) or 0,
        "sha256": sha256,
        "landing_path": _safe_str(metadata.get("landing_path")),
        "relative_landing_path": relative_path,
        "ingest_date": ingest_date,
    }


def load_pcap_artifact_records(
    *,
    base_dir: Path,
    ingest_date: str | None = None,
) -> list[dict[str, Any]]:
    storage = LandingStorage.from_env(base_dir)
    metadata_entries = storage.read_manifest_entries(
        source_id=PCAP_SOURCE_ID,
        ingest_date=ingest_date,
    )
    cataloged_at = utc_now().isoformat()
    records: dict[tuple[str, int | None, str, str, str], dict[str, Any]] = {}

    for entry in metadata_entries:
        record = artifact_record_from_metadata(entry, cataloged_at_utc=cataloged_at)
        if record is None:
            continue
        key = (
            record["source_id"],
            record["scenario_number"],
            record["artifact_name"],
            record["ingest_date"],
            record["sha256"],
        )
        records[key] = record

    return sorted(
        records.values(),
        key=lambda item: (
            item["source_id"],
            item["scenario_number"] if item["scenario_number"] is not None else -1,
            item["artifact_name"],
            item["ingest_date"],
        ),
    )


def write_delta_records(
    *,
    table_name: str,
    records: list[dict[str, Any]],
    merge_keys: list[str] | None,
    partition_by: list[str] | None = None,
    strict: bool = False,
) -> int:
    if not records:
        return 0
    if (
        os.getenv("LANDING_BACKEND", "minio").strip().lower() == "local"
        and not os.getenv("DELTA_LOCAL_ROOT", "").strip()
        and not strict
    ):
        return 0
    try:
        delta_storage = DeltaLakeStorage.from_env()
        delta_storage.ensure_bucket()
        return delta_storage.write_or_merge(
            table_name,
            [DeltaLakeStorage.flatten_record(record) for record in records],
            merge_keys=merge_keys,
            partition_by=partition_by,
        )
    except RuntimeError as exc:
        if "deltalake and pyarrow" in str(exc) and not strict:
            return 0
        raise


def build_pcap_catalog(
    *,
    base_dir: Path,
    ingest_date: str | None = None,
    write_delta: bool = True,
    strict_delta: bool = False,
) -> dict[str, int]:
    records = load_pcap_artifact_records(base_dir=base_dir, ingest_date=ingest_date)
    delta_rows = 0
    if write_delta:
        delta_rows = write_delta_records(
            table_name="pcap_artifacts",
            records=records,
            merge_keys=["source_id", "scenario_number", "artifact_name", "ingest_date", "sha256"],
            partition_by=["ingest_date"],
            strict=strict_delta,
        )
    return {
        "source": PCAP_SOURCE_ID,
        "catalog_records": len(records),
        "delta_rows": delta_rows,
    }


def select_replay_candidates(
    records: list[dict[str, Any]],
    *,
    max_artifacts: int,
    allow_decompress: bool,
    max_bytes: int | None,
) -> list[dict[str, Any]]:
    latest_by_artifact: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        artifact_name = _safe_str(record.get("artifact_name"))
        is_plain_pcap = artifact_name.endswith(".pcap")
        is_compressed_pcap = artifact_name.endswith(".pcap.bz2")
        if not is_plain_pcap and not (allow_decompress and is_compressed_pcap):
            continue

        size_bytes = _safe_int(record.get("size_bytes")) or 0
        if max_bytes is not None and size_bytes > max_bytes:
            continue

        key = (_safe_str(record.get("sha256")), artifact_name)
        current = latest_by_artifact.get(key)
        if current is None or _sort_date_value(_safe_str(record.get("ingest_date"))) > _sort_date_value(
            _safe_str(current.get("ingest_date"))
        ):
            latest_by_artifact[key] = record

    ordered = sorted(
        latest_by_artifact.values(),
        key=lambda item: (
            0 if _safe_str(item.get("artifact_name")).endswith(".pcap") else 1,
            item.get("scenario_number") if item.get("scenario_number") is not None else 999999,
            -_sort_date_value(_safe_str(item.get("ingest_date"))),
            _safe_str(item.get("artifact_name")),
        ),
    )
    return ordered[:max_artifacts]


def _runtime_temp_dir() -> Path:
    dag_id = re.sub(r"[^A-Za-z0-9._-]+", "_", os.getenv("AIRFLOW_CTX_DAG_ID", "adhoc"))
    run_id = re.sub(
        r"[^A-Za-z0-9._-]+",
        "_",
        os.getenv("AIRFLOW_CTX_DAG_RUN_ID", utc_now().strftime("%Y%m%dT%H%M%SZ")),
    )
    root = Path("/tmp/cybersecintel") / (dag_id or "adhoc") / (run_id or "adhoc")
    ensure_dir(root)
    return root


def _decompress_bz2(source: Path, destination: Path) -> Path:
    ensure_dir(destination.parent)
    with bz2.open(source, "rb") as src, destination.open("wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    return destination


def stage_artifact(
    *,
    storage: LandingStorage,
    artifact: dict[str, Any],
    temp_dir: Path,
    allow_decompress: bool,
) -> Path:
    relative_path = _safe_str(artifact.get("relative_landing_path"))
    artifact_name = _safe_str(artifact.get("artifact_name"))
    if not relative_path or not artifact_name:
        raise ValueError("Artifact record is missing landing path or artifact name.")

    staged = storage.download_file(relative_path, temp_dir / artifact_name)
    expected_sha = _safe_str(artifact.get("sha256"))
    if expected_sha and sha256_file(staged) != expected_sha:
        raise RuntimeError(f"Checksum mismatch while staging {relative_path}")

    if artifact_name.endswith(".pcap.bz2"):
        if not allow_decompress:
            raise RuntimeError(f"Compressed artifact selected without decompression enabled: {artifact_name}")
        return _decompress_bz2(staged, staged.with_suffix(""))
    return staged


def run_suricata(
    *,
    pcap_path: Path,
    output_dir: Path,
    suricata_bin: str,
) -> SuricataExecution:
    ensure_dir(output_dir)
    command = [suricata_bin, "-r", str(pcap_path), "-l", str(output_dir)]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    return SuricataExecution(
        command=command,
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        eve_path=output_dir / "eve.json",
    )


def read_eve_events(eve_path: Path, event_types: set[str]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not eve_path.exists():
        return events
    with eve_path.open("r", encoding="utf-8", errors="replace") as file:
        for line in file:
            clean = line.strip()
            if not clean:
                continue
            try:
                parsed = json.loads(clean)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict) and _safe_str(parsed.get("event_type")) in event_types:
                events.append(parsed)
    return events


def normalize_eve_event(
    event: dict[str, Any],
    *,
    artifact: dict[str, Any],
    replay_run_id: str,
    ingest_date: str,
) -> dict[str, Any]:
    alert = event.get("alert") if isinstance(event.get("alert"), dict) else {}
    return {
        "source": SURICATA_SOURCE,
        "event_type": _safe_str(event.get("event_type")),
        "timestamp_utc": _safe_str(event.get("timestamp")),
        "src_ip": _safe_str(event.get("src_ip")),
        "src_port": _safe_int_default(event.get("src_port")),
        "dest_ip": _safe_str(event.get("dest_ip")),
        "dest_port": _safe_int_default(event.get("dest_port")),
        "proto": _safe_str(event.get("proto")),
        "app_proto": _safe_str(event.get("app_proto")),
        "signature": _safe_str(alert.get("signature")),
        "severity": _safe_int_default(alert.get("severity")),
        "category": _safe_str(alert.get("category")),
        "flow_id": _safe_str(event.get("flow_id")),
        "pcap_cnt": _safe_int_default(event.get("pcap_cnt")),
        "pcap_filename": _safe_str(event.get("pcap_filename") or artifact.get("artifact_name")),
        "scenario_number": _safe_int_default(artifact.get("scenario_number")),
        "artifact_name": _safe_str(artifact.get("artifact_name")),
        "artifact_sha256": _safe_str(artifact.get("sha256")),
        "relative_pcap_landing_path": _safe_str(artifact.get("relative_landing_path")),
        "replay_run_id": replay_run_id,
        "ingest_date": ingest_date,
        "event_payload_json": json.dumps(event, sort_keys=True),
    }


def ids_alert_record(event_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp_utc": event_record["timestamp_utc"],
        "src_ip": event_record["src_ip"],
        "dst_ip": event_record["dest_ip"],
        "src_port": event_record["src_port"],
        "dst_port": event_record["dest_port"],
        "signature": event_record["signature"],
        "severity": event_record["severity"],
        "source": SURICATA_SOURCE,
        "event_type": event_record["event_type"],
        "category": event_record["category"],
        "scenario_number": event_record["scenario_number"],
        "artifact_sha256": event_record["artifact_sha256"],
        "replay_run_id": event_record["replay_run_id"],
        "ingest_date": event_record["ingest_date"],
    }


def _write_jsonl(storage: LandingStorage, relative_path: Path, records: list[dict[str, Any]]) -> str:
    payload = ("\n".join(json.dumps(record, sort_keys=True) for record in records) + "\n").encode(
        "utf-8"
    )
    return storage.write_bytes(relative_path, payload).landing_path


def publish_records_to_kafka(
    *,
    records: list[dict[str, Any]],
    topic: str,
    bootstrap_servers: str,
    client_id: str,
    strict: bool,
) -> int:
    if not records:
        return 0
    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        if strict:
            raise RuntimeError(
                "kafka-python is required for Kafka publishing. "
                "Install it with: pip install kafka-python"
            ) from exc
        return 0

    producer = KafkaProducer(
        bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",") if server.strip()],
        client_id=client_id,
        key_serializer=lambda value: value.encode("utf-8") if value else None,
        value_serializer=lambda value: json.dumps(value, sort_keys=True).encode("utf-8"),
        retries=3,
        linger_ms=25,
    )
    try:
        futures = []
        for record in records:
            key = _safe_str(
                record.get("replay_run_id")
                or record.get("artifact_sha256")
                or record.get("flow_id")
            )
            futures.append(producer.send(topic, key=key, value=record))

        for future in futures:
            future.get(timeout=30)
        producer.flush(timeout=30)
        return len(records)
    except Exception:
        if strict:
            raise
        return 0
    finally:
        producer.close(timeout=10)


def replay_selected_pcaps(
    *,
    base_dir: Path,
    max_artifacts: int,
    event_types: list[str],
    allow_decompress: bool,
    max_bytes: int | None,
    suricata_bin: str,
    write_catalog: bool = True,
    write_delta: bool = True,
    strict_delta: bool = False,
    ingest_date_filter: str | None = None,
    kafka_enabled: bool = False,
    kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    kafka_suricata_topic: str = DEFAULT_SURICATA_EVENTS_TOPIC,
    kafka_ids_alert_topic: str = DEFAULT_IDS_ALERTS_TOPIC,
    kafka_strict: bool = True,
) -> dict[str, int | str]:
    if write_catalog:
        build_pcap_catalog(
            base_dir=base_dir,
            ingest_date=ingest_date_filter,
            write_delta=write_delta,
            strict_delta=strict_delta,
        )

    partition_at = partition_now()
    replay_ingest_date = ingest_date_str(partition_at)
    replay_hour = partition_at.strftime("%H")
    event_type_set = set(event_types)
    storage = LandingStorage.from_env(base_dir)
    records = load_pcap_artifact_records(base_dir=base_dir, ingest_date=ingest_date_filter)
    candidates = select_replay_candidates(
        records,
        max_artifacts=max_artifacts,
        allow_decompress=allow_decompress,
        max_bytes=max_bytes,
    )

    temp_root = _runtime_temp_dir()
    replay_run_records: list[dict[str, Any]] = []
    normalized_events: list[dict[str, Any]] = []
    alert_records: list[dict[str, Any]] = []

    try:
        for artifact in candidates:
            replay_run_id = f"{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:12]}"
            started_at = utc_now()
            replay_work_dir = temp_root / replay_run_id
            output_dir = replay_work_dir / "suricata"
            event_counts: Counter[str] = Counter()
            status = "succeeded"
            error_summary = ""
            eve_landing_path = ""
            normalized_for_artifact: list[dict[str, Any]] = []

            try:
                staged_artifact = stage_artifact(
                    storage=storage,
                    artifact=artifact,
                    temp_dir=replay_work_dir,
                    allow_decompress=allow_decompress,
                )
                execution = run_suricata(
                    pcap_path=staged_artifact,
                    output_dir=output_dir,
                    suricata_bin=suricata_bin,
                )
                if execution.returncode != 0:
                    status = "failed"
                    error_summary = (execution.stderr or execution.stdout)[-1000:]
                elif not execution.eve_path.exists():
                    status = "failed"
                    error_summary = "Suricata completed but did not create eve.json."
                else:
                    eve_relative = (
                        Path("unstructured")
                        / "pcap_replay"
                        / "source=ctu13"
                        / f"scenario={artifact.get('scenario_number')}"
                        / f"artifact_sha256={artifact.get('sha256')}"
                        / f"ingest_date={replay_ingest_date}"
                        / "eve.jsonl"
                    )
                    storage.clear_prefix(eve_relative.parent)
                    eve_landing_path = storage.write_file(eve_relative, execution.eve_path).landing_path

                    eve_events = read_eve_events(execution.eve_path, event_type_set)
                    for event in eve_events:
                        normalized = normalize_eve_event(
                            event,
                            artifact=artifact,
                            replay_run_id=replay_run_id,
                            ingest_date=replay_ingest_date,
                        )
                        normalized_for_artifact.append(normalized)
                        event_counts[normalized["event_type"]] += 1
            except Exception as exc:  # pragma: no cover - exercised by integration/runtime
                status = "failed"
                error_summary = str(exc)[-1000:]

            completed_at = utc_now()
            normalized_events.extend(normalized_for_artifact)
            alert_records.extend(
                ids_alert_record(record)
                for record in normalized_for_artifact
                if record["event_type"] == "alert"
            )

            run_record = {
                "source_id": REPLAY_SOURCE_ID,
                "replay_run_id": replay_run_id,
                "status": status,
                "error_summary": error_summary,
                "suricata_bin": suricata_bin,
                "event_types": ",".join(event_types),
                "event_counts_json": json.dumps(dict(sorted(event_counts.items())), sort_keys=True),
                "total_events": sum(event_counts.values()),
                "alert_events": event_counts.get("alert", 0),
                "started_at_utc": started_at.isoformat(),
                "completed_at_utc": completed_at.isoformat(),
                "duration_seconds": round((completed_at - started_at).total_seconds(), 3),
                "scenario_number": artifact.get("scenario_number"),
                "artifact_name": _safe_str(artifact.get("artifact_name")),
                "artifact_sha256": _safe_str(artifact.get("sha256")),
                "artifact_size_bytes": _safe_int(artifact.get("size_bytes")) or 0,
                "relative_pcap_landing_path": _safe_str(artifact.get("relative_landing_path")),
                "pcap_landing_path": _safe_str(artifact.get("landing_path")),
                "eve_landing_path": eve_landing_path,
                "ingest_date": replay_ingest_date,
            }
            replay_run_records.append(run_record)
            storage.append_manifest_entry(
                source_id=REPLAY_SOURCE_ID,
                ingest_date=replay_ingest_date,
                entry=run_record,
            )

        ids_landing_path = ""
        if alert_records:
            ids_relative = (
                Path("stream")
                / "ids_alerts"
                / "source=suricata"
                / f"ingest_date={replay_ingest_date}"
                / f"hour={replay_hour}"
                / "ids_suricata.jsonl"
            )
            storage.clear_prefix(ids_relative.parent)
            ids_landing_path = _write_jsonl(storage, ids_relative, alert_records)

        replay_run_delta_rows = 0
        suricata_event_delta_rows = 0
        ids_alert_delta_rows = 0
        if write_delta:
            replay_run_delta_rows = write_delta_records(
                table_name="pcap_replay_runs",
                records=replay_run_records,
                merge_keys=["replay_run_id"],
                partition_by=["ingest_date"],
                strict=strict_delta,
            )
            suricata_event_delta_rows = write_delta_records(
                table_name="suricata_events",
                records=normalized_events,
                merge_keys=None,
                partition_by=["ingest_date"],
                strict=strict_delta,
            )
            ids_alert_delta_rows = write_delta_records(
                table_name="ids_alerts",
                records=alert_records,
                merge_keys=None,
                partition_by=["ingest_date"],
                strict=strict_delta,
            )

        kafka_suricata_events = 0
        kafka_ids_alerts = 0
        if kafka_enabled:
            kafka_suricata_events = publish_records_to_kafka(
                records=normalized_events,
                topic=kafka_suricata_topic,
                bootstrap_servers=kafka_bootstrap_servers,
                client_id="cybersecintel-pcap-replay-suricata-events",
                strict=kafka_strict,
            )
            kafka_ids_alerts = publish_records_to_kafka(
                records=alert_records,
                topic=kafka_ids_alert_topic,
                bootstrap_servers=kafka_bootstrap_servers,
                client_id="cybersecintel-pcap-replay-ids-alerts",
                strict=kafka_strict,
            )

        failed_replays = sum(1 for record in replay_run_records if record["status"] != "succeeded")
        return {
            "source": REPLAY_SOURCE_ID,
            "candidate_artifacts": len(candidates),
            "successful_replays": len(replay_run_records) - failed_replays,
            "failed_replays": failed_replays,
            "normalized_events": len(normalized_events),
            "alert_events": len(alert_records),
            "replay_run_delta_rows": replay_run_delta_rows,
            "suricata_event_delta_rows": suricata_event_delta_rows,
            "ids_alert_delta_rows": ids_alert_delta_rows,
            "kafka_enabled": int(kafka_enabled),
            "kafka_suricata_events": kafka_suricata_events,
            "kafka_ids_alerts": kafka_ids_alerts,
            "kafka_suricata_topic": kafka_suricata_topic,
            "kafka_ids_alert_topic": kafka_ids_alert_topic,
            "ids_landing_path": ids_landing_path,
        }
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay landed CTU PCAP artifacts with Suricata.")
    parser.add_argument("--base-dir", type=Path, default=Path("data"))
    parser.add_argument(
        "--max-artifacts",
        type=int,
        default=_int_env("PCAP_REPLAY_MAX_ARTIFACTS", 1),
    )
    parser.add_argument(
        "--event-types",
        default=os.getenv("PCAP_REPLAY_EVENT_TYPES", ",".join(DEFAULT_EVENT_TYPES)),
        help="Comma-separated Suricata EVE event types to persist.",
    )
    parser.add_argument(
        "--allow-decompress",
        action="store_true",
        default=_bool_env("PCAP_REPLAY_ALLOW_DECOMPRESS", False),
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=_int_env("PCAP_REPLAY_MAX_BYTES", None),
        help="Skip artifacts larger than this many bytes. Omit for no limit.",
    )
    parser.add_argument(
        "--suricata-bin",
        default=os.getenv("PCAP_REPLAY_SURICATA_BIN", "suricata"),
    )
    parser.add_argument("--ingest-date", default=os.getenv("PCAP_REPLAY_INGEST_DATE", ""))
    parser.add_argument(
        "--kafka-enabled",
        action="store_true",
        default=_bool_env("PCAP_REPLAY_KAFKA_ENABLED", False),
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS),
    )
    parser.add_argument(
        "--kafka-suricata-topic",
        default=os.getenv("PCAP_REPLAY_KAFKA_SURICATA_TOPIC", DEFAULT_SURICATA_EVENTS_TOPIC),
    )
    parser.add_argument(
        "--kafka-ids-alert-topic",
        default=os.getenv("PCAP_REPLAY_KAFKA_IDS_ALERT_TOPIC", DEFAULT_IDS_ALERTS_TOPIC),
    )
    parser.add_argument(
        "--kafka-nonstrict",
        action="store_true",
        help="Do not fail the replay command if Kafka publishing fails.",
    )
    parser.add_argument("--strict-delta", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = replay_selected_pcaps(
        base_dir=args.base_dir,
        max_artifacts=args.max_artifacts,
        event_types=_csv_values(args.event_types, DEFAULT_EVENT_TYPES),
        allow_decompress=args.allow_decompress,
        max_bytes=args.max_bytes,
        suricata_bin=args.suricata_bin,
        strict_delta=args.strict_delta,
        ingest_date_filter=args.ingest_date or None,
        kafka_enabled=args.kafka_enabled,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        kafka_suricata_topic=args.kafka_suricata_topic,
        kafka_ids_alert_topic=args.kafka_ids_alert_topic,
        kafka_strict=not args.kafka_nonstrict,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if int(result["failed_replays"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
