"""Bounded warm-stream aggregate materialization for P2."""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
from pathlib import Path
from typing import Any

from exploitation.warm_path import read_kafka_alerts
from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.storage import LandingStorage


AGGREGATE_TYPES = {
    "alerts_per_minute",
    "alerts_by_severity_per_minute",
    "top_source_ips_per_minute",
    "top_destination_ips_per_minute",
    "top_signatures_per_minute",
}


def compute_warm_stream_aggregates(
    alerts: list[dict[str, Any]],
    *,
    window_minutes: int = 1,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    """Compute deterministic minute-window aggregates from IDS alert rows."""
    windows: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for alert in alerts:
        timestamp = _parse_timestamp(alert.get("timestamp_utc") or alert.get("event_timestamp_utc") or alert.get("timestamp"))
        if timestamp is None:
            continue
        window_start = _floor_window(timestamp, window_minutes)
        windows[window_start.isoformat()].append(alert)

    rows: list[dict[str, Any]] = []
    for window_start_raw, window_alerts in sorted(windows.items()):
        window_start = dt.datetime.fromisoformat(window_start_raw)
        window_end = window_start + dt.timedelta(minutes=window_minutes)
        base = {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "window_minutes": window_minutes,
            "ingest_date": window_start.date().isoformat(),
        }
        rows.append({
            **base,
            "aggregate_type": "alerts_per_minute",
            "aggregate_key": "all",
            "aggregate_value": "all",
            "alert_count": len(window_alerts),
            "rank": 1,
        })
        rows.extend(_counter_rows(base, "alerts_by_severity_per_minute", "severity", window_alerts, top_n=None))
        rows.extend(_counter_rows(base, "top_source_ips_per_minute", "src_ip", window_alerts, top_n=top_n))
        rows.extend(_counter_rows(base, "top_destination_ips_per_minute", "dst_ip", window_alerts, top_n=top_n))
        rows.extend(_counter_rows(base, "top_signatures_per_minute", "signature", window_alerts, top_n=top_n))
    return rows


def materialize_warm_stream_aggregates(
    *,
    strict_kafka: bool = False,
    base_dir: Path = Path("data"),
) -> dict[str, int | str]:
    """Read warm alerts, compute aggregates, and write landing plus Delta assets."""
    source_name = "kafka://ids.alerts"
    try:
        alerts = read_kafka_alerts()
    except Exception as exc:
        if strict_kafka:
            raise
        silver = DeltaLakeStorage.from_env()
        alerts = silver.read_records("ids_alerts")
        source_name = f"s3://{silver.bucket}/ids_alerts fallback after Kafka skip: {type(exc).__name__}"
    if not alerts:
        silver = DeltaLakeStorage.from_env()
        alerts = silver.read_records("ids_alerts")
        source_name = f"s3://{silver.bucket}/ids_alerts fallback"

    aggregate_rows = compute_warm_stream_aggregates(alerts)
    landing_writes = _write_landing_aggregates(aggregate_rows, base_dir=base_dir)
    silver = DeltaLakeStorage.from_env()
    rows_written = silver.overwrite(
        "warm_stream_aggregates",
        aggregate_rows,
        partition_by=["ingest_date"] if aggregate_rows else None,
    )
    return {
        "alerts_read": len(alerts),
        "aggregate_rows": len(aggregate_rows),
        "landing_files_written": landing_writes,
        "delta_rows_written": rows_written,
        "source": source_name,
    }


def validate_warm_stream_aggregates() -> dict[str, int | str]:
    """Return a small validation summary for the Airflow DAG."""
    silver = DeltaLakeStorage.from_env()
    rows = silver.read_records("warm_stream_aggregates")
    invalid = [
        row for row in rows
        if row.get("aggregate_type") not in AGGREGATE_TYPES
        or not row.get("window_start")
        or not row.get("aggregate_key")
        or _int(row.get("alert_count")) is None
    ]
    return {
        "rows_checked": len(rows),
        "invalid_rows": len(invalid),
        "status": "ok" if rows and not invalid else "warn",
    }


def _counter_rows(
    base: dict[str, Any],
    aggregate_type: str,
    source_key: str,
    alerts: list[dict[str, Any]],
    *,
    top_n: int | None,
) -> list[dict[str, Any]]:
    counts: collections.Counter[str] = collections.Counter()
    for alert in alerts:
        value = _string(alert.get(source_key)) or "unknown"
        counts[value] += 1
    most_common = counts.most_common(top_n)
    return [
        {
            **base,
            "aggregate_type": aggregate_type,
            "aggregate_key": value,
            "aggregate_value": value,
            "alert_count": count,
            "rank": rank,
        }
        for rank, (value, count) in enumerate(most_common, start=1)
    ]


def _write_landing_aggregates(rows: list[dict[str, Any]], *, base_dir: Path) -> int:
    if not rows:
        return 0
    storage = LandingStorage.from_env(base_dir)
    by_window: dict[tuple[str, str], list[dict[str, Any]]] = collections.defaultdict(list)
    for row in rows:
        by_window[(_string(row.get("ingest_date")), _safe_partition(_string(row.get("window_start"))))].append(row)
    for (ingest_date, window_start), window_rows in sorted(by_window.items()):
        relative = (
            Path("warm")
            / "stream_aggregates"
            / f"ingest_date={ingest_date}"
            / f"window_start={window_start}"
            / "part-00000.json"
        )
        payload = {
            "source_id": "warm_stream_aggregates",
            "ingest_date": ingest_date,
            "window_start": window_rows[0]["window_start"],
            "record_count": len(window_rows),
            "records": window_rows,
        }
        result = storage.write_bytes(relative, json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"))
        storage.append_manifest_entry(
            source_id="warm_stream_aggregates",
            ingest_date=ingest_date,
            entry={
                "source_id": "warm_stream_aggregates",
                "relative_landing_path": result.relative_path,
                "landing_path": result.landing_path,
                "size_bytes": result.size_bytes,
                "sha256": result.sha256,
                "record_count": len(window_rows),
                "ingest_date": ingest_date,
            },
        )
    return len(by_window)


def _floor_window(timestamp: dt.datetime, window_minutes: int) -> dt.datetime:
    minute = (timestamp.minute // window_minutes) * window_minutes
    return timestamp.replace(minute=minute, second=0, microsecond=0)


def _parse_timestamp(value: Any) -> dt.datetime | None:
    raw = _string(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _safe_partition(value: str) -> str:
    return value.replace(":", "").replace("+", "p").replace("/", "-")


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize bounded warm stream aggregates.")
    parser.add_argument("--strict-kafka", action="store_true", help="Fail instead of falling back to Delta when Kafka is unavailable.")
    args = parser.parse_args()
    result = materialize_warm_stream_aggregates(strict_kafka=args.strict_kafka)
    for key, value in result.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
