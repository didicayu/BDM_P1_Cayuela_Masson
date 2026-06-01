"""Information-preserving cleaning for the P2 Trusted Zone."""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import Any


SOURCE_TABLES = (
    "kev",
    "epss",
    "nvd",
    "urlhaus",
    "circl_vulnlookup",
    "threatfox",
    "shodan_seeded",
    "pcap_artifacts",
    "pcap_replay_runs",
    "suricata_events",
    "ids_alerts",
    "warm_stream_aggregates",
)

MANDATORY_FIELDS: dict[str, tuple[str, ...]] = {
    "kev": ("cveID", "vulnerabilityName"),
    "epss": ("cve", "date"),
    "nvd": ("cve_id",),
    "urlhaus": ("id",),
    "circl_vulnlookup": ("cve_id",),
    "threatfox": ("id",),
    "shodan_seeded": ("ip_str",),
    "pcap_artifacts": ("source_id", "artifact_name", "sha256"),
    "pcap_replay_runs": ("replay_run_id",),
    "suricata_events": ("event_type",),
    "ids_alerts": ("event_type", "src_ip"),
    "warm_stream_aggregates": ("aggregate_type", "window_start", "window_end", "aggregate_key"),
}

DEDUP_KEYS: dict[str, tuple[str, ...]] = {
    "kev": ("cve_id",),
    "epss": ("cve_id", "date"),
    "nvd": ("cve_id",),
    "urlhaus": ("id",),
    "circl_vulnlookup": ("cve_id",),
    "threatfox": ("id",),
    "shodan_seeded": ("ip_str",),
    "pcap_artifacts": ("source_id", "scenario_number", "artifact_name", "ingest_date", "sha256"),
    "pcap_replay_runs": ("replay_run_id",),
    "suricata_events": ("flow_id", "timestamp_utc", "event_type"),
    "ids_alerts": ("timestamp_utc", "src_ip", "dst_ip", "signature"),
    "warm_stream_aggregates": ("window_start", "aggregate_type", "aggregate_key"),
}

URL_PATTERN = re.compile(r"^https?://", re.IGNORECASE)
CVE_PATTERN = re.compile(r"^CVE-\d{4}-\d{4,}$", re.IGNORECASE)


def clean_table(table_name: str, records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Clean and deduplicate records for one Trusted Zone table."""
    cleaned: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()

    for index, record in enumerate(records):
        normalized = normalize_record(table_name, record)
        missing = [
            field
            for field in MANDATORY_FIELDS.get(table_name, ())
            if not _present(record.get(field)) and not _present(normalized.get(field))
        ]
        if missing:
            rejected.append(_reject(table_name, record, index, f"missing mandatory fields: {', '.join(missing)}"))
            continue
        validation_error = _validation_error(table_name, normalized)
        if validation_error:
            rejected.append(_reject(table_name, record, index, validation_error))
            continue

        key = _dedup_key(table_name, normalized)
        if key in seen:
            rejected.append(_reject(table_name, record, index, "duplicate record"))
            continue
        seen.add(key)
        cleaned.append(normalized)

    return cleaned, rejected


def normalize_record(table_name: str, record: dict[str, Any]) -> dict[str, Any]:
    """Return a cleaned copy of *record* with stable P2 helper fields."""
    out = dict(record)
    warnings: list[str] = []

    if table_name == "kev":
        out["cve_id"] = normalize_cve(out.get("cveID") or out.get("cve_id"))
        _date_field(out, "dateAdded", warnings)
        _date_field(out, "dueDate", warnings)
        if out["cve_id"] and not CVE_PATTERN.match(out["cve_id"]):
            warnings.append("invalid cve id")
    elif table_name == "epss":
        out["cve_id"] = normalize_cve(out.get("cve") or out.get("cve_id"))
        out["epss_score"] = _float_field(out.get("epss"), warnings, "epss")
        out["epss_percentile"] = _float_field(out.get("percentile"), warnings, "percentile")
        if out["epss_score"] is not None and not 0 <= out["epss_score"] <= 1:
            warnings.append("epss outside [0,1]")
        if out["epss_percentile"] is not None and not 0 <= out["epss_percentile"] <= 1:
            warnings.append("percentile outside [0,1]")
        _date_field(out, "date", warnings)
    elif table_name in {"nvd", "circl_vulnlookup"}:
        out["cve_id"] = normalize_cve(out.get("cve_id") or out.get("id"))
        _date_field(out, "published", warnings)
        _date_field(out, "last_modified", warnings)
        out["cvss_v3_score"] = _float_field(
            out.get("cvss_v3_score") or out.get("cvss_score"),
            warnings,
            "cvss_v3_score",
        )
        if out["cvss_v3_score"] is not None and not 0 <= out["cvss_v3_score"] <= 10:
            warnings.append("cvss outside [0,10]")
    elif table_name == "urlhaus":
        url = _string(out.get("url"))
        out["url"] = url
        if url and not URL_PATTERN.match(url):
            warnings.append("url does not start with http")
    elif table_name in {"suricata_events", "ids_alerts", "pcap_replay_runs"}:
        timestamp = out.get("timestamp_utc") or out.get("timestamp") or out.get("started_at_utc")
        out["timestamp_utc"] = _timestamp(timestamp, warnings)
        out["severity"] = _int_field(out.get("severity"), warnings, "severity")
    elif table_name == "warm_stream_aggregates":
        out["window_start"] = _timestamp(out.get("window_start"), warnings)
        out["window_end"] = _timestamp(out.get("window_end"), warnings)
        out["aggregate_type"] = _string(out.get("aggregate_type"))
        out["aggregate_key"] = _string(out.get("aggregate_key") or out.get("aggregate_value"))
        out["aggregate_value"] = _string(out.get("aggregate_value") or out.get("aggregate_key"))
        alert_count = out.get("alert_count") if out.get("alert_count") not in (None, "") else out.get("count")
        out["alert_count"] = _int_field(alert_count, warnings, "alert_count")
        out["rank"] = _int_field(out.get("rank"), warnings, "rank")
        out["window_minutes"] = _int_field(out.get("window_minutes"), warnings, "window_minutes")
    elif table_name == "pcap_artifacts":
        out["size_bytes"] = _int_field(out.get("size_bytes"), warnings, "size_bytes")
        out["integrity_fail"] = bool(out.get("integrity_fail", False))

    out["ingest_date"] = _string(out.get("ingest_date")) or _today()
    out["_quality_warn"] = bool(warnings)
    out["_quality_reasons"] = "; ".join(sorted(set(warnings)))
    return {key: _json_safe(value) for key, value in out.items()}


def normalize_cve(value: Any) -> str:
    clean = _string(value).strip().upper()
    return clean


def _dedup_key(table_name: str, record: dict[str, Any]) -> tuple[Any, ...]:
    keys = DEDUP_KEYS.get(table_name, ())
    if keys and any(_present(record.get(key)) for key in keys):
        return tuple(record.get(key) for key in keys)
    return (json.dumps(record, sort_keys=True, default=str),)


def _reject(table_name: str, record: dict[str, Any], index: int, reason: str) -> dict[str, Any]:
    return {
        "source_table": table_name,
        "source_index": index,
        "reject_reason": reason,
        "record_json": json.dumps(record, sort_keys=True, default=str),
        "ingest_date": _string(record.get("ingest_date")) or _today(),
    }


def _validation_error(table_name: str, record: dict[str, Any]) -> str:
    if table_name != "warm_stream_aggregates":
        return ""
    if not _present(record.get("aggregate_key")):
        return "missing aggregate key"
    count = record.get("alert_count")
    if count is None:
        return "invalid aggregate count"
    if _int_field(count, [], "alert_count") is None:
        return "invalid aggregate count"
    if int(count) < 0:
        return "invalid aggregate count"
    if not _present(record.get("window_start")) or not _present(record.get("window_end")):
        return "missing aggregate window"
    return ""


def _present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _float_field(value: Any, warnings: list[str], field_name: str) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        warnings.append(f"invalid float: {field_name}")
        return None


def _int_field(value: Any, warnings: list[str], field_name: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        warnings.append(f"invalid integer: {field_name}")
        return None


def _date_field(record: dict[str, Any], field_name: str, warnings: list[str]) -> None:
    value = record.get(field_name)
    if value in (None, ""):
        return
    parsed = _parse_datetime(value)
    if parsed is None:
        warnings.append(f"invalid date: {field_name}")
        return
    record[field_name] = parsed.date().isoformat()


def _timestamp(value: Any, warnings: list[str]) -> str:
    if value in (None, ""):
        warnings.append("missing timestamp")
        return ""
    parsed = _parse_datetime(value)
    if parsed is None:
        warnings.append("invalid timestamp")
        return _string(value)
    return parsed.isoformat()


def _parse_datetime(value: Any) -> dt.datetime | None:
    raw = _string(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    for candidate in (raw, raw[:19], raw[:10]):
        try:
            parsed = dt.datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed
        except ValueError:
            continue
    return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value


def _today() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()
