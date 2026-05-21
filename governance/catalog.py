"""Governance artifacts for the P2 exploitation-zone data products."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def today() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def make_run_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def data_product_catalog() -> list[dict[str, Any]]:
    """Return the governed data products exposed by the P2 exploitation zone."""
    produced_at = utc_now()
    products = [
        {
            "data_product": "vuln_enriched",
            "domain": "Vulnerability Intelligence",
            "owner": "CyberSecIntel data engineering team",
            "description": "One row per CVE combining KEV, NVD, EPSS, and CIRCL context for vulnerability triage.",
            "zone": "exploitation",
            "storage_path": "s3://exploitation/vuln_enriched",
            "primary_key": "cve_id",
            "upstream_assets": "trusted/kev; trusted/nvd; trusted/epss; trusted/circl_vulnlookup",
            "quality_rules": "cve_id non-empty; epss_score between 0 and 1 when present; cvss_v3_score between 0 and 10 when present",
            "access_policy": "Analyst read access; exploitation DAG write access",
            "retention_policy": "Delta overwrite on each run; transaction history retained by Delta log",
        },
        {
            "data_product": "network_events",
            "domain": "Network Security Events",
            "owner": "CyberSecIntel data engineering team",
            "description": "Unified IDS and Suricata event schema for SOC event investigation.",
            "zone": "exploitation",
            "storage_path": "s3://exploitation/network_events",
            "primary_key": "event_timestamp_utc,src_ip,dst_ip,signature",
            "upstream_assets": "trusted/ids_alerts; trusted/suricata_events",
            "quality_rules": "timestamp parseable when present; severity integer; source and destination fields normalized",
            "access_policy": "SOC analyst read access; exploitation DAG write access",
            "retention_policy": "Delta overwrite on each run for reproducible coursework demo",
        },
        {
            "data_product": "ioc_correlations",
            "domain": "Threat Intelligence",
            "owner": "CyberSecIntel data engineering team",
            "description": "Network events matched against known indicators from ThreatFox, URLhaus, and Shodan.",
            "zone": "exploitation",
            "storage_path": "s3://exploitation/ioc_correlations",
            "primary_key": "ioc_value,event_timestamp_utc,src_ip,dst_ip",
            "upstream_assets": "exploitation/network_events; trusted/threatfox; trusted/urlhaus; trusted/shodan_seeded",
            "quality_rules": "ioc_value non-empty; event context retained for analyst traceability",
            "access_policy": "SOC analyst read access; exploitation DAG write access",
            "retention_policy": "Delta overwrite on each run",
        },
        {
            "data_product": "anomaly_flags",
            "domain": "Network Security Events",
            "owner": "CyberSecIntel data engineering team",
            "description": "ML anomaly predictions over network-event features with human-readable anomaly reasons.",
            "zone": "exploitation",
            "storage_path": "s3://exploitation/anomaly_flags",
            "primary_key": "event_timestamp_utc,src_ip,dst_ip,signature",
            "upstream_assets": "exploitation/network_events; exploitation/ml_anomaly_model",
            "quality_rules": "anomaly_score between 0 and 1; ml_score present; anomaly_reasons non-empty",
            "access_policy": "SOC analyst read access; exploitation DAG write access",
            "retention_policy": "Delta overwrite on each run",
        },
        {
            "data_product": "ml_anomaly_model",
            "domain": "ML Artifacts",
            "owner": "CyberSecIntel data engineering team",
            "description": "Dependency-free unsupervised anomaly model trained from network event severity, ports, entity frequencies, and signature frequencies.",
            "zone": "exploitation",
            "storage_path": "s3://exploitation/ml_anomaly_model",
            "primary_key": "model_type,trained_at_utc",
            "upstream_assets": "exploitation/network_events",
            "quality_rules": "trained_rows greater than zero when network_events exists; threshold non-negative; feature_names documented",
            "access_policy": "Analyst read access; exploitation DAG write access",
            "retention_policy": "Current model metadata in Delta; JSON artifact under models/ids_anomaly_detector/model.json",
        },
        {
            "data_product": "consumption_outputs",
            "domain": "SOC Consumption",
            "owner": "CyberSecIntel data engineering team",
            "description": "CSV, JSON, and HTML artifacts exported for analyst-facing review.",
            "zone": "consumption",
            "storage_path": "consumption/outputs",
            "primary_key": "file_name",
            "upstream_assets": "exploitation KPI tables; exploitation/ioc_correlations; exploitation/anomaly_flags",
            "quality_rules": "all expected files are written; row counts are recorded in governance_quality_metrics",
            "access_policy": "Analyst local read access; consumption DAG write access",
            "retention_policy": "Files regenerated on each run",
        },
    ]
    return [{**product, "produced_at_utc": produced_at, "ingest_date": today()} for product in products]


def lineage_record(
    *,
    run_id: str,
    dag_id: str,
    task_id: str,
    transformation_name: str,
    source_assets: list[str],
    target_asset: str,
    rows_read: int,
    rows_written: int,
    rows_rejected: int = 0,
    status: str = "success",
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "task_id": task_id,
        "transformation_name": transformation_name,
        "source_assets": "; ".join(source_assets),
        "target_asset": target_asset,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_rejected": rows_rejected,
        "status": status,
        "recorded_at_utc": utc_now(),
        "ingest_date": today(),
    }


def quality_metric(
    *,
    run_id: str,
    zone: str,
    table_name: str,
    metric_name: str,
    metric_value: int | float | str,
    status: str = "ok",
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "zone": zone,
        "table_name": table_name,
        "metric_name": metric_name,
        "metric_value": str(metric_value),
        "status": status,
        "recorded_at_utc": utc_now(),
        "ingest_date": today(),
    }
