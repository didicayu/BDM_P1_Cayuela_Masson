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
            "description": "scikit-learn Isolation Forest model metadata trained from network event severity, ports, entity frequencies, and signature frequencies, with robust fallback metadata when sklearn is unavailable.",
            "zone": "exploitation",
            "storage_path": "s3://exploitation/ml_anomaly_model",
            "primary_key": "model_type,trained_at_utc",
            "upstream_assets": "exploitation/network_events",
            "quality_rules": "trained_rows greater than zero when network_events exists; contamination documented; threshold non-negative; feature_names documented",
            "access_policy": "Analyst read access; exploitation DAG write access",
            "retention_policy": "Current model metadata in Delta; JSON and joblib artifacts under models/ids_anomaly_detector/",
        },
        {
            "data_product": "sklearn_isolation_forest_model",
            "domain": "ML Artifacts",
            "owner": "CyberSecIntel data engineering team",
            "description": "Preferred Isolation Forest anomaly model backend used for the final P2 anomaly_flags product.",
            "zone": "model",
            "storage_path": "models/ids_anomaly_detector/model.json",
            "primary_key": "model_type,trained_at_utc",
            "upstream_assets": "exploitation/network_events",
            "quality_rules": "model_type equals sklearn_isolation_forest; feature_names match the network-event feature contract",
            "access_policy": "Coursework reviewer local read access; exploitation DAG write access",
            "retention_policy": "Rebuilt by exploitation materialization and included in final archive",
        },
        {
            "data_product": "model_joblib_artifact",
            "domain": "ML Artifacts",
            "owner": "CyberSecIntel data engineering team",
            "description": "Serialized scikit-learn Isolation Forest estimator for reuse outside the Delta metadata record.",
            "zone": "model",
            "storage_path": "models/ids_anomaly_detector/model.joblib",
            "primary_key": "artifact_path",
            "upstream_assets": "exploitation/network_events",
            "quality_rules": "file exists when sklearn backend is used; path recorded in ml_anomaly_model",
            "access_policy": "Coursework reviewer local read access; exploitation DAG write access",
            "retention_policy": "Rebuilt by exploitation materialization and included in final archive",
        },
        {
            "data_product": "warm_stream_aggregates",
            "domain": "Network Security Events",
            "owner": "CyberSecIntel data engineering team",
            "description": "Bounded warm alert aggregates per minute, severity, source IP, destination IP, and signature.",
            "zone": "warm/trusted",
            "storage_path": "s3://trusted/warm_stream_aggregates",
            "primary_key": "window_start,aggregate_type,aggregate_key",
            "upstream_assets": "kafka/ids.alerts; deltalake/ids_alerts fallback",
            "quality_rules": "window_start and aggregate_key present; alert_count integer and non-negative",
            "access_policy": "SOC analyst read access; warm aggregate DAG write access",
            "retention_policy": "Landing raw records and Trusted Delta table regenerated on each warm aggregate run",
        },
        {
            "data_product": "warm_enriched_alerts_spark",
            "domain": "Network Security Events",
            "owner": "CyberSecIntel data engineering team",
            "description": "Spark Structured Streaming enriched IDS alerts joined with vulnerability priority context.",
            "zone": "exploitation",
            "storage_path": "s3://exploitation/warm_enriched_alerts_spark",
            "primary_key": "event_timestamp_utc,src_ip,dst_ip,signature",
            "upstream_assets": "kafka/ids.alerts; exploitation/vuln_enriched",
            "quality_rules": "checkpoint path recorded; enriched rows preserve alert identity and CVE fields when present",
            "access_policy": "SOC analyst read access; exploitation DAG write access",
            "retention_policy": "Append-style streaming Delta output with checkpointed progress",
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
        {
            "data_product": "grafana_serving_tables",
            "domain": "SOC Consumption",
            "owner": "CyberSecIntel data engineering team",
            "description": "Postgres serving mirror used by the provisioned Grafana dashboard.",
            "zone": "consumption",
            "storage_path": "postgres://cybersecintel_consumption",
            "primary_key": "table-specific",
            "upstream_assets": "exploitation KPI tables; exploitation/ioc_correlations; exploitation/anomaly_flags; exploitation/ml_anomaly_model",
            "quality_rules": "expected serving tables are replaced on each publish; dashboard and datasource provisioning files exist",
            "access_policy": "Grafana read access; consumption DAG write access",
            "retention_policy": "Serving cache regenerated from authoritative exploitation Delta products",
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
