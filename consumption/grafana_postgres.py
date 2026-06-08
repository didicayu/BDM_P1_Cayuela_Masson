"""Publish exploitation products into Postgres tables for Grafana."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from governance.catalog import make_run_id, quality_metric
from ingestion.common.delta_storage import DeltaLakeStorage


SERVING_SCHEMA = "cybersecintel_consumption"

SERVING_TABLE_COLUMNS: dict[str, list[str]] = {
    "kpi_vuln_priority_top50": [
        "cve_id",
        "vuln_priority_score",
        "kev_listed",
        "epss_score",
        "cvss_v3_score",
        "vendor_project",
        "product",
        "date_added",
        "due_date",
        "ingest_date",
    ],
    "kpi_daily_alert_counts": ["day", "severity", "signature", "alert_count", "ingest_date"],
    "kpi_top_signatures": ["signature", "event_count", "ingest_date"],
    "kpi_top_source_ips": ["src_ip", "event_count", "ingest_date"],
    "kpi_top_destination_ips": ["dst_ip", "event_count", "ingest_date"],
    "anomaly_flags": [
        "event_timestamp_utc",
        "src_ip",
        "dst_ip",
        "src_port",
        "dst_port",
        "severity",
        "signature",
        "anomaly_score",
        "ml_score",
        "ml_threshold",
        "prediction_label",
        "anomaly_reasons",
        "model_type",
        "ingest_date",
    ],
    "ioc_correlations": [
        "ioc_value",
        "ioc_source",
        "ioc_type",
        "malware_family",
        "event_timestamp_utc",
        "src_ip",
        "dst_ip",
        "signature",
        "severity",
        "ingest_date",
    ],
    "ml_anomaly_model": [
        "model_type",
        "model_backend",
        "feature_names",
        "trained_rows",
        "threshold",
        "contamination",
        "anomaly_decision_boundary",
        "artifact_path",
        "joblib_artifact_path",
        "trained_at_utc",
        "ingest_date",
    ],
}


def load_serving_products() -> dict[str, list[dict[str, Any]]]:
    exploitation = DeltaLakeStorage.from_env_bucket("EXPLOITATION_DELTA_BUCKET", "exploitation")
    return {
        table_name: exploitation.read_records(table_name)
        for table_name in SERVING_TABLE_COLUMNS
    }


def publish_grafana_serving_tables(
    products: dict[str, list[dict[str, Any]]] | None = None,
    *,
    schema: str = SERVING_SCHEMA,
    dsn: str | None = None,
    record_governance: bool = True,
) -> dict[str, int | str]:
    """Replace Grafana serving tables with the current exploitation products."""
    products = products or load_serving_products()
    connection = _connect(dsn)
    rows_published = 0
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                for table_name, columns in SERVING_TABLE_COLUMNS.items():
                    rows = normalize_rows(products.get(table_name, []), columns)
                    _replace_table(cursor, schema=schema, table_name=table_name, columns=columns, rows=rows)
                    rows_published += len(rows)
    finally:
        connection.close()
    result = {
        "schema": schema,
        "tables_published": len(SERVING_TABLE_COLUMNS),
        "rows_published": rows_published,
        "status": "ok",
    }
    if record_governance:
        _record_grafana_quality_metrics(result)
    return result


def normalize_rows(rows: list[dict[str, Any]], columns: list[str]) -> list[dict[str, str]]:
    """Coerce rows to a stable all-text serving schema for simple Grafana SQL."""
    normalized: list[dict[str, str]] = []
    for row in rows:
        normalized.append({column: _string(row.get(column)) for column in columns})
    return normalized


def _replace_table(cursor, *, schema: str, table_name: str, columns: list[str], rows: list[dict[str, str]]) -> None:
    quoted_table = f'"{schema}"."{table_name}"'
    column_defs = ", ".join(f'"{column}" TEXT' for column in columns)
    cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
    cursor.execute(f"CREATE TABLE {quoted_table} ({column_defs})")
    if not rows:
        return
    placeholders = ", ".join(["%s"] * len(columns))
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    values = [tuple(row.get(column, "") for column in columns) for row in rows]
    cursor.executemany(
        f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})",
        values,
    )


def _connect(dsn: str | None):
    try:
        import psycopg2
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency checked by Docker/local validation.
        raise RuntimeError("psycopg2-binary is required for Grafana Postgres publishing.") from exc

    if dsn:
        return psycopg2.connect(dsn)
    host = os.getenv("GRAFANA_POSTGRES_HOST", os.getenv("POSTGRES_HOST", "postgres"))
    port = os.getenv("GRAFANA_POSTGRES_PORT", os.getenv("POSTGRES_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)


def _record_grafana_quality_metrics(result: dict[str, int | str]) -> None:
    run_id = make_run_id("grafana")
    exploitation = DeltaLakeStorage.from_env_bucket("EXPLOITATION_DELTA_BUCKET", "exploitation")
    dashboard_exists = Path("config/grafana/dashboards/cybersecintel_soc.json").exists()
    datasource_exists = Path("config/grafana/provisioning/datasources/postgres.yml").exists()
    metrics = [
        quality_metric(
            run_id=run_id,
            zone="consumption",
            table_name="grafana_serving_tables",
            metric_name="tables_published",
            metric_value=result.get("tables_published", 0),
        ),
        quality_metric(
            run_id=run_id,
            zone="consumption",
            table_name="grafana_serving_tables",
            metric_name="rows_published",
            metric_value=result.get("rows_published", 0),
        ),
        quality_metric(
            run_id=run_id,
            zone="consumption",
            table_name="grafana_serving_tables",
            metric_name="dashboard_json_exists",
            metric_value=str(dashboard_exists).lower(),
            status="ok" if dashboard_exists else "warn",
        ),
        quality_metric(
            run_id=run_id,
            zone="consumption",
            table_name="grafana_serving_tables",
            metric_name="datasource_provisioning_exists",
            metric_value=str(datasource_exists).lower(),
            status="ok" if datasource_exists else "warn",
        ),
    ]
    exploitation.write_or_merge("governance_quality_metrics", metrics, merge_keys=None, partition_by=["ingest_date"])


def _string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish exploitation products to Postgres for Grafana.")
    parser.add_argument("--dsn", default=None, help="Optional psycopg2 DSN. Defaults to POSTGRES_* environment variables.")
    args = parser.parse_args()
    result = publish_grafana_serving_tables(dsn=args.dsn)
    for name, value in result.items():
        print(f"{name}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
