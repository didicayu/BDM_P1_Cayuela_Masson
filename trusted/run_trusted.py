"""Run the P2 Trusted Zone materialization."""

from __future__ import annotations

import argparse
import os
from typing import Any

from governance.catalog import lineage_record, make_run_id, quality_metric
from ingestion.common.delta_storage import DeltaLakeStorage
from trusted.cleaning import SOURCE_TABLES, clean_table


def run_trusted_zone(source_tables: tuple[str, ...] = SOURCE_TABLES) -> dict[str, dict[str, int]]:
    run_id = make_run_id("trusted")
    source = DeltaLakeStorage.from_env()
    trusted = DeltaLakeStorage.from_env_bucket("TRUSTED_DELTA_BUCKET", "trusted")
    trusted.ensure_bucket()

    spark_cleaned_tables: set[str] = set()
    spark_summary: dict[str, int | str] = {}
    if os.getenv("P2_TRUSTED_ENGINE", "").strip().lower() == "spark":
        from trusted.spark_jobs.clean_sources import SPARK_TRUSTED_TABLES, run_spark_trusted_cleaning

        spark_summary = run_spark_trusted_cleaning()
        spark_cleaned_tables = set(SPARK_TRUSTED_TABLES)

    summary: dict[str, dict[str, int]] = {}
    lineage: list[dict[str, Any]] = []
    quality_metrics: list[dict[str, Any]] = []
    for table_name in source_tables:
        records = source.read_records(table_name)
        if table_name in spark_cleaned_tables:
            cleaned = trusted.read_records(table_name)
            rejected = []
            rows_written = len(cleaned)
            rejected_written = trusted.overwrite(f"rejected_{table_name}", [], partition_by=None)
        else:
            cleaned, rejected = clean_table(table_name, records)
            partition_by = ["ingest_date"] if cleaned and "ingest_date" in cleaned[0] else None
            rejected_partition_by = ["ingest_date"] if rejected else None
            rows_written = trusted.overwrite(table_name, cleaned, partition_by=partition_by)
            rejected_written = trusted.overwrite(
                f"rejected_{table_name}",
                rejected,
                partition_by=rejected_partition_by,
            )
        summary[table_name] = {
            "rows_read": len(records),
            "rows_written": rows_written,
            "rows_rejected": rejected_written,
        }
        lineage.append(lineage_record(
            run_id=run_id,
            dag_id="cybersecintel_trusted_zone",
            task_id="materialize_trusted_zone",
            transformation_name=f"spark_trusted_clean_{table_name}" if table_name in spark_cleaned_tables else f"trusted_clean_{table_name}",
            source_assets=[f"s3://deltalake/{table_name}"],
            target_asset=f"s3://trusted/{table_name}",
            rows_read=len(records),
            rows_written=rows_written,
            rows_rejected=rejected_written,
        ))
        quality_metrics.extend([
            quality_metric(
                run_id=run_id,
                zone="trusted",
                table_name=table_name,
                metric_name="transformation_engine",
                metric_value="spark" if table_name in spark_cleaned_tables else "python",
            ),
            quality_metric(
                run_id=run_id,
                zone="trusted",
                table_name=table_name,
                metric_name="rows_read",
                metric_value=len(records),
            ),
            quality_metric(
                run_id=run_id,
                zone="trusted",
                table_name=table_name,
                metric_name="rows_written",
                metric_value=rows_written,
            ),
            quality_metric(
                run_id=run_id,
                zone="trusted",
                table_name=table_name,
                metric_name="rows_rejected",
                metric_value=rejected_written,
                status="warn" if rejected_written else "ok",
            ),
            quality_metric(
                run_id=run_id,
                zone="trusted",
                table_name=table_name,
                metric_name="quality_warning_rows",
                metric_value=sum(1 for row in cleaned if row.get("_quality_warn")),
                status="warn" if any(row.get("_quality_warn") for row in cleaned) else "ok",
            ),
        ])
    if spark_summary:
        quality_metrics.append(quality_metric(
            run_id=run_id,
            zone="trusted",
            table_name="spark_trusted_cleaning",
            metric_name="spark_summary",
            metric_value=str(spark_summary),
        ))
    trusted.overwrite("governance_lineage", lineage, partition_by=["ingest_date"])
    trusted.overwrite("governance_quality_metrics", quality_metrics, partition_by=["ingest_date"])
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Materialize CyberSecIntel Trusted Zone tables.")
    parser.parse_args()
    result: dict[str, Any] = run_trusted_zone()
    for table_name, stats in result.items():
        print(
            f"{table_name}: read={stats['rows_read']} "
            f"trusted={stats['rows_written']} rejected={stats['rows_rejected']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
