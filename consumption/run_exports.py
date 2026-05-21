"""Run P2 consumption exports from exploitation Delta assets."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from consumption.exports import export_consumption_outputs
from governance.catalog import lineage_record, make_run_id, quality_metric
from ingestion.common.delta_storage import DeltaLakeStorage


def run_consumption_exports(output_dir: Path = Path("consumption/outputs")) -> dict[str, str | int]:
    run_id = make_run_id("consumption")
    exploitation = DeltaLakeStorage.from_env_bucket("EXPLOITATION_DELTA_BUCKET", "exploitation")
    vuln_priority = exploitation.read_records("kpi_vuln_priority_top50")
    ioc_correlations = exploitation.read_records("ioc_correlations")
    anomaly_flags = exploitation.read_records("anomaly_flags")
    alert_trends = exploitation.read_records("kpi_daily_alert_counts")
    top_signatures = exploitation.read_records("kpi_top_signatures")
    top_source_ips = exploitation.read_records("kpi_top_source_ips")
    top_destination_ips = exploitation.read_records("kpi_top_destination_ips")
    ml_model = exploitation.read_records("ml_anomaly_model")

    result = export_consumption_outputs(
        output_dir=output_dir,
        vuln_priority=vuln_priority,
        ioc_correlations=ioc_correlations,
        anomaly_flags=anomaly_flags,
        alert_trends=alert_trends,
        top_signatures=top_signatures,
        top_source_ips=top_source_ips,
        top_destination_ips=top_destination_ips,
        ml_model=ml_model,
    )
    lineage = [lineage_record(
        run_id=run_id,
        dag_id="cybersecintel_consumption_exports",
        task_id="export_consumption_outputs",
        transformation_name="export_analyst_consumption_files",
        source_assets=[
            "s3://exploitation/kpi_vuln_priority_top50",
            "s3://exploitation/ioc_correlations",
            "s3://exploitation/anomaly_flags",
            "s3://exploitation/kpi_daily_alert_counts",
            "s3://exploitation/kpi_top_signatures",
            "s3://exploitation/kpi_top_source_ips",
            "s3://exploitation/kpi_top_destination_ips",
            "s3://exploitation/ml_anomaly_model",
        ],
        target_asset=str(output_dir),
        rows_read=(
            len(vuln_priority)
            + len(ioc_correlations)
            + len(anomaly_flags)
            + len(alert_trends)
            + len(top_signatures)
            + len(top_source_ips)
            + len(top_destination_ips)
            + len(ml_model)
        ),
        rows_written=int(result["files_written"]),
    )]
    exploitation.write_or_merge("governance_lineage", lineage, merge_keys=None, partition_by=["ingest_date"])
    metrics = [
        quality_metric(
            run_id=run_id,
            zone="consumption",
            table_name="consumption_outputs",
            metric_name=str(name),
            metric_value=value,
        )
        for name, value in result.items()
        if isinstance(value, int)
    ]
    if metrics:
        exploitation.write_or_merge("governance_quality_metrics", metrics, merge_keys=None, partition_by=["ingest_date"])
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Export CyberSecIntel consumption artifacts.")
    parser.add_argument("--output-dir", type=Path, default=Path("consumption/outputs"))
    args = parser.parse_args()
    result: dict[str, Any] = run_consumption_exports(args.output_dir)
    for name, value in result.items():
        print(f"{name}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
