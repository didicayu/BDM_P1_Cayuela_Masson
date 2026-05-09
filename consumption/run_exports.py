"""Run P2 consumption exports from exploitation Delta assets."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from consumption.exports import export_consumption_outputs
from ingestion.common.delta_storage import DeltaLakeStorage


def run_consumption_exports(output_dir: Path = Path("consumption/outputs")) -> dict[str, str | int]:
    exploitation = DeltaLakeStorage.from_env_bucket("EXPLOITATION_DELTA_BUCKET", "exploitation")
    return export_consumption_outputs(
        output_dir=output_dir,
        vuln_priority=exploitation.read_records("kpi_vuln_priority_top50"),
        ioc_correlations=exploitation.read_records("ioc_correlations"),
        anomaly_flags=exploitation.read_records("anomaly_flags"),
        alert_trends=exploitation.read_records("kpi_daily_alert_counts"),
        top_signatures=exploitation.read_records("kpi_top_signatures"),
        top_source_ips=exploitation.read_records("kpi_top_source_ips"),
    )


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
