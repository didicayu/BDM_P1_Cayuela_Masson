"""Run the P2 Trusted Zone materialization."""

from __future__ import annotations

import argparse
from typing import Any

from ingestion.common.delta_storage import DeltaLakeStorage
from trusted.cleaning import SOURCE_TABLES, clean_table


def run_trusted_zone(source_tables: tuple[str, ...] = SOURCE_TABLES) -> dict[str, dict[str, int]]:
    source = DeltaLakeStorage.from_env()
    trusted = DeltaLakeStorage.from_env_bucket("TRUSTED_DELTA_BUCKET", "trusted")
    trusted.ensure_bucket()

    summary: dict[str, dict[str, int]] = {}
    for table_name in source_tables:
        records = source.read_records(table_name)
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
