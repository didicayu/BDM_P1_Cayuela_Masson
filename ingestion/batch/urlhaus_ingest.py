"""Batch ingestion for URLhaus feed."""

from __future__ import annotations

import argparse
import csv
import io
from pathlib import Path

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.http_utils import fetch_bytes
from ingestion.common.landing_utils import (
    ingest_date_str,
    partition_now,
    utc_now,
)
from ingestion.common.storage import LandingStorage

SOURCE_ID = "urlhaus_recent_csv"
SOURCE_URL = "https://urlhaus.abuse.ch/downloads/csv_recent/"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest URLhaus recent CSV.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=30,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retries.",
    )
    return parser


def parse_urlhaus_csv(payload: bytes) -> list[dict]:
    """Parse URLhaus CSV payload into a list of row dicts.

    URLhaus prefixes the header line with '# ' (e.g. '# id,dateadded,...').
    Comment lines and blank lines are skipped.
    """
    text = payload.decode("utf-8", errors="replace")
    header_line = None
    data_lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            candidate = stripped.lstrip("#").strip()
            if candidate.startswith("id,"):
                header_line = candidate
        else:
            data_lines.append(stripped)

    if header_line is None or not data_lines:
        return []

    reader = csv.DictReader(io.StringIO("\n".join([header_line] + data_lines)))
    return [dict(row) for row in reader]


def count_csv_rows(payload: bytes) -> int:
    text = payload.decode("utf-8", errors="replace")
    rows = 0
    for line in text.splitlines():
        clean_line = line.strip()
        if not clean_line:
            continue
        if clean_line.startswith("#"):
            continue
        rows += 1
    return rows


def run(base_dir: Path, timeout_seconds: int, retries: int) -> dict[str, str | int]:
    partition_at = partition_now()
    retrieved_at = utc_now()
    ingest_date = ingest_date_str(partition_at)
    storage = LandingStorage.from_env(base_dir)

    payload = fetch_bytes(
        SOURCE_URL,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )

    relative_dir = Path("semi_structured") / "urlhaus" / f"ingest_date={ingest_date}"
    storage.clear_prefix(relative_dir)
    raw_relative = relative_dir / "urlhaus_recent.csv"
    raw_written = storage.write_bytes(raw_relative, payload)

    record_count = count_csv_rows(payload)

    delta_records = parse_urlhaus_csv(payload)
    for r in delta_records:
        r["ingest_date"] = ingest_date
        r["retrieved_at_utc"] = retrieved_at.isoformat()
    if delta_records:
        DeltaLakeStorage.from_env().write_or_merge(
            "urlhaus",
            delta_records,
            merge_keys=["id"],
            partition_by=["ingest_date"],
        )

    metadata = {
        "source_id": SOURCE_ID,
        "source_url": SOURCE_URL,
        "retrieved_at_utc": retrieved_at.isoformat(),
        "landing_path": raw_written.landing_path,
        "relative_landing_path": raw_written.relative_path,
        "sha256": raw_written.sha256,
        "size_bytes": raw_written.size_bytes,
        "record_count": record_count,
    }
    meta_relative = raw_relative.with_suffix(".csv.meta.json")
    storage.write_json(meta_relative, metadata)

    storage.append_manifest_entry(
        source_id=SOURCE_ID,
        ingest_date=ingest_date,
        entry=metadata,
    )

    return {
        "source": SOURCE_ID,
        "landing_path": raw_written.landing_path,
        "record_count": record_count,
    }


def main() -> int:
    args = build_parser().parse_args()
    result = run(args.base_dir, args.timeout_seconds, args.retries)
    print(
        f"[{result['source']}] Ingested to {result['landing_path']} "
        f"(rows={result['record_count']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
