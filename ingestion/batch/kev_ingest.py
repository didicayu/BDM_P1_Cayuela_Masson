"""Batch ingestion for CISA KEV catalog."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.http_utils import fetch_bytes
from ingestion.common.landing_utils import (
    ingest_date_str,
    partition_now,
    utc_now,
)
from ingestion.common.storage import LandingStorage

SOURCE_ID = "cisa_kev"
SOURCE_URLS = [
    "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
    "https://www.cisa.gov/sites/default/files/csv/known_exploited_vulnerabilities.csv",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest CISA KEV feed into landing zone.")
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
        help="HTTP retries per URL.",
    )
    return parser


def fetch_kev(timeout_seconds: int, retries: int) -> tuple[bytes, str]:
    last_error: Exception | None = None
    for url in SOURCE_URLS:
        try:
            payload = fetch_bytes(
                url,
                timeout_seconds=timeout_seconds,
                retries=retries,
            )
            return payload, url
        except Exception as exc:  # pragma: no cover - handled at runtime
            last_error = exc
    raise RuntimeError(f"Unable to fetch CISA KEV from candidates: {last_error}")


def run(base_dir: Path, timeout_seconds: int, retries: int) -> dict[str, str | int]:
    partition_at = partition_now()
    retrieved_at = utc_now()
    ingest_date = ingest_date_str(partition_at)
    storage = LandingStorage.from_env(base_dir)

    payload, source_url = fetch_kev(timeout_seconds=timeout_seconds, retries=retries)

    relative_dir = Path("structured") / "kev" / f"ingest_date={ingest_date}"
    extension = "json" if source_url.endswith(".json") else "csv"
    storage.clear_prefix(relative_dir)
    raw_relative = relative_dir / f"kev_latest.{extension}"
    raw_written = storage.write_bytes(raw_relative, payload)

    record_count = None
    delta_records: list[dict] = []
    if extension == "json":
        json_payload = json.loads(payload.decode("utf-8"))
        vulnerabilities = json_payload.get("vulnerabilities", [])
        record_count = len(vulnerabilities)
        delta_records = [
            {**v, "ingest_date": ingest_date, "retrieved_at_utc": retrieved_at.isoformat()}
            for v in vulnerabilities
        ]

    metadata = {
        "source_id": SOURCE_ID,
        "source_url": source_url,
        "retrieved_at_utc": retrieved_at.isoformat(),
        "landing_path": raw_written.landing_path,
        "relative_landing_path": raw_written.relative_path,
        "sha256": raw_written.sha256,
        "size_bytes": raw_written.size_bytes,
        "record_count": record_count,
    }
    meta_relative = raw_relative.with_suffix(raw_relative.suffix + ".meta.json")
    storage.write_json(meta_relative, metadata)

    storage.append_manifest_entry(
        source_id=SOURCE_ID,
        ingest_date=ingest_date,
        entry=metadata,
    )

    if delta_records:
        DeltaLakeStorage.from_env().write_or_merge(
            "kev",
            delta_records,
            merge_keys=["cveID"],
            partition_by=["ingest_date"],
        )

    return {
        "source": SOURCE_ID,
        "landing_path": raw_written.landing_path,
        "record_count": record_count or 0,
    }


def main() -> int:
    args = build_parser().parse_args()
    result = run(args.base_dir, args.timeout_seconds, args.retries)
    print(
        f"[{result['source']}] Ingested to {result['landing_path']} "
        f"(records={result['record_count']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
