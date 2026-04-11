"""Batch ingestion for FIRST EPSS API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.http_utils import build_url, fetch_bytes
from ingestion.common.landing_utils import ingest_date_str, partition_now, utc_now
from ingestion.common.storage import LandingStorage

SOURCE_ID = "first_epss_api_v1"
SOURCE_URL = "https://api.first.org/data/v1/epss"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest FIRST EPSS API pages.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--results-per-page",
        type=int,
        default=1000,
        help="Results per page (default: 1000).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Safety limit for number of pages per run (default: 10).",
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
        help="HTTP retries per page.",
    )
    return parser


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def run(
    base_dir: Path,
    results_per_page: int,
    max_pages: int,
    timeout_seconds: int,
    retries: int,
) -> dict[str, str | int]:
    partition_at = partition_now()
    retrieved_at = utc_now()
    ingest_date = ingest_date_str(partition_at)
    storage = LandingStorage.from_env(base_dir)

    relative_dir = Path("structured") / "epss" / f"ingest_date={ingest_date}"
    storage.clear_prefix(relative_dir)

    total_records = 0
    pages_ingested = 0

    for page in range(max_pages):
        offset = page * results_per_page
        params = {"limit": results_per_page, "offset": offset}
        url = build_url(SOURCE_URL, params=params)
        payload = fetch_bytes(
            url,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )

        raw_relative = relative_dir / f"epss_page={page:03d}.json"
        raw_written = storage.write_bytes(raw_relative, payload)

        parsed = json.loads(payload.decode("utf-8"))
        records = parsed.get("data", [])
        if not isinstance(records, list):
            records = []

        record_count = len(records)
        total_records += record_count
        pages_ingested += 1
        total_results = _int_or_none(parsed.get("total"))

        if records:
            delta_records = [
                {**r, "ingest_date": ingest_date, "retrieved_at_utc": retrieved_at.isoformat()}
                for r in records
            ]
            DeltaLakeStorage.from_env().write_or_merge(
                "epss",
                delta_records,
                merge_keys=["cve", "date"],
                partition_by=["ingest_date"],
            )

        metadata = {
            "source_id": SOURCE_ID,
            "source_url": SOURCE_URL,
            "request_url": url,
            "retrieved_at_utc": retrieved_at.isoformat(),
            "landing_path": raw_written.landing_path,
            "relative_landing_path": raw_written.relative_path,
            "sha256": raw_written.sha256,
            "size_bytes": raw_written.size_bytes,
            "page_index": page,
            "offset": offset,
            "results_per_page": results_per_page,
            "total_results": total_results,
            "record_count": record_count,
        }
        meta_relative = raw_relative.with_suffix(".json.meta.json")
        storage.write_json(meta_relative, metadata)
        storage.append_manifest_entry(
            source_id=SOURCE_ID,
            ingest_date=ingest_date,
            entry=metadata,
        )

        if record_count == 0:
            break
        if total_results is not None and offset + results_per_page >= total_results:
            break

    return {
        "source": SOURCE_ID,
        "landing_dir": str(relative_dir),
        "records": total_records,
        "pages": pages_ingested,
    }


def main() -> int:
    args = build_parser().parse_args()
    result = run(
        base_dir=args.base_dir,
        results_per_page=args.results_per_page,
        max_pages=args.max_pages,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
    )
    print(
        f"[{result['source']}] Ingested {result['records']} records across "
        f"{result['pages']} page(s) in {result['landing_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
