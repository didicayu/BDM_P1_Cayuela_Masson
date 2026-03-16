"""Batch ingestion for NVD CVE API v2."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
from pathlib import Path

from ingestion.common.http_utils import build_url, fetch_bytes
from ingestion.common.landing_utils import (
    ingest_date_str,
    partition_now,
    utc_now,
)
from ingestion.common.storage import LandingStorage

SOURCE_ID = "nvd_cve_api_v2"
SOURCE_URL = "https://services.nvd.nist.gov/rest/json/cves/2.0"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest NVD CVE API pages.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Incremental window in hours (default: 24).",
    )
    parser.add_argument(
        "--results-per-page",
        type=int,
        default=100,
        help="NVD results per page (max depends on API policy).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Safety limit for number of pages per run.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=45,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retries per page.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=7.0,
        help="Sleep between page calls to avoid rate-limit issues.",
    )
    return parser


def nvd_time(value: dt.datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.000")


def run(
    base_dir: Path,
    window_hours: int,
    results_per_page: int,
    max_pages: int,
    timeout_seconds: int,
    retries: int,
    sleep_seconds: float,
) -> dict[str, str | int]:
    partition_at = partition_now()
    retrieved_at = utc_now()
    ingest_date = ingest_date_str(partition_at)
    start_window = partition_at - dt.timedelta(hours=window_hours)
    storage = LandingStorage.from_env(base_dir)

    relative_dir = Path("semi_structured") / "nvd" / f"ingest_date={ingest_date}"
    storage.clear_prefix(relative_dir)

    api_key = os.getenv("NVD_API_KEY")
    headers: dict[str, str] = {}
    if api_key:
        headers["apiKey"] = api_key

    total_records = 0
    total_results: int | None = None
    pages_ingested = 0

    for page in range(max_pages):
        start_index = page * results_per_page
        params = {
            "startIndex": start_index,
            "resultsPerPage": results_per_page,
            "lastModStartDate": nvd_time(start_window),
            "lastModEndDate": nvd_time(partition_at),
        }
        url = build_url(SOURCE_URL, params=params)
        payload = fetch_bytes(
            url,
            headers=headers,
            timeout_seconds=timeout_seconds,
            retries=retries,
        )

        file_name = f"nvd_cves_page={page:03d}.json"
        raw_relative = relative_dir / file_name
        raw_written = storage.write_bytes(raw_relative, payload)

        parsed = json.loads(payload.decode("utf-8"))
        vulnerabilities = parsed.get("vulnerabilities", [])
        page_count = len(vulnerabilities)
        total_records += page_count
        total_results = parsed.get("totalResults", total_results)
        pages_ingested += 1

        metadata = {
            "source_id": SOURCE_ID,
            "source_url": SOURCE_URL,
            "request_url": url,
            "retrieved_at_utc": retrieved_at.isoformat(),
            "landing_path": raw_written.landing_path,
            "relative_landing_path": raw_written.relative_path,
            "sha256": raw_written.sha256,
            "size_bytes": raw_written.size_bytes,
            "record_count": page_count,
            "page_index": page,
            "start_index": start_index,
            "results_per_page": results_per_page,
            "window_hours": window_hours,
        }
        meta_relative = raw_relative.with_suffix(".json.meta.json")
        storage.write_json(meta_relative, metadata)
        storage.append_manifest_entry(
            source_id=SOURCE_ID,
            ingest_date=ingest_date,
            entry=metadata,
        )

        if page_count == 0:
            break
        if total_results is not None and start_index + results_per_page >= total_results:
            break
        if page < max_pages - 1:
            time.sleep(sleep_seconds)

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
        window_hours=args.window_hours,
        results_per_page=args.results_per_page,
        max_pages=args.max_pages,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        sleep_seconds=args.sleep_seconds,
    )
    print(
        f"[{result['source']}] Ingested {result['records']} records across "
        f"{result['pages']} page(s) in {result['landing_dir']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
