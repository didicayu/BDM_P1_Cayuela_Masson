"""Batch ingestion for abuse.ch ThreatFox API."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.http_utils import fetch_bytes
from ingestion.common.landing_utils import ingest_date_str, partition_now, utc_now
from ingestion.common.storage import LandingStorage

SOURCE_ID = "abuse_ch_threatfox_api_v1"
SOURCE_URL = "https://threatfox-api.abuse.ch/api/v1/"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest abuse.ch ThreatFox IOC feed.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Lookback days for IOC retrieval (default: 1).",
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


def run(
    base_dir: Path,
    days: int,
    timeout_seconds: int,
    retries: int,
) -> dict[str, str | int]:
    api_key = os.getenv("ABUSE_CH_API_KEY", "").strip()
    if not api_key:
        return {
            "source": SOURCE_ID,
            "landing_path": "",
            "ioc_count": 0,
            "skipped": 1,
            "reason": "missing_api_key",
        }

    partition_at = partition_now()
    retrieved_at = utc_now()
    ingest_date = ingest_date_str(partition_at)
    storage = LandingStorage.from_env(base_dir)

    relative_dir = Path("semi_structured") / "threatfox" / f"ingest_date={ingest_date}"
    storage.clear_prefix(relative_dir)

    request_payload = {"query": "get_iocs", "days": days}
    payload = fetch_bytes(
        SOURCE_URL,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Auth-Key": api_key,
        },
        body=json.dumps(request_payload).encode("utf-8"),
        timeout_seconds=timeout_seconds,
        retries=retries,
    )

    raw_relative = relative_dir / "threatfox_iocs.json"
    raw_written = storage.write_bytes(raw_relative, payload)

    parsed = json.loads(payload.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise RuntimeError("ThreatFox response is not a JSON object.")

    query_status = str(parsed.get("query_status", "")).strip().lower()
    if "unauthor" in query_status or "forbidden" in query_status or "auth" in query_status:
        raise RuntimeError(f"ThreatFox authorization failure: query_status={query_status}")

    records = parsed.get("data", [])
    if not isinstance(records, list):
        records = []
    ioc_type_breakdown: dict[str, int] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        ioc_type = str(record.get("ioc_type", "unknown")).strip() or "unknown"
        ioc_type_breakdown[ioc_type] = ioc_type_breakdown.get(ioc_type, 0) + 1

    metadata = {
        "source_id": SOURCE_ID,
        "source_url": SOURCE_URL,
        "retrieved_at_utc": retrieved_at.isoformat(),
        "landing_path": raw_written.landing_path,
        "relative_landing_path": raw_written.relative_path,
        "sha256": raw_written.sha256,
        "size_bytes": raw_written.size_bytes,
        "request_days": days,
        "query_status": query_status,
        "ioc_count": len(records),
        "ioc_type_breakdown": ioc_type_breakdown,
    }
    meta_relative = raw_relative.with_suffix(".json.meta.json")
    storage.write_json(meta_relative, metadata)
    storage.append_manifest_entry(
        source_id=SOURCE_ID,
        ingest_date=ingest_date,
        entry=metadata,
    )

    if records:
        delta = DeltaLakeStorage.from_env()
        delta_records = [
            delta.flatten_record({
                **r,
                "ingest_date": ingest_date,
                "retrieved_at_utc": retrieved_at.isoformat(),
            })
            for r in records
            if isinstance(r, dict)
        ]
        delta.write_or_merge(
            "threatfox",
            delta_records,
            merge_keys=["id"],
            partition_by=["ingest_date"],
        )

    return {
        "source": SOURCE_ID,
        "landing_path": raw_written.landing_path,
        "ioc_count": len(records),
    }


def main() -> int:
    args = build_parser().parse_args()
    result = run(
        base_dir=args.base_dir,
        days=args.days,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
    )
    if result.get("skipped"):
        print(f"[{result['source']}] Skipped ({result['reason']})")
        return 0
    print(
        f"[{result['source']}] Ingested {result['ioc_count']} IOC records "
        f"to {result['landing_path']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
