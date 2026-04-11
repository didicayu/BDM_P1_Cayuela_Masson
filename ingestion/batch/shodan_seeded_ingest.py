"""Batch seeded ingestion for Shodan Host API."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import time
from typing import Any

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.http_utils import build_url, fetch_bytes
from ingestion.common.landing_utils import ingest_date_str, partition_now, utc_now
from ingestion.common.storage import LandingStorage

SOURCE_ID = "shodan_host_seeded_api"
SOURCE_URL = "https://api.shodan.io/shodan/host"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest seeded Shodan host details.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--max-indicators-per-run",
        type=int,
        default=50,
        help="Maximum seeded indicators to query per run (default: 50).",
    )
    parser.add_argument(
        "--request-sleep-seconds",
        type=float,
        default=1.0,
        help="Sleep between Shodan requests (default: 1.0).",
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
        default=2,
        help="HTTP retries per IP.",
    )
    return parser


def _extract_http_status(exc: Exception) -> int | None:
    match = re.search(r"HTTP Error (\d+)", str(exc))
    if match:
        return int(match.group(1))
    return None


def _normalize_ip(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None

    try:
        return str(ipaddress.ip_address(candidate))
    except ValueError:
        pass

    if ":" in candidate and candidate.count(":") == 1 and "." in candidate:
        host_part = candidate.split(":", 1)[0].strip()
        try:
            return str(ipaddress.ip_address(host_part))
        except ValueError:
            return None
    return None


def _extract_ips_from_threatfox(payload: dict[str, Any]) -> list[str]:
    records = payload.get("data", [])
    if not isinstance(records, list):
        return []

    extracted: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            continue

        ioc_type = str(record.get("ioc_type", "")).lower()
        candidates = [
            record.get("ioc"),
            record.get("ioc_value"),
            record.get("ip"),
            record.get("host"),
        ]
        for candidate in candidates:
            if not isinstance(candidate, str):
                continue
            normalized = _normalize_ip(candidate)
            if normalized is None:
                continue
            if "ip" in ioc_type or candidate == record.get("ip") or "." in normalized:
                extracted.append(normalized)
                break
    return extracted


def _extract_ips_from_env() -> list[str]:
    raw = os.getenv("SHODAN_SEED_IPS", "")
    ips: list[str] = []
    for token in raw.split(","):
        normalized = _normalize_ip(token)
        if normalized:
            ips.append(normalized)
    return ips


def run(
    base_dir: Path,
    max_indicators_per_run: int,
    request_sleep_seconds: float,
    timeout_seconds: int,
    retries: int,
) -> dict[str, str | int]:
    shodan_enabled = os.getenv("SHODAN_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not shodan_enabled:
        return {
            "source": SOURCE_ID,
            "landing_path": "",
            "seed_count": 0,
            "successful_lookups": 0,
            "failed_lookups": 0,
            "skipped": 1,
            "reason": "disabled",
        }

    api_key = os.getenv("SHODAN_API_KEY", "").strip()
    if not api_key:
        return {
            "source": SOURCE_ID,
            "landing_path": "",
            "seed_count": 0,
            "successful_lookups": 0,
            "failed_lookups": 0,
            "skipped": 1,
            "reason": "missing_api_key",
        }

    partition_at = partition_now()
    retrieved_at = utc_now()
    ingest_date = ingest_date_str(partition_at)
    storage = LandingStorage.from_env(base_dir)

    threatfox_relative = (
        Path("semi_structured")
        / "threatfox"
        / f"ingest_date={ingest_date}"
        / "threatfox_iocs.json"
    )
    threatfox_ips: list[str] = []
    if storage.exists(threatfox_relative):
        threatfox_payload = storage.read_json(threatfox_relative)
        threatfox_ips = _extract_ips_from_threatfox(threatfox_payload)

    env_ips = _extract_ips_from_env()
    combined_candidates = threatfox_ips + env_ips
    unique_candidates = list(dict.fromkeys(combined_candidates))
    selected_ips = unique_candidates[:max_indicators_per_run]
    if not selected_ips:
        return {
            "source": SOURCE_ID,
            "landing_path": "",
            "seed_count": 0,
            "successful_lookups": 0,
            "failed_lookups": 0,
            "skipped": 1,
            "reason": "no_seed_indicators",
        }

    relative_dir = Path("semi_structured") / "shodan_seeded" / f"ingest_date={ingest_date}"
    storage.clear_prefix(relative_dir)
    output_relative = relative_dir / "shodan_hosts.jsonl"

    lines: list[str] = []
    delta_records: list[dict[str, Any]] = []
    failed_requests: list[dict[str, Any]] = []
    success_count = 0

    for index, ip_value in enumerate(selected_ips):
        request_url = build_url(f"{SOURCE_URL}/{ip_value}", params={"key": api_key})
        try:
            payload = fetch_bytes(
                request_url,
                timeout_seconds=timeout_seconds,
                retries=retries,
            )
        except Exception as exc:
            status_code = _extract_http_status(exc)
            if status_code in {401, 403}:
                return {
                    "source": SOURCE_ID,
                    "landing_path": "",
                    "seed_count": len(selected_ips),
                    "successful_lookups": 0,
                    "failed_lookups": 0,
                    "skipped": 1,
                    "reason": "authorization_failure",
                }
            failed_requests.append(
                {
                    "ip": ip_value,
                    "request_url": request_url,
                    "status_code": status_code,
                    "error": str(exc),
                }
            )
        else:
            try:
                parsed = json.loads(payload.decode("utf-8"))
            except json.JSONDecodeError as exc:
                failed_requests.append(
                    {
                        "ip": ip_value,
                        "request_url": request_url,
                        "status_code": None,
                        "error": f"Invalid JSON response: {exc}",
                    }
                )
            else:
                retrieved_at_row = utc_now().isoformat()
                row = {
                    "source_id": SOURCE_ID,
                    "ip": ip_value,
                    "request_url": request_url,
                    "retrieved_at_utc": retrieved_at_row,
                    "payload": parsed,
                }
                lines.append(json.dumps(row, sort_keys=True))
                delta_records.append(
                    DeltaLakeStorage.flatten_record({
                        "ip_str": ip_value,
                        "source_id": SOURCE_ID,
                        "retrieved_at_utc": retrieved_at_row,
                        "ingest_date": ingest_date,
                        "payload": parsed,
                    })
                )
                success_count += 1

        if request_sleep_seconds > 0 and index < len(selected_ips) - 1:
            time.sleep(request_sleep_seconds)

    output_payload = b""
    if lines:
        output_payload = ("\n".join(lines) + "\n").encode("utf-8")
    raw_written = storage.write_bytes(output_relative, output_payload)

    metadata = {
        "source_id": SOURCE_ID,
        "source_url": SOURCE_URL,
        "retrieved_at_utc": retrieved_at.isoformat(),
        "landing_path": raw_written.landing_path,
        "relative_landing_path": raw_written.relative_path,
        "sha256": raw_written.sha256,
        "size_bytes": raw_written.size_bytes,
        "selected_indicators": len(selected_ips),
        "selected_ips": selected_ips,
        "threatfox_seed_count": len(threatfox_ips),
        "env_seed_count": len(env_ips),
        "successful_lookups": success_count,
        "failed_lookups": len(failed_requests),
        "failed_requests": failed_requests,
    }
    meta_relative = output_relative.with_suffix(".jsonl.meta.json")
    storage.write_json(meta_relative, metadata)
    storage.append_manifest_entry(
        source_id=SOURCE_ID,
        ingest_date=ingest_date,
        entry=metadata,
    )

    if delta_records:
        DeltaLakeStorage.from_env().write_or_merge(
            "shodan_seeded",
            delta_records,
            merge_keys=["ip_str"],
            partition_by=["ingest_date"],
        )

    return {
        "source": SOURCE_ID,
        "landing_path": raw_written.landing_path,
        "seed_count": len(selected_ips),
        "successful_lookups": success_count,
        "failed_lookups": len(failed_requests),
    }


def main() -> int:
    args = build_parser().parse_args()
    result = run(
        base_dir=args.base_dir,
        max_indicators_per_run=args.max_indicators_per_run,
        request_sleep_seconds=args.request_sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
    )
    if result.get("skipped"):
        print(f"[{result['source']}] Skipped ({result['reason']})")
        return 0
    print(
        f"[{result['source']}] Queried {result['seed_count']} seeded IP(s), "
        f"success={result['successful_lookups']}, failed={result['failed_lookups']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
