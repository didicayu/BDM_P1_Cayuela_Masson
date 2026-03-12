"""Batch enrichment ingestion for CIRCL Vulnerability-Lookup API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import time
from typing import Any

from ingestion.common.http_utils import fetch_bytes
from ingestion.common.landing_utils import ingest_date_str, utc_now
from ingestion.common.storage import LandingStorage

SOURCE_ID = "circl_vulnlookup_api"
SOURCE_URL = "https://vulnerability.circl.lu/api/cve"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest CIRCL CVE details from EPSS-ranked CVEs.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--top-n-cves",
        type=int,
        default=200,
        help="Top EPSS CVEs to request from CIRCL (default: 200).",
    )
    parser.add_argument(
        "--epss-max-pages",
        type=int,
        default=10,
        help="Max EPSS pages to scan for candidates (default: 10).",
    )
    parser.add_argument(
        "--request-sleep-seconds",
        type=float,
        default=0.2,
        help="Sleep between CIRCL requests (default: 0.2).",
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
        help="HTTP retries per CVE.",
    )
    return parser


def _extract_http_status(exc: Exception) -> int | None:
    match = re.search(r"HTTP Error (\d+)", str(exc))
    if match:
        return int(match.group(1))
    return None


def _epss_score(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _load_top_epss_cves(
    *,
    storage: LandingStorage,
    ingest_date: str,
    epss_max_pages: int,
    top_n_cves: int,
) -> list[str]:
    scores: dict[str, float] = {}

    for page in range(epss_max_pages):
        epss_relative = (
            Path("structured")
            / "epss"
            / f"ingest_date={ingest_date}"
            / f"epss_page={page:03d}.json"
        )
        if not storage.exists(epss_relative):
            continue

        payload = storage.read_json(epss_relative)
        records = payload.get("data", [])
        if not isinstance(records, list):
            continue

        for record in records:
            if not isinstance(record, dict):
                continue
            cve = record.get("cve")
            if not isinstance(cve, str) or not cve.startswith("CVE-"):
                continue
            score = _epss_score(record.get("epss"))
            previous = scores.get(cve)
            if previous is None or score > previous:
                scores[cve] = score

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return [cve for cve, _ in ranked[:top_n_cves]]


def run(
    base_dir: Path,
    top_n_cves: int,
    epss_max_pages: int,
    request_sleep_seconds: float,
    timeout_seconds: int,
    retries: int,
) -> dict[str, str | int]:
    now = utc_now()
    ingest_date = ingest_date_str(now)
    storage = LandingStorage.from_env(base_dir)

    selected_cves = _load_top_epss_cves(
        storage=storage,
        ingest_date=ingest_date,
        epss_max_pages=epss_max_pages,
        top_n_cves=top_n_cves,
    )
    if not selected_cves:
        raise RuntimeError(
            "No EPSS CVE candidates found for current ingest_date. Run EPSS ingestion first."
        )

    relative_dir = Path("semi_structured") / "circl_vulnlookup" / f"ingest_date={ingest_date}"
    storage.clear_prefix(relative_dir)
    output_relative = relative_dir / "circl_cve_details.jsonl"

    lines: list[str] = []
    failed_requests: list[dict[str, Any]] = []
    success_count = 0

    for index, cve_id in enumerate(selected_cves):
        request_url = f"{SOURCE_URL}/{cve_id}"
        try:
            payload = fetch_bytes(
                request_url,
                timeout_seconds=timeout_seconds,
                retries=retries,
            )
        except Exception as exc:
            status_code = _extract_http_status(exc)
            if index == 0 and (status_code in {401, 403} or (status_code and status_code >= 500)):
                raise RuntimeError(
                    f"CIRCL API unavailable/auth issue at startup for {cve_id}: {exc}"
                ) from exc
            failed_requests.append(
                {
                    "cve_id": cve_id,
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
                        "cve_id": cve_id,
                        "request_url": request_url,
                        "status_code": None,
                        "error": f"Invalid JSON response: {exc}",
                    }
                )
            else:
                row = {
                    "source_id": SOURCE_ID,
                    "cve_id": cve_id,
                    "request_url": request_url,
                    "retrieved_at_utc": utc_now().isoformat(),
                    "payload": parsed,
                }
                lines.append(json.dumps(row, sort_keys=True))
                success_count += 1

        if request_sleep_seconds > 0 and index < len(selected_cves) - 1:
            time.sleep(request_sleep_seconds)

    output_payload = b""
    if lines:
        output_payload = ("\n".join(lines) + "\n").encode("utf-8")
    raw_written = storage.write_bytes(output_relative, output_payload)

    metadata = {
        "source_id": SOURCE_ID,
        "source_url": SOURCE_URL,
        "retrieved_at_utc": now.isoformat(),
        "landing_path": raw_written.landing_path,
        "relative_landing_path": raw_written.relative_path,
        "sha256": raw_written.sha256,
        "size_bytes": raw_written.size_bytes,
        "top_n_cves": top_n_cves,
        "epss_max_pages": epss_max_pages,
        "requested_cves": len(selected_cves),
        "successful_cves": success_count,
        "failed_cves": len(failed_requests),
        "failed_requests": failed_requests,
    }
    meta_relative = output_relative.with_suffix(".jsonl.meta.json")
    storage.write_json(meta_relative, metadata)
    storage.append_manifest_entry(
        source_id=SOURCE_ID,
        ingest_date=ingest_date,
        entry=metadata,
    )

    return {
        "source": SOURCE_ID,
        "landing_path": raw_written.landing_path,
        "requested_cves": len(selected_cves),
        "successful_cves": success_count,
        "failed_cves": len(failed_requests),
    }


def main() -> int:
    args = build_parser().parse_args()
    result = run(
        base_dir=args.base_dir,
        top_n_cves=args.top_n_cves,
        epss_max_pages=args.epss_max_pages,
        request_sleep_seconds=args.request_sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
    )
    print(
        f"[{result['source']}] Ingested {result['successful_cves']} CVE details "
        f"({result['failed_cves']} failed) to {result['landing_path']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
