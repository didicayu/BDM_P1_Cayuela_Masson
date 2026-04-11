"""Generate synthetic IDS events and store them in landing zone.

This is a lightweight P1.2 placeholder for streaming ingestion progress.
"""

from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import json
import random
import time
from pathlib import Path

from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.landing_utils import ingest_date_str, partition_now, utc_now
from ingestion.common.storage import LandingStorage


SIGNATURES = [
    "ET WEB_SERVER Possible SQL Injection Attempt",
    "ET TROJAN Generic CnC Beacon",
    "ET POLICY Suspicious DNS TXT Query",
    "ET SCAN Nmap Scripting Engine User-Agent",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write synthetic IDS events to landing zone.")
    parser.add_argument("--base-dir", type=Path, default=Path("data"))
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--interval-ms", type=int, default=10)
    return parser


def random_ip() -> str:
    block = ipaddress.IPv4Network("10.0.0.0/8")
    host_value = random.randint(int(block.network_address) + 1, int(block.broadcast_address) - 1)
    return str(ipaddress.IPv4Address(host_value))


def generate_event(now: dt.datetime) -> dict[str, str | int]:
    return {
        "timestamp_utc": now.isoformat(),
        "src_ip": random_ip(),
        "dst_ip": random_ip(),
        "src_port": random.randint(1024, 65535),
        "dst_port": random.choice([22, 53, 80, 443, 3389, 8080]),
        "signature": random.choice(SIGNATURES),
        "severity": random.choice([1, 2, 3, 4]),
    }


def run(base_dir: Path, events: int, interval_ms: int) -> Path:
    partition_at = partition_now()
    ingest_date = ingest_date_str(partition_at)
    hour = partition_at.strftime("%H")
    storage = LandingStorage.from_env(base_dir)

    output_relative = (
        Path("stream")
        / "ids_alerts"
        / f"ingest_date={ingest_date}"
        / f"hour={hour}"
        / "ids_synthetic.jsonl"
    )
    storage.clear_prefix(output_relative.parent)

    lines: list[str] = []
    event_dicts: list[dict] = []
    for _ in range(events):
        event_now = utc_now()
        event = generate_event(event_now)
        lines.append(json.dumps(event, sort_keys=True))
        event_dicts.append({**event, "ingest_date": ingest_date})
        if interval_ms > 0:
            time.sleep(interval_ms / 1000.0)

    payload = ("\n".join(lines) + "\n").encode("utf-8")
    result = storage.write_bytes(output_relative, payload)

    if event_dicts:
        DeltaLakeStorage.from_env().write_or_merge(
            "ids_alerts",
            event_dicts,
            merge_keys=None,
            partition_by=["ingest_date"],
        )

    return Path(result.landing_path)


def main() -> int:
    args = build_parser().parse_args()
    output_path = run(args.base_dir, args.events, args.interval_ms)
    print(f"Synthetic stream events written to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
