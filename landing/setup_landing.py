"""Create landing-zone folder conventions for P1.2."""

from __future__ import annotations

import argparse
from pathlib import Path

from ingestion.common.storage import LandingStorage


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create landing zone folder structure.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    return parser


def setup_landing(base_dir: Path) -> list[str]:
    prefixes = [
        "structured/kev",
        "structured/epss",
        "semi_structured/nvd",
        "semi_structured/urlhaus",
        "semi_structured/circl_vulnlookup",
        "semi_structured/threatfox",
        "semi_structured/shodan_seeded",
        "stream/ids_alerts",
        "warm/stream_aggregates",
        "unstructured/pcap/source=ctu13",
        "metadata/manifests",
    ]
    storage = LandingStorage.from_env(base_dir)
    return storage.ensure_layout(prefixes)


def main() -> int:
    args = build_parser().parse_args()
    paths = setup_landing(args.base_dir)
    print("Landing zone structure ready:")
    for path in paths:
        print(f"- {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
