"""Import downloaded datasets into landing zone.

This script covers non-API sources (e.g., CIC-IDS2017 CSV files, CTU-13 PCAP files).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import zipfile

from ingestion.common.landing_utils import (
    ingest_date_str,
    utc_now,
)
from ingestion.common.storage import LandingStorage, StorageWriteResult

DATASET_TARGETS = {
    "cic_ids2017_csv": {
        "prefix": "structured/cic_ids2017",
        "extensions": {".csv", ".zip"},
    },
    "ctu13_pcap": {
        "prefix": "unstructured/pcap/source=ctu13",
        "extensions": {".pcap", ".pcapng"},
    },
    "cic_ids2017_pcap": {
        "prefix": "unstructured/pcap/source=cic_ids2017",
        "extensions": {".pcap", ".pcapng"},
    },
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Import local dataset files into landing zone.")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=sorted(DATASET_TARGETS),
        help="Dataset target definition.",
    )
    parser.add_argument(
        "--source-path",
        type=Path,
        required=True,
        help="Input file or directory with downloaded dataset files.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory.",
    )
    parser.add_argument(
        "--copy-mode",
        choices=["copy", "move"],
        default="copy",
        help="Use copy (safe) or move.",
    )
    return parser


def collect_files(source_path: Path, allowed_extensions: set[str]) -> list[Path]:
    if source_path.is_file():
        return [source_path]
    files = [path for path in source_path.rglob("*") if path.is_file()]
    filtered = [path for path in files if path.suffix.lower() in allowed_extensions]
    return sorted(filtered)


def write_file_metadata(
    *,
    storage: LandingStorage,
    dataset: str,
    source_file: Path,
    written: StorageWriteResult,
    ingest_date: str,
    archive_member: str | None = None,
) -> None:
    metadata = {
        "source_id": dataset,
        "source_file": str(source_file),
        "ingested_file": written.landing_path,
        "relative_landing_path": written.relative_path,
        "size_bytes": written.size_bytes,
        "sha256": written.sha256,
    }
    if archive_member is not None:
        metadata["archive_member"] = archive_member
    meta_relative = Path(written.relative_path).with_suffix(
        Path(written.relative_path).suffix + ".meta.json"
    )
    storage.write_json(meta_relative, metadata)
    storage.append_manifest_entry(
        source_id=dataset,
        ingest_date=ingest_date,
        entry=metadata,
    )


def import_zip_csv(
    *,
    storage: LandingStorage,
    source_file: Path,
    destination_prefix: Path,
    dataset: str,
    ingest_date: str,
) -> int:
    imported = 0
    with zipfile.ZipFile(source_file, "r") as archive:
        members = [
            member
            for member in archive.infolist()
            if not member.is_dir() and Path(member.filename).suffix.lower() == ".csv"
        ]
        for member in members:
            member_name = Path(member.filename).name
            destination_name = f"{source_file.stem}__{member_name}"
            destination_relative = destination_prefix / destination_name
            with archive.open(member, "r") as src:
                payload = src.read()
            written = storage.write_bytes(destination_relative, payload)
            write_file_metadata(
                storage=storage,
                dataset=dataset,
                source_file=source_file,
                written=written,
                ingest_date=ingest_date,
                archive_member=member.filename,
            )
            imported += 1
    return imported


def run(dataset: str, source_path: Path, base_dir: Path, copy_mode: str) -> dict[str, int]:
    target = DATASET_TARGETS[dataset]
    ingest_date = ingest_date_str(utc_now())
    storage = LandingStorage.from_env(base_dir)
    destination_prefix = Path(target["prefix"]) / f"ingest_date={ingest_date}"
    storage.clear_prefix(destination_prefix)

    files = collect_files(source_path, target["extensions"])
    if not files:
        raise FileNotFoundError(
            f"No files found in {source_path} with extensions {sorted(target['extensions'])}"
        )

    imported = 0
    for source_file in files:
        suffix = source_file.suffix.lower()
        if dataset == "cic_ids2017_csv" and suffix == ".zip":
            imported += import_zip_csv(
                storage=storage,
                source_file=source_file,
                destination_prefix=destination_prefix,
                dataset=dataset,
                ingest_date=ingest_date,
            )
            if copy_mode == "move":
                source_file.unlink(missing_ok=True)
            continue

        destination_relative = destination_prefix / source_file.name
        written = storage.write_file(destination_relative, source_file)
        write_file_metadata(
            storage=storage,
            dataset=dataset,
            source_file=source_file,
            written=written,
            ingest_date=ingest_date,
        )
        if copy_mode == "move":
            source_file.unlink(missing_ok=True)
        imported += 1

    return {"imported_files": imported}


def main() -> int:
    args = build_parser().parse_args()
    result = run(args.dataset, args.source_path, args.base_dir, args.copy_mode)
    print(f"Imported {result['imported_files']} file(s) for dataset '{args.dataset}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
