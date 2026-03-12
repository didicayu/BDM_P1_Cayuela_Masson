"""Landing-zone helper functions shared by ingestion scripts."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def utc_timestamp_str(now: dt.datetime | None = None) -> str:
    value = now or utc_now()
    return value.strftime("%Y%m%dT%H%M%SZ")


def ingest_date_str(now: dt.datetime | None = None) -> str:
    value = now or utc_now()
    return value.strftime("%Y-%m-%d")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def write_bytes(path: Path, payload: bytes) -> None:
    ensure_dir(path.parent)
    path.write_bytes(payload)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, sort_keys=True))
        file.write("\n")


def write_manifest_entry(
    *,
    base_dir: Path,
    source_id: str,
    ingest_date: str,
    entry: dict[str, Any],
) -> Path:
    manifest_path = (
        base_dir
        / "landing"
        / "metadata"
        / "manifests"
        / f"ingest_date={ingest_date}"
        / f"{source_id}.jsonl"
    )
    append_jsonl(manifest_path, entry)
    return manifest_path
