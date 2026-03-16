"""Storage abstraction for landing writes (local filesystem or MinIO/S3)."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from botocore.client import Config
    from botocore.exceptions import ClientError
    import boto3
except ModuleNotFoundError:  # pragma: no cover - optional for local-only usage
    Config = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment,misc]
    boto3 = None  # type: ignore[assignment]

from ingestion.common.landing_utils import ensure_dir, sha256_bytes, sha256_file


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass
class StorageWriteResult:
    relative_path: str
    landing_path: str
    size_bytes: int
    sha256: str


class LandingStorage:
    def __init__(self, backend: str, base_dir: Path):
        self.backend = backend
        self.base_dir = base_dir
        self.local_root = base_dir / "landing"
        self.key_prefix = os.getenv("MINIO_KEY_PREFIX", "").strip("/")

        self.bucket_name = os.getenv("MINIO_BUCKET", "landing")
        self.endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
        self.access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        self.secret_key = os.getenv(
            "MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        )
        self.secure = _bool_env("MINIO_SECURE", False)

        self._client = None

    @classmethod
    def from_env(cls, base_dir: Path) -> "LandingStorage":
        backend = os.getenv("LANDING_BACKEND", "minio").strip().lower()
        if backend not in {"minio", "local"}:
            raise ValueError(f"Unsupported LANDING_BACKEND='{backend}'. Use 'minio' or 'local'.")
        return cls(backend=backend, base_dir=base_dir)

    def _s3_key(self, relative_path: Path | str) -> str:
        relative = Path(relative_path).as_posix().lstrip("/")
        if self.key_prefix:
            return f"{self.key_prefix}/{relative}"
        return relative

    def _s3_uri(self, key: str) -> str:
        return f"s3://{self.bucket_name}/{key}"

    def _get_client(self):
        if boto3 is None or Config is None:
            raise RuntimeError("boto3 and botocore are required for the MinIO backend.")
        if self._client is None:
            self._client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(signature_version="s3v4"),
                region_name="us-east-1",
                use_ssl=self.secure,
            )
        return self._client

    def ensure_bucket(self) -> None:
        if self.backend != "minio":
            return
        client = self._get_client()
        try:
            client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            client.create_bucket(Bucket=self.bucket_name)

    def ensure_layout(self, prefixes: list[str]) -> list[str]:
        if self.backend == "local":
            created: list[str] = []
            for prefix in prefixes:
                path = self.local_root / prefix
                ensure_dir(path)
                created.append(str(path))
            return created

        self.ensure_bucket()
        created = []
        for prefix in prefixes:
            key = self._s3_key(prefix.rstrip("/") + "/.keep")
            body = b""
            self._get_client().put_object(Bucket=self.bucket_name, Key=key, Body=body)
            created.append(self._s3_uri(key))
        return created

    def write_bytes(self, relative_path: Path | str, payload: bytes) -> StorageWriteResult:
        relative = Path(relative_path)
        if self.backend == "local":
            absolute = self.local_root / relative
            ensure_dir(absolute.parent)
            absolute.write_bytes(payload)
            return StorageWriteResult(
                relative_path=relative.as_posix(),
                landing_path=str(absolute),
                size_bytes=len(payload),
                sha256=sha256_bytes(payload),
            )

        self.ensure_bucket()
        key = self._s3_key(relative)
        self._get_client().put_object(Bucket=self.bucket_name, Key=key, Body=payload)
        return StorageWriteResult(
            relative_path=relative.as_posix(),
            landing_path=self._s3_uri(key),
            size_bytes=len(payload),
            sha256=sha256_bytes(payload),
        )

    def write_json(self, relative_path: Path | str, payload: dict[str, Any]) -> StorageWriteResult:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        return self.write_bytes(relative_path, body)

    def write_file(self, relative_path: Path | str, source_file: Path) -> StorageWriteResult:
        relative = Path(relative_path)
        if self.backend == "local":
            absolute = self.local_root / relative
            ensure_dir(absolute.parent)
            shutil.copy2(source_file, absolute)
            return StorageWriteResult(
                relative_path=relative.as_posix(),
                landing_path=str(absolute),
                size_bytes=absolute.stat().st_size,
                sha256=sha256_file(absolute),
            )

        self.ensure_bucket()
        key = self._s3_key(relative)
        self._get_client().upload_file(str(source_file), self.bucket_name, key)
        return StorageWriteResult(
            relative_path=relative.as_posix(),
            landing_path=self._s3_uri(key),
            size_bytes=source_file.stat().st_size,
            sha256=sha256_file(source_file),
        )

    def exists(self, relative_path: Path | str) -> bool:
        relative = Path(relative_path)
        if self.backend == "local":
            return (self.local_root / relative).exists()

        self.ensure_bucket()
        key = self._s3_key(relative)
        try:
            self._get_client().head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in {"NoSuchKey", "404", "NotFound"}:
                return False
            raise

    def read_bytes(self, relative_path: Path | str) -> bytes:
        relative = Path(relative_path)
        if self.backend == "local":
            return (self.local_root / relative).read_bytes()

        self.ensure_bucket()
        key = self._s3_key(relative)
        response = self._get_client().get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read()

    def read_json(self, relative_path: Path | str) -> dict[str, Any]:
        payload = self.read_bytes(relative_path)
        parsed = json.loads(payload.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError(f"Expected JSON object in {relative_path}")
        return parsed

    @staticmethod
    def _manifest_identity(entry: dict[str, Any]) -> str:
        relative = str(entry.get("relative_landing_path", "")).strip()
        if relative:
            return f"relative::{relative}"
        landing = str(entry.get("landing_path", "")).strip()
        if landing:
            return f"landing::{landing}"
        return "content::" + sha256_bytes(json.dumps(entry, sort_keys=True).encode("utf-8"))

    @staticmethod
    def _parse_manifest(payload: bytes) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        if not payload:
            return entries
        text = payload.decode("utf-8", errors="replace")
        for line in text.splitlines():
            clean = line.strip()
            if not clean:
                continue
            try:
                value = json.loads(clean)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                entries.append(value)
        return entries

    @staticmethod
    def _serialize_manifest(entries: list[dict[str, Any]]) -> bytes:
        if not entries:
            return b""
        lines = [json.dumps(entry, sort_keys=True) for entry in entries]
        return ("\n".join(lines) + "\n").encode("utf-8")

    def append_manifest_entry(
        self,
        *,
        source_id: str,
        ingest_date: str,
        entry: dict[str, Any],
    ) -> str:
        relative = (
            Path("metadata")
            / "manifests"
            / f"ingest_date={ingest_date}"
            / f"{source_id}.jsonl"
        )
        identity = self._manifest_identity(entry)

        if self.backend == "local":
            manifest_path = self.local_root / relative
            ensure_dir(manifest_path.parent)
            current = b""
            if manifest_path.exists():
                current = manifest_path.read_bytes()
            entries = self._parse_manifest(current)
            index_by_identity = {
                self._manifest_identity(existing): idx
                for idx, existing in enumerate(entries)
            }
            if identity in index_by_identity:
                entries[index_by_identity[identity]] = entry
            else:
                entries.append(entry)
            manifest_path.write_bytes(self._serialize_manifest(entries))
            return str(manifest_path)

        self.ensure_bucket()
        key = self._s3_key(relative)
        client = self._get_client()
        current = b""
        try:
            response = client.get_object(Bucket=self.bucket_name, Key=key)
            current = response["Body"].read()
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"NoSuchKey", "404"}:
                raise
        entries = self._parse_manifest(current)
        index_by_identity = {
            self._manifest_identity(existing): idx
            for idx, existing in enumerate(entries)
        }
        if identity in index_by_identity:
            entries[index_by_identity[identity]] = entry
        else:
            entries.append(entry)
        body = self._serialize_manifest(entries)
        client.put_object(Bucket=self.bucket_name, Key=key, Body=body)
        return self._s3_uri(key)

    def clear_prefix(self, relative_prefix: Path | str) -> int:
        prefix = Path(relative_prefix).as_posix().strip("/")

        if self.backend == "local":
            target_dir = self.local_root / prefix
            removed = 0
            if target_dir.exists():
                for path in sorted(target_dir.rglob("*"), reverse=True):
                    if path.is_file():
                        path.unlink()
                        removed += 1
                    elif path.is_dir():
                        try:
                            path.rmdir()
                        except OSError:
                            pass
            return removed

        self.ensure_bucket()
        client = self._get_client()
        s3_prefix = self._s3_key(prefix + "/")
        token = None
        removed = 0
        while True:
            kwargs = {"Bucket": self.bucket_name, "Prefix": s3_prefix}
            if token:
                kwargs["ContinuationToken"] = token
            response = client.list_objects_v2(**kwargs)
            contents = response.get("Contents", [])
            for obj in contents:
                client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])
                removed += 1
            if not response.get("IsTruncated"):
                break
            token = response.get("NextContinuationToken")
        return removed
