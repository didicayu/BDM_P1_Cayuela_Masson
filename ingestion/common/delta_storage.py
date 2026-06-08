"""Delta Lake storage abstraction for lakehouse zones.

Architecture (mirrors deltalake_demo):
  Bronze  s3://landing/       raw immutable JSON/CSV/PCAP  (LandingStorage)
  Silver  s3://deltalake/     typed Delta tables            (DeltaLakeStorage)
  Trusted s3://trusted/       cleaned Delta tables
  Gold    s3://exploitation/  analyst-ready Delta assets
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    from botocore.client import Config
    from botocore.exceptions import ClientError
    import boto3
except ModuleNotFoundError:  # pragma: no cover
    Config = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment,misc]
    boto3 = None  # type: ignore[assignment]


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


class DeltaLakeStorage:
    """Write/merge records into Delta tables backed by MinIO or local filesystem."""

    def __init__(
        self,
        backend: str,
        bucket: str,
        local_root: Path,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        secure: bool,
    ) -> None:
        self.backend = backend
        self.bucket = bucket
        self.local_root = local_root
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure

    @classmethod
    def from_env(cls) -> "DeltaLakeStorage":
        backend = os.getenv("LANDING_BACKEND", "minio").strip().lower()
        bucket = os.getenv("DELTA_BUCKET", "deltalake")
        local_root_str = os.getenv("DELTA_LOCAL_ROOT", "").strip()
        local_root = Path(local_root_str) if local_root_str else Path("data/deltalake")
        endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        secure = _bool_env("MINIO_SECURE", False)
        return cls(
            backend=backend,
            bucket=bucket,
            local_root=local_root,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    @classmethod
    def from_env_bucket(
        cls,
        bucket_env: str,
        default_bucket: str,
        *,
        local_root_env: str | None = None,
    ) -> "DeltaLakeStorage":
        """Build storage for a named lakehouse zone.

        The original ``from_env`` method remains the P1 silver/landing Delta
        contract. P2 adds separate trusted and exploitation buckets while
        keeping the same MinIO credentials and local-mode behavior.
        """
        backend = os.getenv("LANDING_BACKEND", "minio").strip().lower()
        bucket = os.getenv(bucket_env, default_bucket).strip() or default_bucket
        local_override = os.getenv(local_root_env or "", "").strip() if local_root_env else ""
        if local_override:
            local_root = Path(local_override)
        elif backend == "local":
            local_root = Path("data") / bucket
        else:
            local_root = Path("data") / bucket
        endpoint_url = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
        secure = _bool_env("MINIO_SECURE", False)
        return cls(
            backend=backend,
            bucket=bucket,
            local_root=local_root,
            endpoint_url=endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    # ── Storage helpers ────────────────────────────────────────────────────────

    def _storage_options(self) -> dict[str, str]:
        """Return delta-rs storage options for MinIO (mirrors deltalake_demo notebook 02)."""
        return {
            "endpoint_url": self.endpoint_url,
            "access_key_id": self.access_key,
            "secret_access_key": self.secret_key,
            "region": "us-east-1",
            "allow_http": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # required for MinIO
            "AWS_ALLOW_HTTP": "true",
            "conditional_put": "etag",
        }

    def _table_uri(self, table_name: str) -> str:
        if self.backend == "local":
            return str((self.local_root / table_name).resolve())
        return f"s3://{self.bucket}/{table_name}"

    def table_uri(self, table_name: str, *, spark: bool = False) -> str:
        """Return the table URI, optionally converted for Spark's S3A connector."""
        uri = self._table_uri(table_name)
        if spark and uri.startswith("s3://"):
            return f"s3a://{uri.removeprefix('s3://')}"
        return uri

    def _get_boto_client(self):
        if boto3 is None or Config is None:
            raise RuntimeError("boto3 and botocore are required for the MinIO backend.")
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
            use_ssl=self.secure,
        )

    def ensure_bucket(self) -> None:
        """Create the Delta Lake bucket in MinIO if it does not already exist."""
        if self.backend != "minio":
            self.local_root.mkdir(parents=True, exist_ok=True)
            return
        client = self._get_boto_client()
        try:
            client.head_bucket(Bucket=self.bucket)
        except ClientError:
            client.create_bucket(Bucket=self.bucket)

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def flatten_record(record: dict[str, Any]) -> dict[str, Any]:
        """Serialize nested list/dict values to JSON strings.

        PyArrow infers a flat schema from the first batch, so nested types that
        vary between runs (e.g. ``tags`` as ``null`` vs ``list``) cause schema
        conflicts.  Converting them to strings keeps the schema stable.
        """
        import json as _json

        out: dict[str, Any] = {}
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                out[k] = _json.dumps(v, sort_keys=True)
            else:
                out[k] = v
        return out

    @staticmethod
    def _records_for_delta(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return records that Delta Lake can infer without Null-only columns."""
        if not records:
            return records
        columns = {key for record in records for key in record}
        null_only = {
            column
            for column in columns
            if all(record.get(column) is None for record in records)
        }
        if not null_only:
            return records
        normalized: list[dict[str, Any]] = []
        for record in records:
            out = dict(record)
            for column in null_only:
                out[column] = ""
            normalized.append(out)
        return normalized

    @staticmethod
    def _deduplicate_merge_records(
        records: list[dict[str, Any]],
        merge_keys: list[str] | None,
    ) -> list[dict[str, Any]]:
        """Keep the last source row for each merge key to satisfy Delta MERGE semantics."""
        if not merge_keys:
            return records
        unique: dict[tuple[Any, ...], dict[str, Any]] = {}
        for record in records:
            unique[tuple(record.get(key) for key in merge_keys)] = record
        return list(unique.values())

    # ── Core write/merge ───────────────────────────────────────────────────────

    def write_or_merge(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        merge_keys: list[str] | None = None,
        partition_by: list[str] | None = None,
    ) -> int:
        """Write *records* to a Delta table, merging on *merge_keys* if the table exists.

        Parameters
        ----------
        table_name:
            Logical table name, e.g. ``"kev"``.  Resolved to a URI automatically.
        records:
            List of flat dicts (one per row).  Must be non-empty.
        merge_keys:
            Column names that uniquely identify a row.  When provided and the table
            already exists, a MERGE (upsert) is performed — matched rows are updated,
            new rows are inserted.  Passing ``None`` does an append-only write (used
            for stream events with no natural primary key).
        partition_by:
            Hive-partition columns written as directory prefixes.

        Returns
        -------
        int
            Number of records in the batch (0 if *records* is empty).
        """
        if not records:
            return 0

        try:
            import pyarrow as pa
            from deltalake import DeltaTable, write_deltalake
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "deltalake and pyarrow are required for the Delta Lake backend. "
                "Install them with: pip install deltalake pyarrow"
            ) from exc

        uri = self._table_uri(table_name)
        opts: dict[str, str] | None = self._storage_options() if self.backend != "local" else None

        records = self._deduplicate_merge_records(records, merge_keys)
        table = pa.Table.from_pylist(self._records_for_delta(records))

        table_exists = DeltaTable.is_deltatable(uri, storage_options=opts)

        if merge_keys and table_exists:
            dt = DeltaTable(uri, storage_options=opts)
            predicate = " AND ".join(f"s.{k} = t.{k}" for k in merge_keys)
            try:
                (
                    dt.merge(
                        source=table,
                        predicate=predicate,
                        source_alias="s",
                        target_alias="t",
                    )
                    .when_matched_update_all()
                    .when_not_matched_insert_all()
                    .execute()
                )
            except Exception as exc:
                if "MERGE matched a target row with multiple source rows" not in str(exc):
                    raise
                repaired = self._deduplicate_merge_records(
                    dt.to_pyarrow_table().to_pylist() + records,
                    merge_keys,
                )
                repaired_table = pa.Table.from_pylist(self._records_for_delta(repaired))
                write_deltalake(
                    uri,
                    repaired_table,
                    mode="overwrite",
                    partition_by=partition_by,
                    storage_options=opts,
                    schema_mode="overwrite",
                )
        elif merge_keys is None and table_exists:
            # Append-only (stream events)
            write_deltalake(
                uri,
                table,
                mode="append",
                partition_by=partition_by,
                storage_options=opts,
            )
        else:
            # First write — create the table
            write_deltalake(
                uri,
                table,
                mode="overwrite",
                partition_by=partition_by,
                storage_options=opts,
            )

        return len(records)

    def overwrite(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        partition_by: list[str] | None = None,
    ) -> int:
        """Overwrite a Delta table with *records*.

        P2 zone materializations are checkpoint tables. Overwrite semantics make
        reruns deterministic and keep follow-up deliverables idempotent.
        """
        try:
            import pyarrow as pa
            from deltalake import write_deltalake
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "deltalake and pyarrow are required for the Delta Lake backend. "
                "Install them with: pip install deltalake pyarrow"
            ) from exc

        if not records:
            self.ensure_bucket()
            return 0

        self.ensure_bucket()
        uri = self._table_uri(table_name)
        opts: dict[str, str] | None = self._storage_options() if self.backend != "local" else None
        table = pa.Table.from_pylist(self._records_for_delta(records))
        write_deltalake(
            uri,
            table,
            mode="overwrite",
            partition_by=partition_by,
            storage_options=opts,
            schema_mode="overwrite",
        )
        return len(records)

    def read_records(self, table_name: str) -> list[dict[str, Any]]:
        """Read a Delta table as Python dictionaries.

        Missing tables return an empty list so downstream P2 DAGs can run
        against partial P1 data and still prove the zone wiring.
        """
        try:
            from deltalake import DeltaTable
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "deltalake and pyarrow are required for the Delta Lake backend. "
                "Install them with: pip install deltalake pyarrow"
            ) from exc

        uri = self._table_uri(table_name)
        opts: dict[str, str] | None = self._storage_options() if self.backend != "local" else None
        if not DeltaTable.is_deltatable(uri, storage_options=opts):
            return []
        table = DeltaTable(uri, storage_options=opts).to_pyarrow_table()
        return table.to_pylist()
