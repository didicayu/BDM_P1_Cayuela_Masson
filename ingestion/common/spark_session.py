"""Spark session helpers for optional P2 Spark jobs."""

from __future__ import annotations

import os
from typing import Any


DEFAULT_SPARK_VERSION = "3.5.1"
DEFAULT_DELTA_VERSION = "3.2.0"
DEFAULT_SCALA_VERSION = "2.12"
DEFAULT_HADOOP_VERSION = "3.3.4"


def default_spark_packages(*, include_kafka: bool = False) -> str:
    packages = [f"io.delta:delta-spark_{DEFAULT_SCALA_VERSION}:{DEFAULT_DELTA_VERSION}"]
    packages.extend(extra_spark_packages(include_kafka=include_kafka))
    return ",".join(packages)


def extra_spark_packages(*, include_kafka: bool = False) -> list[str]:
    packages = [f"org.apache.hadoop:hadoop-aws:{DEFAULT_HADOOP_VERSION}"]
    if include_kafka:
        packages.append(f"org.apache.spark:spark-sql-kafka-0-10_{DEFAULT_SCALA_VERSION}:{DEFAULT_SPARK_VERSION}")
    return packages


def spark_base_conf(*, include_kafka: bool = False) -> dict[str, str]:
    endpoint = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000").replace("http://", "").replace("https://", "")
    packages = os.getenv("SPARK_JARS_PACKAGES", default_spark_packages(include_kafka=include_kafka))
    return {
        "spark.jars.packages": packages,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": endpoint,
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin")),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false" if os.getenv("MINIO_SECURE", "false").lower() != "true" else "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
    }


def build_spark_session(app_name: str, *, include_kafka: bool = False, extra_conf: dict[str, Any] | None = None):
    """Build a SparkSession with Delta and MinIO-compatible S3A settings."""
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ModuleNotFoundError as exc:  # pragma: no cover - optional runtime dependency.
        raise RuntimeError("pyspark and delta-spark are required for Spark execution.") from exc

    master = os.getenv("SPARK_MASTER_URL", "local[*]")
    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in spark_base_conf(include_kafka=include_kafka).items():
        builder = builder.config(key, value)
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, str(value))
    return configure_spark_with_delta_pip(builder, extra_packages=extra_spark_packages(include_kafka=include_kafka)).getOrCreate()
