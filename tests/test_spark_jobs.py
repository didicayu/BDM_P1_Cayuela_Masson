from __future__ import annotations

import unittest
from pathlib import Path

from exploitation.spark_jobs.materialize_products import SPARK_KPI_TABLES
from exploitation.spark_jobs.warm_alert_enrichment import extract_cve_from_alert_fields
from ingestion.common.delta_storage import DeltaLakeStorage
from ingestion.common.spark_session import default_spark_packages, spark_base_conf
from trusted.spark_jobs.clean_sources import SPARK_TRUSTED_TABLES


class SparkJobConfigurationTests(unittest.TestCase):
    def test_spark_package_configuration_includes_delta_and_kafka_when_needed(self) -> None:
        packages = default_spark_packages(include_kafka=True)
        conf = spark_base_conf(include_kafka=True)

        self.assertIn("io.delta:delta-spark_2.12", packages)
        self.assertIn("org.apache.hadoop:hadoop-aws", packages)
        self.assertIn("org.apache.spark:spark-sql-kafka-0-10_2.12", packages)
        self.assertEqual(conf["spark.sql.catalog.spark_catalog"], "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.assertIn("spark.hadoop.fs.s3a.endpoint", conf)

    def test_delta_storage_exposes_spark_s3a_uri(self) -> None:
        storage = DeltaLakeStorage(
            backend="minio",
            bucket="exploitation",
            local_root=__import__("pathlib").Path("data/exploitation"),
            endpoint_url="http://minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

        self.assertEqual(storage.table_uri("network_events", spark=True), "s3a://exploitation/network_events")

    def test_warm_streaming_cve_extraction(self) -> None:
        self.assertEqual(
            extract_cve_from_alert_fields("Possible exploit", "CVE-2026-12345", "metadata"),
            "CVE-2026-12345",
        )
        self.assertEqual(extract_cve_from_alert_fields("No CVE here"), "")

    def test_spark_jobs_cover_promised_products(self) -> None:
        self.assertIn("kpi_daily_alert_counts", SPARK_KPI_TABLES)
        self.assertIn("kpi_top_source_ips", SPARK_KPI_TABLES)
        self.assertEqual(SPARK_TRUSTED_TABLES, ("kev", "epss", "nvd"))

    def test_airflow_image_and_dag_require_a_working_spark_runtime(self) -> None:
        dockerfile = Path("orchestration/airflow/Dockerfile").read_text(encoding="utf-8")
        dag = Path("orchestration/airflow/dags/exploitation_zone_dag.py").read_text(encoding="utf-8")

        self.assertIn("openjdk-17-jre-headless", dockerfile)
        self.assertIn("ENV JAVA_HOME=/opt/java", dockerfile)
        self.assertIn("run_spark_warm_path(strict=True)", dag)


if __name__ == "__main__":
    unittest.main()
