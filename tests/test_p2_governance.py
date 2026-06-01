from __future__ import annotations

import unittest

from governance.catalog import data_product_catalog, lineage_record, quality_metric


class GovernanceArtifactTests(unittest.TestCase):
    def test_catalog_contains_required_data_products(self) -> None:
        products = {row["data_product"]: row for row in data_product_catalog()}

        self.assertIn("vuln_enriched", products)
        self.assertIn("network_events", products)
        self.assertIn("ioc_correlations", products)
        self.assertIn("anomaly_flags", products)
        self.assertIn("ml_anomaly_model", products)
        self.assertIn("sklearn_isolation_forest_model", products)
        self.assertIn("model_joblib_artifact", products)
        self.assertIn("warm_stream_aggregates", products)
        self.assertIn("warm_enriched_alerts_spark", products)
        self.assertIn("consumption_outputs", products)
        self.assertIn("grafana_serving_tables", products)
        self.assertEqual(products["vuln_enriched"]["domain"], "Vulnerability Intelligence")
        self.assertIn("quality_rules", products["vuln_enriched"])

    def test_lineage_and_quality_records_are_auditable(self) -> None:
        lineage = lineage_record(
            run_id="test-run",
            dag_id="dag",
            task_id="task",
            transformation_name="transform",
            source_assets=["s3://trusted/a", "s3://trusted/b"],
            target_asset="s3://exploitation/c",
            rows_read=10,
            rows_written=8,
            rows_rejected=2,
        )
        metric = quality_metric(
            run_id="test-run",
            zone="trusted",
            table_name="kev",
            metric_name="rows_rejected",
            metric_value=2,
            status="warn",
        )

        self.assertEqual(lineage["rows_rejected"], 2)
        self.assertIn("s3://trusted/a", lineage["source_assets"])
        self.assertEqual(metric["status"], "warn")
        self.assertEqual(metric["metric_value"], "2")


if __name__ == "__main__":
    unittest.main()
