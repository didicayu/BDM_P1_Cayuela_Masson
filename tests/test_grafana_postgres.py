from __future__ import annotations

import json
from pathlib import Path
import unittest

from consumption.grafana_postgres import SERVING_TABLE_COLUMNS, normalize_rows


class GrafanaPostgresTests(unittest.TestCase):
    def test_serving_tables_cover_dashboard_products(self) -> None:
        expected_tables = {
            "kpi_vuln_priority_top50",
            "kpi_daily_alert_counts",
            "kpi_top_signatures",
            "kpi_top_source_ips",
            "kpi_top_destination_ips",
            "anomaly_flags",
            "ioc_correlations",
            "ml_anomaly_model",
        }

        self.assertTrue(expected_tables.issubset(SERVING_TABLE_COLUMNS))
        self.assertIn("ml_score", SERVING_TABLE_COLUMNS["anomaly_flags"])
        self.assertIn("joblib_artifact_path", SERVING_TABLE_COLUMNS["ml_anomaly_model"])

    def test_normalize_rows_uses_stable_text_columns(self) -> None:
        rows = normalize_rows(
            [{"cve_id": "CVE-2026-0001", "kev_listed": True, "vuln_priority_score": 0.7}],
            ["cve_id", "kev_listed", "vuln_priority_score", "missing"],
        )

        self.assertEqual(rows[0]["kev_listed"], "true")
        self.assertEqual(rows[0]["vuln_priority_score"], "0.7")
        self.assertEqual(rows[0]["missing"], "")

    def test_grafana_dashboard_json_is_valid_and_uses_postgres_datasource(self) -> None:
        dashboard_path = Path("config/grafana/dashboards/cybersecintel_soc.json")
        dashboard = json.loads(dashboard_path.read_text(encoding="utf-8"))
        datasource_uids = {
            target["datasource"]["uid"]
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
        }

        self.assertEqual(dashboard["uid"], "cybersecintel-soc")
        self.assertIn("cybersecintel-postgres", datasource_uids)
        self.assertGreaterEqual(len(dashboard["panels"]), 8)


if __name__ == "__main__":
    unittest.main()
