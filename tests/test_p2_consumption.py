from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from consumption.exports import export_consumption_outputs


class ConsumptionExportTests(unittest.TestCase):
    def test_exports_write_expected_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = export_consumption_outputs(
                output_dir=Path(temp_dir),
                vuln_priority=[{"cve_id": "CVE-2026-0001", "vuln_priority_score": 0.8}],
                ioc_correlations=[],
                anomaly_flags=[{"src_ip": "10.0.0.1", "anomaly_score": 0.9}],
                alert_trends=[{"day": "2026-05-09", "alert_count": 1}],
                top_signatures=[],
                top_source_ips=[],
                top_destination_ips=[{"dst_ip": "10.0.0.2", "event_count": 1}],
                ml_model=[{"trained_rows": 10, "threshold": 0.7}],
            )

            self.assertEqual(result["files_written"], 5)
            self.assertEqual(result["top_destination_rows"], 1)
            self.assertTrue((Path(temp_dir) / "cve_prioritization.csv").exists())
            self.assertTrue((Path(temp_dir) / "ioc_correlations.csv").read_text().startswith("ioc_value,"))
            self.assertTrue((Path(temp_dir) / "alert_trends.json").exists())
            self.assertTrue((Path(temp_dir) / "soc_dashboard.html").exists())


if __name__ == "__main__":
    unittest.main()
