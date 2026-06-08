from __future__ import annotations

import unittest

from exploitation.analytics import build_kpis
from ingestion.stream.warm_aggregates import compute_warm_stream_aggregates, materialize_warm_stream_aggregates
from trusted.cleaning import clean_table


class WarmStreamAggregateTests(unittest.TestCase):
    def test_materialization_rejects_unknown_source_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "auto, kafka, delta"):
            materialize_warm_stream_aggregates(source="unknown")

    def test_compute_warm_stream_aggregates_outputs_expected_products(self) -> None:
        rows = compute_warm_stream_aggregates(
            [
                {
                    "timestamp_utc": "2026-05-09T10:00:10+00:00",
                    "src_ip": "10.0.0.1",
                    "dst_ip": "10.0.0.2",
                    "signature": "CVE-2026-0001 exploit attempt",
                    "severity": 4,
                },
                {
                    "timestamp_utc": "2026-05-09T10:00:45+00:00",
                    "src_ip": "10.0.0.1",
                    "dst_ip": "10.0.0.3",
                    "signature": "Suspicious scan",
                    "severity": 2,
                },
            ],
            top_n=3,
        )

        aggregate_types = {row["aggregate_type"] for row in rows}
        self.assertIn("alerts_per_minute", aggregate_types)
        self.assertIn("alerts_by_severity_per_minute", aggregate_types)
        self.assertIn("top_source_ips_per_minute", aggregate_types)
        self.assertIn("top_destination_ips_per_minute", aggregate_types)
        self.assertIn("top_signatures_per_minute", aggregate_types)
        self.assertTrue(any(row["aggregate_key"] == "10.0.0.1" and row["alert_count"] == 2 for row in rows))

    def test_trusted_cleaning_rejects_invalid_warm_aggregate_counts(self) -> None:
        cleaned, rejected = clean_table(
            "warm_stream_aggregates",
            [
                {
                    "window_start": "2026-05-09T10:00:00+00:00",
                    "window_end": "2026-05-09T10:01:00+00:00",
                    "aggregate_type": "alerts_per_minute",
                    "aggregate_key": "all",
                    "alert_count": "2",
                },
                {
                    "window_start": "2026-05-09T10:00:00+00:00",
                    "window_end": "2026-05-09T10:01:00+00:00",
                    "aggregate_type": "alerts_per_minute",
                    "aggregate_key": "",
                    "alert_count": "bad",
                },
            ],
        )

        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["alert_count"], 2)
        self.assertEqual(len(rejected), 1)
        self.assertIn("missing mandatory", rejected[0]["reject_reason"])

    def test_kpis_prefer_warm_aggregate_trends_when_available(self) -> None:
        warm_rows = [
            {
                "window_start": "2026-05-09T10:00:00+00:00",
                "aggregate_type": "alerts_by_severity_per_minute",
                "aggregate_key": "4",
                "alert_count": 7,
            },
            {
                "window_start": "2026-05-09T10:00:00+00:00",
                "aggregate_type": "top_signatures_per_minute",
                "aggregate_key": "Exploit attempt",
                "alert_count": 5,
            },
        ]

        kpis = build_kpis([], [], warm_rows)

        self.assertEqual(kpis["kpi_daily_alert_counts"][0]["alert_count"], 7)
        self.assertEqual(kpis["kpi_daily_alert_counts"][0]["signature"], "warm_stream_aggregate")
        self.assertEqual(kpis["kpi_top_signatures"][0]["signature"], "Exploit attempt")


if __name__ == "__main__":
    unittest.main()
