from __future__ import annotations

import unittest

from trusted.cleaning import clean_table


class TrustedZoneCleaningTests(unittest.TestCase):
    def test_kev_cleaning_rejects_missing_and_duplicates(self) -> None:
        cleaned, rejected = clean_table(
            "kev",
            [
                {"cveID": "cve-2026-0001", "vulnerabilityName": "Issue", "dateAdded": "2026-05-01"},
                {"cveID": "CVE-2026-0001", "vulnerabilityName": "Issue duplicate"},
                {"cveID": "", "vulnerabilityName": "No CVE"},
            ],
        )

        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["cve_id"], "CVE-2026-0001")
        self.assertEqual(len(rejected), 2)
        self.assertIn("duplicate", rejected[0]["reject_reason"])
        self.assertIn("missing mandatory", rejected[1]["reject_reason"])

    def test_epss_cleaning_flags_bad_scores_without_rejecting(self) -> None:
        cleaned, rejected = clean_table(
            "epss",
            [{"cve": "CVE-2026-0002", "date": "2026-05-01", "epss": "1.25", "percentile": "bad"}],
        )

        self.assertEqual(len(cleaned), 1)
        self.assertEqual(rejected, [])
        self.assertTrue(cleaned[0]["_quality_warn"])
        self.assertIn("epss outside [0,1]", cleaned[0]["_quality_reasons"])
        self.assertIn("invalid float: percentile", cleaned[0]["_quality_reasons"])

    def test_suricata_compact_utc_offset_preserves_microseconds_for_deduplication(self) -> None:
        cleaned, rejected = clean_table(
            "suricata_events",
            [
                {
                    "event_type": "dns",
                    "flow_id": "783695327277305",
                    "timestamp_utc": "2011-08-10T09:04:24.405753+0000",
                },
                {
                    "event_type": "dns",
                    "flow_id": "783695327277305",
                    "timestamp_utc": "2011-08-10T09:04:24.405759+0000",
                },
            ],
        )

        self.assertEqual(rejected, [])
        self.assertEqual(len(cleaned), 2)
        self.assertEqual(cleaned[0]["timestamp_utc"], "2011-08-10T09:04:24.405753+00:00")
        self.assertEqual(cleaned[1]["timestamp_utc"], "2011-08-10T09:04:24.405759+00:00")


if __name__ == "__main__":
    unittest.main()
