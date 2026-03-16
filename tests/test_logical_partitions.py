from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ingestion.batch.circl_vulnlookup_ingest import run as run_circl
from ingestion.common.landing_utils import ingest_date_str, partition_now


class LogicalPartitionTests(unittest.TestCase):
    def test_partition_now_prefers_airflow_logical_date(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "AIRFLOW_CTX_EXECUTION_DATE": "2026-03-12T08:15:00+00:00",
                "AIRFLOW_CTX_LOGICAL_DATE": "2026-03-13T09:30:00+00:00",
            },
            clear=False,
        ):
            self.assertEqual(ingest_date_str(partition_now()), "2026-03-13")

    def test_circl_reads_epss_from_logical_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            epss_dir = (
                base_dir
                / "landing"
                / "structured"
                / "epss"
                / "ingest_date=2026-03-12"
            )
            epss_dir.mkdir(parents=True, exist_ok=True)
            (epss_dir / "epss_page=000.json").write_text(
                json.dumps(
                    {
                        "data": [
                            {
                                "cve": "CVE-2026-0001",
                                "epss": "0.975",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            with patch.dict(
                "os.environ",
                {
                    "LANDING_BACKEND": "local",
                    "AIRFLOW_CTX_EXECUTION_DATE": "2026-03-12T08:15:00+00:00",
                },
                clear=False,
            ), patch(
                "ingestion.batch.circl_vulnlookup_ingest.fetch_bytes",
                return_value=b'{"id":"CVE-2026-0001"}',
            ):
                result = run_circl(
                    base_dir=base_dir,
                    top_n_cves=1,
                    epss_max_pages=1,
                    request_sleep_seconds=0.0,
                    timeout_seconds=30,
                    retries=2,
                )

            expected = (
                base_dir
                / "landing"
                / "semi_structured"
                / "circl_vulnlookup"
                / "ingest_date=2026-03-12"
                / "circl_cve_details.jsonl"
            )
            self.assertTrue(expected.exists())
            self.assertEqual(result["successful_cves"], 1)


if __name__ == "__main__":
    unittest.main()
