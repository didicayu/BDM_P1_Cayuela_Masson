from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ingestion.batch.shodan_seeded_ingest import run as run_shodan
from ingestion.batch.threatfox_ingest import run as run_threatfox


class OptionalSourceTests(unittest.TestCase):
    def test_threatfox_skips_without_api_key(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict("os.environ", {"ABUSE_CH_API_KEY": ""}, clear=False):
                result = run_threatfox(
                    base_dir=Path(temp_dir),
                    days=1,
                    timeout_seconds=30,
                    retries=3,
                )
        self.assertEqual(result["source"], "abuse_ch_threatfox_api_v1")
        self.assertEqual(result["reason"], "missing_api_key")
        self.assertEqual(result["skipped"], 1)

    def test_shodan_skips_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(
                "os.environ",
                {
                    "LANDING_BACKEND": "local",
                    "SHODAN_ENABLED": "false",
                    "SHODAN_API_KEY": "",
                },
                clear=False,
            ):
                result = run_shodan(
                    base_dir=Path(temp_dir),
                    max_indicators_per_run=10,
                    request_sleep_seconds=0.0,
                    timeout_seconds=30,
                    retries=2,
                )
        self.assertEqual(result["source"], "shodan_host_seeded_api")
        self.assertEqual(result["reason"], "disabled")
        self.assertEqual(result["skipped"], 1)


if __name__ == "__main__":
    unittest.main()
