from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ingestion.common.http_utils import DownloadResult
from ingestion.datasets.remote_pcap_ingest import (
    Scenario,
    discover_scenarios,
    run,
    select_artifact,
)


class RemotePcapIngestTests(unittest.TestCase):
    def test_discover_scenarios_filters_range_and_sorts(self) -> None:
        root_html = """
        <html><body>
        <a href="../">Parent</a>
        <a href="CTU-Malware-Capture-Botnet-54/">54</a>
        <a href="CTU-Malware-Capture-Botnet-42/">42</a>
        <a href="CTU-Malware-Capture-Botnet-41/">41</a>
        </body></html>
        """
        with patch("ingestion.datasets.remote_pcap_ingest.fetch_text", return_value=root_html):
            scenarios = discover_scenarios(
                root_index_url="https://mcfp.felk.cvut.cz/publicDatasets/",
                scenario_pattern=r"^CTU-Malware-Capture-Botnet-(\d+)/$",
                scenario_start=42,
                scenario_end=54,
                timeout_seconds=30,
                retries=2,
            )
        self.assertEqual([scenario.number for scenario in scenarios], [42, 54])

    def test_select_artifact_respects_profile_order(self) -> None:
        file_names = [
            "capture20110810.truncated.pcap.bz2",
            "botnet-capture-20110810-neris.pcap",
            "README.txt",
        ]
        subset = select_artifact(
            scenario_url="https://example.test/CTU-Malware-Capture-Botnet-42/",
            file_names=file_names,
            selection_rules=[
                {"label": "subset_botnet_capture_pcap", "pattern": r"^botnet-capture-.*\.pcap$"},
                {"label": "subset_truncated_pcap_bz2", "pattern": r"^.*\.truncated\.pcap\.bz2$"},
            ],
        )
        full = select_artifact(
            scenario_url="https://example.test/CTU-Malware-Capture-Botnet-42/",
            file_names=file_names,
            selection_rules=[
                {"label": "full_truncated_pcap_bz2", "pattern": r"^.*\.truncated\.pcap\.bz2$"},
                {"label": "full_botnet_capture_pcap", "pattern": r"^botnet-capture-.*\.pcap$"},
            ],
        )
        self.assertIsNotNone(subset)
        self.assertEqual(subset.artifact_name, "botnet-capture-20110810-neris.pcap")
        self.assertIsNotNone(full)
        self.assertEqual(full.artifact_name, "capture20110810.truncated.pcap.bz2")

    def test_run_writes_metadata_and_skips_existing(self) -> None:
        config = {
            "root_index_url": "https://mcfp.felk.cvut.cz/publicDatasets/",
            "scenario_pattern": r"^CTU-Malware-Capture-Botnet-(\d+)/$",
            "scenario_range": {"start": 42, "end": 54},
            "profiles": {
                "subset": {
                    "mode": "first_successful",
                    "selection_rules": [
                        {
                            "label": "subset_botnet_capture_pcap",
                            "pattern": r"^botnet-capture-.*\.pcap$",
                        }
                    ],
                }
            },
        }

        def fake_download(url: str, destination: Path, **_: object) -> DownloadResult:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"pcap-payload")
            return DownloadResult(
                source_url=url,
                final_url=url,
                destination=destination,
                content_type="application/vnd.tcpdump.pcap",
                size_bytes=destination.stat().st_size,
            )

        scenario = Scenario(
            number=42,
            directory_name="CTU-Malware-Capture-Botnet-42",
            url="https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-42/",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            with patch.dict("os.environ", {"LANDING_BACKEND": "local"}, clear=False):
                with patch(
                    "ingestion.datasets.remote_pcap_ingest.load_source_config",
                    return_value=config,
                ), patch(
                    "ingestion.datasets.remote_pcap_ingest.discover_scenarios",
                    return_value=[scenario],
                ), patch(
                    "ingestion.datasets.remote_pcap_ingest.list_scenario_files",
                    return_value=["botnet-capture-20110810-neris.pcap"],
                ), patch(
                    "ingestion.datasets.remote_pcap_ingest.download_to_file",
                    side_effect=fake_download,
                ) as download_mock:
                    first = run(
                        base_dir=base_dir,
                        profile="subset",
                        config_path=base_dir / "pcap_sources.json",
                        timeout_seconds=30,
                        retries=2,
                        force=False,
                    )
                    second = run(
                        base_dir=base_dir,
                        profile="subset",
                        config_path=base_dir / "pcap_sources.json",
                        timeout_seconds=30,
                        retries=2,
                        force=False,
                    )

            self.assertEqual(first["downloaded_artifacts"], 1)
            self.assertEqual(second["skipped_existing"], 1)
            self.assertEqual(download_mock.call_count, 1)

            landing_root = base_dir / "landing"
            artifact_candidates = sorted(
                landing_root.glob(
                    "unstructured/pcap/source=ctu13/scenario=42/ingest_date=*/botnet-capture-20110810-neris.pcap"
                )
            )
            self.assertEqual(len(artifact_candidates), 1)
            self.assertIn("ingest_date=", artifact_candidates[0].as_posix())

            metadata_candidates = sorted(
                landing_root.glob(
                    "unstructured/pcap/source=ctu13/scenario=42/ingest_date=*/botnet-capture-20110810-neris.pcap.meta.json"
                )
            )
            self.assertEqual(len(metadata_candidates), 1)
            metadata = json.loads(metadata_candidates[0].read_text(encoding="utf-8"))
            self.assertEqual(metadata["scenario_number"], 42)
            self.assertEqual(metadata["profile"], "subset")
            self.assertEqual(metadata["selection_rule"], "subset_botnet_capture_pcap")

            manifest_candidates = sorted(
                landing_root.glob("metadata/manifests/ingest_date=*/ctu13_remote_pcap.jsonl")
            )
            self.assertEqual(len(manifest_candidates), 1)
            manifest_lines = manifest_candidates[0].read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(manifest_lines), 1)

    def test_run_uses_airflow_logical_date_for_partitioning(self) -> None:
        config = {
            "root_index_url": "https://mcfp.felk.cvut.cz/publicDatasets/",
            "scenario_pattern": r"^CTU-Malware-Capture-Botnet-(\d+)/$",
            "scenario_range": {"start": 42, "end": 54},
            "profiles": {
                "subset": {
                    "mode": "first_successful",
                    "selection_rules": [
                        {
                            "label": "subset_botnet_capture_pcap",
                            "pattern": r"^botnet-capture-.*\.pcap$",
                        }
                    ],
                }
            },
        }

        def fake_download(url: str, destination: Path, **_: object) -> DownloadResult:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"pcap-payload")
            return DownloadResult(
                source_url=url,
                final_url=url,
                destination=destination,
                content_type="application/vnd.tcpdump.pcap",
                size_bytes=destination.stat().st_size,
            )

        scenario = Scenario(
            number=42,
            directory_name="CTU-Malware-Capture-Botnet-42",
            url="https://mcfp.felk.cvut.cz/publicDatasets/CTU-Malware-Capture-Botnet-42/",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            with patch.dict(
                "os.environ",
                {
                    "LANDING_BACKEND": "local",
                    "AIRFLOW_CTX_EXECUTION_DATE": "2026-03-12T08:15:00+00:00",
                },
                clear=False,
            ):
                with patch(
                    "ingestion.datasets.remote_pcap_ingest.load_source_config",
                    return_value=config,
                ), patch(
                    "ingestion.datasets.remote_pcap_ingest.discover_scenarios",
                    return_value=[scenario],
                ), patch(
                    "ingestion.datasets.remote_pcap_ingest.list_scenario_files",
                    return_value=["botnet-capture-20110810-neris.pcap"],
                ), patch(
                    "ingestion.datasets.remote_pcap_ingest.download_to_file",
                    side_effect=fake_download,
                ):
                    run(
                        base_dir=base_dir,
                        profile="subset",
                        config_path=base_dir / "pcap_sources.json",
                        timeout_seconds=30,
                        retries=2,
                        force=True,
                    )

            expected = (
                base_dir
                / "landing"
                / "unstructured"
                / "pcap"
                / "source=ctu13"
                / "scenario=42"
                / "ingest_date=2026-03-12"
                / "botnet-capture-20110810-neris.pcap"
            )
            self.assertTrue(expected.exists())


if __name__ == "__main__":
    unittest.main()
