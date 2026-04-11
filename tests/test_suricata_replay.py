from __future__ import annotations

import bz2
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from ingestion.common.landing_utils import sha256_file
from ingestion.common.storage import LandingStorage
from ingestion.replay.suricata_replay import (
    PCAP_SOURCE_ID,
    artifact_record_from_metadata,
    build_pcap_catalog,
    ids_alert_record,
    load_pcap_artifact_records,
    normalize_eve_event,
    publish_records_to_kafka,
    select_replay_candidates,
    stage_artifact,
)


def sample_metadata(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "source_id": PCAP_SOURCE_ID,
        "scenario_number": 42,
        "scenario_directory": "CTU-Malware-Capture-Botnet-42",
        "scenario_url": "https://example.test/ctu42/",
        "artifact_name": "botnet-capture-20110810-neris.pcap",
        "artifact_url": "https://example.test/ctu42/botnet-capture-20110810-neris.pcap",
        "profile": "subset",
        "selection_rule": "subset_botnet_capture_pcap",
        "retrieved_at_utc": "2026-03-12T08:15:00+00:00",
        "content_type": "application/vnd.tcpdump.pcap",
        "size_bytes": 12,
        "sha256": "abc123",
        "landing_path": "s3://landing/unstructured/pcap/source=ctu13/scenario=42/ingest_date=2026-03-12/botnet-capture-20110810-neris.pcap",
        "relative_landing_path": "unstructured/pcap/source=ctu13/scenario=42/ingest_date=2026-03-12/botnet-capture-20110810-neris.pcap",
    }
    payload.update(overrides)
    return payload


class SuricataReplayTests(unittest.TestCase):
    def test_catalog_records_are_built_from_pcap_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            with patch.dict("os.environ", {"LANDING_BACKEND": "local"}, clear=False):
                storage = LandingStorage.from_env(base_dir)
                storage.append_manifest_entry(
                    source_id=PCAP_SOURCE_ID,
                    ingest_date="2026-03-12",
                    entry=sample_metadata(),
                )

                records = load_pcap_artifact_records(base_dir=base_dir)
                summary = build_pcap_catalog(base_dir=base_dir, write_delta=False)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["ingest_date"], "2026-03-12")
        self.assertEqual(records[0]["artifact_format"], "pcap")
        self.assertFalse(records[0]["is_compressed"])
        self.assertEqual(summary["catalog_records"], 1)

    def test_select_replay_candidates_prefers_plain_pcap_and_respects_limits(self) -> None:
        plain = artifact_record_from_metadata(
            sample_metadata(
                artifact_name="botnet-capture-20110810-neris.pcap",
                sha256="plain",
                size_bytes=100,
            )
        )
        compressed = artifact_record_from_metadata(
            sample_metadata(
                artifact_name="capture20110810.truncated.pcap.bz2",
                sha256="compressed",
                size_bytes=50,
            )
        )
        huge = artifact_record_from_metadata(
            sample_metadata(
                artifact_name="huge-botnet-capture.pcap",
                sha256="huge",
                size_bytes=10_000,
            )
        )

        records = [record for record in [compressed, huge, plain] if record is not None]
        selected = select_replay_candidates(
            records,
            max_artifacts=2,
            allow_decompress=False,
            max_bytes=1_000,
        )
        self.assertEqual([record["sha256"] for record in selected], ["plain"])

        selected_with_decompress = select_replay_candidates(
            records,
            max_artifacts=2,
            allow_decompress=True,
            max_bytes=1_000,
        )
        self.assertEqual(
            [record["sha256"] for record in selected_with_decompress],
            ["plain", "compressed"],
        )

    def test_normalize_eve_alert_and_ids_projection(self) -> None:
        artifact = artifact_record_from_metadata(sample_metadata(sha256="artifact-sha"))
        self.assertIsNotNone(artifact)
        eve = {
            "timestamp": "2026-03-12T08:15:01.000000+0000",
            "event_type": "alert",
            "src_ip": "10.0.0.1",
            "src_port": 4444,
            "dest_ip": "10.0.0.2",
            "dest_port": 80,
            "proto": "TCP",
            "app_proto": "http",
            "flow_id": 12345,
            "pcap_cnt": 7,
            "alert": {
                "signature": "ET TROJAN Generic CnC Beacon",
                "severity": 2,
                "category": "A Network Trojan was detected",
            },
        }

        normalized = normalize_eve_event(
            eve,
            artifact=artifact or {},
            replay_run_id="run-1",
            ingest_date="2026-03-12",
        )
        ids_record = ids_alert_record(normalized)

        self.assertEqual(normalized["event_type"], "alert")
        self.assertEqual(normalized["dest_ip"], "10.0.0.2")
        self.assertEqual(normalized["signature"], "ET TROJAN Generic CnC Beacon")
        self.assertEqual(normalized["artifact_sha256"], "artifact-sha")
        self.assertEqual(ids_record["dst_ip"], "10.0.0.2")
        self.assertEqual(ids_record["source"], "suricata")

    def test_normalize_eve_flow_uses_stable_numeric_defaults(self) -> None:
        artifact = artifact_record_from_metadata(sample_metadata(sha256="artifact-sha"))
        self.assertIsNotNone(artifact)
        eve = {
            "timestamp": "2026-03-12T08:15:01.000000+0000",
            "event_type": "flow",
            "src_ip": "10.0.0.1",
            "dest_ip": "10.0.0.2",
            "proto": "UDP",
        }

        normalized = normalize_eve_event(
            eve,
            artifact=artifact or {},
            replay_run_id="run-1",
            ingest_date="2026-03-12",
        )

        self.assertEqual(normalized["src_port"], 0)
        self.assertEqual(normalized["dest_port"], 0)
        self.assertEqual(normalized["severity"], 0)
        self.assertEqual(normalized["pcap_cnt"], 0)

    def test_stage_artifact_can_decompress_bz2_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            relative = Path(
                "unstructured/pcap/source=ctu13/scenario=42/ingest_date=2026-03-12/sample.pcap.bz2"
            )
            compressed = bz2.compress(b"pcap-bytes")

            with patch.dict("os.environ", {"LANDING_BACKEND": "local"}, clear=False):
                storage = LandingStorage.from_env(base_dir)
                storage.write_bytes(relative, compressed)
                artifact = artifact_record_from_metadata(
                    sample_metadata(
                        artifact_name="sample.pcap.bz2",
                        relative_landing_path=relative.as_posix(),
                        sha256=sha256_file(base_dir / "landing" / relative),
                        size_bytes=len(compressed),
                    )
                )
                staged = stage_artifact(
                    storage=storage,
                    artifact=artifact or {},
                    temp_dir=base_dir / "tmp",
                    allow_decompress=True,
                )

                self.assertEqual(staged.name, "sample.pcap")
                self.assertEqual(staged.read_bytes(), b"pcap-bytes")

    def test_publish_records_to_kafka_uses_replay_key_and_json_values(self) -> None:
        sent: list[dict[str, object]] = []

        class FakeFuture:
            def get(self, timeout: int) -> None:
                self.timeout = timeout

        class FakeProducer:
            def __init__(self, **kwargs: object) -> None:
                self.kwargs = kwargs

            def send(self, topic: str, key: str, value: dict[str, object]) -> FakeFuture:
                sent.append({"topic": topic, "key": key, "value": value})
                return FakeFuture()

            def flush(self, timeout: int) -> None:
                self.flush_timeout = timeout

            def close(self, timeout: int) -> None:
                self.close_timeout = timeout

        fake_kafka = types.SimpleNamespace(KafkaProducer=FakeProducer)
        records = [{"replay_run_id": "run-1", "event_type": "alert"}]

        with patch.dict(sys.modules, {"kafka": fake_kafka}):
            published = publish_records_to_kafka(
                records=records,
                topic="suricata.events",
                bootstrap_servers="kafka:29092",
                client_id="test-client",
                strict=True,
            )

        self.assertEqual(published, 1)
        self.assertEqual(sent[0]["topic"], "suricata.events")
        self.assertEqual(sent[0]["key"], "run-1")
        self.assertEqual(sent[0]["value"], records[0])


if __name__ == "__main__":
    unittest.main()
