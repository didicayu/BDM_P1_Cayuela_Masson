"""Automatic remote ingestion for CTU PCAP artifacts."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
from typing import Any
from urllib.parse import urljoin

from ingestion.common.http_utils import download_to_file, fetch_text
from ingestion.common.landing_utils import ingest_date_str, partition_now, utc_now
from ingestion.common.storage import LandingStorage

SOURCE_ID = "ctu13_remote_pcap"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pcap_sources.json"


@dataclass(frozen=True)
class Scenario:
    number: int
    directory_name: str
    url: str


@dataclass(frozen=True)
class SelectedArtifact:
    artifact_name: str
    artifact_url: str
    selection_rule: str


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for attr_name, attr_value in attrs:
            if attr_name.lower() == "href" and attr_value:
                self.links.append(attr_value)
                return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest remote CTU PCAP artifacts.")
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=Path("data"),
        help="Base data directory (default: data).",
    )
    parser.add_argument(
        "--profile",
        choices=["subset", "full"],
        default=os.getenv("PCAP_SOURCE_PROFILE", "subset"),
        help="CTU artifact ingestion profile.",
    )
    parser.add_argument(
        "--config-path",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to PCAP source configuration.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTP retries.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and overwrite the current ingest-date partition.",
    )
    return parser


def load_source_config(config_path: Path) -> dict[str, Any]:
    return json.loads(config_path.read_text(encoding="utf-8"))


def extract_links(html: str) -> list[str]:
    parser = LinkExtractor()
    parser.feed(html)
    return parser.links


def discover_scenarios(
    *,
    root_index_url: str,
    scenario_pattern: str,
    scenario_start: int,
    scenario_end: int,
    timeout_seconds: int,
    retries: int,
) -> list[Scenario]:
    index_html = fetch_text(
        root_index_url,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )
    pattern = re.compile(scenario_pattern)
    scenarios: list[Scenario] = []
    seen_numbers: set[int] = set()

    for href in extract_links(index_html):
        match = pattern.match(href)
        if match is None:
            continue
        number = int(match.group(1))
        if number < scenario_start or number > scenario_end or number in seen_numbers:
            continue
        seen_numbers.add(number)
        scenarios.append(
            Scenario(
                number=number,
                directory_name=href.rstrip("/"),
                url=urljoin(root_index_url, href),
            )
        )

    return sorted(scenarios, key=lambda scenario: scenario.number)


def list_scenario_files(
    *,
    scenario_url: str,
    timeout_seconds: int,
    retries: int,
) -> list[str]:
    scenario_html = fetch_text(
        scenario_url,
        timeout_seconds=timeout_seconds,
        retries=retries,
    )
    files: list[str] = []
    seen: set[str] = set()
    for href in extract_links(scenario_html):
        candidate = href.strip()
        if not candidate or candidate.endswith("/"):
            continue
        name = Path(candidate).name
        if name in {"", ".", ".."} or name in seen:
            continue
        seen.add(name)
        files.append(name)
    return sorted(files)


def select_artifact(
    *,
    scenario_url: str,
    file_names: list[str],
    selection_rules: list[dict[str, str]],
) -> SelectedArtifact | None:
    for rule in selection_rules:
        pattern = re.compile(rule["pattern"])
        matches = sorted(name for name in file_names if pattern.match(name))
        if not matches:
            continue
        artifact_name = matches[0]
        return SelectedArtifact(
            artifact_name=artifact_name,
            artifact_url=urljoin(scenario_url, artifact_name),
            selection_rule=rule["label"],
        )
    return None


def _sanitize_segment(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_") or "adhoc"


def runtime_temp_dir() -> Path:
    dag_id = _sanitize_segment(os.getenv("AIRFLOW_CTX_DAG_ID", "adhoc"))
    run_id = _sanitize_segment(os.getenv("AIRFLOW_CTX_DAG_RUN_ID", utc_now().strftime("%Y%m%dT%H%M%SZ")))
    path = Path("/tmp/cybersecintel") / dag_id / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def run(
    *,
    base_dir: Path,
    profile: str,
    config_path: Path,
    timeout_seconds: int,
    retries: int,
    force: bool,
) -> dict[str, str | int]:
    config = load_source_config(config_path)
    profiles = config.get("profiles", {})
    if profile not in profiles:
        raise ValueError(f"Unknown PCAP profile '{profile}'")

    profile_config = profiles[profile]
    scenario_range = config["scenario_range"]
    scenarios = discover_scenarios(
        root_index_url=config["root_index_url"],
        scenario_pattern=config["scenario_pattern"],
        scenario_start=int(scenario_range["start"]),
        scenario_end=int(scenario_range["end"]),
        timeout_seconds=timeout_seconds,
        retries=retries,
    )
    if not scenarios:
        raise RuntimeError("No CTU scenarios discovered for the configured range.")

    partition_at = partition_now()
    retrieved_at = utc_now()
    ingest_date = ingest_date_str(partition_at)
    storage = LandingStorage.from_env(base_dir)
    temp_root = runtime_temp_dir()

    downloaded_artifacts = 0
    skipped_existing = 0
    skipped_missing_artifact = 0
    processed_scenarios = 0

    try:
        for scenario in scenarios:
            file_names = list_scenario_files(
                scenario_url=scenario.url,
                timeout_seconds=timeout_seconds,
                retries=retries,
            )
            selected = select_artifact(
                scenario_url=scenario.url,
                file_names=file_names,
                selection_rules=profile_config["selection_rules"],
            )
            if selected is None:
                skipped_missing_artifact += 1
                continue

            scenario_relative_dir = (
                Path("unstructured")
                / "pcap"
                / "source=ctu13"
                / f"scenario={scenario.number}"
                / f"ingest_date={ingest_date}"
            )
            destination_relative = scenario_relative_dir / selected.artifact_name

            if storage.exists(destination_relative) and not force:
                skipped_existing += 1
                processed_scenarios += 1
                if profile_config["mode"] == "first_successful":
                    break
                continue

            if force:
                storage.clear_prefix(scenario_relative_dir)

            local_artifact = temp_root / f"scenario-{scenario.number}-{selected.artifact_name}"
            download_result = download_to_file(
                selected.artifact_url,
                local_artifact,
                timeout_seconds=timeout_seconds,
                retries=retries,
            )
            raw_written = storage.write_file(destination_relative, local_artifact)

            metadata = {
                "source_id": SOURCE_ID,
                "scenario_number": scenario.number,
                "scenario_directory": scenario.directory_name,
                "scenario_url": scenario.url,
                "page_url": scenario.url,
                "artifact_name": selected.artifact_name,
                "artifact_url": selected.artifact_url,
                "profile": profile,
                "selection_rule": selected.selection_rule,
                "retrieved_at_utc": retrieved_at.isoformat(),
                "content_type": download_result.content_type,
                "size_bytes": raw_written.size_bytes,
                "sha256": raw_written.sha256,
                "landing_path": raw_written.landing_path,
                "relative_landing_path": raw_written.relative_path,
            }
            meta_relative = destination_relative.with_suffix(destination_relative.suffix + ".meta.json")
            storage.write_json(meta_relative, metadata)
            storage.append_manifest_entry(
                source_id=SOURCE_ID,
                ingest_date=ingest_date,
                entry=metadata,
            )

            downloaded_artifacts += 1
            processed_scenarios += 1
            if profile_config["mode"] == "first_successful":
                break
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    return {
        "source": SOURCE_ID,
        "profile": profile,
        "processed_scenarios": processed_scenarios,
        "downloaded_artifacts": downloaded_artifacts,
        "skipped_existing": skipped_existing,
        "skipped_missing_artifact": skipped_missing_artifact,
    }


def main() -> int:
    args = build_parser().parse_args()
    result = run(
        base_dir=args.base_dir,
        profile=args.profile,
        config_path=args.config_path,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        force=args.force,
    )
    print(
        f"[{result['source']}] profile={result['profile']} "
        f"downloaded={result['downloaded_artifacts']} "
        f"processed={result['processed_scenarios']} "
        f"skipped_existing={result['skipped_existing']} "
        f"skipped_missing_artifact={result['skipped_missing_artifact']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
