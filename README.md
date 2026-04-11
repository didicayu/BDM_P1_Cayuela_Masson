# CyberSecIntel - BDM P1.2

This repository contains the P1.2 implementation for CyberSecIntel:
- automated ingestion from public cybersecurity APIs and feeds
- automatic discovery and download of CTU PCAP artifacts for the unstructured branch
- offline Suricata replay for selected landed PCAP artifacts
- two-layer storage in MinIO: raw landing zone (bronze) and Delta Lake tables (silver)
- Delta Lake silver layer provides ACID writes, MERGE-based deduplication, and transaction-log versioning
- Airflow orchestration for public-source ingestion, remote dataset artifacts, and the streaming scaffold

## Project Structure
- `ingestion/batch/`: API and feed ingestors
- `ingestion/datasets/`: automatic remote dataset discovery/download
- `ingestion/replay/`: PCAP cataloging and Suricata offline replay
- `ingestion/stream/`: synthetic IDS event scaffold
- `landing/`: landing-zone setup utilities
- `orchestration/airflow/`: Airflow DAGs/logs/plugins mounts for Docker
- `config/`: source configuration and CTU discovery rules
- `docs/`: follow-up and delivery documentation

## Docker Stack
From repository root:

```bash
cp .env.example .env
docker compose up -d minio zookeeper kafka postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Service endpoints:
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Kafka bootstrap: `localhost:9092`
- Postgres: `localhost:${POSTGRES_PORT:-5433}`
- Airflow UI: `http://localhost:8080`

## Required Environment
Copy `.env.example` to `.env` and set values before running:

```bash
cp .env.example .env
```

Important knobs:
- `PCAP_SOURCE_PROFILE=subset` for normal development/demo runs
- `PCAP_SOURCE_PROFILE=full` for the heavier CTU validation run
- `PCAP_REPLAY_ENABLED=false` by default; set to `true` only when intentionally replaying PCAPs
- `PCAP_REPLAY_MAX_ARTIFACTS=1` keeps replay demos bounded
- `PCAP_REPLAY_ALLOW_DECOMPRESS=false` skips `.pcap.bz2` unless explicitly enabled
- `PCAP_REPLAY_MAX_BYTES` can cap replay input size for local demos
- `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` is used by Airflow containers
- `PCAP_REPLAY_KAFKA_ENABLED=true` publishes replay output to Kafka topics
- `PCAP_REPLAY_KAFKA_SURICATA_TOPIC=suricata.events` receives normalized EVE events
- `PCAP_REPLAY_KAFKA_IDS_ALERT_TOPIC=ids.alerts` receives alert-compatible records
- `SHODAN_ENABLED=false` unless you explicitly want the Shodan branch
- `NVD_API_KEY` recommended
- `ABUSE_CH_API_KEY` required if you want ThreatFox enabled
- `SHODAN_API_KEY` only required if `SHODAN_ENABLED=true`

## Intended Run Path
1. Start services and initialize Airflow.

```bash
docker compose up -d minio zookeeper kafka postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

2. Trigger the CTU dataset artifact DAG.

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_dataset_artifact_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_dataset_artifact_ingestion
```

3. Trigger the API/feed DAG.

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_api_expansion_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_api_expansion_ingestion
```

4. Optionally trigger PCAP replay after at least one CTU artifact exists.

Set `PCAP_REPLAY_ENABLED=true` in `.env`, restart Airflow services, then run:

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_pcap_replay
docker compose exec airflow-webserver airflow dags trigger cybersecintel_pcap_replay
```

5. View task logs in Airflow UI and validate objects in MinIO buckets `landing` (bronze) and `deltalake` (silver).

What `cybersecintel_dataset_artifact_ingestion` does:
- prepares landing-zone prefixes
- discovers CTU scenarios `42..54` from the public dataset index
- selects one capture artifact per scenario according to the active profile
- downloads the artifact automatically and lands it in MinIO with metadata and manifest lineage
- materializes `pcap_artifacts` metadata when Delta dependencies are available

What `cybersecintel_pcap_replay` does:
- builds or refreshes the `pcap_artifacts` Delta catalog from CTU manifests
- selects replay candidates, preferring plain `.pcap` artifacts
- stages selected PCAPs in `/tmp/cybersecintel/`, runs Suricata offline with `-r` against the bundled Emerging Threats Open ruleset, and cleans up temporary files
- lands Suricata `eve.json` output as `eve.jsonl`
- normalizes configured EVE event types into `suricata_events` and appends alerts to `ids_alerts`
- publishes normalized events to Kafka topic `suricata.events` and alert records to `ids.alerts`

What `cybersecintel_api_expansion_ingestion` does:
- ingests KEV, NVD, URLhaus, EPSS, and CIRCL to bronze (`s3://landing/`) and merges records into silver Delta tables (`s3://deltalake/`)
- ingests ThreatFox when `ABUSE_CH_API_KEY` is available
- skips Shodan cleanly unless `SHODAN_ENABLED=true`
- writes the synthetic IDS stream scaffold to bronze and appends events to the `ids_alerts` Delta table

## CTU Remote Artifact Strategy
The unstructured branch no longer depends on a manually staged local folder.

- Reference page: `https://www.stratosphereips.org/datasets-ctu13`
- Artifact host: `https://mcfp.felk.cvut.cz/publicDatasets/`
- Discovery rule: directories matching `CTU-Malware-Capture-Botnet-<n>/` for scenarios `42..54`

Profiles:
- `subset`: first successful scenario only, preferring the smallest practical PCAP artifact
- `full`: all scenarios `42..54`, preferring compressed captures for a stronger big-data story

## Storage Layout

### Bronze layer — `s3://landing/` (raw, immutable)
- `structured/kev/ingest_date=YYYY-MM-DD/`
- `structured/epss/ingest_date=YYYY-MM-DD/`
- `semi_structured/nvd/ingest_date=YYYY-MM-DD/`
- `semi_structured/urlhaus/ingest_date=YYYY-MM-DD/`
- `semi_structured/circl_vulnlookup/ingest_date=YYYY-MM-DD/`
- `semi_structured/threatfox/ingest_date=YYYY-MM-DD/`
- `semi_structured/shodan_seeded/ingest_date=YYYY-MM-DD/`
- `stream/ids_alerts/ingest_date=YYYY-MM-DD/hour=HH/`
- `stream/ids_alerts/source=suricata/ingest_date=YYYY-MM-DD/hour=HH/`
- `unstructured/pcap/source=ctu13/scenario=<n>/ingest_date=YYYY-MM-DD/`
- `unstructured/pcap_replay/source=ctu13/scenario=<n>/artifact_sha256=<sha>/ingest_date=YYYY-MM-DD/`
- `metadata/manifests/ingest_date=YYYY-MM-DD/`

Each ingested artifact has:
- raw source-native payload
- sidecar metadata file (`*.meta.json`)
- manifest entry (`jsonl`) for traceability

### Silver layer — `s3://deltalake/` (Delta tables)

Each source gets its own Delta table. The transaction log (`_delta_log/`) records every write as a numbered commit, enabling version history and time travel.

| Table | Merge key | Notes |
|-------|-----------|-------|
| `kev/` | `cveID` | CISA Known Exploited Vulnerabilities |
| `nvd/` | `cve_id` | NVD CVE details (flattened) |
| `epss/` | `cve` + `date` | EPSS scores change daily |
| `urlhaus/` | `id` | URLhaus malicious URLs |
| `threatfox/` | `id` | ThreatFox IOCs |
| `circl_vulnlookup/` | `cve_id` | CIRCL CVE enrichments |
| `shodan_seeded/` | `ip_str` | Shodan host data |
| `ids_alerts/` | *(append-only)* | Synthetic IDS stream events |
| `pcap_artifacts/` | `source_id` + `scenario_number` + `artifact_name` + `ingest_date` + `sha256` | Queryable catalog of landed PCAP binaries |
| `pcap_replay_runs/` | `replay_run_id` | Suricata replay attempts and lineage |
| `suricata_events/` | *(append-only)* | Normalized Suricata EVE JSON events from PCAP replay |

PCAP files remain in bronze only — binary data has no Delta equivalent.

Re-running a DAG will MERGE into the existing Delta table: matching records are updated, new records are inserted, and the `_delta_log/` gains one new commit. No duplicate rows are created.

## Validation
Open `http://localhost:9001`, log in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`, and check:

**Bronze** (`landing` bucket):
- `structured/`
- `semi_structured/`
- `stream/`
- `unstructured/pcap/source=ctu13/`
- `metadata/manifests/`

**Silver** (`deltalake` bucket):
- `kev/_delta_log/` — transaction log (one JSON commit per run)
- `kev/ingest_date=YYYY-MM-DD/` — Parquet data files
- (same pattern for `nvd/`, `epss/`, `urlhaus/`, etc.)

To inspect a Delta table from Python:
```python
from deltalake import DeltaTable

storage_options = {
    "endpoint_url": "http://localhost:9000",
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
    "allow_http": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}
dt = DeltaTable("s3://deltalake/kev", storage_options=storage_options)
print(dt.version())   # current version number
print(dt.history())   # all commits
df = dt.to_pandas()   # read current state
```

## Report Build
To rebuild the delivery PDF:

```bash
make -C docs/p1_2_delivery rebuild
```

## Stop Services

```bash
docker compose down
```
