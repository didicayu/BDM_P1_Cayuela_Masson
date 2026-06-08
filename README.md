# CyberSecIntel — BDM P2 Final Delivery

A cybersecurity data platform for small SOC workflows, built as the P2 final deliverable for Big Data Management at UPC. The platform automates ingestion of vulnerability intelligence, exploit-probability scores, IOC feeds, CTU-13 packet captures, synthetic IDS records, and replay-derived Suricata events. P2 completes the lakehouse pipeline with Trusted Zone cleaning, Exploitation Zone data products, ML anomaly prediction, analyst-facing consumption outputs, and governance artifacts for cataloging, lineage, and quality metrics.

**Authors:** Dídac Cayuela and Sindri Masson

## Project Structure

```
├── ingestion/
│   ├── batch/          # Source-specific API/feed clients (KEV, NVD, EPSS, URLhaus, CIRCL, ThreatFox, Shodan)
│   ├── common/         # Shared HTTP, storage, metadata, and Delta helpers
│   ├── datasets/       # CTU-13 remote PCAP discovery and download
│   ├── replay/         # Suricata offline replay, normalization, Delta writes, Kafka publishing
│   └── stream/         # Synthetic IDS events and warm aggregate materialization
├── landing/            # Landing-zone prefix setup utilities
├── orchestration/
│   └── airflow/
│       ├── Dockerfile              # Custom image with Suricata + ET Open ruleset
│       ├── requirements-airflow.txt
│       └── dags/                   # Airflow DAG definitions
├── scripts/            # Host-side pipeline and delivery build helpers
├── config/             # Source configuration, CTU discovery rules, and Grafana provisioning
├── tests/              # Unit tests (ingestion, replay, P2 zones, ML, consumption, governance)
├── governance/          # P2 data product catalog, lineage, and quality metrics
├── models/              # Trained Isolation Forest metadata and joblib artifact
├── docs/
│   ├── p2_final_delivery/       # Final P2 report (LaTeX source + PDF)
│   ├── p1_final_delivery/          # Final P1 report (LaTeX source + PDF)
│   ├── p1_2_delivery/              # Earlier P1.2 report and architecture diagram
│   └── pcap_replay.md             # PCAP replay runbook
├── docker-compose.yml
├── .env.example
└── README.md
```

## Prerequisites

- Docker Desktop or Docker Engine with Compose
- Network access for public data ingestion and image build (downloads Emerging Threats Open ruleset)
- API keys are optional except for sources that depend on them:
  - `NVD_API_KEY` — recommended for NVD
  - `ABUSE_CH_API_KEY` — required for ThreatFox
  - `SHODAN_API_KEY` — only needed if `SHODAN_ENABLED=true`

## Quick Start

```bash
git clone https://github.com/didicayu/BDM_P1_Cayuela_Masson.git
cd BDM_P1_Cayuela_Masson
cp .env.example .env
```

Build the shared custom Airflow image and start the stack:

```bash
docker compose build airflow-webserver
docker compose up -d minio zookeeper kafka postgres grafana
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Service endpoints:

| Service         | URL / Address                       |
|-----------------|-------------------------------------|
| Airflow UI      | `http://localhost:8080`             |
| MinIO Console   | `http://localhost:${MINIO_CONSOLE_PORT:-9001}` |
| MinIO API       | `http://localhost:${MINIO_API_PORT:-9000}` |
| Kafka bootstrap | `localhost:9092`                    |
| Postgres        | `localhost:${POSTGRES_PORT:-5433}`  |
| Grafana         | `http://localhost:${GRAFANA_PORT:-3000}` |

## Running the Pipelines

### 1. CTU dataset artifact ingestion

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_dataset_artifact_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_dataset_artifact_ingestion
```

Discovers CTU-13 scenario folders (42–54), downloads selected PCAP artifacts, writes sidecars and manifests, and materializes the `pcap_artifacts` Delta catalog.

### 2. API and feed ingestion

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_api_expansion_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_api_expansion_ingestion
```

Ingests KEV, NVD, URLhaus, EPSS, CIRCL, optional ThreatFox, and optional Shodan to bronze and merges records into silver Delta tables. Also writes synthetic IDS stream events.

### 3. PCAP replay (after at least one artifact exists)

Set `PCAP_REPLAY_ENABLED=true` in `.env`, restart Airflow, then:

```bash
docker compose up -d airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags unpause cybersecintel_pcap_replay
docker compose exec airflow-webserver airflow dags trigger cybersecintel_pcap_replay
```

Selects replay candidates, runs Suricata offline with the ET Open ruleset, persists EVE output, writes Delta rows (`suricata_events`, `pcap_replay_runs`, `ids_alerts`), and publishes to Kafka topics `suricata.events` and `ids.alerts`.

## Environment Configuration

Copy `.env.example` to `.env` and adjust as needed:

| Variable | Default | Purpose |
|----------|---------|---------|
| `PCAP_SOURCE_PROFILE` | `subset` | `subset` = first scenario only; `full` = all scenarios 42–54 |
| `PCAP_REPLAY_ENABLED` | `false` | Gate for PCAP replay; set `true` only when intentional |
| `PCAP_REPLAY_MAX_ARTIFACTS` | `1` | Bounds replay demos |
| `PCAP_REPLAY_ALLOW_DECOMPRESS` | `false` | Skips `.pcap.bz2` unless enabled |
| `PCAP_REPLAY_MAX_BYTES` | — | Optional cap on replay input size |
| `PCAP_REPLAY_KAFKA_ENABLED` | `true` | Publishes replay output to Kafka |
| `PCAP_REPLAY_KAFKA_SURICATA_TOPIC` | `suricata.events` | Topic for normalized EVE events |
| `PCAP_REPLAY_KAFKA_IDS_ALERT_TOPIC` | `ids.alerts` | Topic for alert-compatible records |
| `PCAP_REPLAY_KAFKA_STRICT` | `true` | Fails replay task on publish errors |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Used by Airflow containers |
| `WARM_STREAM_SOURCE` | `auto` | `auto` tries Kafka then Delta; `delta` gives a deterministic full-data rebuild |
| `P2_ENGINE` | `python` | Set to `spark` to run Spark-backed Exploitation products with Python fallback |
| `P2_TRUSTED_ENGINE` | — | Set to `spark` to run Spark-backed Trusted cleaning for KEV/EPSS/NVD |
| `GRAFANA_PORT` | `3000` | Host port for Grafana |
| `SHODAN_ENABLED` | `false` | Disabled by default |

## Storage Layout

### Bronze — `s3://landing/` (raw, immutable)

| Prefix | Content |
|--------|---------|
| `structured/kev/` | CISA KEV feed, partitioned by ingestion date |
| `structured/epss/` | FIRST EPSS API pages |
| `semi_structured/nvd/` | NVD CVE API pages |
| `semi_structured/urlhaus/` | URLhaus recent CSV |
| `semi_structured/circl_vulnlookup/` | CIRCL enrichment responses |
| `semi_structured/threatfox/` | ThreatFox IOCs (optional) |
| `semi_structured/shodan_seeded/` | Shodan host data (optional) |
| `stream/ids_alerts/` | Synthetic and replay-derived IDS alerts |
| `warm/stream_aggregates/` | Bounded warm alert aggregate batches |
| `unstructured/pcap/source=ctu13/` | Immutable CTU PCAP artifacts |
| `unstructured/pcap_replay/source=ctu13/` | Suricata EVE JSONL from replay |
| `metadata/manifests/` | Per-source JSONL manifests for lineage |

Each ingested artifact has a raw payload, a sidecar metadata file (`*.meta.json`), and manifest entries for traceability.

### Silver — `s3://deltalake/` (Delta tables)

| Table | Merge Key | Notes |
|-------|-----------|-------|
| `kev/` | `cveID` | CISA Known Exploited Vulnerabilities |
| `nvd/` | `cve_id` | NVD CVE details (flattened) |
| `epss/` | `cve` + `date` | EPSS scores change daily |
| `urlhaus/` | `id` | URLhaus malicious URLs |
| `threatfox/` | `id` | ThreatFox IOCs |
| `circl_vulnlookup/` | `cve_id` | CIRCL CVE enrichments |
| `shodan_seeded/` | `ip_str` | Shodan host data |
| `ids_alerts/` | *(append-only)* | Synthetic + Suricata alert events |
| `pcap_artifacts/` | `source_id` + `scenario_number` + `artifact_name` + `ingest_date` + `sha256` | Catalog of landed PCAP binaries |
| `pcap_replay_runs/` | `replay_run_id` | Suricata replay attempts and lineage |
| `suricata_events/` | *(append-only)* | Normalized Suricata EVE JSON events |

Re-running a DAG merges into existing Delta tables — matching records are updated, new records are inserted, and the `_delta_log/` gains a new commit. No duplicate rows are created.

## Tests

```bash
.venv/bin/python -m unittest discover -s tests
```

The test suite covers PCAP catalog construction, candidate selection, EVE normalization, compressed artifact staging, Kafka publishing, logical partitions, optional source handling, replay edge cases, P2 zone transformations, sklearn Isolation Forest scoring and fallback, warm aggregates, Spark job configuration, Grafana provisioning, consumption exports, and governance artifacts.

## Validation

Open `http://localhost:9001`, log in with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`, and inspect the `landing` (bronze) and `deltalake` (silver) buckets.

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
print(dt.version())
print(dt.history())
df = dt.to_pandas()
```

Verify Kafka hot-path publication:

```bash
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 --topic suricata.events --time -1
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 --topic ids.alerts --time -1
```

## P2 Final Trusted, Exploitation, Consumption, and Governance

The parent DAG executes the required path in dependency order: API ingestion, warm
aggregate materialization, Trusted cleaning, Exploitation products, and Consumption:

```bash
docker compose exec airflow-webserver airflow dags trigger cybersecintel_end_to_end
```

The CTU artifact and PCAP replay DAGs remain optional because replay is intentionally
gated by `PCAP_REPLAY_ENABLED`. To run only P2 after P1 tables already exist, trigger:

```bash
docker compose exec airflow-webserver airflow dags trigger cybersecintel_warm_stream_aggregates
docker compose exec airflow-webserver airflow dags trigger cybersecintel_trusted_zone
docker compose exec airflow-webserver airflow dags trigger cybersecintel_exploitation_zone
docker compose exec airflow-webserver airflow dags trigger cybersecintel_consumption_exports
```

These DAGs materialize cleaned Trusted Zone Delta tables in `s3://trusted/`, warm aggregates in landing and Delta, analyst-ready Exploitation Zone assets in `s3://exploitation/`, a trained sklearn Isolation Forest model, CSV/JSON/HTML consumption files under `consumption/outputs/`, and Postgres serving tables for Grafana.

P2 also writes governance outputs:

| Table | Location | Purpose |
|-------|----------|---------|
| `governance_data_product_catalog` | `s3://exploitation/` | Data product domain, owner, storage path, upstream assets, quality rules, access policy, and retention policy |
| `governance_lineage` | `s3://trusted/` and `s3://exploitation/` | Run-level source-to-target lineage for Trusted, Exploitation, warm-path, and Consumption tasks |
| `governance_quality_metrics` | `s3://trusted/` and `s3://exploitation/` | Rows read/written/rejected, warning counts, and consumption output counts |

The ML artifact is recorded in `s3://exploitation/ml_anomaly_model` and written under `models/ids_anomaly_detector/model.json` plus `models/ids_anomaly_detector/model.joblib`.

Grafana is provisioned from `config/grafana/provisioning/` and `config/grafana/dashboards/cybersecintel_soc.json`. It reads the `cybersecintel_consumption` Postgres schema populated by the consumption DAG.

For a deterministic host-side rebuild, the helper translates Docker-internal service
names to published localhost ports and uses the complete Delta IDS table for warm
aggregates:

```bash
scripts/run_p2_local.sh
scripts/run_p2_local.sh --spark
```

## Report Build

To rebuild the final P2 delivery PDF:

```bash
make -C docs/p2_final_delivery rebuild
```

To rebuild the PDF and integrity-check the submission archive together:

```bash
scripts/build_delivery.sh
```

## Stop Services

```bash
docker compose down
```
