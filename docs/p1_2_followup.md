# BDM P1.2 Follow-up Deliverable

## 0. Runbook
Execution is Airflow-first from Docker services.

```bash
cp .env.example .env
```

Set at minimum:
- `PCAP_SOURCE_PROFILE=subset`
- `SHODAN_ENABLED=false`
- `NVD_API_KEY` recommended
- `ABUSE_CH_API_KEY` if ThreatFox should run
- `SHODAN_API_KEY` only if Shodan is enabled

Start services:

```bash
docker compose up -d minio zookeeper kafka postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Trigger DAGs:

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_dataset_artifact_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_dataset_artifact_ingestion

docker compose exec airflow-webserver airflow dags unpause cybersecintel_api_expansion_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_api_expansion_ingestion
```

## 1. Context Update
Project: **CyberSecIntel**.

This follow-up moves the project from the conceptual P1.1 design to a runnable ingestion + landing implementation. The main revision is in the unstructured branch: instead of relying on manually staged CIC files, P1.2 uses automatic discovery and download of CTU capture artifacts from the public CTU hosting server.

## 2. Design Revision from P1.1
The original P1.1 proposal centered the offline branch on CIC-IDS2017 CSV and PCAP downloads. During implementation, that approach proved weaker for the automation objective because the unstructured path depended on manual dataset staging. We therefore revised the design and adopted automatic CTU scenario discovery and download.

This preserves the original architectural intent:
- unstructured PCAP data stored in native format
- replay potential for the future hot path
- sufficiently large artifacts to support the big-data narrative

At the same time, it improves alignment with the project statement’s preference for automated ingestion from non-local sources.

## 3. Data Source Feasibility
| Source | Type | Access | Automation status |
|---|---|---|---|
| CISA KEV | Structured | HTTP JSON/CSV feed | Implemented |
| NVD CVE v2 | Semi-structured | REST API | Implemented |
| URLhaus recent | Semi-structured | CSV feed/API | Implemented |
| FIRST EPSS | Structured | REST API | Implemented |
| CIRCL Vulnerability-Lookup | Semi-structured | REST API | Implemented |
| abuse.ch ThreatFox | Semi-structured | API (keyed) | Implemented, non-blocking when key missing |
| Shodan Host | Semi-structured | REST API (keyed) | Implemented, disabled by default |
| CTU-13 remote PCAP artifacts | Unstructured | Directory-style public HTTP dataset host | Implemented |

Machine-readable source registry:
- `config/data_sources.json`
- `config/pcap_sources.json`

## 4. Basic Ingestion Implementation
Implemented components:
- `ingestion/batch/kev_ingest.py`
- `ingestion/batch/nvd_ingest.py`
- `ingestion/batch/urlhaus_ingest.py`
- `ingestion/batch/epss_ingest.py`
- `ingestion/batch/circl_vulnlookup_ingest.py`
- `ingestion/batch/threatfox_ingest.py`
- `ingestion/batch/shodan_seeded_ingest.py`
- `ingestion/datasets/remote_pcap_ingest.py`
- `ingestion/stream/synthetic_ids_stream.py`
- `orchestration/airflow/dags/api_expansion_ingestion_dag.py`
- `orchestration/airflow/dags/dataset_artifact_ingestion_dag.py`

## 5. CTU Discovery Strategy
Reference page:
- `https://www.stratosphereips.org/datasets-ctu13`

Artifact host:
- `https://mcfp.felk.cvut.cz/publicDatasets/`

Discovery logic:
- inspect the public index
- match directories `CTU-Malware-Capture-Botnet-42/` through `CTU-Malware-Capture-Botnet-54/`
- inspect each scenario directory
- select one preferred artifact according to the active profile

Profiles:
- `subset`: first successful scenario only, optimized for local testing
- `full`: all scenarios `42..54`, optimized for the heavier validation run

## 6. Landing Zone
Implemented landing prefixes:
- `s3://landing/structured/kev/ingest_date=YYYY-MM-DD/`
- `s3://landing/structured/epss/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/nvd/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/urlhaus/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/circl_vulnlookup/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/threatfox/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/shodan_seeded/ingest_date=YYYY-MM-DD/`
- `s3://landing/stream/ids_alerts/ingest_date=YYYY-MM-DD/hour=HH/`
- `s3://landing/unstructured/pcap/source=ctu13/scenario=<n>/ingest_date=YYYY-MM-DD/`
- `s3://landing/metadata/manifests/ingest_date=YYYY-MM-DD/`

Traceability:
- each artifact gets checksum and size metadata
- each write appends or updates the per-source JSONL manifest

## 7. Scope Toward P2
Current scope is intentionally focused on automated ingestion and the landing zone.

Next increments:
- trusted-zone normalization
- warm aggregates
- exploitation-layer materialization
- future replay-based hot-path integration from landed CTU artifacts
