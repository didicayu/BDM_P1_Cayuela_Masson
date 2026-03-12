# BDM P1.2 Follow-up Deliverable

## 0. Instructions (Runbook)
Execution is Airflow-first from Docker services.
Repository root commands:

```bash
cp .env.example .env
# Fill NVD_API_KEY in .env
```

Start services:

```bash
docker compose up -d minio zookeeper kafka postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Recommended env:
- `LANDING_BACKEND=minio` (direct landing writes to object storage)
- `DATASET_IMPORT_MODE=move` (prevents duplicated local dataset copies)

Trigger DAG:

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_p1_ingestion_landing
docker compose exec airflow-webserver airflow dags trigger cybersecintel_p1_ingestion_landing
```

Optional manual imports for non-API sources (fallback):

```bash
python3 -m ingestion.imports.dataset_import --dataset cic_ids2017_csv --source-path /path/to/cic_csvs
python3 -m ingestion.imports.dataset_import --dataset cic_ids2017_pcap --source-path /path/to/cic_pcaps
python3 -m ingestion.imports.dataset_import --dataset ctu13_pcap --source-path /path/to/ctu13_pcaps
```

## 1. Context Update
Project: **CyberSecIntel** (SOC-oriented cybersecurity data platform).

Objective for this follow-up:
- move from conceptual P1.1 design to a basic runnable ingestion + landing implementation
- explicitly show which sources are API-based vs dataset-download based
- keep architecture aligned with planned cold/hot/warm paths

## 2. Data Source Feasibility (API Clarification)
| Source | Type | Access | API-ready | Current implementation status |
|---|---|---|---|---|
| CISA KEV | Structured | HTTP JSON/CSV feed | Yes | Implemented in `ingestion/batch/kev_ingest.py` |
| NVD CVE v2 | Semi-structured | REST API | Yes | Implemented in `ingestion/batch/nvd_ingest.py` |
| URLhaus recent | Semi-structured | CSV feed/API | Yes | Implemented in `ingestion/batch/urlhaus_ingest.py` |
| CIC-IDS2017 | Structured (large) | Dataset download | No | Implemented via import utility (`dataset_import.py`) |
| CTU-13 PCAP | Unstructured | Dataset download | No | Implemented via import utility (`dataset_import.py`) |

Machine-readable source registry: `config/data_sources.json`.

## 3. Basic Ingestion Implementation
Implemented components:
- `ingestion/batch/kev_ingest.py`
  - fetches CISA KEV payload
  - stores raw file + metadata + manifest entry
- `ingestion/batch/nvd_ingest.py`
  - fetches NVD CVE v2 incrementally with paging controls
  - supports API key via `NVD_API_KEY` env var
- `ingestion/batch/urlhaus_ingest.py`
  - fetches recent URLhaus CSV feed
  - stores raw feed with row count metadata
- `ingestion/imports/dataset_import.py`
  - ingests downloaded CSV/PCAP files to landing prefixes
  - supports CIC zipped CSV downloads by extracting CSV files directly from `.zip`
- `ingestion/stream/synthetic_ids_stream.py`
  - lightweight synthetic stream placeholder for hot-path progress
- `orchestration/airflow/dags/p1_ingestion_landing_dag.py`
  - canonical orchestration path for P1.2
  - coordinates setup, API ingestion, dataset import, and synthetic stream generation

## 4. Landing Zone Implementation
Landing-zone setup script:
- `landing/setup_landing.py`

Implemented landing organization:
- `s3://landing/structured/kev/ingest_date=YYYY-MM-DD/`
- `s3://landing/structured/cic_ids2017/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/nvd/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/urlhaus/ingest_date=YYYY-MM-DD/`
- `s3://landing/stream/ids_alerts/ingest_date=YYYY-MM-DD/hour=HH/`
- `s3://landing/warm/stream_aggregates/`
- `s3://landing/unstructured/pcap/source=cic_ids2017/ingest_date=YYYY-MM-DD/`
- `s3://landing/unstructured/pcap/source=ctu13/ingest_date=YYYY-MM-DD/`
- `s3://landing/metadata/manifests/ingest_date=YYYY-MM-DD/`

Traceability and reproducibility:
- each artifact gets checksum + size + origin metadata (`*.meta.json`)
- per-source append-only manifest in JSONL format
- direct object-storage landing in MinIO (bucket-level validation through web UI)

## 5. Scope and Next Step Toward P1 Final / P2
Current delivery is intentionally basic and focused on P1.2 requirements:
- ingestion automation exists and is source-specific
- landing zone is implemented and organized
- data-source API availability concern is explicitly addressed

Planned next increments:
- move from MinIO raw landing to Delta table materialization
- connect trusted zone transformations
- implement ambitious exploitation layer (relational + vector retrieval)
