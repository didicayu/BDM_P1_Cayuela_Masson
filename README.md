# CyberSecIntel - BDM P1.2

This repository contains a basic implementation for the P1.2 follow-up deliverable:
- automated ingestion from public cybersecurity data sources
- landing-zone storage with organized prefixes and metadata manifests
- orchestration through Airflow running in Docker
- API-expansion ingestion for EPSS, CIRCL, ThreatFox, and seeded Shodan lookups

## Project Structure
- `ingestion/`: ingestion scripts by type (`batch`, `imports`, `stream`)
- `landing/`: landing-zone setup utilities
- `orchestration/airflow/`: Airflow DAGs/logs/plugins mounts for Docker
- `config/`: data-source access matrix in machine-readable format
- `docs/`: follow-up deliverable document

## Docker Stack (MinIO + Kafka + Postgres + Airflow)
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

## Intended Run Path
1. Copy env and set API keys.

```bash
cp .env.example .env
# Edit .env and set NVD_API_KEY, ABUSE_CH_API_KEY, SHODAN_API_KEY
```

2. Start services and initialize Airflow.

```bash
docker compose up -d minio zookeeper kafka postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

3. Trigger DAG `cybersecintel_p1_ingestion_landing`.

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_p1_ingestion_landing
docker compose exec airflow-webserver airflow dags trigger cybersecintel_p1_ingestion_landing
```

4. Trigger API expansion DAG `cybersecintel_api_expansion_ingestion`.

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_api_expansion_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_api_expansion_ingestion
```

5. View task logs in Airflow UI and validate objects in MinIO bucket `landing`.

What `cybersecintel_p1_ingestion_landing` does:
- setup landing-zone structure in object storage
- import CIC-IDS2017 CSV from `data/raw_downloads` (`.zip` and `.csv` supported)
- import CIC-IDS2017 PCAP from `data/raw_downloads` when files are available

What `cybersecintel_api_expansion_ingestion` does:
- ingest KEV, NVD, URLhaus directly to MinIO landing bucket
- ingest paginated EPSS data
- ingest ThreatFox IOC feed (API key required)
- enrich top EPSS CVEs with CIRCL Vulnerability-Lookup
- query Shodan host API for seeded IP indicators (API key required)
- generate synthetic stream events and store them in MinIO

Storage note:
- `LANDING_BACKEND=minio` means ingestion writes directly to MinIO (no local landing staging).
- `DATASET_IMPORT_MODE=move` avoids duplicate local copies after dataset upload.

MinIO validation:
- Open `http://localhost:9001`
- Login with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
- Check bucket `landing` contains prefixes `structured/`, `semi_structured/`, `stream/`, `unstructured/`, `metadata/`

To stop:

```bash
docker compose down
```

## Non-API Dataset Imports
Main path is now Airflow DAG import from `data/raw_downloads`.
Manual fallback for downloaded datasets:

```bash
python3 -m ingestion.imports.dataset_import --dataset cic_ids2017_csv --source-path /path/to/cic_csvs
python3 -m ingestion.imports.dataset_import --dataset cic_ids2017_pcap --source-path /path/to/cic_pcaps
python3 -m ingestion.imports.dataset_import --dataset ctu13_pcap --source-path /path/to/ctu13_pcaps
```

## Landing Layout
- `s3://landing/structured/kev/ingest_date=YYYY-MM-DD/`
- `s3://landing/structured/epss/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/nvd/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/urlhaus/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/circl_vulnlookup/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/threatfox/ingest_date=YYYY-MM-DD/`
- `s3://landing/semi_structured/shodan_seeded/ingest_date=YYYY-MM-DD/`
- `s3://landing/stream/ids_alerts/ingest_date=YYYY-MM-DD/hour=HH/`
- `s3://landing/unstructured/pcap/source=.../ingest_date=YYYY-MM-DD/`
- `s3://landing/metadata/manifests/ingest_date=YYYY-MM-DD/`

Each ingested artifact has:
- raw payload file
- sidecar metadata file (`*.meta.json`)
- manifest entry (`jsonl`) for traceability
