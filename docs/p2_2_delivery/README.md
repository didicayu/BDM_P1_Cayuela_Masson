# CyberSecIntel P2.2 Follow-Up Deliverable

This folder documents the basic implementation checkpoint for P2: Trusted Zone,
Exploitation Zone, and Consumption outputs.

The implementation is intentionally a data-movement checkpoint. It proves that
data can move from the existing P1 Delta tables into separate Trusted and
Exploitation Delta buckets and then into analyst-facing output files. The final
P2 delivery can deepen the cleaning rules, governance, dashboards, and ML model.

## Rebuild the PDF

```bash
make -C docs/p2_2_delivery rebuild
```

## Run the P2.2 Pipeline

Start the P1 stack and run ingestion first, then trigger the P2 DAGs:

```bash
docker compose build airflow-webserver
docker compose up -d minio zookeeper kafka postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler

docker compose exec airflow-webserver airflow dags trigger cybersecintel_api_expansion_ingestion
docker compose exec airflow-webserver airflow dags trigger cybersecintel_trusted_zone
docker compose exec airflow-webserver airflow dags trigger cybersecintel_exploitation_zone
docker compose exec airflow-webserver airflow dags trigger cybersecintel_consumption_exports
```

Optional replay path:

```bash
docker compose exec airflow-webserver airflow dags trigger cybersecintel_dataset_artifact_ingestion
PCAP_REPLAY_ENABLED=true docker compose up -d airflow-webserver airflow-scheduler
docker compose exec airflow-webserver airflow dags trigger cybersecintel_pcap_replay
```

Inspect outputs in MinIO:

- `s3://trusted/`
- `s3://exploitation/`
- `consumption/outputs/`
