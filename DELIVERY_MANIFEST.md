# BDM P1 Final Delivery Package

This archive contains the final P1 deliverable for CyberSecIntel.

## Main Report

- `docs/p1_final_delivery/build/p1_final_delivery.pdf`: final P1 report.
- `docs/p1_final_delivery/src/p1_final_delivery.tex`: LaTeX source for the report.
- `docs/p1_final_delivery/Makefile`: report build command.
- `docs/p1_2_delivery/assets/BDM_P1.1.drawio.png`: architecture figure used by the final report.

## Runnable Project

- `README.md`: project overview and local run instructions.
- `.env.example`: reproducible environment template without local secrets.
- `docker-compose.yml`: MinIO, Kafka, Zookeeper, Postgres, and Airflow stack.
- `orchestration/airflow/`: custom Airflow image, DAGs, and Airflow dependencies.
- `ingestion/`: batch, dataset, stream, replay, and shared ingestion code.
- `landing/`: landing-zone setup code.
- `config/`: source configuration files.
- `tests/`: unit tests for ingestion, optional sources, partitions, and PCAP replay.
- `docs/pcap_replay.md`: PCAP replay runbook.

## Excluded Local Artifacts

The archive intentionally excludes `.env`, `.git`, `.venv`, local `data/` volumes, Airflow logs, Python caches, LaTeX auxiliary files, and generated local runtime state.
