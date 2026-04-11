# PCAP Replay Runbook

The PCAP replay branch turns landed CTU PCAP artifacts into Suricata EVE JSON and stream-compatible IDS events. Raw PCAP files remain immutable in bronze.

## Runtime

Airflow uses the custom image defined in `orchestration/airflow/Dockerfile`. It installs:

- Suricata for offline PCAP processing with `suricata -r`
- `suricata-update` and the Emerging Threats Open ruleset at `/etc/suricata/rules/suricata.rules`
- `deltalake` and `pyarrow` for Delta writes
- `boto3` and `botocore` for MinIO access

The image build runs `suricata-update` and validates the generated rules with `suricata -T`. Rebuild the Airflow image when you want to refresh the bundled ruleset:

```bash
docker compose build airflow-webserver airflow-scheduler airflow-init
```

Build/start through Docker Compose as usual:

```bash
docker compose up -d minio zookeeper kafka postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

## Safe Defaults

Replay is disabled unless explicitly enabled:

```bash
PCAP_REPLAY_ENABLED=true
PCAP_REPLAY_MAX_ARTIFACTS=1
PCAP_REPLAY_EVENT_TYPES=alert,flow,dns,http,tls
PCAP_REPLAY_ALLOW_DECOMPRESS=false
PCAP_REPLAY_MAX_BYTES=
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
PCAP_REPLAY_KAFKA_ENABLED=true
PCAP_REPLAY_KAFKA_STRICT=true
PCAP_REPLAY_KAFKA_SURICATA_TOPIC=suricata.events
PCAP_REPLAY_KAFKA_IDS_ALERT_TOPIC=ids.alerts
```

Use `PCAP_REPLAY_MAX_BYTES` for demos on limited machines. Leave `PCAP_REPLAY_ALLOW_DECOMPRESS=false` unless you intentionally want to process `.pcap.bz2` artifacts.

## Airflow Path

1. Run `cybersecintel_dataset_artifact_ingestion` first so CTU artifacts and manifests exist.
2. Set `PCAP_REPLAY_ENABLED=true` and restart Airflow services.
3. Trigger:

```bash
docker compose exec airflow-webserver airflow dags unpause cybersecintel_pcap_replay
docker compose exec airflow-webserver airflow dags trigger cybersecintel_pcap_replay
```

The DAG runs:

- `build_pcap_catalog`
- `replay_selected_pcaps`
- `persist_replay_outputs`

When `PCAP_REPLAY_KAFKA_ENABLED=true`, `replay_selected_pcaps` also publishes:

- normalized Suricata EVE events to `suricata.events`
- alert-compatible IDS records to `ids.alerts`

With `PCAP_REPLAY_KAFKA_STRICT=true`, Kafka publish failures fail the task. This is the recommended demo setting because it proves the hot-path handoff actually works.

## CLI Path

For local development with `LANDING_BACKEND=local` and Suricata installed:

```bash
python3 -m ingestion.replay.suricata_replay --max-artifacts 1 --event-types alert,flow,dns,http,tls
```

## Outputs

Bronze:

- `unstructured/pcap_replay/source=ctu13/scenario=<n>/artifact_sha256=<sha>/ingest_date=YYYY-MM-DD/eve.jsonl`
- `stream/ids_alerts/source=suricata/ingest_date=YYYY-MM-DD/hour=HH/ids_suricata.jsonl`
- `metadata/manifests/ingest_date=YYYY-MM-DD/ctu13_pcap_replay.jsonl`

Delta:

- `pcap_artifacts`
- `pcap_replay_runs`
- `suricata_events`
- `ids_alerts`

Kafka:

- `suricata.events`
- `ids.alerts`

Inspect Kafka messages after a replay run:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ids.alerts --from-beginning --max-messages 5
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic suricata.events --from-beginning --max-messages 5
```
