#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo "Missing .env. Copy .env.example to .env and configure it first." >&2
  exit 1
fi
if [[ ! -x .venv/bin/python ]]; then
  echo "Missing .venv/bin/python. Create the project virtual environment first." >&2
  exit 1
fi

run_spark=false
if [[ "${1:-}" == "--spark" ]]; then
  run_spark=true
elif [[ $# -gt 0 ]]; then
  echo "Usage: $0 [--spark]" >&2
  exit 2
fi

set -a
source .env
set +a

export MINIO_ENDPOINT_URL="${HOST_MINIO_ENDPOINT_URL:-http://localhost:${MINIO_API_PORT:-9000}}"
export KAFKA_BOOTSTRAP_SERVERS="${HOST_KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export GRAFANA_POSTGRES_HOST="${GRAFANA_POSTGRES_HOST:-localhost}"
export GRAFANA_POSTGRES_PORT="${GRAFANA_POSTGRES_PORT:-${POSTGRES_PORT:-5433}}"

if [[ "$run_spark" == true ]]; then
  export P2_ENGINE=spark
fi

# Delta mode makes the bounded aggregate step reproducible even if Kafka retains old demo messages.
.venv/bin/python -m ingestion.stream.warm_aggregates --source "${HOST_WARM_STREAM_SOURCE:-delta}"
.venv/bin/python -m trusted.run_trusted
if [[ "$run_spark" == true ]]; then
  .venv/bin/python -m exploitation.run_exploitation --warm
  .venv/bin/python -m exploitation.run_exploitation --spark-warm-only --strict-warm
else
  .venv/bin/python -m exploitation.run_exploitation --warm
fi
.venv/bin/python -m consumption.run_exports
.venv/bin/python -m consumption.grafana_postgres
