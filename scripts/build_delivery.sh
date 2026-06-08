#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

archive="BDM_P2_Cayuela_Masson_final_delivery.zip"
temporary_archive="${archive%.zip}.tmp.zip"

make -C docs/p2_final_delivery rebuild
cp docs/p2_final_delivery/build/p2_final_delivery.pdf CyberSecIntel_P2_Cayuela_Masson.pdf

rm -f "$temporary_archive"
zip -qr "$temporary_archive" \
  .dockerignore .env.example .gitignore \
  README.md DELIVERY_MANIFEST.md docker-compose.yml \
  CyberSecIntel_P2_Cayuela_Masson.pdf \
  ingestion landing trusted exploitation consumption governance models \
  orchestration config tests scripts docs/p2_final_delivery docs/pcap_replay.md \
  -x '*/__pycache__/*' '*.pyc' '*.pyo' '*.DS_Store' \
     '*.aux' '*.fdb_latexmk' '*.fls' '*.log' '*.out' '*.synctex.gz' \
     'docs/p2_final_delivery/src/*.pdf' 'docs/p2_final_delivery/build/*' \
     'orchestration/airflow/logs/*'
zip -q "$temporary_archive" docs/p2_final_delivery/build/p2_final_delivery.pdf
mv "$temporary_archive" "$archive"
unzip -t "$archive"
