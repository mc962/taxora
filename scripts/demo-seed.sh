#!/usr/bin/env bash
# Load the small countries so the demo starts with existing data. Skips if already loaded.
set -euo pipefail
cd "$(dirname "$0")/.."
SEED_COUNTRIES=${SEED_COUNTRIES:-"IS VA SM"}
first=${SEED_COUNTRIES%% *}
have=$(curl -sf "localhost:8000/records/count?country_code=$first" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')
if [ "$have" -gt 0 ] && [ -z "${FORCE:-}" ]; then
  echo "Already seeded ($first has $have rows). Use FORCE=1 make seed to ingest again."
  exit 0
fi
echo "Seeding: $SEED_COUNTRIES (about 4 minutes; these are single-window countries)"
COUNTRIES="$SEED_COUNTRIES" ./scripts/demo-run.sh
