#!/usr/bin/env bash
# Start partitioned ingests and watch them live. COUNTRIES="DE IS" make run to customise.
set -euo pipefail
cd "$(dirname "$0")/.."
KEY=$(cat .demo-key)
API=http://localhost:8000
COUNTRIES=${COUNTRIES:-"DE"}

declare -A WF
for cc in $COUNTRIES; do
  WF[$cc]=$(curl -sf -X POST "$API/ingest/partitioned" \
    -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
    -d "{\"country_code\": \"$cc\"}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["workflow_id"])')
  echo "started $cc -> ${WF[$cc]}"
done
echo
echo "Watch the fan-out in Temporal: http://localhost:8080  (workflows named partitioned-ingest-*)"
echo

while true; do
  running=0; line=""
  for cc in $COUNTRIES; do
    status=$(curl -sf "$API/status/${WF[$cc]}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
    [ "$status" = RUNNING ] && running=$((running+1))
    count=$(curl -sf "$API/records/count?country_code=$cc" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')
    line+=$(printf "%s: %8s rows (%s)   " "$cc" "$count" "$status")
  done
  printf "\r%s" "$line"
  [ "$running" -eq 0 ] && break
  sleep 3
done
echo; echo "Done."
