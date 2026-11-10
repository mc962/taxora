#!/usr/bin/env bash
# One-command demo setup: env file (if missing) -> full stack -> API key. Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
ENV_FILE="${ENV_FILE:-.dev.env}"
COMPOSE=(docker compose -f deployment/docker/docker-compose.yml --env-file "$ENV_FILE")

if [ ! -f "$ENV_FILE" ]; then
  echo "Creating $ENV_FILE with random secrets..."
  cp deployment/docker/taxora.env.example "$ENV_FILE"
  sed -i "s|^DATABASE__PASSWORD=.*|DATABASE__PASSWORD=$(openssl rand -hex 12)|" "$ENV_FILE"
  sed -i "s|^SESSION_SECRET=.*|SESSION_SECRET=$(openssl rand -hex 24)|" "$ENV_FILE"
fi

echo "Starting stack (first run builds the image and pulls Temporal; takes a few minutes)..."
ENV_FILE="../../$ENV_FILE" "${COMPOSE[@]}" up -d --build --wait

if [ ! -s .demo-key ]; then
  "${COMPOSE[@]}" exec -T api python -m taxora.cli create-key --name demo --superuser \
    | grep -o 'txra_[A-Za-z0-9_-]*' > .demo-key
fi

cat <<MSG

Demo is up.
  Taxora UI / API : http://localhost:8000   (log in with the key in .demo-key)
  Temporal UI     : http://localhost:8080
  Mock GBIF       : http://localhost:9000

Next:  make run     # start an ingest and watch it live
MSG
