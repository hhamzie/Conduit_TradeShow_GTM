#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-conduit-tradeshow-dashboard:latest}"
STACK_NAME="${STACK_NAME:-conduit-tradeshow}"
ENV_FILE="${ENV_FILE:-.env}"
WEB_PORT="${WEB_PORT:-8000}"
DB_NAME="${POSTGRES_DB:-tradeshow}"
DB_USER="${POSTGRES_USER:-app}"
DB_PASSWORD="${POSTGRES_PASSWORD:-app}"

NETWORK_NAME="${STACK_NAME}-net"
DB_CONTAINER="${STACK_NAME}-db"
WEB_CONTAINER="${STACK_NAME}-web"
WORKER_CONTAINER="${STACK_NAME}-worker"
POSTGRES_VOLUME="${STACK_NAME}-postgres"
EXPORT_VOLUME="${STACK_NAME}-exports"
DATABASE_URL="postgresql+psycopg://${DB_USER}:${DB_PASSWORD}@${DB_CONTAINER}:5432/${DB_NAME}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing ${ENV_FILE}. Copy .env.example to .env and fill in the real values first."
  exit 1
fi

docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1 || docker network create "${NETWORK_NAME}" >/dev/null
docker volume inspect "${POSTGRES_VOLUME}" >/dev/null 2>&1 || docker volume create "${POSTGRES_VOLUME}" >/dev/null
docker volume inspect "${EXPORT_VOLUME}" >/dev/null 2>&1 || docker volume create "${EXPORT_VOLUME}" >/dev/null

docker rm -f "${WORKER_CONTAINER}" >/dev/null 2>&1 || true
docker rm -f "${WEB_CONTAINER}" >/dev/null 2>&1 || true
docker rm -f "${DB_CONTAINER}" >/dev/null 2>&1 || true

docker run -d \
  --name "${DB_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --restart unless-stopped \
  -e POSTGRES_DB="${DB_NAME}" \
  -e POSTGRES_USER="${DB_USER}" \
  -e POSTGRES_PASSWORD="${DB_PASSWORD}" \
  -v "${POSTGRES_VOLUME}:/var/lib/postgresql/data" \
  postgres:16 >/dev/null

echo "Waiting for Postgres to become healthy..."
for _ in $(seq 1 30); do
  if docker exec "${DB_CONTAINER}" pg_isready -U "${DB_USER}" -d "${DB_NAME}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

if ! docker exec "${DB_CONTAINER}" pg_isready -U "${DB_USER}" -d "${DB_NAME}" >/dev/null 2>&1; then
  echo "Postgres did not become ready in time."
  exit 1
fi

docker run -d \
  --name "${WEB_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --restart unless-stopped \
  --env-file "${ENV_FILE}" \
  -e DATABASE_URL="${DATABASE_URL}" \
  -e EXPORT_DIR=/app/data/exports \
  -p "${WEB_PORT}:8000" \
  -v "${EXPORT_VOLUME}:/app/data/exports" \
  "${IMAGE_NAME}" >/dev/null

docker run -d \
  --name "${WORKER_CONTAINER}" \
  --network "${NETWORK_NAME}" \
  --restart unless-stopped \
  --env-file "${ENV_FILE}" \
  -e DATABASE_URL="${DATABASE_URL}" \
  -e EXPORT_DIR=/app/data/exports \
  -v "${EXPORT_VOLUME}:/app/data/exports" \
  "${IMAGE_NAME}" \
  python -m app.worker >/dev/null

echo "Stack is up."
echo "Dashboard: http://127.0.0.1:${WEB_PORT}"
echo "Containers: ${DB_CONTAINER}, ${WEB_CONTAINER}, ${WORKER_CONTAINER}"
