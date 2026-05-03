#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-conduit-tradeshow}"

docker rm -f "${STACK_NAME}-worker" >/dev/null 2>&1 || true
docker rm -f "${STACK_NAME}-web" >/dev/null 2>&1 || true
docker rm -f "${STACK_NAME}-db" >/dev/null 2>&1 || true

echo "Stopped containers for ${STACK_NAME}."
