#!/bin/sh
set -eu

python -m app.worker &
worker_pid="$!"

cleanup() {
  kill "$worker_pid" 2>/dev/null || true
  wait "$worker_pid" 2>/dev/null || true
}

trap cleanup INT TERM EXIT

uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
