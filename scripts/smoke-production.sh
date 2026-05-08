#!/bin/sh
set -eu

TMP_ROOT="${TMPDIR:-/tmp}"
SMOKE_DIR="$(mktemp -d "${TMP_ROOT}/hpointscraper-smoke.XXXXXX")"
PORT="${PORT:-8011}"
SERVER_LOG="${SMOKE_DIR}/server.log"
SERVER_PID=""

cleanup() {
  if [ -n "${SERVER_PID}" ]; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${SMOKE_DIR}"
}

trap cleanup EXIT INT TERM

export PORT
export SESSION_SECRET="${SESSION_SECRET:-smoke-session-secret}"
export DASHBOARD_USERNAME="${DASHBOARD_USERNAME:-admin}"
export DASHBOARD_PASSWORD="${DASHBOARD_PASSWORD:-change-me-now}"
export DATABASE_URL="${DATABASE_URL:-sqlite:///${SMOKE_DIR}/trade_show_app.db}"
export EXPORT_DIR="${EXPORT_DIR:-${SMOKE_DIR}/exports}"

python -m uvicorn app.main:app --host 127.0.0.1 --port "${PORT}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID="$!"

attempt=0
until curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "${attempt}" -ge 20 ]; then
    cat "${SERVER_LOG}" >&2
    echo "Smoke check failed: server never became healthy." >&2
    exit 1
  fi
  sleep 1
done

HEALTH_RESPONSE="$(curl -fsS "http://127.0.0.1:${PORT}/healthz")"
if [ "${HEALTH_RESPONSE}" != '{"status":"ok"}' ]; then
  echo "Unexpected /healthz response: ${HEALTH_RESPONSE}" >&2
  exit 1
fi

ROOT_HEADERS="$(curl -sS -D - -o /dev/null "http://127.0.0.1:${PORT}/")"
printf '%s' "${ROOT_HEADERS}" | grep -q "303 See Other"
printf '%s' "${ROOT_HEADERS}" | grep -qi "^location: /shows/dashboard"

DASHBOARD_HTML="$(curl -fsS "http://127.0.0.1:${PORT}/shows/dashboard")"
printf '%s' "${DASHBOARD_HTML}" | grep -q "<title>Show Dashboard</title>"
printf '%s' "${DASHBOARD_HTML}" | grep -q "Trade Show Dashboard"
