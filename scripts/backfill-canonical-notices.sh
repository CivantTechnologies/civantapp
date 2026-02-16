#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/backfill-canonical-notices.sh TENANT_ID [LIMIT]
# Requires:
#   SUPABASE_DB_URL (or DATABASE_URL)

TENANT_ID="${1:-${TENANT_ID:-}}"
LIMIT="${2:-${LIMIT:-}}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"

DEFAULT_PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "${PSQL_BIN}" ]]; then
  if [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
    PSQL_BIN="${DEFAULT_PSQL_BIN}"
  else
    PSQL_BIN="psql"
  fi
fi

if [[ -z "${TENANT_ID}" ]]; then
  echo "ERROR: TENANT_ID is required"
  echo "Usage: $0 TENANT_ID [LIMIT]"
  exit 1
fi

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required"
  exit 1
fi

if [[ -n "${LIMIT}" && ! "${LIMIT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: LIMIT must be an integer"
  exit 1
fi

if [[ "${PSQL_BIN}" == */* ]]; then
  if [[ ! -x "${PSQL_BIN}" ]]; then
    echo "ERROR: psql not found at ${PSQL_BIN}"
    exit 1
  fi
else
  if ! command -v "${PSQL_BIN}" >/dev/null 2>&1; then
    echo "ERROR: psql not found on PATH"
    exit 1
  fi
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

echo "== Backfill canonical notices =="
echo "tenant_id=${TENANT_ID} limit=${LIMIT:-<all>}"

echo "-- trigger status before backfill --"
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -c \
  "select tgname, tgenabled from pg_trigger where tgrelid = 'public.\"TendersCurrent\"'::regclass and tgname = 'trg_tenderscurrent_sync_notice';"

if [[ -n "${LIMIT}" ]]; then
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -c \
    "set statement_timeout = 0; select * from public.backfill_notices_from_tenderscurrent('${TENANT_ID}'::text, ${LIMIT}::integer, true);"
else
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -c \
    "set statement_timeout = 0; select * from public.backfill_notices_from_tenderscurrent('${TENANT_ID}'::text, null, true);"
fi

echo "-- trigger status after backfill --"
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -c \
  "select tgname, tgenabled from pg_trigger where tgrelid = 'public.\"TendersCurrent\"'::regclass and tgname = 'trg_tenderscurrent_sync_notice';"
