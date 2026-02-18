#!/usr/bin/env bash
set -euo pipefail

# Drops the two biggest/redundant indexes CONCURRENTLY (must run outside a transaction).
# Safe to re-run.
#
# Usage:
#   ./scripts/stop-bleeding-drop-indexes.sh
#
# Requires:
#   SUPABASE_DB_URL or DATABASE_URL

DEFAULT_PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "${PSQL_BIN}" ]]; then
  if [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
    PSQL_BIN="${DEFAULT_PSQL_BIN}"
  else
    PSQL_BIN="psql"
  fi
fi

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required."
  exit 1
fi

echo "[stop-bleeding] Dropping public.idx_notices_raw_payload_gin (CONCURRENTLY)..."
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c \
  "DROP INDEX CONCURRENTLY IF EXISTS public.idx_notices_raw_payload_gin;"

echo "[stop-bleeding] Dropping public.idx_canonical_notice_links_tenant_notice (CONCURRENTLY)..."
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c \
  "DROP INDEX CONCURRENTLY IF EXISTS public.idx_canonical_notice_links_tenant_notice;"

echo "[stop-bleeding] Done."

