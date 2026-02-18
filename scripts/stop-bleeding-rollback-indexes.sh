#!/usr/bin/env bash
set -euo pipefail

# Rollback for stop-the-bleeding index drops (CONCURRENTLY, outside a transaction).
# Safe to run even if indexes already exist.
#
# Usage:
#   ./scripts/stop-bleeding-rollback-indexes.sh
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

echo "[rollback] Recreating public.idx_notices_raw_payload_gin (CONCURRENTLY)..."
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c \
  "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_notices_raw_payload_gin ON public.notices USING gin (raw_payload);"

echo "[rollback] Recreating public.idx_canonical_notice_links_tenant_notice (CONCURRENTLY)..."
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c \
  "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_canonical_notice_links_tenant_notice ON public.canonical_notice_links (tenant_id, notice_id);"

echo "[rollback] Done."

