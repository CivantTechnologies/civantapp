#!/usr/bin/env bash
set -euo pipefail

# Read-only coverage check for canonical_tenders vs "TendersCurrent".
# Defaults to tenant civant_default; optionally narrow to a single SOURCE.

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${TENANT_ID:-civant_default}"
SOURCE="${SOURCE:-}"
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-10min}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: DATABASE_URL (or SUPABASE_DB_URL) is required."
  exit 1
fi

if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi

SQL_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/check-tenderscurrent-coverage.sql"
if [[ ! -f "${SQL_FILE}" ]]; then
  echo "ERROR: SQL file not found at ${SQL_FILE}"
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-60}"

"${PSQL_BIN}" "${DATABASE_URL}" \
  -v ON_ERROR_STOP=1 \
  -P pager=off \
  -v tenant_id="${TENANT_ID}" \
  -v source="${SOURCE}" \
  -v statement_timeout="${STATEMENT_TIMEOUT}" \
  -f "${SQL_FILE}"
