#!/usr/bin/env bash
set -euo pipefail

# Canonical E2E wrapper
# Usage:
#   ./scripts/run-canonical-e2e-test.sh [TENANT_ID]
#
# Env:
#   SUPABASE_DB_URL or DATABASE_URL (required)
#   PSQL_BIN (optional; defaults to /opt/homebrew/opt/libpq/bin/psql when available)

TENANT_ID="${1:-${TENANT_ID:-qa_canonical_test}}"
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/qa-canonical-e2e.sql"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "FAIL: SUPABASE_DB_URL (or DATABASE_URL) is required"
  exit 1
fi

if [[ "${PSQL_BIN}" == */* ]]; then
  if [[ ! -x "${PSQL_BIN}" ]]; then
    echo "FAIL: psql not found at ${PSQL_BIN}"
    exit 1
  fi
else
  if ! command -v "${PSQL_BIN}" >/dev/null 2>&1; then
    echo "FAIL: psql not found on PATH"
    exit 1
  fi
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "FAIL: SQL file not found: ${SQL_FILE}"
  exit 1
fi

cleanup() {
  local rc=$?

  # Best-effort cleanup, always scoped to the dedicated tenant.
  "${PSQL_BIN}" "${DATABASE_URL}" \
    -v ON_ERROR_STOP=0 \
    -P pager=off \
    -v tenant_id="${TENANT_ID}" \
    -f - >/dev/null 2>&1 <<'SQL'
\set ON_ERROR_STOP on

-- Cleanup in FK-safe order.
delete from public.canonical_notice_links
where tenant_id = :'tenant_id'::text;

delete from public.notices
where tenant_id = :'tenant_id'::text;

delete from public.reconciliation_queue
where tenant_id = :'tenant_id'::text;

delete from public.canonical_tenders
where tenant_id = :'tenant_id'::text;
SQL

  if [[ ${rc} -eq 0 ]]; then
    echo "PASS: canonical E2E test passed (tenant=${TENANT_ID})"
    echo "PASS: cleanup completed for tenant=${TENANT_ID}"
  else
    echo "FAIL: canonical E2E test failed (tenant=${TENANT_ID})"
    echo "INFO: cleanup attempted for tenant=${TENANT_ID}"
  fi

  exit ${rc}
}
trap cleanup EXIT

echo "== Canonical E2E test run =="
echo "tenant_id=${TENANT_ID}"

# Pre-clean to ensure deterministic isolated state.
"${PSQL_BIN}" "${DATABASE_URL}" \
  -v ON_ERROR_STOP=1 \
  -P pager=off \
  -v tenant_id="${TENANT_ID}" \
  -f - <<'SQL'
\set ON_ERROR_STOP on

delete from public.canonical_notice_links
where tenant_id = :'tenant_id'::text;

delete from public.notices
where tenant_id = :'tenant_id'::text;

delete from public.reconciliation_queue
where tenant_id = :'tenant_id'::text;

delete from public.canonical_tenders
where tenant_id = :'tenant_id'::text;
SQL

"${PSQL_BIN}" "${DATABASE_URL}" \
  -v ON_ERROR_STOP=1 \
  -P pager=off \
  -v tenant_id="${TENANT_ID}" \
  -f "${SQL_FILE}"
