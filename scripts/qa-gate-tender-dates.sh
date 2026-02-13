#!/usr/bin/env bash
set -euo pipefail

# Civant: Tender Date QA Gate
# Convenience wrapper for scripts/qa-gate-tender-dates.sql.
#
# Fails (non-zero) if deadline < publication, or publication is missing, beyond configured thresholds.
#
# Env vars:
#   DATABASE_URL / SUPABASE_DB_URL (required)
#   TENANT_ID (default: civant_default)
#   SOURCE (optional; default: all sources for tenant)
#   MAX_INVERSIONS (default: 0)
#   MAX_INVERSION_PCT (default: 0)
#   MAX_PUB_NULLS (default: 0)
#   MAX_PUB_NULL_PCT (default: 0)
#   STATEMENT_TIMEOUT (default: 0)

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${TENANT_ID:-civant_default}"
SOURCE="${SOURCE:-}"
MAX_INVERSIONS="${MAX_INVERSIONS:-0}"
MAX_INVERSION_PCT="${MAX_INVERSION_PCT:-0}"
MAX_PUB_NULLS="${MAX_PUB_NULLS:-0}"
MAX_PUB_NULL_PCT="${MAX_PUB_NULL_PCT:-0}"
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-0}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: DATABASE_URL (or SUPABASE_DB_URL) is required."
  exit 1
fi

if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi

SQL_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/qa-gate-tender-dates.sql"
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
  -v max_inversions="${MAX_INVERSIONS}" \
  -v max_inversion_pct="${MAX_INVERSION_PCT}" \
  -v max_pub_nulls="${MAX_PUB_NULLS}" \
  -v max_pub_null_pct="${MAX_PUB_NULL_PCT}" \
  -v statement_timeout="${STATEMENT_TIMEOUT}" \
  -f "${SQL_FILE}"

