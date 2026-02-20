#!/usr/bin/env bash
set -euo pipefail

# Backfill buyer entities from canonical tenders (ETENDERS_IE by default).
# Resumable by design: each run only processes currently-unlinked buyers.
#
# Usage:
#   ./scripts/backfill-ie-buyer-entities.sh [TENANT_ID] [SOURCE] [BATCH_SIZE] [MAX_BATCHES]
#
# Defaults:
#   TENANT_ID=civant_default
#   SOURCE=ETENDERS_IE
#   BATCH_SIZE=500
#   MAX_BATCHES=0   (0 = run until fully complete)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEFAULT_PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "${PSQL_BIN}" ]]; then
  if [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
    PSQL_BIN="${DEFAULT_PSQL_BIN}"
  else
    PSQL_BIN="psql"
  fi
fi

if [[ "${PSQL_BIN}" == */* ]]; then
  if [[ ! -x "${PSQL_BIN}" ]]; then
    echo "ERROR: psql not found at ${PSQL_BIN}" >&2
    exit 1
  fi
else
  if ! command -v "${PSQL_BIN}" >/dev/null 2>&1; then
    echo "ERROR: psql not found on PATH: ${PSQL_BIN}" >&2
    exit 1
  fi
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required." >&2
  exit 1
fi

TENANT_ID="${1:-civant_default}"
SOURCE="${2:-ETENDERS_IE}"
BATCH_SIZE="${3:-500}"
MAX_BATCHES="${4:-0}"

if [[ ! "${BATCH_SIZE}" =~ ^[0-9]+$ || "${BATCH_SIZE}" -le 0 ]]; then
  echo "ERROR: BATCH_SIZE must be a positive integer. Got: ${BATCH_SIZE}" >&2
  exit 1
fi
if [[ ! "${MAX_BATCHES}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_BATCHES must be a non-negative integer. Got: ${MAX_BATCHES}" >&2
  exit 1
fi

STATE_DIR="${REPO_ROOT}/scripts/.state"
mkdir -p "${STATE_DIR}"
STATE_FILE="${STATE_DIR}/ie-buyer-entity-backfill-${TENANT_ID}-${SOURCE}.state"

batch_no=0
total_entities=0
total_linked=0
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "== IE buyer entity backfill start =="
echo "tenant=${TENANT_ID} source=${SOURCE} batch_size=${BATCH_SIZE} max_batches=${MAX_BATCHES}"
echo "started_at=${started_at}"

after_batch_status() {
  local entities="$1"
  local linked="$2"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  cat > "${STATE_FILE}" <<STATE
updated_at=${ts}
tenant_id=${TENANT_ID}
source=${SOURCE}
batch_size=${BATCH_SIZE}
last_batch=${batch_no}
last_entities_created=${entities}
last_rows_linked=${linked}
total_entities_created=${total_entities}
total_rows_linked=${total_linked}
STATE
}

while true; do
  batch_no=$((batch_no + 1))

  result_line="$(${PSQL_BIN} "${DATABASE_URL}" -v ON_ERROR_STOP=1 -At \
    -v tenant_id="${TENANT_ID}" \
    -v source="${SOURCE}" \
    -v batch_size="${BATCH_SIZE}" <<'SQL'
select coalesce(entities_created, 0)::text || '|' || coalesce(rows_linked, 0)::text
from public.backfill_buyer_entities_from_canonical(
  :'tenant_id',
  :'source',
  (:'batch_size')::int
);
SQL
)"

  entities_created="${result_line%%|*}"
  rows_linked="${result_line##*|}"

  if [[ ! "${entities_created}" =~ ^[0-9]+$ || ! "${rows_linked}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Unexpected function output: ${result_line}" >&2
    exit 1
  fi

  total_entities=$((total_entities + entities_created))
  total_linked=$((total_linked + rows_linked))

  echo "batch=${batch_no} entities_created=${entities_created} rows_linked=${rows_linked} total_entities=${total_entities} total_linked=${total_linked}"
  after_batch_status "${entities_created}" "${rows_linked}"

  if [[ "${entities_created}" -eq 0 && "${rows_linked}" -eq 0 ]]; then
    echo "No more work detected (0/0). Backfill complete."
    break
  fi

  if [[ "${MAX_BATCHES}" -gt 0 && "${batch_no}" -ge "${MAX_BATCHES}" ]]; then
    echo "Reached MAX_BATCHES=${MAX_BATCHES}. Stop requested. Resume by running the script again."
    break
  fi

done

ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "ended_at=${ended_at}"
echo "state_file=${STATE_FILE}"
