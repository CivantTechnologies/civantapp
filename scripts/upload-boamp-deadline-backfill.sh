#!/usr/bin/env bash
set -euo pipefail

# BOAMP_FR deadline backfill (tender rows only):
# - Finds BOAMP tender rows with deadline_date IS NULL in canonical_tenders (tenant-scoped)
# - Parses boamp.csv to extract deadlines (datelimitereponse or DONNEES fallback)
# - Updates canonical_tenders + "TendersCurrent" only when safe:
#   - deadline_date is currently NULL
#   - publication_date is present
#   - new_deadline_date >= publication_date (prevents inversions)
#
# Idempotent and safe to rerun.

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
NODE_BIN="${NODE_BIN:-node}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${TENANT_ID:-civant_default}"
INPUT_CSV="${INPUT_CSV:-/Users/davidmanrique/projects/boamp.csv}"
TARGETS_FILE="${TARGETS_FILE:-/tmp/boamp-deadline-targets.txt}"
MAP_FILE="${MAP_FILE:-/tmp/boamp-deadline-map.tsv}"
LOG_FILE="${LOG_FILE:-/tmp/boamp-deadline-backfill.log}"
PARSER_SCRIPT="${PARSER_SCRIPT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/build-boamp-deadline-map.mjs}"
MAX_ROWS="${MAX_ROWS:-}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: DATABASE_URL (or SUPABASE_DB_URL) is required."
  exit 1
fi

if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi

if ! command -v "${NODE_BIN}" >/dev/null 2>&1; then
  echo "ERROR: node not found (${NODE_BIN})"
  exit 1
fi

if [[ ! -f "${PARSER_SCRIPT}" ]]; then
  echo "ERROR: parser script not found: ${PARSER_SCRIPT}"
  exit 1
fi

if [[ ! -f "${INPUT_CSV}" ]]; then
  echo "ERROR: input CSV not found: ${INPUT_CSV}"
  exit 1
fi

if [[ ! "${TENANT_ID}" =~ ^[A-Za-z0-9_:-]+$ ]]; then
  echo "ERROR: TENANT_ID contains unsupported characters: ${TENANT_ID}"
  exit 1
fi

if [[ "${MAP_FILE}" == *"'"* || "${INPUT_CSV}" == *"'"* || "${TARGETS_FILE}" == *"'"* ]]; then
  echo "ERROR: INPUT_CSV, TARGETS_FILE and MAP_FILE cannot contain single quotes."
  exit 1
fi

if [[ -n "${MAX_ROWS}" && ! "${MAX_ROWS}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_ROWS must be a positive integer."
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-60}"

echo "[0/4] Exporting BOAMP tender rows missing deadlines (targets)..."
echo "  tenant : ${TENANT_ID}"
echo "  targets: ${TARGETS_FILE}"

"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -c "\\copy (select canonical_id from canonical_tenders where tenant_id = '${TENANT_ID}' and source = 'BOAMP_FR' and coalesce(normalized_json->>'notice_type','') = 'tender' and deadline_date is null) to '${TARGETS_FILE}' with (format text);" >/dev/null

if [[ ! -s "${TARGETS_FILE}" ]]; then
  echo "No targets found (already complete). Exiting."
  exit 0
fi

echo "[1/4] Building deadline map from CSV..."
echo "  input  : ${INPUT_CSV}"
echo "  map    : ${MAP_FILE}"

PARSER_ARGS=(--input "${INPUT_CSV}" --targets "${TARGETS_FILE}" --output "${MAP_FILE}")
if [[ -n "${MAX_ROWS}" ]]; then
  PARSER_ARGS+=(--max-rows "${MAX_ROWS}")
fi
"${NODE_BIN}" "${PARSER_SCRIPT}" "${PARSER_ARGS[@]}"

if [[ ! -s "${MAP_FILE}" ]]; then
  echo "ERROR: generated map is empty: ${MAP_FILE}"
  exit 1
fi

echo "[2/4] Merging backfilled deadlines into Supabase..."

SQL_FILE="$(mktemp /tmp/boamp-deadline-backfill.XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

cat > "${SQL_FILE}" <<SQL
\\set ON_ERROR_STOP on
\\timing on

set statement_timeout = 0;
set lock_timeout = 0;
set idle_in_transaction_session_timeout = 0;

\\echo 'Loading stage map'
drop table if exists _stg_boamp_deadline_backfill;
create unlogged table _stg_boamp_deadline_backfill (
  canonical_id text primary key,
  deadline_date date not null,
  deadline_date_source text
);
\\copy _stg_boamp_deadline_backfill (canonical_id, deadline_date, deadline_date_source) from '${MAP_FILE}' with (format text);
analyze _stg_boamp_deadline_backfill;

\\echo 'Pre-merge: targets + stage rows'
select
  (select count(*) from _stg_boamp_deadline_backfill) as stage_rows,
  (select count(*) from canonical_tenders
    where tenant_id = '${TENANT_ID}'
      and source = 'BOAMP_FR'
      and coalesce(normalized_json->>'notice_type','') = 'tender'
      and deadline_date is null
  ) as canonical_missing_deadline_before;

begin;

\\echo 'Updating canonical_tenders'
with updated as (
  update canonical_tenders c
  set
    deadline_date = s.deadline_date,
    normalized_json = jsonb_set(
      jsonb_set(
        coalesce(c.normalized_json, '{}'::jsonb),
        '{deadline_date}',
        to_jsonb(to_char(s.deadline_date, 'YYYY-MM-DD')),
        true
      ),
      '{deadline_date_source}',
      to_jsonb(coalesce(nullif(s.deadline_date_source, ''), 'backfill'::text)),
      true
    ),
    updated_at = now()
  from _stg_boamp_deadline_backfill s
  where c.canonical_id = s.canonical_id
    and c.tenant_id = '${TENANT_ID}'
    and c.source = 'BOAMP_FR'
    and coalesce(c.normalized_json->>'notice_type','') = 'tender'
    and c.publication_date is not null
    and c.deadline_date is null
    and s.deadline_date >= c.publication_date
  returning 1
)
select count(*) as canonical_updated from updated;

\\echo 'Updating \"TendersCurrent\"'
with updated as (
  update "TendersCurrent" t
  set
    data = jsonb_set(
      jsonb_set(
        coalesce(t.data, '{}'::jsonb),
        '{deadline_date}',
        to_jsonb(to_char(s.deadline_date, 'YYYY-MM-DD')),
        true
      ),
      '{deadline_date_source}',
      to_jsonb(coalesce(nullif(s.deadline_date_source, ''), 'backfill'::text)),
      true
    ),
    updated_at = now()
  from _stg_boamp_deadline_backfill s
  join canonical_tenders c
    on c.tenant_id = '${TENANT_ID}'
   and c.source = 'BOAMP_FR'
   and c.canonical_id = s.canonical_id
  where t.tenant_id = '${TENANT_ID}'
    and t.source = 'BOAMP_FR'
    and t.tender_id = s.canonical_id
    and c.deadline_date = s.deadline_date
    and (
      nullif(t.data->>'deadline_date','') is distinct from to_char(s.deadline_date, 'YYYY-MM-DD')
      or nullif(t.data->>'deadline_date_source','') is distinct from coalesce(nullif(s.deadline_date_source, ''), 'backfill')
    )
  returning 1
)
select count(*) as current_updated from updated;

commit;

\\echo 'Post-merge validation'
select
  count(*) as boamp_tender_total,
  count(*) filter (where deadline_date is null) as boamp_tender_deadline_null_after,
  count(*) filter (where deadline_date is not null) as boamp_tender_deadline_present_after
from canonical_tenders
where tenant_id = '${TENANT_ID}'
  and source = 'BOAMP_FR'
  and coalesce(normalized_json->>'notice_type','') = 'tender';

select
  count(*) as inversions_after
from canonical_tenders
where tenant_id = '${TENANT_ID}'
  and source = 'BOAMP_FR'
  and publication_date is not null
  and deadline_date is not null
  and deadline_date < publication_date;
SQL

"${PSQL_BIN}" "${DATABASE_URL}" -f "${SQL_FILE}" 2>&1 | tee "${LOG_FILE}"

echo "[3/4] Done: BOAMP deadline backfill completed."
echo "Log: ${LOG_FILE}"
