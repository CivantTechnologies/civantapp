#!/usr/bin/env bash
set -euo pipefail

# Merge-only CPV backfill for BOAMP_FR:
# - Parses BOAMP CSV into canonical_id -> cpv_csv map
# - Loads map into staging table
# - Updates canonical_tenders and "TendersCurrent" CPV fields (tenant-scoped)
# - Idempotent and safe to rerun

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
NODE_BIN="${NODE_BIN:-node}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${TENANT_ID:-civant_default}"
INPUT_CSV="${INPUT_CSV:-/Users/davidmanrique/projects/boamp.csv}"
MAP_FILE="${MAP_FILE:-/tmp/boamp-cpv-map.tsv}"
LOG_FILE="${LOG_FILE:-/tmp/boamp-cpv-merge-only.log}"
PARSER_SCRIPT="${PARSER_SCRIPT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/build-boamp-cpv-map.mjs}"
MAX_ROWS="${MAX_ROWS:-}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: DATABASE_URL (or SUPABASE_DB_URL) is required."
  exit 1
fi

if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-60}"

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

if [[ "${MAP_FILE}" == *"'"* || "${INPUT_CSV}" == *"'"* ]]; then
  echo "ERROR: INPUT_CSV and MAP_FILE cannot contain single quotes."
  exit 1
fi

if [[ -n "${MAX_ROWS}" && ! "${MAX_ROWS}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_ROWS must be a positive integer."
  exit 1
fi

echo "[0/4] Building BOAMP CPV map..."
echo "  input  : ${INPUT_CSV}"
echo "  map    : ${MAP_FILE}"
echo "  tenant : ${TENANT_ID}"

PARSER_ARGS=(--input "${INPUT_CSV}" --output "${MAP_FILE}")
if [[ -n "${MAX_ROWS}" ]]; then
  PARSER_ARGS+=(--max-rows "${MAX_ROWS}")
fi
"${NODE_BIN}" "${PARSER_SCRIPT}" "${PARSER_ARGS[@]}"

if [[ ! -s "${MAP_FILE}" ]]; then
  echo "ERROR: generated map is empty: ${MAP_FILE}"
  exit 1
fi

SQL_FILE="$(mktemp /tmp/boamp-cpv-merge-only.XXXXXX.sql)"
trap 'rm -f "${SQL_FILE}"' EXIT

cat > "${SQL_FILE}" <<SQL
\set ON_ERROR_STOP on
\timing on

set statement_timeout = 0;
set lock_timeout = 0;
set idle_in_transaction_session_timeout = 0;

\echo '[1/4] Loading CPV stage map'
drop table if exists _stg_boamp_cpv;
create unlogged table _stg_boamp_cpv (
  canonical_id text primary key,
  cpv_csv text not null
);
\copy _stg_boamp_cpv (canonical_id, cpv_csv) from '${MAP_FILE}' with (format text);
analyze _stg_boamp_cpv;

\echo '[2/4] Updating canonical_tenders'
with updated as (
  update canonical_tenders c
  set
    cpv_codes = string_to_array(s.cpv_csv, ','),
    normalized_json = jsonb_set(
      coalesce(c.normalized_json, '{}'::jsonb),
      '{cpv_codes}',
      to_jsonb(string_to_array(s.cpv_csv, ',')),
      true
    ),
    updated_at = now()
  from _stg_boamp_cpv s
  where c.canonical_id = s.canonical_id
    and c.source = 'BOAMP_FR'
    and c.tenant_id = '${TENANT_ID}'
    and (
      c.cpv_codes is distinct from string_to_array(s.cpv_csv, ',')
      or c.normalized_json->'cpv_codes' is distinct from to_jsonb(string_to_array(s.cpv_csv, ','))
    )
  returning 1
)
select count(*) as canonical_updated from updated;

\echo '[3/4] Updating "TendersCurrent"'
with updated as (
  update "TendersCurrent" t
  set
    data = jsonb_set(
      coalesce(t.data, '{}'::jsonb),
      '{cpv_codes}',
      to_jsonb(string_to_array(s.cpv_csv, ',')),
      true
    ),
    updated_at = now()
  from _stg_boamp_cpv s
  where t.tender_id = s.canonical_id
    and t.source = 'BOAMP_FR'
    and t.tenant_id = '${TENANT_ID}'
    and t.data->'cpv_codes' is distinct from to_jsonb(string_to_array(s.cpv_csv, ','))
  returning 1
)
select count(*) as current_updated from updated;

\echo '[4/4] Validation summary'
select
  count(*) as boamp_total,
  count(*) filter (where coalesce(array_length(cpv_codes, 1), 0) > 0) as boamp_with_cpv
from canonical_tenders
where source = 'BOAMP_FR'
  and tenant_id = '${TENANT_ID}';

select
  count(*) as boamp_current_total,
  count(*) filter (where jsonb_typeof(data->'cpv_codes') = 'array' and jsonb_array_length(data->'cpv_codes') > 0) as boamp_current_with_cpv
from "TendersCurrent"
where source = 'BOAMP_FR'
  and tenant_id = '${TENANT_ID}';

select
  (select count(*) from _stg_boamp_cpv) as stage_rows,
  (select count(*) from canonical_tenders c join _stg_boamp_cpv s on s.canonical_id = c.canonical_id where c.source = 'BOAMP_FR' and c.tenant_id = '${TENANT_ID}') as stage_matches_canonical,
  (select count(*) from canonical_tenders c join _stg_boamp_cpv s on s.canonical_id = c.canonical_id where c.source = 'BOAMP_FR' and c.tenant_id = '${TENANT_ID}' and coalesce(array_length(c.cpv_codes, 1), 0) > 0) as canonical_with_cpv_after,
  (select count(*) from "TendersCurrent" t join _stg_boamp_cpv s on s.canonical_id = t.tender_id where t.source = 'BOAMP_FR' and t.tenant_id = '${TENANT_ID}' and jsonb_typeof(t.data->'cpv_codes') = 'array' and jsonb_array_length(t.data->'cpv_codes') > 0) as current_with_cpv_after;
SQL

echo "Running merge..."
"${PSQL_BIN}" "${DATABASE_URL}" -f "${SQL_FILE}" 2>&1 | tee "${LOG_FILE}"

echo "Done: BOAMP_FR CPV merge-only completed."
echo "Log: ${LOG_FILE}"
