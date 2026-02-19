#!/usr/bin/env bash
set -euo pipefail

# Civant: PLACSP ES incremental rollout
#
# Extracts incremental PLACSP feed updates (local mode) and upserts into:
#   - public."TendersCurrent"
#   - public."TenderVersions"
#
# Cursor is stored per tenant in public."ConnectorConfig".config under connector_key:
#   placsp_es_incremental:<TENANT_ID>
#
# Usage:
#   ./scripts/rollout-placsp-es-incremental.sh TENANT_ID [START_DATE] [DRY_RUN]

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

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${1:-${TENANT_ID:-}}"
START_DATE="${2:-${START_DATE:-}}"
DRY_RUN_RAW="${3:-${DRY_RUN:-false}}"

API_BASE_URL="${API_BASE_URL:-https://civantapp.vercel.app}"
APP_ID="${APP_ID:-civantapp}"
MAX_PAGES="${MAX_PAGES:-40}"
LOOKBACK_MINUTES="${LOOKBACK_MINUTES:-180}"
BATCH_SIZE="${BATCH_SIZE:-120}"
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-0}"

if [[ -z "${TENANT_ID}" ]]; then
  echo "ERROR: TENANT_ID is required."
  echo "Usage: $0 TENANT_ID [START_DATE] [DRY_RUN]"
  exit 1
fi

DRY_RUN="false"
case "${DRY_RUN_RAW}" in
  1|true|TRUE|yes|YES|y|Y|on|ON) DRY_RUN="true" ;;
  0|false|FALSE|no|NO|n|N|off|OFF|"") DRY_RUN="false" ;;
  *)
    echo "ERROR: DRY_RUN must be true/false. Got: ${DRY_RUN_RAW}"
    exit 1
    ;;
esac

if [[ "${DRY_RUN}" != "true" && -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required unless DRY_RUN=true."
  exit 1
fi

if [[ "${PSQL_BIN}" == */* ]]; then
  if [[ ! -x "${PSQL_BIN}" ]]; then
    echo "ERROR: psql not found at ${PSQL_BIN}"
    exit 1
  fi
else
  if ! command -v "${PSQL_BIN}" >/dev/null 2>&1; then
    echo "ERROR: psql not found on PATH: ${PSQL_BIN}"
    exit 1
  fi
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: node is required but was not found on PATH."
  exit 1
fi

iso_days_ago() {
  local days="$1"
  if date -u -v-"${days}"d +"%Y-%m-%d" >/dev/null 2>&1; then
    date -u -v-"${days}"d +"%Y-%m-%d"
  else
    date -u -d "${days} days ago" +"%Y-%m-%d"
  fi
}

if [[ -n "${START_DATE}" && ! "${START_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: START_DATE must be YYYY-MM-DD. Got: ${START_DATE}"
  exit 1
fi

PLACSP_SCRIPT="${REPO_ROOT}/scripts/import-placsp-es.mjs"
QA_SQL="${REPO_ROOT}/scripts/qa-placsp-es-incremental.sql"
RECON_SCRIPT="${REPO_ROOT}/scripts/reconcile-ted-national.sh"
RECONCILE_AFTER_INGEST="${RECONCILE_AFTER_INGEST:-true}"
RECONCILE_STRICT="${RECONCILE_STRICT:-false}"
RECONCILE_LIMIT="${RECONCILE_LIMIT:-20}"

TMP_DIR="${TMPDIR:-/tmp}"
RUN_TMP_DIR="$(mktemp -d "${TMP_DIR%/}/civant_placsp_es_XXXXXX" 2>/dev/null || mktemp -d -t civant_placsp_es)"
STATUS_FILE="${RUN_TMP_DIR}/status.json"
CHECKPOINT_FILE="${RUN_TMP_DIR}/checkpoint.json"
RESULT_FILE="${RUN_TMP_DIR}/result.json"
LOCAL_OUTPUT_DIR="${RUN_TMP_DIR}/local"
CURRENT_FILE="${LOCAL_OUTPUT_DIR}/TendersCurrent.ndjson"

cleanup() {
  rm -rf "${RUN_TMP_DIR}" 2>/dev/null || true
}
trap cleanup EXIT

CONNECTOR_KEY="placsp_es_incremental:${TENANT_ID}"
RUN_ID=""

if [[ -z "${START_DATE}" ]]; then
  if [[ "${DRY_RUN}" != "true" && -n "${DATABASE_URL}" ]]; then
    START_DATE="$(${PSQL_BIN} "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA <<SQL
select
  case
    when (config->'cursor'->>'value') is null or (config->'cursor'->>'value') = '' then ''
    else to_char(((config->'cursor'->>'value')::timestamptz - interval '2 days')::date, 'YYYY-MM-DD')
  end
from public."ConnectorConfig"
where tenant_id = '${TENANT_ID}'
  and connector_key = '${CONNECTOR_KEY}'
limit 1;
SQL
)"
  fi

  if [[ -z "${START_DATE}" ]]; then
    START_DATE="$(iso_days_ago 3)"
  fi
fi

SINCE_ISO="${START_DATE}T00:00:00.000Z"

echo "== PLACSP ES incremental rollout =="
echo "tenant_id=${TENANT_ID} start_date=${START_DATE} since_iso=${SINCE_ISO} dry_run=${DRY_RUN} connector_key=${CONNECTOR_KEY} max_pages=${MAX_PAGES}"

if [[ "${DRY_RUN}" != "true" ]]; then
  if ! "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA -c "select 1;" >/dev/null; then
    echo "ERROR: database connection preflight failed."
    exit 1
  fi

  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v statement_timeout="${STATEMENT_TIMEOUT}" <<SQL >/dev/null
insert into public."ConnectorConfig" (tenant_id, connector_key, enabled, config, updated_at)
values ('${TENANT_ID}', '${CONNECTOR_KEY}', true, '{}'::jsonb, now())
on conflict (connector_key) do update
  set tenant_id = excluded.tenant_id,
      enabled = true,
      updated_at = now();
SQL

  RUN_ID="$(${PSQL_BIN} "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA <<SQL
insert into public."ConnectorRuns" (tenant_id, connector_key, status, started_at, metadata)
values (
  '${TENANT_ID}',
  '${CONNECTOR_KEY}',
  'running',
  now(),
  jsonb_build_object(
    'source', 'PLACSP_ES',
    'mode', 'incremental',
    'since_iso', '${SINCE_ISO}',
    'max_pages', ${MAX_PAGES},
    'dry_run', false
  )
)
returning id;
SQL
)"
fi

run_import_local() {
  node "${PLACSP_SCRIPT}" \
    --mode incremental \
    --sink local \
    --api-base "${API_BASE_URL}" \
    --app-id "${APP_ID}" \
    --tenant-id "${TENANT_ID}" \
    --since-iso "${SINCE_ISO}" \
    --lookback-minutes "${LOOKBACK_MINUTES}" \
    --max-pages "${MAX_PAGES}" \
    --batch-size "${BATCH_SIZE}" \
    --status-file "${STATUS_FILE}" \
    --checkpoint-file "${CHECKPOINT_FILE}" \
    --local-output-dir "${LOCAL_OUTPUT_DIR}" \
    > "${RESULT_FILE}"
}

echo "== Fetching incremental PLACSP feed (local extract) =="
if ! run_import_local; then
  echo "ERROR: PLACSP local extract failed."

  if [[ "${DRY_RUN}" != "true" && -n "${RUN_ID}" ]]; then
    "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v run_id="${RUN_ID}" <<'SQL' >/dev/null
update public."ConnectorRuns"
set
  status = 'failed',
  finished_at = now(),
  metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object('error', 'PLACSP local extract failed')
where id = :'run_id';
SQL
  fi

  exit 1
fi

json_field() {
  local field_path="$1"
  node -e '
const fs = require("fs");
const file = process.argv[1];
const path = process.argv[2].split(".").filter(Boolean);
const obj = JSON.parse(fs.readFileSync(file, "utf8"));
let value = obj;
for (const key of path) {
  if (value && Object.prototype.hasOwnProperty.call(value, key)) value = value[key];
  else { value = ""; break; }
}
if (value === null || value === undefined) process.stdout.write("");
else process.stdout.write(String(value));
' "${STATUS_FILE}" "${field_path}" 2>/dev/null || true
}

PHASE="$(json_field phase)"
PARSED_RECORDS_RAW="$(json_field parsed_records)"
PROCESSED_RAW="$(json_field processed)"
DEDUPED_RAW="$(json_field deduped_in_run)"
PAGES_RAW="$(json_field pages)"
FEEDS_RAW="$(json_field feeds)"
MAX_ENTRY_UPDATED="$(json_field max_entry_updated)"
CURRENT_INSERTED_RAW="$(json_field current_inserted)"
CURRENT_UPDATED_RAW="$(json_field current_updated)"

PARSED_RECORDS="${PARSED_RECORDS_RAW:-0}"
PROCESSED="${PROCESSED_RAW:-0}"
DEDUPED_IN_RUN="${DEDUPED_RAW:-0}"
PAGES="${PAGES_RAW:-0}"
FEEDS="${FEEDS_RAW:-0}"
CURRENT_INSERTED_LOCAL="${CURRENT_INSERTED_RAW:-0}"
CURRENT_UPDATED_LOCAL="${CURRENT_UPDATED_RAW:-0}"

if [[ -f "${CURRENT_FILE}" ]]; then
  CURRENT_ROWS="$(wc -l < "${CURRENT_FILE}" | tr -d ' ')"
else
  CURRENT_ROWS="0"
fi

echo "extract_phase=${PHASE:-<none>} parsed_records=${PARSED_RECORDS} processed=${PROCESSED} current_rows=${CURRENT_ROWS} max_entry_updated=${MAX_ENTRY_UPDATED:-<none>}"

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "== DRY RUN: no database writes =="
  echo "sample_rows:"
  if [[ -f "${CURRENT_FILE}" ]]; then
    head -n 5 "${CURRENT_FILE}" || true
  else
    echo "(no rows emitted)"
  fi
  exit 0
fi

if [[ "${PHASE}" == "failed" ]]; then
  ERR_MSG="$(json_field error)"
  [[ -z "${ERR_MSG}" ]] && ERR_MSG="PLACSP extract phase failed"

  if [[ -n "${RUN_ID}" ]]; then
    "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v run_id="${RUN_ID}" -v err_msg="${ERR_MSG}" <<'SQL' >/dev/null
update public."ConnectorRuns"
set
  status = 'failed',
  finished_at = now(),
  metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object('error', :'err_msg')
where id = :'run_id';
SQL
  fi

  echo "ERROR: ${ERR_MSG}"
  exit 1
fi

if [[ "${CURRENT_ROWS}" == "0" ]]; then
  echo "-- no incremental rows produced; recording successful noop connector run"

  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -v connector_key="${CONNECTOR_KEY}" -v run_id="${RUN_ID}" -v max_entry_updated="${MAX_ENTRY_UPDATED}" -v parsed_records="${PARSED_RECORDS}" -v processed="${PROCESSED}" -v deduped_in_run="${DEDUPED_IN_RUN}" -v pages="${PAGES}" -v feeds="${FEEDS}" <<'SQL'
update public."ConnectorConfig" cfg
set
  config = jsonb_set(
    coalesce(cfg.config, '{}'::jsonb),
    '{cursor}',
    jsonb_build_object(
      'type', 'published',
      'value', coalesce(nullif(:'max_entry_updated',''), (cfg.config->'cursor'->>'value'), now()::text),
      'last_success_at', now()::text
    ),
    true
  ),
  updated_at = now(),
  enabled = true
where cfg.tenant_id = :'tenant_id'
  and cfg.connector_key = :'connector_key';

update public."ConnectorRuns" r
set
  status = 'success',
  finished_at = now(),
  metadata = coalesce(r.metadata, '{}'::jsonb) || jsonb_build_object(
    'source', 'PLACSP_ES',
    'fetched_count', (:'parsed_records')::int,
    'inserted_count', 0,
    'updated_count', 0,
    'noop_count', (:'processed')::int,
    'versioned_count', 0,
    'deduped_in_run', (:'deduped_in_run')::int,
    'pages', (:'pages')::int,
    'feeds', (:'feeds')::int,
    'cursor', (select cfg.config->'cursor' from public."ConnectorConfig" cfg where cfg.tenant_id = :'tenant_id' and cfg.connector_key = :'connector_key' limit 1)
  )
where r.id = :'run_id';
SQL

  exit 0
fi

echo "== Upserting extracted PLACSP rows into Supabase =="

if ! "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v statement_timeout="${STATEMENT_TIMEOUT}" -v tenant_id="${TENANT_ID}" -v connector_key="${CONNECTOR_KEY}" -v run_id="${RUN_ID}" -v max_entry_updated="${MAX_ENTRY_UPDATED}" -v parsed_records="${PARSED_RECORDS}" -v processed="${PROCESSED}" -v deduped_in_run="${DEDUPED_IN_RUN}" -v pages="${PAGES}" -v feeds="${FEEDS}" <<SQL
begin;
set local statement_timeout = :'statement_timeout';

create temp table tmp_placsp_es_line_raw (
  line text
);

\copy tmp_placsp_es_line_raw(line) from '${CURRENT_FILE}' with (format csv, delimiter E'\x1f', quote E'\x1e', escape E'\x1d');

create temp table tmp_placsp_es_line (
  doc jsonb
);

insert into tmp_placsp_es_line(doc)
select line::jsonb
from tmp_placsp_es_line_raw
where coalesce(line, '') <> '';

create temp table tmp_placsp_es_ingest as
with parsed as (
  select
    coalesce(nullif(doc->>'tenant_id',''), :'tenant_id') as tenant_id,
    nullif(doc->>'tender_id','') as tender_id,
    upper(coalesce(nullif(doc->>'source',''), 'PLACSP_ES')) as source,
    case
      when coalesce(doc->>'published_at','') = '' then null
      else (doc->>'published_at')::timestamptz
    end as published_at,
    coalesce(doc->'data', '{}'::jsonb) as data
  from tmp_placsp_es_line
)
select
  tenant_id,
  tender_id,
  source,
  published_at,
  data,
  md5(coalesce(data::text, '')) as version_hash
from (
  select distinct on (tender_id)
    tenant_id,
    tender_id,
    source,
    published_at,
    data
  from parsed
  where tender_id is not null
    and source = 'PLACSP_ES'
  order by tender_id, published_at desc nulls last
) d;

create temp table tmp_placsp_es_counts as
with
v as (
  insert into public."TenderVersions" (tenant_id, tender_id, version_hash, data, created_at)
  select tenant_id, tender_id, version_hash, data, now()
  from tmp_placsp_es_ingest
  on conflict (tender_id, version_hash) do nothing
  returning tender_id
),
up as (
  insert into public."TendersCurrent" (tenant_id, tender_id, source, published_at, data, updated_at)
  select tenant_id, tender_id, source, published_at, data, now()
  from tmp_placsp_es_ingest
  on conflict (tender_id) do update set
    tenant_id = excluded.tenant_id,
    source = excluded.source,
    published_at = excluded.published_at,
    data = excluded.data,
    updated_at = now()
  where public."TendersCurrent".tenant_id is distinct from excluded.tenant_id
     or public."TendersCurrent".source is distinct from excluded.source
     or public."TendersCurrent".published_at is distinct from excluded.published_at
     or public."TendersCurrent".data is distinct from excluded.data
  returning (xmax = 0) as inserted
)
select
  (select count(*)::int from tmp_placsp_es_ingest) as fetched_count,
  (select count(*)::int from up where inserted) as inserted_count,
  (select count(*)::int from up where not inserted) as updated_count,
  ((select count(*)::int from tmp_placsp_es_ingest) - (select count(*)::int from up where inserted) - (select count(*)::int from up where not inserted)) as noop_count,
  (select count(*)::int from v) as versioned_count,
  (select max(published_at) from tmp_placsp_es_ingest) as max_published_at;

select * from tmp_placsp_es_counts;

update public."ConnectorConfig" cfg
set
  config = jsonb_set(
    coalesce(cfg.config, '{}'::jsonb),
    '{cursor}',
    jsonb_build_object(
      'type', 'published',
      'value', coalesce(
        nullif(:'max_entry_updated',''),
        (select (max_published_at at time zone 'UTC')::text from tmp_placsp_es_counts),
        (cfg.config->'cursor'->>'value'),
        now()::text
      ),
      'last_success_at', now()::text
    ),
    true
  ),
  updated_at = now(),
  enabled = true
where cfg.tenant_id = :'tenant_id'
  and cfg.connector_key = :'connector_key';

update public."ConnectorRuns" r
set
  status = 'success',
  finished_at = now(),
  metadata = coalesce(r.metadata, '{}'::jsonb) || jsonb_build_object(
    'source', 'PLACSP_ES',
    'fetched_count', (select fetched_count from tmp_placsp_es_counts),
    'inserted_count', (select inserted_count from tmp_placsp_es_counts),
    'updated_count', (select updated_count from tmp_placsp_es_counts),
    'noop_count', (select noop_count from tmp_placsp_es_counts),
    'versioned_count', (select versioned_count from tmp_placsp_es_counts),
    'parsed_records', (:'parsed_records')::int,
    'processed_rows', (:'processed')::int,
    'deduped_in_run', (:'deduped_in_run')::int,
    'pages', (:'pages')::int,
    'feeds', (:'feeds')::int,
    'cursor', (select cfg.config->'cursor' from public."ConnectorConfig" cfg where cfg.tenant_id = :'tenant_id' and cfg.connector_key = :'connector_key' limit 1)
  )
where r.id = :'run_id';

commit;
SQL
then
  if [[ -n "${RUN_ID}" ]]; then
    "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v run_id="${RUN_ID}" <<'SQL' >/dev/null
update public."ConnectorRuns"
set
  status = 'failed',
  finished_at = now(),
  metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object('error', 'PLACSP DB upsert failed')
where id = :'run_id';
SQL
  fi
  echo "ERROR: PLACSP DB upsert failed."
  exit 1
fi

echo "== QA pack =="
if [[ -f "${QA_SQL}" ]]; then
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -f "${QA_SQL}"
fi

if [[ "${RECONCILE_AFTER_INGEST}" == "true" ]]; then
  echo "== Reconcile TED <-> PLACSP_ES =="
  if [[ -x "${RECON_SCRIPT}" ]]; then
    if ! "${RECON_SCRIPT}" "${TENANT_ID}" "ES" "${RECONCILE_LIMIT}" "true" "PLACSP_ES"; then
      if [[ "${RECONCILE_STRICT}" == "true" ]]; then
        echo "ERROR: post-ingestion reconciliation failed (strict mode)."
        exit 1
      fi
      echo "WARN: post-ingestion reconciliation failed; continuing (RECONCILE_STRICT=false)."
    fi
  else
    echo "WARN: reconcile helper not found: ${RECON_SCRIPT}"
  fi
fi

echo "== Done =="
