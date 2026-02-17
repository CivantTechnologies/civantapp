#!/usr/bin/env bash
set -euo pipefail

# Civant: TED incremental rollout (IE/FR/ES scope)
#
# Fetches NEW/UPDATED TED rows and upserts into:
#   - public."TendersCurrent"
#   - public."TenderVersions"
#
# Cursor is stored per tenant in public."ConnectorConfig".config under connector_key:
#   ted_incremental:<TENANT_ID>
#
# Usage:
#   ./scripts/rollout-ted-incremental.sh TENANT_ID [START_DATE] [DRY_RUN]
#
# Optional env:
#   COUNTRIES=IRL,FRA,ESP
#   MAX_PAGES=40
#   PAGE_SIZE=100

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

COUNTRIES="${COUNTRIES:-IRL,FRA,ESP}"
MAX_PAGES="${MAX_PAGES:-40}"
PAGE_SIZE="${PAGE_SIZE:-100}"
SLEEP_MS="${SLEEP_MS:-120}"

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

if [[ -n "${START_DATE}" && ! "${START_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: START_DATE must be YYYY-MM-DD. Got: ${START_DATE}"
  exit 1
fi

TED_SCRIPT="${REPO_ROOT}/scripts/ted/ted-incremental-ie-fr-es.mjs"
QA_SQL="${REPO_ROOT}/scripts/qa-ted-incremental.sql"
RECON_SCRIPT="${REPO_ROOT}/scripts/reconcile-ted-national.sh"
RECONCILE_AFTER_INGEST="${RECONCILE_AFTER_INGEST:-true}"
RECONCILE_STRICT="${RECONCILE_STRICT:-false}"
RECONCILE_LIMIT="${RECONCILE_LIMIT:-20}"

TMP_DIR="${TMPDIR:-/tmp}"
TSV_FILE="$(mktemp "${TMP_DIR%/}/civant_ted_XXXXXX" 2>/dev/null || mktemp -t civant_ted)"

cleanup() {
  rm -f "${TSV_FILE}" 2>/dev/null || true
}
trap cleanup EXIT

CONNECTOR_KEY="ted_incremental:${TENANT_ID}"

# If START_DATE is omitted, use stored cursor with overlap for replay safety.
if [[ -z "${START_DATE}" && "${DRY_RUN}" != "true" ]]; then
  START_DATE="$(${PSQL_BIN} "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA <<SQL
select
  case
    when (config->'cursor'->>'value') is null or (config->'cursor'->>'value') = '' then ''
    else to_char(((config->'cursor'->>'value')::timestamptz - interval '3 days')::date, 'YYYY-MM-DD')
  end
from public."ConnectorConfig"
where tenant_id = '${TENANT_ID}'
  and connector_key = '${CONNECTOR_KEY}'
limit 1;
SQL
)"
fi

if [[ -z "${START_DATE}" ]]; then
  START_DATE="$(date -u -v-30d +%Y-%m-%d 2>/dev/null || date -u -d '30 days ago' +%Y-%m-%d)"
fi

start_arg=()
if [[ -n "${START_DATE}" ]]; then
  start_arg=("--start-date" "${START_DATE}")
fi

echo "== TED incremental rollout =="
echo "tenant_id=${TENANT_ID} start_date=${START_DATE:-<none>} dry_run=${DRY_RUN} connector_key=${CONNECTOR_KEY} countries=${COUNTRIES} max_pages=${MAX_PAGES}"

echo "== Fetching from TED API =="
fetch_ok=0
for attempt in 1 2 3; do
  if node "${TED_SCRIPT}" \
    --tenant-id "${TENANT_ID}" \
    --countries "${COUNTRIES}" \
    --max-pages "${MAX_PAGES}" \
    --page-size "${PAGE_SIZE}" \
    --sleep-ms "${SLEEP_MS}" \
    "${start_arg[@]}" \
    --dry-run "${DRY_RUN}" >"${TSV_FILE}"
  then
    fetch_ok=1
    break
  fi
  echo "WARN: fetch attempt ${attempt}/3 failed."
  if [[ "${attempt}" -lt 3 ]]; then
    sleep $((attempt * 2))
  fi
done

if [[ "${fetch_ok}" -ne 1 ]]; then
  echo "ERROR: TED fetch failed after retries."
  exit 1
fi

ROWS="$(wc -l <"${TSV_FILE}" | tr -d ' ')"
echo "staged_rows=${ROWS}"

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "== DRY RUN: no database writes =="
  echo "Sample TSV:"
  head -n 5 "${TSV_FILE}" | cut -c1-320 || true
  exit 0
fi

if ! "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA -c "select 1;" >/dev/null; then
  echo "ERROR: database connection preflight failed."
  exit 1
fi

if [[ ! -s "${TSV_FILE}" ]]; then
  echo "-- no rows fetched; recording successful noop connector run"
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
begin;

insert into public."ConnectorConfig" (tenant_id, connector_key, enabled, config, updated_at)
values ('${TENANT_ID}', '${CONNECTOR_KEY}', true, '{}'::jsonb, now())
on conflict (connector_key) do update
  set updated_at = now();

update public."ConnectorConfig" cfg
set
  config = jsonb_set(
    coalesce(cfg.config, '{}'::jsonb),
    '{cursor}',
    jsonb_build_object(
      'type', 'published',
      'value', coalesce((cfg.config->'cursor'->>'value'), now()::text),
      'last_success_at', now()::text
    ),
    true
  ),
  updated_at = now(),
  enabled = true
where cfg.tenant_id = '${TENANT_ID}'
  and cfg.connector_key = '${CONNECTOR_KEY}';

insert into public."ConnectorRuns" (tenant_id, connector_key, status, started_at, finished_at, metadata)
values (
  '${TENANT_ID}',
  '${CONNECTOR_KEY}',
  'success',
  now(),
  now(),
  jsonb_build_object(
    'source', 'TED',
    'countries', '${COUNTRIES}',
    'fetched_count', 0,
    'inserted_count', 0,
    'updated_count', 0,
    'noop_count', 0,
    'versioned_count', 0,
    'note', 'No rows fetched for requested window'
  )
);

commit;
SQL
  exit 0
fi

echo "== Upserting into Supabase =="

"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
begin;

insert into public."ConnectorConfig" (tenant_id, connector_key, enabled, config, updated_at)
values ('${TENANT_ID}', '${CONNECTOR_KEY}', true, '{}'::jsonb, now())
on conflict (connector_key) do update
  set updated_at = now();

insert into public."ConnectorRuns" (tenant_id, connector_key, status, started_at, metadata)
values (
  '${TENANT_ID}',
  '${CONNECTOR_KEY}',
  'running',
  now(),
  jsonb_build_object(
    'source', 'TED',
    'countries', '${COUNTRIES}'
  )
)
returning id as run_id \gset

create temp table tmp_ted_ingest (
  tenant_id text,
  tender_id text,
  source text,
  published_at timestamptz,
  data jsonb,
  version_hash text
);
\copy tmp_ted_ingest from '${TSV_FILE}' with (format text, delimiter E'\t');

create temp table tmp_ted_counts as
with
v as (
  insert into public."TenderVersions" (tenant_id, tender_id, version_hash, data, created_at)
  select tenant_id, tender_id, version_hash, data, now()
  from tmp_ted_ingest
  on conflict (tender_id, version_hash) do nothing
  returning tender_id
),
up as (
  insert into public."TendersCurrent" (tenant_id, tender_id, source, published_at, data, updated_at)
  select tenant_id, tender_id, source, published_at, data, now()
  from tmp_ted_ingest
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
  (select count(*)::int from tmp_ted_ingest) as fetched_count,
  (select count(*)::int from up where inserted) as inserted_count,
  (select count(*)::int from up where not inserted) as updated_count,
  ((select count(*)::int from tmp_ted_ingest) - (select count(*)::int from up where inserted) - (select count(*)::int from up where not inserted)) as noop_count,
  (select count(*)::int from v) as versioned_count,
  (select max(published_at) from tmp_ted_ingest) as max_published_at;

select * from tmp_ted_counts;

update public."ConnectorConfig" cfg
set
  config = jsonb_set(
    coalesce(cfg.config, '{}'::jsonb),
    '{cursor}',
    jsonb_build_object(
      'type', 'published',
      'value', (
        select coalesce(
          (select (max_published_at at time zone 'UTC')::text from tmp_ted_counts),
          (cfg.config->'cursor'->>'value'),
          now()::text
        )
      ),
      'last_success_at', now()::text
    ),
    true
  ),
  updated_at = now(),
  enabled = true
where cfg.tenant_id = '${TENANT_ID}'
  and cfg.connector_key = '${CONNECTOR_KEY}';

update public."ConnectorRuns" r
set
  status = 'success',
  finished_at = now(),
  metadata = jsonb_build_object(
    'source', 'TED',
    'countries', '${COUNTRIES}',
    'fetched_count', (select fetched_count from tmp_ted_counts),
    'inserted_count', (select inserted_count from tmp_ted_counts),
    'updated_count', (select updated_count from tmp_ted_counts),
    'noop_count', (select noop_count from tmp_ted_counts),
    'versioned_count', (select versioned_count from tmp_ted_counts),
    'cursor', (select cfg.config->'cursor' from public."ConnectorConfig" cfg where cfg.tenant_id='${TENANT_ID}' and cfg.connector_key='${CONNECTOR_KEY}' limit 1)
  )
where r.id = :'run_id';

commit;
SQL

echo "== QA pack =="
if [[ -f "${QA_SQL}" ]]; then
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -f "${QA_SQL}"
fi

if [[ "${RECONCILE_AFTER_INGEST}" == "true" ]]; then
  echo "== Reconcile TED <-> national by country =="
  countries_csv="$(echo "${COUNTRIES}" | tr '[:lower:]' '[:upper:]' | tr -d ' ')"
  IFS=',' read -r -a ted_countries <<< "${countries_csv}"
  for c in "${ted_countries[@]}"; do
    case "${c}" in
      IE|IRL) country_code="IE" ;;
      FR|FRA) country_code="FR" ;;
      ES|ESP) country_code="ES" ;;
      *)
        echo "WARN: skipping unsupported country token for reconciliation: ${c}"
        continue
        ;;
    esac
    if [[ -x "${RECON_SCRIPT}" ]]; then
      if ! "${RECON_SCRIPT}" "${TENANT_ID}" "${country_code}" "${RECONCILE_LIMIT}" "true"; then
        if [[ "${RECONCILE_STRICT}" == "true" ]]; then
          echo "ERROR: post-ingestion reconciliation failed for ${country_code} (strict mode)."
          exit 1
        fi
        echo "WARN: post-ingestion reconciliation failed for ${country_code}; continuing (RECONCILE_STRICT=false)."
      fi
    else
      echo "WARN: reconcile helper not found: ${RECON_SCRIPT}"
      break
    fi
  done
fi

echo "== Done =="
