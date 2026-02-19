#!/usr/bin/env bash
set -euo pipefail

# Backfill buyer_category_stats (deterministic, resumable)
# Usage:
#   ./scripts/backfill-buyer-category-stats.sh TENANT_ID [BATCH_SIZE] [STRATEGY] [MAX_BATCHES]
#
# STRATEGY: recent | region | bucket
# Env:
#   SUPABASE_DB_URL or DATABASE_URL
#   REGIONS (comma list, default: IE,FR,ES)
#   REGION_BATCHES (default: 10 per region)
#   BUCKET_COUNT (default: 16)
#   STATEMENT_TIMEOUT (default: 15min)

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

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required."
  exit 1
fi

TENANT_ID="${1:-civant_default}"
BATCH_SIZE="${2:-1000}"
STRATEGY="${3:-recent}"
MAX_BATCHES="${4:-10}"

if [[ ! "${BATCH_SIZE}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: BATCH_SIZE must be an integer. Got: ${BATCH_SIZE}"
  exit 1
fi
if [[ ! "${MAX_BATCHES}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_BATCHES must be an integer. Got: ${MAX_BATCHES}"
  exit 1
fi

REGIONS="${REGIONS:-IE,FR,ES}"
REGION_BATCHES="${REGION_BATCHES:-10}"
BUCKET_COUNT="${BUCKET_COUNT:-16}"
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-15min}"

STATE_DIR="${REPO_ROOT}/scripts/.state"
mkdir -p "${STATE_DIR}"
STATE_FILE="${STATE_DIR}/buyer-category-stats-${TENANT_ID}-${STRATEGY}.state"

state_get() {
  local key="$1"
  if [[ -f "${STATE_FILE}" ]]; then
    local line
    line="$(grep -E "^${key}=" "${STATE_FILE}" || true)"
    if [[ -n "${line}" ]]; then
      echo "${line#${key}=}"
      return
    fi
  fi
  echo ""
}

state_set() {
  local key="$1"
  local value="$2"
  local tmpfile
  tmpfile="$(mktemp)"
  if [[ -f "${STATE_FILE}" ]]; then
    grep -Ev "^${key}=|^updated_at=" "${STATE_FILE}" > "${tmpfile}" || true
  fi
  echo "${key}=${value}" >> "${tmpfile}"
  echo "updated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "${tmpfile}"
  mv "${tmpfile}" "${STATE_FILE}"
}

run_batch_sql() {
  local batch_label="$1"
  local sql_filter="$2"
  local extra_vars="$3"

  echo "== Batch: ${batch_label} =="

  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off \
    -v tenant_id="${TENANT_ID}" \
    -v batch_size="${BATCH_SIZE}" \
    ${extra_vars} <<SQL
\set ON_ERROR_STOP on
set statement_timeout = :'statement_timeout';
begin;

create temp table pair_scope (
  tenant_id text,
  buyer_entity_id text,
  cpv_cluster_id text,
  region text,
  last_ingested_at timestamptz
) on commit drop;

insert into pair_scope
select
  s.tenant_id,
  s.buyer_entity_id,
  coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
  coalesce(public.normalize_prediction_region(s.region, s.source), s.region, 'IE') as region,
  max(s.ingested_at) as last_ingested_at
from public.signals s
where s.tenant_id = :'tenant_id'
  and s.buyer_entity_id not like 'unresolved:%'
  and s.cpv_cluster_id is not null
  and upper(s.region) in ('IE', 'FR', 'ES')
  and s.signal_type in ('notice_published', 'award_published')
  ${sql_filter}
group by 1, 2, 3, 4
order by last_ingested_at desc nulls last
limit (:'batch_size')::int;

select count(*) as pairs_in_batch from pair_scope;

select public.recompute_buyer_category_stats_v1(
  tenant_id,
  buyer_entity_id,
  cpv_cluster_id,
  region,
  interval '15 years'
)
from pair_scope;

commit;
SQL
}

if [[ "${STRATEGY}" == "recent" ]]; then
  batch_index="$(state_get recent_batch)"
  batch_index="${batch_index:-0}"
  processed_batches=0
  end=$((batch_index + MAX_BATCHES))
  while [[ "${batch_index}" -lt "${end}" ]]; do
    offset=$((batch_index * BATCH_SIZE))
    run_batch_sql "recent:offset=${offset}" "and s.buyer_entity_id in (\
      select buyer_entity_id from (\
        select buyer_entity_id, max(ingested_at) as last_ingested_at\
        from public.signals\
        where tenant_id = :'tenant_id'\
          and signal_type in ('notice_published', 'award_published')\
        group by buyer_entity_id\
        order by last_ingested_at desc nulls last\
        offset (:'offset')::int\
        limit (:'batch_size')::int\
      ) r\
    )" "-v offset=${offset} -v statement_timeout=${STATEMENT_TIMEOUT}"
    batch_index=$((batch_index + 1))
    processed_batches=$((processed_batches + 1))
    state_set "recent_batch" "${batch_index}"
  done
  echo "Recent backfill complete (processed ${processed_batches} batches)."
elif [[ "${STRATEGY}" == "region" ]]; then
  if [[ ! "${REGION_BATCHES}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: REGION_BATCHES must be an integer. Got: ${REGION_BATCHES}"
    exit 1
  fi
  IFS=',' read -r -a region_list <<< "${REGIONS}"
  region_index="$(state_get region_index)"
  region_batch="$(state_get region_batch)"
  region_index="${region_index:-0}"
  region_batch="${region_batch:-0}"
  processed_batches=0

  while [[ "${region_index}" -lt "${#region_list[@]}" && "${processed_batches}" -lt "${MAX_BATCHES}" ]]; do
    region="${region_list[$region_index]}"
    batch_offset=$((region_batch * BATCH_SIZE))
    run_batch_sql "region:${region} batch:${region_batch}" "and s.region = :'region' and s.buyer_entity_id in (\
      select buyer_entity_id from (\
        select buyer_entity_id, max(ingested_at) as last_ingested_at\
        from public.signals\
        where tenant_id = :'tenant_id'\
          and region = :'region'\
          and signal_type in ('notice_published', 'award_published')\
        group by buyer_entity_id\
        order by last_ingested_at desc nulls last\
        offset (:'offset')::int\
        limit (:'batch_size')::int\
      ) r\
    )" "-v region=${region} -v offset=${batch_offset} -v statement_timeout=${STATEMENT_TIMEOUT}"

    region_batch=$((region_batch + 1))
    processed_batches=$((processed_batches + 1))
    if [[ "${region_batch}" -ge "${REGION_BATCHES}" ]]; then
      region_batch=0
      region_index=$((region_index + 1))
    fi
    state_set "region_index" "${region_index}"
    state_set "region_batch" "${region_batch}"
  done
  echo "Region backfill checkpoint (processed ${processed_batches} batches; REGION_BATCHES=${REGION_BATCHES})."
elif [[ "${STRATEGY}" == "bucket" ]]; then
  if [[ ! "${BUCKET_COUNT}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: BUCKET_COUNT must be an integer. Got: ${BUCKET_COUNT}"
    exit 1
  fi
  bucket_index="$(state_get bucket_index)"
  bucket_index="${bucket_index:-0}"
  processed_batches=0
  while [[ "${bucket_index}" -lt "${BUCKET_COUNT}" && "${processed_batches}" -lt "${MAX_BATCHES}" ]]; do
    run_batch_sql "bucket:${bucket_index}/${BUCKET_COUNT}" "and (mod(abs(('x'||substr(md5(s.buyer_entity_id),1,8))::bit(32)::int), :'bucket_count') = :'bucket_index')" "-v bucket_count=${BUCKET_COUNT} -v bucket_index=${bucket_index} -v statement_timeout=${STATEMENT_TIMEOUT}"
    bucket_index=$((bucket_index + 1))
    processed_batches=$((processed_batches + 1))
    state_set "bucket_index" "${bucket_index}"
  done
  echo "Bucket backfill checkpoint (processed ${processed_batches} batches)."
else
  echo "ERROR: Unsupported STRATEGY: ${STRATEGY}. Use recent|region|bucket."
  exit 1
fi
