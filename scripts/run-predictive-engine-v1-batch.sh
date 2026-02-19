#!/usr/bin/env bash
set -euo pipefail

# Predictive Engine V1 batch runner (scoped)
# Usage:
#   ./scripts/run-predictive-engine-v1-batch.sh TENANT_ID full [MAX_PAIRS_PER_BATCH] [STRATEGY] [FORMULA_VERSION]
#
# STRATEGY: region | bucket | recent
# Env:
#   SUPABASE_DB_URL or DATABASE_URL
#   REGIONS (comma list, default: IE,FR,ES)
#   REGION_BATCHES (default: 10 per region)
#   BUCKET_COUNT (default: 16)
#   RECENT_BATCHES (default: 10)
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
MODE="${2:-full}"
MAX_PAIRS_PER_BATCH="${3:-2000}"
STRATEGY="${4:-region}"
FORMULA_VERSION="${5:-v1.0.0}"

if [[ "${MODE}" != "full" ]]; then
  echo "ERROR: mode must be 'full' (got: ${MODE})"
  exit 1
fi

if [[ ! "${MAX_PAIRS_PER_BATCH}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_PAIRS_PER_BATCH must be an integer. Got: ${MAX_PAIRS_PER_BATCH}"
  exit 1
fi

REGIONS="${REGIONS:-IE,FR,ES}"
BUCKET_COUNT="${BUCKET_COUNT:-16}"
RECENT_BATCHES="${RECENT_BATCHES:-10}"
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-15min}"

STATE_DIR="${REPO_ROOT}/scripts/.state"
mkdir -p "${STATE_DIR}"
STATE_FILE="${STATE_DIR}/predictive-engine-v1-batch-${TENANT_ID}-${STRATEGY}.state"

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
    grep -Ev "^${key}=" "${STATE_FILE}" > "${tmpfile}" || true
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
    -v max_pairs="${MAX_PAIRS_PER_BATCH}" \
    -v formula_version="${FORMULA_VERSION}" \
    ${extra_vars} <<SQL
\set ON_ERROR_STOP on
set statement_timeout = :'statement_timeout';
begin;

create temp table prediction_pair_scope (
  tenant_id text,
  buyer_entity_id text,
  cpv_cluster_id text,
  region text,
  last_ingested_at timestamptz,
  last_occurred_at timestamptz
) on commit drop;

insert into prediction_pair_scope
select
  s.tenant_id,
  s.buyer_entity_id,
  coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
  coalesce(public.normalize_prediction_region(s.region, s.source), s.region, 'IE') as region,
  max(s.ingested_at) as last_ingested_at,
  max(s.occurred_at) as last_occurred_at
from public.signals s
where s.tenant_id = :'tenant_id'
  and s.buyer_entity_id is not null
  ${sql_filter}
group by 1, 2, 3, 4;

select *
from public.run_predictive_engine_v1_scoped(
  :'tenant_id',
  'pg_temp.prediction_pair_scope',
  'full',
  :'formula_version',
  (:'max_pairs')::int,
  false
);

select
  run_id,
  status,
  pairs_processed,
  metadata_json->>'computed_prediction_rows' as computed_prediction_rows,
  metadata_json->>'skipped_pairs' as skipped_pairs,
  started_at,
  finished_at
from public.prediction_runs
where tenant_id = :'tenant_id'
order by started_at desc
limit 1;
commit;
SQL
}

if [[ "${STRATEGY}" == "region" ]]; then
  REGION_BATCHES="${REGION_BATCHES:-10}"
  if [[ ! "${REGION_BATCHES}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: REGION_BATCHES must be an integer. Got: ${REGION_BATCHES}"
    exit 1
  fi

  IFS=',' read -r -a region_list <<< "${REGIONS}"
  region_index="$(state_get region_index)"
  region_batch="$(state_get region_batch)"
  region_index="${region_index:-0}"
  region_batch="${region_batch:-0}"

  while [[ "${region_index}" -lt "${#region_list[@]}" ]]; do
    region="${region_list[$region_index]}"
    batch_offset=$((region_batch * MAX_PAIRS_PER_BATCH))
    run_batch_sql "region:${region} batch:${region_batch}" "and s.region = :'region' and s.buyer_entity_id in (\
      select buyer_entity_id from (\
        select buyer_entity_id, max(ingested_at) as last_ingested_at\
        from public.signals\
        where tenant_id = :'tenant_id' and region = :'region'\
        group by buyer_entity_id\
        order by last_ingested_at desc nulls last\
        offset (:'offset')::int\
        limit (:'max_pairs')::int\
      ) r\
    )" "-v region=${region} -v offset=${batch_offset} -v statement_timeout=${STATEMENT_TIMEOUT}"

    region_batch=$((region_batch + 1))
    if [[ "${region_batch}" -ge "${REGION_BATCHES}" ]]; then
      region_batch=0
      region_index=$((region_index + 1))
    fi
    state_set "region_index" "${region_index}"
    state_set "region_batch" "${region_batch}"
  done
  echo "Region batches complete (REGION_BATCHES=${REGION_BATCHES})."
elif [[ "${STRATEGY}" == "bucket" ]]; then
  if [[ ! "${BUCKET_COUNT}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: BUCKET_COUNT must be an integer. Got: ${BUCKET_COUNT}"
    exit 1
  fi
  idx="$(state_get bucket_index)"
  idx="${idx:-0}"
  while [[ "${idx}" -lt "${BUCKET_COUNT}" ]]; do
    run_batch_sql "bucket:${idx}/${BUCKET_COUNT}" "and (mod(abs(('x'||substr(md5(s.buyer_entity_id),1,8))::bit(32)::int), :'bucket_count') = :'bucket_index')" "-v bucket_count=${BUCKET_COUNT} -v bucket_index=${idx} -v statement_timeout=${STATEMENT_TIMEOUT}"
    idx=$((idx + 1))
    state_set "bucket_index" "${idx}"
  done
  echo "All bucket batches complete."
elif [[ "${STRATEGY}" == "recent" ]]; then
  if [[ ! "${RECENT_BATCHES}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: RECENT_BATCHES must be an integer. Got: ${RECENT_BATCHES}"
    exit 1
  fi
  idx="$(state_get recent_batch)"
  idx="${idx:-0}"
  end=$((idx + RECENT_BATCHES))
  while [[ "${idx}" -lt "${end}" ]]; do
    offset=$((idx * MAX_PAIRS_PER_BATCH))
    run_batch_sql "recent:offset=${offset}" "and s.buyer_entity_id in (\
      select buyer_entity_id from (\
        select buyer_entity_id, max(ingested_at) as last_ingested_at\
        from public.signals\
        where tenant_id = :'tenant_id'\
        group by buyer_entity_id\
        order by last_ingested_at desc nulls last\
        offset (:'offset')::int\
        limit (:'max_pairs')::int\
      ) r\
    )" "-v offset=${offset} -v statement_timeout=${STATEMENT_TIMEOUT}"
    idx=$((idx + 1))
    state_set "recent_batch" "${idx}"
  done
  echo "Recent-activity batches complete (processed ${RECENT_BATCHES} batches)."
else
  echo "ERROR: Unsupported STRATEGY: ${STRATEGY}. Use region|bucket|recent."
  exit 1
fi
