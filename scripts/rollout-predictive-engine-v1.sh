#!/usr/bin/env bash
set -euo pipefail

# Civant: Predictive Engine V1 runner
# Usage:
#   ./scripts/rollout-predictive-engine-v1.sh TENANT_ID [RUN_TYPE] [SINCE_ISO] [MAX_PAIRS] [NORMALIZE_SIGNALS] [FORMULA_VERSION]
#
# RUN_TYPE: incremental | full | signals_only
# NORMALIZE_SIGNALS: true | false

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

TENANT_ID="${1:-${TENANT_ID:-}}"
RUN_TYPE_RAW="${2:-${RUN_TYPE:-incremental}}"
SINCE_ISO="${3:-${SINCE_ISO:-}}"
MAX_PAIRS_RAW="${4:-${MAX_PAIRS:-5000}}"
NORMALIZE_RAW="${5:-${NORMALIZE_SIGNALS:-true}}"
FORMULA_VERSION="${6:-${FORMULA_VERSION:-v1.0.0}}"

if [[ -z "${TENANT_ID}" ]]; then
  echo "ERROR: TENANT_ID is required."
  echo "Usage: $0 TENANT_ID [RUN_TYPE] [SINCE_ISO] [MAX_PAIRS] [NORMALIZE_SIGNALS] [FORMULA_VERSION]"
  exit 1
fi

RUN_TYPE="$(echo "${RUN_TYPE_RAW}" | tr '[:upper:]' '[:lower:]')"
case "${RUN_TYPE}" in
  incremental|full|signals_only) ;;
  *)
    echo "ERROR: RUN_TYPE must be one of incremental|full|signals_only. Got: ${RUN_TYPE_RAW}"
    exit 1
    ;;
esac

if [[ ! "${MAX_PAIRS_RAW}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_PAIRS must be an integer. Got: ${MAX_PAIRS_RAW}"
  exit 1
fi
MAX_PAIRS="${MAX_PAIRS_RAW}"

NORMALIZE_SIGNALS="false"
case "${NORMALIZE_RAW}" in
  1|true|TRUE|yes|YES|y|Y|on|ON) NORMALIZE_SIGNALS="true" ;;
  0|false|FALSE|no|NO|n|N|off|OFF|"") NORMALIZE_SIGNALS="false" ;;
  *)
    echo "ERROR: NORMALIZE_SIGNALS must be true/false. Got: ${NORMALIZE_RAW}"
    exit 1
    ;;
esac

if [[ -n "${SINCE_ISO}" && ! "${SINCE_ISO}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}([ T][0-9]{2}:[0-9]{2}(:[0-9]{2})?(\.[0-9]+)?)?(Z|[+-][0-9]{2}:[0-9]{2})?$ ]]; then
  echo "ERROR: SINCE_ISO must be an ISO datetime/date string. Got: ${SINCE_ISO}"
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-15}"

echo "== Predictive Engine V1 =="
echo "tenant_id=${TENANT_ID} run_type=${RUN_TYPE} since=${SINCE_ISO:-<auto>} max_pairs=${MAX_PAIRS} normalize_signals=${NORMALIZE_SIGNALS} formula=${FORMULA_VERSION}"

"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off \
  -v tenant_id="${TENANT_ID}" \
  -v run_type="${RUN_TYPE}" \
  -v since_iso="${SINCE_ISO}" \
  -v max_pairs="${MAX_PAIRS}" \
  -v normalize_signals="${NORMALIZE_SIGNALS}" \
  -v formula_version="${FORMULA_VERSION}" <<'SQL'
with run as (
  select *
  from public.run_predictive_engine_v1(
    :'tenant_id',
    :'run_type',
    :'formula_version',
    nullif(:'since_iso', '')::timestamptz,
    (:'max_pairs')::int,
    (:'normalize_signals')::boolean
  )
)
select * from run;

select
  tenant_id,
  run_type,
  status,
  started_at,
  finished_at,
  pairs_processed,
  coalesce(error_message, '') as error_message
from public.prediction_runs
where tenant_id = :'tenant_id'
order by started_at desc
limit 5;

select
  tenant_id,
  region,
  confidence_band,
  count(*) as predictions
from public.predictions_current
where tenant_id = :'tenant_id'
group by 1, 2, 3
order by 2, 3;
SQL

echo "== Done =="
