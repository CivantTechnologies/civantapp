#!/usr/bin/env bash
set -euo pipefail

# Civant: France external funding signals rollout (data.gouv.fr)
# Usage:
#   ./scripts/rollout-external-signals-fr.sh TENANT_ID [START_DATE] [DRY_RUN]

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

FETCH_SCRIPT="${REPO_ROOT}/scripts/funding/funding-fr-datagouv-incremental.mjs"
QA_SQL="${REPO_ROOT}/scripts/qa-external-signals-fr.sql"
TMP_DIR="${TMPDIR:-/tmp}"
TSV_FILE="$(mktemp "${TMP_DIR%/}/civant_extsig_fr_XXXXXX" 2>/dev/null || mktemp -t civant_extsig_fr)"

cleanup() {
  rm -f "${TSV_FILE}" 2>/dev/null || true
}
trap cleanup EXIT

start_arg=()
if [[ -n "${START_DATE}" ]]; then
  start_arg=("--start-date" "${START_DATE}")
fi

echo "== External signals FR rollout =="
echo "tenant_id=${TENANT_ID} start_date=${START_DATE:-<none>} dry_run=${DRY_RUN}"

node "${FETCH_SCRIPT}" --tenant-id "${TENANT_ID}" "${start_arg[@]}" --dry-run "${DRY_RUN}" >"${TSV_FILE}"

ROWS="$(wc -l <"${TSV_FILE}" | tr -d ' ')"
echo "staged_rows=${ROWS}"

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "== DRY RUN: no database writes =="
  head -n 5 "${TSV_FILE}" || true
  exit 0
fi

if [[ ! -s "${TSV_FILE}" ]]; then
  echo "No rows staged; refreshing rollup only."
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
select * from public.refresh_external_signal_rollup_fr('${TENANT_ID}'::text);
SQL
  exit 0
fi

"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
begin;

create temp table tmp_external_signals_fr (
  tenant_id text,
  signal_type text,
  signal_source text,
  source_event_id text,
  signal_date date,
  evidence_url text,
  evidence_text text,
  buyer_name_raw text,
  signal_strength numeric,
  extracted_entities_text text,
  raw_payload_text text
);

\copy tmp_external_signals_fr from '${TSV_FILE}' with (format text, delimiter E'\t');

with staged as (
  select
    tenant_id,
    signal_type,
    signal_source,
    source_event_id,
    signal_date,
    evidence_url,
    nullif(evidence_text, '') as evidence_text,
    nullif(buyer_name_raw, '') as buyer_name_raw,
    signal_strength,
    coalesce(nullif(extracted_entities_text,''), '{}')::jsonb as extracted_entities,
    coalesce(nullif(raw_payload_text,''), '{}')::jsonb as raw_payload
  from tmp_external_signals_fr
), upserted as (
  select public.upsert_external_signal_fr(
    s.tenant_id,
    s.signal_type,
    s.signal_source,
    s.source_event_id,
    s.signal_date,
    s.evidence_url,
    s.evidence_text,
    null,
    s.buyer_name_raw,
    s.signal_strength,
    s.extracted_entities,
    s.raw_payload
  ) as id
  from staged s
)
select
  (select count(*) from staged) as staged_rows,
  (select count(*) from upserted) as upsert_calls;

select
  signal_source,
  count(*) as staged_rows
from tmp_external_signals_fr
group by 1
order by staged_rows desc, signal_source;

select * from public.refresh_external_signal_rollup_fr('${TENANT_ID}'::text);

commit;
SQL

if [[ -f "${QA_SQL}" ]]; then
  echo "== QA pack =="
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -f "${QA_SQL}"
fi

echo "== Done =="
