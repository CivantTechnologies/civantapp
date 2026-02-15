#!/usr/bin/env bash
set -euo pipefail

# Civant: Ireland-only External Signals rollout (Hiring + Funding)
#
# Writes to: public.external_signals_ie via public.upsert_external_signal_ie(...)
#
# Usage:
#   ./scripts/rollout-external-signals-ie.sh TENANT_ID [START_DATE] [DRY_RUN]
#
# Examples:
#   ./scripts/rollout-external-signals-ie.sh civant_default
#   ./scripts/rollout-external-signals-ie.sh civant_default 2026-02-01
#   ./scripts/rollout-external-signals-ie.sh civant_default 2026-02-01 true
#
# Required (unless DRY_RUN=true):
#   SUPABASE_DB_URL or DATABASE_URL
#
# Optional:
#   PSQL_BIN (defaults to Homebrew libpq psql)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
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
    echo "ERROR: DRY_RUN must be a boolean (true/false). Got: ${DRY_RUN_RAW}"
    exit 1
    ;;
esac

if [[ "${DRY_RUN}" != "true" && -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required unless DRY_RUN=true."
  exit 1
fi
if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-15}"

PUBLICJOBS_SCRIPT="${REPO_ROOT}/scripts/external-signals/publicjobs-ie.mjs"
CKAN_SCRIPT="${REPO_ROOT}/scripts/external-signals/funding-ckan-ie.mjs"
QA_PUBLICJOBS="${REPO_ROOT}/scripts/qa-external-signals-publicjobs-ie.sql"
QA_CKAN="${REPO_ROOT}/scripts/qa-external-signals-funding-ckan-ie.sql"

TMP_DIR="${TMPDIR:-/tmp}"
TSV_PUBLICJOBS="$(mktemp "${TMP_DIR%/}/civant_publicjobs_ie_XXXXXX.tsv")"
TSV_CKAN="$(mktemp "${TMP_DIR%/}/civant_ckan_ie_XXXXXX.tsv")"

cleanup() {
  rm -f "${TSV_PUBLICJOBS}" "${TSV_CKAN}" 2>/dev/null || true
}
trap cleanup EXIT

start_arg=()
if [[ -n "${START_DATE}" ]]; then
  start_arg=("--start-date" "${START_DATE}")
fi

echo "== External Signals IE rollout =="
echo "tenant_id=${TENANT_ID} start_date=${START_DATE:-<none>} dry_run=${DRY_RUN}"

echo "== Fetching Hiring (PublicJobs) =="
node "${PUBLICJOBS_SCRIPT}" --tenant-id "${TENANT_ID}" "${start_arg[@]}" --dry-run "${DRY_RUN}" >"${TSV_PUBLICJOBS}"
PUBLICJOBS_ROWS="$(wc -l <"${TSV_PUBLICJOBS}" | tr -d ' ')"
echo "publicjobs_ie rows=${PUBLICJOBS_ROWS}"

echo "== Fetching Funding (CKAN data.gov.ie) =="
node "${CKAN_SCRIPT}" --tenant-id "${TENANT_ID}" "${start_arg[@]}" --dry-run "${DRY_RUN}" >"${TSV_CKAN}"
CKAN_ROWS="$(wc -l <"${TSV_CKAN}" | tr -d ' ')"
echo "ckan_ie_dataportal rows=${CKAN_ROWS}"

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "== DRY RUN: no database writes =="
  echo "Sample (publicjobs_ie):"
  head -n 3 "${TSV_PUBLICJOBS}" || true
  echo "Sample (ckan_ie_dataportal):"
  head -n 3 "${TSV_CKAN}" || true
  echo "== Done (dry run) =="
  exit 0
fi

upsert_tsv() {
  local tsv_file="$1"
  local label="$2"

  if [[ ! -s "${tsv_file}" ]]; then
    echo "-- ${label}: no rows to upsert"
    return 0
  fi

  echo "-- ${label}: upserting..."

  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
begin;
create temp table tmp_external_signal_ingest (
  tenant_id text,
  signal_type text,
  signal_source text,
  source_event_id text,
  signal_date date,
  evidence_url text,
  evidence_text text,
  buyer_id text,
  buyer_name_raw text,
  signal_strength numeric,
  extracted_entities jsonb,
  raw_payload jsonb
);
\copy tmp_external_signal_ingest from '${tsv_file}' with (format text, delimiter E'\t');

with upserted as (
  select public.upsert_external_signal_ie(
    tenant_id,
    signal_type,
    signal_source,
    source_event_id,
    signal_date,
    evidence_url,
    nullif(evidence_text, ''),
    nullif(buyer_id, ''),
    nullif(buyer_name_raw, ''),
    signal_strength,
    extracted_entities,
    raw_payload
  ) as id
  from tmp_external_signal_ingest
)
select
  '${label}'::text as connector,
  (select count(*) from tmp_external_signal_ingest) as staged_rows,
  (select count(*) from upserted) as upserted_rows;

commit;
SQL
}

echo "== Upserting to Supabase =="
upsert_tsv "${TSV_PUBLICJOBS}" "publicjobs_ie"
upsert_tsv "${TSV_CKAN}" "ckan_ie_dataportal"

echo "== QA packs =="
if [[ -f "${QA_PUBLICJOBS}" ]]; then
  echo "Running: ${QA_PUBLICJOBS}"
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -f "${QA_PUBLICJOBS}"
fi
if [[ -f "${QA_CKAN}" ]]; then
  echo "Running: ${QA_CKAN}"
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -f "${QA_CKAN}"
fi

echo "== Done =="
