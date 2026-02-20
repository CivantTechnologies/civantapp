#!/usr/bin/env bash
set -euo pipefail

# Reconcile TED notices against national canonicals for one country.
# Usage:
#   ./scripts/reconcile-ted-national.sh TENANT_ID COUNTRY [LIMIT] [APPLY] [NATIONAL_SOURCES_CSV]
# Example:
#   ./scripts/reconcile-ted-national.sh civant_default IE 2500 true ETENDERS_IE
#   ./scripts/reconcile-ted-national.sh civant_default FR 2500 true BOAMP_FR
#   ./scripts/reconcile-ted-national.sh civant_default ES 2500 true PLACSP_ES

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
  [[ -x "${PSQL_BIN}" ]] || { echo "ERROR: psql not found at ${PSQL_BIN}"; exit 1; }
else
  command -v "${PSQL_BIN}" >/dev/null 2>&1 || { echo "ERROR: psql not found on PATH: ${PSQL_BIN}"; exit 1; }
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${1:-${TENANT_ID:-}}"
COUNTRY_RAW="${2:-${COUNTRY:-}}"
RECON_LIMIT="${3:-${RECON_LIMIT:-2500}}"
APPLY_RAW="${4:-${APPLY_RECONCILE:-true}}"
NATIONAL_SOURCES_CSV="${5:-${NATIONAL_SOURCES_CSV:-}}"
# Keep reconciliation bounded by default to avoid hanging connector workflows.
# Can be overridden with STATEMENT_TIMEOUT or RECON_STATEMENT_TIMEOUT.
STATEMENT_TIMEOUT="${STATEMENT_TIMEOUT:-${RECON_STATEMENT_TIMEOUT:-300000}}"

if [[ -z "${TENANT_ID}" || -z "${COUNTRY_RAW}" ]]; then
  echo "ERROR: TENANT_ID and COUNTRY are required."
  echo "Usage: $0 TENANT_ID COUNTRY [LIMIT] [APPLY] [NATIONAL_SOURCES_CSV]"
  exit 1
fi
if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required."
  exit 1
fi
if ! [[ "${RECON_LIMIT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: LIMIT must be a non-negative integer."
  exit 1
fi

APPLY="false"
case "${APPLY_RAW}" in
  1|true|TRUE|yes|YES|y|Y|on|ON) APPLY="true" ;;
  0|false|FALSE|no|NO|n|N|off|OFF|"") APPLY="false" ;;
  *)
    echo "ERROR: APPLY must be true/false. Got: ${APPLY_RAW}"
    exit 1
    ;;
esac

COUNTRY="$(echo "${COUNTRY_RAW}" | tr '[:lower:]' '[:upper:]' | tr -d ' ')"
case "${COUNTRY}" in
  IRL) COUNTRY="IE" ;;
  FRA) COUNTRY="FR" ;;
  ESP) COUNTRY="ES" ;;
esac
if [[ ! "${COUNTRY}" =~ ^[A-Z]{2}$ ]]; then
  echo "ERROR: COUNTRY must resolve to a 2-letter code (IE/FR/ES). Got: ${COUNTRY_RAW}"
  exit 1
fi

NATIONAL_SOURCES_SQL="null"
if [[ -n "${NATIONAL_SOURCES_CSV}" ]]; then
  IFS=',' read -r -a source_tokens <<< "${NATIONAL_SOURCES_CSV}"
  source_literals=()
  for token in "${source_tokens[@]}"; do
    source="$(echo "${token}" | tr '[:lower:]' '[:upper:]' | tr -d ' ')"
    [[ -z "${source}" ]] && continue
    if [[ ! "${source}" =~ ^[A-Z0-9_]+$ ]]; then
      echo "ERROR: invalid national source token: ${token}"
      exit 1
    fi
    source_literals+=("'${source}'")
  done
  if [[ ${#source_literals[@]} -gt 0 ]]; then
    NATIONAL_SOURCES_SQL="array[$(IFS=,; echo "${source_literals[*]}")]::text[]"
  fi
fi

echo "== Reconcile TED to national =="
echo "tenant_id=${TENANT_ID} country=${COUNTRY} limit=${RECON_LIMIT} apply=${APPLY} national_sources=${NATIONAL_SOURCES_CSV:-<auto>}"

result="$(${PSQL_BIN} "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA -F '|' -v statement_timeout="${STATEMENT_TIMEOUT}" <<SQL
set statement_timeout = :'statement_timeout';

select
  scanned_count,
  relinked_count,
  queued_count,
  unchanged_count,
  coalesce(last_notice_id::text, '')
from public.reconcile_ted_national_links(
  p_tenant_id => '${TENANT_ID}',
  p_country => '${COUNTRY}',
  p_national_sources => ${NATIONAL_SOURCES_SQL},
  p_limit => ${RECON_LIMIT},
  p_apply => ${APPLY},
  p_auto_link_min => 0.90,
  p_queue_min => 0.75,
  p_after_notice_id => null
);
SQL
)"

scanned="$(echo "${result}" | cut -d'|' -f1 | tr -d ' ')"
relinked="$(echo "${result}" | cut -d'|' -f2 | tr -d ' ')"
queued="$(echo "${result}" | cut -d'|' -f3 | tr -d ' ')"
unchanged="$(echo "${result}" | cut -d'|' -f4 | tr -d ' ')"
last_notice_id="$(echo "${result}" | cut -d'|' -f5 | tr -d ' ')"

[[ -z "${scanned}" ]] && scanned=0
[[ -z "${relinked}" ]] && relinked=0
[[ -z "${queued}" ]] && queued=0
[[ -z "${unchanged}" ]] && unchanged=0

echo "reconcile_result scanned=${scanned} relinked=${relinked} queued=${queued} unchanged=${unchanged} last_notice_id=${last_notice_id:-<none>}"
