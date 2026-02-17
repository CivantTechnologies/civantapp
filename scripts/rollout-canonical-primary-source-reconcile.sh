#!/usr/bin/env bash
set -euo pipefail

# Civant canonical integrity rollout:
# 1) Refresh canonical primary source fields from linked notices
# 2) Reconcile TED notices against national canonicals (IE/FR/ES) with deterministic scoring
#
# Usage:
#   ./scripts/rollout-canonical-primary-source-reconcile.sh TENANT_ID [BACKFILL_LIMIT] [RECON_LIMIT] [APPLY_RECONCILE] [COUNTRY_LIST]
#
# Example dry-run reconciliation (no relink writes):
#   ./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 10000 500 false IE,FR,ES
#
# Example apply reconciliation:
#   ./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 0 0 true IE,FR,ES

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
BACKFILL_LIMIT="${2:-${BACKFILL_LIMIT:-10000}}"
RECON_LIMIT="${3:-${RECON_LIMIT:-500}}"
APPLY_RECON_RAW="${4:-${APPLY_RECONCILE:-false}}"
COUNTRY_LIST="${5:-${COUNTRY_LIST:-${COUNTRY:-IE}}}"
BACKFILL_BATCH_SIZE="${BACKFILL_BATCH_SIZE:-5000}"
RECON_BATCH_SIZE="${RECON_BATCH_SIZE:-20}"

if [[ -z "${TENANT_ID}" ]]; then
  echo "ERROR: TENANT_ID is required."
  echo "Usage: $0 TENANT_ID [BACKFILL_LIMIT] [RECON_LIMIT] [APPLY_RECONCILE] [COUNTRY_LIST]"
  exit 1
fi

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required."
  exit 1
fi

APPLY_RECONCILE="false"
case "${APPLY_RECON_RAW}" in
  1|true|TRUE|yes|YES|y|Y|on|ON) APPLY_RECONCILE="true" ;;
  0|false|FALSE|no|NO|n|N|off|OFF|"") APPLY_RECONCILE="false" ;;
  *)
    echo "ERROR: APPLY_RECONCILE must be true/false. Got: ${APPLY_RECON_RAW}"
    exit 1
    ;;
esac

if ! [[ "${BACKFILL_LIMIT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: BACKFILL_LIMIT must be a non-negative integer (0 means full scan)."
  exit 1
fi
if ! [[ "${RECON_LIMIT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: RECON_LIMIT must be a non-negative integer (0 means full scan)."
  exit 1
fi
if ! [[ "${BACKFILL_BATCH_SIZE}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: BACKFILL_BATCH_SIZE must be a positive integer."
  exit 1
fi
if ! [[ "${RECON_BATCH_SIZE}" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: RECON_BATCH_SIZE must be a positive integer."
  exit 1
fi

echo "== Canonical primary source + TED/national reconciliation =="
echo "tenant_id=${TENANT_ID} backfill_limit=${BACKFILL_LIMIT} recon_limit=${RECON_LIMIT} apply_reconcile=${APPLY_RECONCILE} countries=${COUNTRY_LIST} backfill_batch=${BACKFILL_BATCH_SIZE} recon_batch=${RECON_BATCH_SIZE}"

total_backfill_processed=0
total_backfill_errors=0
backfill_after=""
backfill_remaining="${BACKFILL_LIMIT}"

echo "== Backfill primary sources (batched) =="
while true; do
  if [[ "${BACKFILL_LIMIT}" -eq 0 ]]; then
    chunk_limit="${BACKFILL_BATCH_SIZE}"
  else
    if [[ "${backfill_remaining}" -le 0 ]]; then
      break
    fi
    if [[ "${backfill_remaining}" -lt "${BACKFILL_BATCH_SIZE}" ]]; then
      chunk_limit="${backfill_remaining}"
    else
      chunk_limit="${BACKFILL_BATCH_SIZE}"
    fi
  fi

  backfill_after_sql="null"
  if [[ -n "${backfill_after}" ]]; then
    backfill_after_escaped="${backfill_after//\'/\'\'}"
    backfill_after_sql="'${backfill_after_escaped}'"
  fi

  chunk_result="$("${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA -F '|' <<SQL
select processed_count, error_count, coalesce(last_canonical_id, '')
from public.backfill_canonical_primary_sources_chunk(
  p_tenant_id => '${TENANT_ID}',
  p_limit => ${chunk_limit},
  p_after_canonical_id => ${backfill_after_sql}
);
SQL
)"

  chunk_processed="$(echo "${chunk_result}" | cut -d'|' -f1 | tr -d ' ')"
  chunk_errors="$(echo "${chunk_result}" | cut -d'|' -f2 | tr -d ' ')"
  chunk_last="$(echo "${chunk_result}" | cut -d'|' -f3- | tr -d ' ')"
  [[ -z "${chunk_processed}" ]] && chunk_processed=0
  [[ -z "${chunk_errors}" ]] && chunk_errors=0

  total_backfill_processed=$((total_backfill_processed + chunk_processed))
  total_backfill_errors=$((total_backfill_errors + chunk_errors))
  [[ -n "${chunk_last}" ]] && backfill_after="${chunk_last}"

  echo "backfill_batch processed=${chunk_processed} errors=${chunk_errors} last_canonical_id=${chunk_last:-<none>}"

  if [[ "${BACKFILL_LIMIT}" -ne 0 ]]; then
    backfill_remaining=$((backfill_remaining - chunk_processed))
  fi
  if [[ "${chunk_processed}" -lt "${chunk_limit}" || -z "${chunk_last}" ]]; then
    break
  fi
done

echo "backfill_total processed=${total_backfill_processed} errors=${total_backfill_errors}"

total_scanned=0
total_relinked=0
total_queued=0
total_unchanged=0

country_csv="$(echo "${COUNTRY_LIST}" | tr '[:lower:]' '[:upper:]' | tr -d ' ')"
IFS=',' read -r -a countries <<< "${country_csv}"

for country in "${countries[@]}"; do
  [[ -z "${country}" ]] && continue
  recon_after=""
  recon_remaining="${RECON_LIMIT}"
  country_scanned=0
  country_relinked=0
  country_queued=0
  country_unchanged=0

  echo "== Reconcile country=${country} (batched) =="
  while true; do
    if [[ "${RECON_LIMIT}" -eq 0 ]]; then
      recon_chunk_limit="${RECON_BATCH_SIZE}"
    else
      if [[ "${recon_remaining}" -le 0 ]]; then
        break
      fi
      if [[ "${recon_remaining}" -lt "${RECON_BATCH_SIZE}" ]]; then
        recon_chunk_limit="${recon_remaining}"
      else
        recon_chunk_limit="${RECON_BATCH_SIZE}"
      fi
    fi

    recon_after_sql="null"
    if [[ -n "${recon_after}" ]]; then
      recon_after_escaped="${recon_after//\'/\'\'}"
      recon_after_sql="'${recon_after_escaped}'::uuid"
    fi

    recon_result="$("${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA -F '|' <<SQL
select scanned_count, relinked_count, queued_count, unchanged_count, coalesce(last_notice_id::text, '')
from public.reconcile_ted_national_links(
  p_tenant_id => '${TENANT_ID}',
  p_country => '${country}',
  p_national_sources => null,
  p_limit => ${recon_chunk_limit},
  p_apply => ${APPLY_RECONCILE},
  p_auto_link_min => 0.90,
  p_queue_min => 0.75,
  p_after_notice_id => ${recon_after_sql}
);
SQL
)"

    scanned="$(echo "${recon_result}" | cut -d'|' -f1 | tr -d ' ')"
    relinked="$(echo "${recon_result}" | cut -d'|' -f2 | tr -d ' ')"
    queued="$(echo "${recon_result}" | cut -d'|' -f3 | tr -d ' ')"
    unchanged="$(echo "${recon_result}" | cut -d'|' -f4 | tr -d ' ')"
    recon_last="$(echo "${recon_result}" | cut -d'|' -f5 | tr -d ' ')"
    [[ -z "${scanned}" ]] && scanned=0
    [[ -z "${relinked}" ]] && relinked=0
    [[ -z "${queued}" ]] && queued=0
    [[ -z "${unchanged}" ]] && unchanged=0

    country_scanned=$((country_scanned + scanned))
    country_relinked=$((country_relinked + relinked))
    country_queued=$((country_queued + queued))
    country_unchanged=$((country_unchanged + unchanged))
    [[ -n "${recon_last}" ]] && recon_after="${recon_last}"

    echo "recon_batch country=${country} scanned=${scanned} relinked=${relinked} queued=${queued} unchanged=${unchanged} last_notice_id=${recon_last:-<none>}"

    if [[ "${RECON_LIMIT}" -ne 0 ]]; then
      recon_remaining=$((recon_remaining - scanned))
    fi
    if [[ "${scanned}" -lt "${recon_chunk_limit}" || -z "${recon_last}" ]]; then
      break
    fi
  done

  total_scanned=$((total_scanned + country_scanned))
  total_relinked=$((total_relinked + country_relinked))
  total_queued=$((total_queued + country_queued))
  total_unchanged=$((total_unchanged + country_unchanged))

  echo "recon_total country=${country} scanned=${country_scanned} relinked=${country_relinked} queued=${country_queued} unchanged=${country_unchanged}"
done

echo "recon_grand_total scanned=${total_scanned} relinked=${total_relinked} queued=${total_queued} unchanged=${total_unchanged}"

echo "== Post-run summary =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
select
  coverage_status,
  verification_level,
  count(*)::int as rows
from public.canonical_tenders
where tenant_id = '${TENANT_ID}'
group by 1,2
order by 1,2;

select
  count(*)::int as pending_reconciliation_items
from public.reconciliation_queue
where tenant_id = '${TENANT_ID}'
  and status = 'pending';
SQL

echo "Done."
