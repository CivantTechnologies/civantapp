#!/usr/bin/env bash
set -euo pipefail

# Civant canonical integrity rollout:
# 1) Refresh canonical primary source fields from linked notices
# 2) Reconcile TED notices against eTenders canonicals (deterministic scoring)
#
# Usage:
#   ./scripts/rollout-canonical-primary-source-reconcile.sh TENANT_ID [BACKFILL_LIMIT] [RECON_LIMIT] [APPLY_RECONCILE] [COUNTRY]
#
# Example dry-run reconciliation (no relink writes):
#   ./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 10000 500 false IE
#
# Example apply reconciliation:
#   ./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 10000 500 true IE

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
COUNTRY="${5:-${COUNTRY:-IE}}"

if [[ -z "${TENANT_ID}" ]]; then
  echo "ERROR: TENANT_ID is required."
  echo "Usage: $0 TENANT_ID [BACKFILL_LIMIT] [RECON_LIMIT] [APPLY_RECONCILE] [COUNTRY]"
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
  echo "ERROR: BACKFILL_LIMIT must be a non-negative integer."
  exit 1
fi
if ! [[ "${RECON_LIMIT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: RECON_LIMIT must be a non-negative integer."
  exit 1
fi

echo "== Canonical primary source + TED/eTenders reconciliation =="
echo "tenant_id=${TENANT_ID} backfill_limit=${BACKFILL_LIMIT} recon_limit=${RECON_LIMIT} apply_reconcile=${APPLY_RECONCILE} country=${COUNTRY}"

"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
begin;

select *
from public.backfill_canonical_primary_sources('${TENANT_ID}', ${BACKFILL_LIMIT});

select *
from public.reconcile_ted_etenders_links(
  p_tenant_id => '${TENANT_ID}',
  p_country => '${COUNTRY}',
  p_limit => ${RECON_LIMIT},
  p_apply => ${APPLY_RECONCILE},
  p_auto_link_min => 0.90,
  p_queue_min => 0.75
);

commit;
SQL

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
