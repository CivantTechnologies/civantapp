#!/usr/bin/env bash
set -euo pipefail

# BOAMP FR Backfill by Month
#
# Usage (from repo root):
#   ./backfill-boamp-fr-by-month.sh [START_YYYY-MM] [END_YYYY-MM]
#
# Example:
#   ./backfill-boamp-fr-by-month.sh 2022-07 2025-12

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOAMP_SCRIPT="${SCRIPT_DIR}/scripts/boamp/boamp-fr-incremental.mjs"

if [[ ! -f "${BOAMP_SCRIPT}" ]]; then
  echo "ERROR: Cannot find ${BOAMP_SCRIPT}. Run this from the repo root."
  exit 1
fi

DATABASE_URL="${SUPABASE_DB_URL:?SUPABASE_DB_URL is required}"
DEFAULT_PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "${PSQL_BIN}" ]]; then
  if [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
    PSQL_BIN="${DEFAULT_PSQL_BIN}"
  else
    PSQL_BIN="psql"
  fi
fi

TENANT_ID="${TENANT_ID:-civant_default}"
START_MONTH="${1:-2022-07}"
END_MONTH="${2:-2025-12}"

echo "=========================================="
echo "BOAMP FR Monthly Backfill"
echo "tenant=${TENANT_ID}  range=${START_MONTH} to ${END_MONTH}"
echo "=========================================="

next_month_fn() {
  date -j -v+1m -f "%Y-%m-%d" "$1" "+%Y-%m-%d" 2>/dev/null \
    || date -d "$1 + 1 month" "+%Y-%m-%d" 2>/dev/null
}

months=()
current="${START_MONTH}-01"
end_boundary="$(next_month_fn "${END_MONTH}-01")"

while [[ "${current}" < "${end_boundary}" ]]; do
  months+=("${current}")
  current="$(next_month_fn "${current}")"
done

echo "Months to process: ${#months[@]}"
echo ""

succeeded=0
failed=0
total_fetched=0
total_inserted=0
total_updated=0
failed_list=()

for month_start in "${months[@]}"; do
  month_end="$(next_month_fn "${month_start}")"
  label="${month_start:0:7}"

  echo "-- [${label}] fetching (${month_start} to ${month_end}) --"

  TSV_FILE="$(mktemp)"

  # Fetch this month only (--end-date constrains to single month)
  node "${BOAMP_SCRIPT}" \
    --tenant-id "${TENANT_ID}" \
    --start-date "${month_start}" \
    --end-date "${month_end}" \
    --dry-run false \
    --max-pages 100 \
    >"${TSV_FILE}" 2>/dev/null || {
    echo "-- [${label}] FETCH FAILED --"
    failed=$((failed + 1))
    failed_list+=("${label}")
    rm -f "${TSV_FILE}"
    continue
  }

  ROWS="$(wc -l <"${TSV_FILE}" | tr -d ' ')"
  echo "-- [${label}] fetched ${ROWS} rows --"

  if [[ "${ROWS}" -eq 0 || ! -s "${TSV_FILE}" ]]; then
    echo "-- [${label}] no data, skipping --"
    rm -f "${TSV_FILE}"
    succeeded=$((succeeded + 1))
    continue
  fi

  total_fetched=$((total_fetched + ROWS))

  # Write SQL to a temp file so heredoc quoting doesn't bite us
  SQL_FILE="$(mktemp)"
  cat >"${SQL_FILE}" <<EOSQL
set statement_timeout = '300000';
begin;

create temp table tmp_boamp_backfill (
  tenant_id text,
  tender_id text,
  source text,
  published_at timestamptz,
  data jsonb,
  version_hash text
) on commit drop;

\copy tmp_boamp_backfill from '${TSV_FILE}' with (format text, delimiter E'\t');

insert into public."TenderVersions" (tenant_id, tender_id, version_hash, data, created_at)
select tenant_id, tender_id, version_hash, data, now()
from tmp_boamp_backfill
on conflict (tender_id, version_hash) do nothing;

with up as (
  insert into public."TendersCurrent" (tenant_id, tender_id, source, published_at, data, updated_at)
  select tenant_id, tender_id, source, published_at, data, now()
  from tmp_boamp_backfill
  on conflict (tender_id) do update set
    tenant_id = excluded.tenant_id,
    source = excluded.source,
    published_at = excluded.published_at,
    data = excluded.data,
    updated_at = now()
  where public."TendersCurrent".data is distinct from excluded.data
  returning (xmax = 0) as inserted
)
select
  count(*) filter (where inserted) as ins,
  count(*) filter (where not inserted) as upd
from up;

commit;
EOSQL

  result=$("${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA -f "${SQL_FILE}" 2>&1) || {
    echo "-- [${label}] DB UPSERT FAILED --"
    echo "${result}" | tail -3
    failed=$((failed + 1))
    failed_list+=("${label}")
    rm -f "${TSV_FILE}" "${SQL_FILE}"
    sleep 1
    continue
  }

  rm -f "${SQL_FILE}"

  ins=$(echo "${result}" | grep -v '^$' | tail -1 | awk -F'|' '{print $1}' | tr -d ' ')
  upd=$(echo "${result}" | grep -v '^$' | tail -1 | awk -F'|' '{print $2}' | tr -d ' ')
  ins="${ins:-0}"
  upd="${upd:-0}"

  echo "-- [${label}] OK (inserted=${ins} updated=${upd}) --"
  total_inserted=$((total_inserted + ins))
  total_updated=$((total_updated + upd))
  succeeded=$((succeeded + 1))

  rm -f "${TSV_FILE}"
  sleep 2
done

echo ""
echo "=========================================="
echo "Backfill complete"
echo "Months: ${succeeded} succeeded, ${failed} failed"
echo "Total fetched: ${total_fetched}  inserted: ${total_inserted}  updated: ${total_updated}"
if [[ ${#failed_list[@]} -gt 0 ]]; then
  echo "Failed: ${failed_list[*]}"
fi
echo "=========================================="
