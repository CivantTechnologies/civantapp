#!/usr/bin/env bash
set -euo pipefail

# Civant: "Don't break production" rollout helper for Award-to-Prediction Signals v1 (Ireland only).
#
# Required:
#   SUPABASE_DB_URL or DATABASE_URL
#
# Optional:
#   TENANT_ID (defaults to null = all tenants)
#   FORCE_RELATION (e.g. public.canonical_tenders or public."TendersCurrent")
#   PSQL_BIN (defaults to Homebrew libpq psql)
#
# Examples:
#   SUPABASE_DB_URL='postgresql://...' TENANT_ID='civant_default' ./scripts/rollout-award-signals-ie.sh
#   SUPABASE_DB_URL='postgresql://...' TENANT_ID='civant_default' FORCE_RELATION='public.canonical_tenders' ./scripts/rollout-award-signals-ie.sh

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${TENANT_ID:-}"
FORCE_RELATION="${FORCE_RELATION:-}"

MIGRATION_GLOB="/Users/davidmanrique/Documents/Civant/database/migrations/20260215_award_to_prediction_signals_ie_v1*.sql"
QA_FILE="/Users/davidmanrique/Documents/Civant/scripts/qa-award-signals-ie.sql"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required."
  exit 1
fi
if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi
shopt -s nullglob
migrations=( ${MIGRATION_GLOB} )
shopt -u nullglob
if [[ "${#migrations[@]}" -eq 0 ]]; then
  echo "ERROR: no migration files found matching ${MIGRATION_GLOB}"
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-15}"

tenant_sql="null"
if [[ -n "${TENANT_ID}" ]]; then
  tenant_sql="'${TENANT_ID}'::text"
fi

rel_sql="null"
if [[ -n "${FORCE_RELATION}" ]]; then
  # regclass cast is safest; FORCE_RELATION must be a valid, schema-qualified name.
  rel_sql="'${FORCE_RELATION}'::regclass"
fi

echo "== Applying migrations =="
for f in "${migrations[@]}"; do
  echo "Applying: ${f}"
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -f "${f}"
done

echo "== Confirming objects exist =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<'SQL'
select
  to_regclass('public.award_fact_ie') is not null as has_fact,
  to_regclass('public.award_signals_ie') is not null as has_signals,
  to_regclass('public.cpv_cluster_map_ie') is not null as has_map;

select proname
from pg_proc
where proname in ('refresh_award_fact_ie','refresh_award_signals_ie')
order by 1;
SQL

echo "== Field discovery (candidate relations/columns) =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<'SQL'
with
patterns as (
  select * from unnest(array[
    'canonical_id','tender_id','source','source_system','notice_type',
    'buyer','authority','contracting','supplier','winner','awardee',
    'value','amount','price','cpv','procedure','title','description',
    'publication_date','published_at','award_date','date_awarded','contract_award_date',
    'start_date','end_date','duration'
  ]) as p(pattern)
),
cols as (
  select
    c.table_schema,
    c.table_name,
    c.column_name,
    p.pattern
  from information_schema.columns c
  join patterns p
    on c.column_name ilike '%' || p.pattern || '%'
  where c.table_schema not in ('pg_catalog','information_schema')
)
select
  table_schema,
  table_name,
  count(*) as matched_columns,
  array_agg(distinct column_name order by column_name) as columns
from cols
group by 1,2
order by matched_columns desc, table_schema, table_name
limit 60;
SQL

echo "== Refresh facts (run 1) =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
select * from refresh_award_fact_ie(${tenant_sql}, ${rel_sql});
SQL

echo "== Refresh facts (run 2, idempotence check) =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
select * from refresh_award_fact_ie(${tenant_sql}, ${rel_sql});
SQL

echo "== Refresh signals =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<'SQL'
select * from refresh_award_signals_ie();
SQL

echo "== Fact rowcount + award_date range =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
select
  count(*) as fact_rows,
  min(award_date) as min_award_date,
  max(award_date) as max_award_date
from award_fact_ie
where (${tenant_sql} is null or tenant_id = ${tenant_sql});
SQL

echo "== Fact completeness (% null) =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL
select
  round(100.0 * count(*) filter (where supplier_id is null) / nullif(count(*),0), 2) as pct_supplier_id_null,
  round(100.0 * count(*) filter (where award_value_eur is null) / nullif(count(*),0), 2) as pct_award_value_null,
  round(100.0 * count(*) filter (where cpv_primary is null) / nullif(count(*),0), 2) as pct_cpv_primary_null
from award_fact_ie
where source_system = 'ETENDERS_IE'
  and (${tenant_sql} is null or tenant_id = ${tenant_sql});
SQL

echo "== Signals UNKNOWN share =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<'SQL'
select
  count(*) as signals_total,
  count(*) filter (where cpv_cluster = 'UNKNOWN') as signals_unknown,
  round(100.0 * count(*) filter (where cpv_cluster = 'UNKNOWN') / nullif(count(*), 0), 2) as pct_unknown
from award_signals_ie;
SQL

echo "== Quick spot check (top confidence) =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<'SQL'
select
  buyer_id,
  cpv_cluster,
  confidence_score,
  expected_next_tender_date,
  expected_window_start,
  expected_window_end,
  explanations
from award_signals_ie
order by confidence_score desc
limit 25;
SQL

if [[ -f "${QA_FILE}" ]]; then
  echo "== QA pack =="
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -f "${QA_FILE}"
else
  echo "== QA pack skipped (missing ${QA_FILE}) =="
fi

echo "== Done =="
