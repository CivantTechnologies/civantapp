-- QA: search zero-result telemetry
-- Usage:
--   psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-search-zero-results-log.sql

\echo '== QA: search zero-result telemetry =='
\echo 'tenant_id = :' tenant_id

\echo '== Count by day (last 30 days) =='
select
  (created_at at time zone 'UTC')::date as day_utc,
  count(*) as zero_result_searches
from public.search_zero_results_log
where tenant_id = :'tenant_id'
  and created_at >= (now() - interval '30 days')
group by 1
order by 1 desc;

\echo '== Top zero-result filter combinations (last 30 days) =='
select
  coalesce(filters_json->>'country', 'all') as country,
  coalesce(filters_json->>'source', 'all') as source,
  coalesce(filters_json->>'deadline_within', 'all') as deadline_within,
  coalesce(filters_json->>'industry', 'all') as industry,
  coalesce(filters_json->>'institution_type', 'all') as institution_type,
  coalesce(filters_json->>'last_tendered', 'all') as last_tendered,
  count(*) as searches
from public.search_zero_results_log
where tenant_id = :'tenant_id'
  and created_at >= (now() - interval '30 days')
group by 1,2,3,4,5,6
order by searches desc
limit 30;

\echo '== Engine / latency summary (last 7 days) =='
select
  search_engine,
  count(*) as searches,
  round(avg(latency_ms)::numeric, 2) as avg_latency_ms,
  max(latency_ms) as max_latency_ms
from public.search_zero_results_log
where tenant_id = :'tenant_id'
  and created_at >= (now() - interval '7 days')
group by 1
order by searches desc;

\echo '== Latest 25 rows =='
select
  created_at,
  search_engine,
  returned_rows,
  scanned_rows,
  candidate_rows,
  latency_ms,
  filters_json,
  metadata_json
from public.search_zero_results_log
where tenant_id = :'tenant_id'
order by created_at desc
limit 25;

\echo '== Done =='
