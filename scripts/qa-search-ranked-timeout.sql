-- QA: ranked search timeout / engine behavior
-- Usage:
--   psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-search-ranked-timeout.sql

\echo '== QA: ranked search timeout checks =='
\echo 'tenant_id = :' tenant_id

\echo '== Latest zero-result engine distribution (last 1 day) =='
select
  search_engine,
  count(*) as searches,
  round(avg(latency_ms)::numeric, 2) as avg_latency_ms,
  max(latency_ms) as max_latency_ms,
  sum(case when coalesce(metadata_json->>'rpc_error','') ilike '%timeout%' then 1 else 0 end) as timeout_errors
from public.search_zero_results_log
where tenant_id = :'tenant_id'
  and created_at >= (now() - interval '1 day')
group by 1
order by searches desc;

\echo '== Smoke RPC (broad but bounded) =='
select
  count(*) as rpc_rows
from public.search_tenders_ranked(
  :'tenant_id',
  50,
  null,
  'ES',
  'PLACSP_ES',
  null,
  null,
  90,
  null,
  null,
  90
);

\echo '== Explain analyze (ES + PLACSP + keyword) =='
explain (analyze, buffers, costs, timing)
select count(*)
from public.search_tenders_ranked(
  :'tenant_id',
  100,
  'software',
  'ES',
  'PLACSP_ES',
  null,
  null,
  180,
  'it',
  null,
  365
);

\echo '== Done =='
