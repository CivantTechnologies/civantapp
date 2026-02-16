-- QA: Search relevance tuning for PLACSP ES (A + B)
-- Usage:
--   psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-search-relevance-placsp-es.sql

\echo '== QA: Search relevance (PLACSP ES) =='
\echo 'tenant_id = :' tenant_id

\echo '== Baseline open count (deadline in next 90 days, non-closed) =='
select
  count(*) as open_next_90d
from public."TendersCurrent" tc
where tc.tenant_id = :'tenant_id'
  and tc.source = 'PLACSP_ES'
  and coalesce(tc.data->>'notice_type', '') <> 'award'
  and upper(coalesce(tc.data->>'status_code', '')) not in ('RES', 'ADJ', 'CAN', 'DES')
  and left(coalesce(tc.data->>'deadline_date', ''), 10) ~ '^\d{4}-\d{2}-\d{2}$'
  and left(tc.data->>'deadline_date', 10)::date between current_date and (current_date + 90);

\echo '== Ranked RPC sample (Spain open preset) =='
select
  r.relevance_score,
  r.source,
  r.published_at,
  r.data->>'title' as title,
  r.data->>'buyer_name' as buyer_name,
  r.data->>'deadline_date' as deadline_date,
  coalesce(r.data->>'source_url', r.data->>'url') as source_url
from public.search_tenders_ranked(
  :'tenant_id',
  25,
  null,            -- keyword
  'ES',            -- country
  'PLACSP_ES',     -- source
  null,            -- buyer
  null,            -- cpv list
  90,              -- deadline within days
  null,            -- industry
  null,            -- institution type
  90               -- published within days
) r
order by r.relevance_score desc, r.published_at desc nulls last;

\echo '== Keyword ranking sample: "software" =='
select
  r.relevance_score,
  r.data->>'title' as title,
  r.data->>'buyer_name' as buyer_name,
  r.data->>'deadline_date' as deadline_date
from public.search_tenders_ranked(
  :'tenant_id',
  20,
  'software',
  'ES',
  'PLACSP_ES',
  null,
  null,
  180,
  'it',
  null,
  365
) r
order by r.relevance_score desc, r.published_at desc nulls last;

\echo '== Closed leakage check (should be 0 rows) =='
select
  r.tender_id,
  r.data->>'title' as title,
  r.data->>'status_code' as status_code,
  r.data->>'notice_type' as notice_type
from public.search_tenders_ranked(
  :'tenant_id',
  200,
  null,
  'ES',
  'PLACSP_ES',
  null,
  null,
  30,
  null,
  null,
  90
) r
where upper(coalesce(r.data->>'status_code', '')) in ('RES', 'ADJ', 'CAN', 'DES')
   or lower(coalesce(r.data->>'notice_type', '')) = 'award'
limit 25;

\echo '== Done =='
