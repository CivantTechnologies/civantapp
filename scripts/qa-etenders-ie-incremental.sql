-- QA: eTenders IE incremental connector
-- Usage:
--   psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-etenders-ie-incremental.sql

\echo '== QA: eTenders IE incremental =='
\echo 'tenant_id = :' tenant_id

-- 1) Counts by day (last 30 days)
\echo '== Counts By Day (last 30d) =='
select
  (published_at at time zone 'UTC')::date as day_utc,
  count(*) as tenders
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and tender_id like 'ETENDERS_IE:%'
  and published_at >= (now() - interval '30 days')
group by 1
order by 1 desc;

-- 2) Null-rate sanity checks
\echo '== Null-rate sanity checks (last 30d) =='
select
  count(*) as rows,
  sum(case when published_at is null then 1 else 0 end) as published_at_null,
  sum(case when coalesce(data->>'title','') = '' then 1 else 0 end) as title_null,
  sum(case when coalesce(data->>'buyer_name','') = '' then 1 else 0 end) as buyer_name_null,
  sum(case when coalesce(data->>'source_url','') = '' then 1 else 0 end) as source_url_null,
  sum(case when coalesce(data->>'deadline_date','') = '' then 1 else 0 end) as deadline_date_null
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and tender_id like 'ETENDERS_IE:%'
  and published_at >= (now() - interval '30 days');

-- 3) Duplicate checks
-- TendersCurrent is primary keyed by tender_id; this checks for any accidental tenant collisions.
\echo '== Duplicate tender_id across tenants (should be 0) =='
select
  tender_id,
  count(distinct tenant_id) as tenant_count
from public."TendersCurrent"
where tender_id like 'ETENDERS_IE:%'
group by 1
having count(distinct tenant_id) > 1
order by tenant_count desc, tender_id
limit 25;

\echo '== Duplicate versions (should be 0) =='
select
  tender_id,
  version_hash,
  count(*) as c
from public."TenderVersions"
where tenant_id = :'tenant_id'
  and tender_id like 'ETENDERS_IE:%'
group by 1,2
having count(*) > 1
order by c desc
limit 25;

-- 4) Sample newest 25 rows with evidence links
\echo '== Sample newest 25 =='
select
  tender_id,
  published_at,
  data->>'title' as title,
  data->>'buyer_name' as buyer_name,
  data->>'deadline_date' as deadline_date,
  data->>'source_url' as source_url,
  data->>'status' as status
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and tender_id like 'ETENDERS_IE:%'
order by published_at desc nulls last
limit 25;

\echo '== Done =='
