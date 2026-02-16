-- QA: TED incremental connector
-- Usage:
--   psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-ted-incremental.sql

\echo '== QA: TED incremental =='
\echo 'tenant_id = :' tenant_id

\echo '== Counts By Day (last 30d) =='
select
  (published_at at time zone 'UTC')::date as day_utc,
  count(*) as tenders
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and source = 'TED'
  and published_at >= (now() - interval '30 days')
group by 1
order by 1 desc;

\echo '== Counts by country (last 30d) =='
select
  coalesce(data->>'country', 'unknown') as country,
  count(*) as tenders
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and source = 'TED'
  and published_at >= (now() - interval '30 days')
group by 1
order by tenders desc, country asc;

\echo '== Null-rate sanity checks (last 30d) =='
select
  count(*) as rows,
  sum(case when published_at is null then 1 else 0 end) as published_at_null,
  sum(case when coalesce(data->>'source_notice_id','') = '' then 1 else 0 end) as source_notice_id_null,
  sum(case when coalesce(data->>'title','') = '' then 1 else 0 end) as title_null,
  sum(case when coalesce(data->>'buyer_name','') = '' then 1 else 0 end) as buyer_name_null,
  sum(case when coalesce(data->>'source_url','') = '' then 1 else 0 end) as source_url_null
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and source = 'TED'
  and published_at >= (now() - interval '30 days');

\echo '== Duplicate tender_id across tenants (should be 0) =='
select
  tender_id,
  count(distinct tenant_id) as tenant_count
from public."TendersCurrent"
where tender_id like 'TED:%'
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
  and tender_id like 'TED:%'
group by 1,2
having count(*) > 1
order by c desc
limit 25;

\echo '== Sample newest 25 =='
select
  tender_id,
  published_at,
  data->>'country' as country,
  data->>'title' as title,
  data->>'buyer_name' as buyer_name,
  data->>'notice_type' as notice_type,
  data->>'source_url' as source_url
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and source = 'TED'
order by published_at desc nulls last
limit 25;

\echo '== Recent connector runs =='
select
  started_at,
  status,
  metadata->>'fetched_count' as fetched_count,
  metadata->>'inserted_count' as inserted_count,
  metadata->>'updated_count' as updated_count,
  metadata->>'noop_count' as noop_count,
  metadata->>'versioned_count' as versioned_count,
  metadata->>'error' as error
from public."ConnectorRuns"
where tenant_id = :'tenant_id'
  and connector_key = ('ted_incremental:' || :'tenant_id')
order by started_at desc
limit 10;

\echo '== Done =='
