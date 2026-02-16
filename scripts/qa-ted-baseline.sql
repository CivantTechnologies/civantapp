-- QA: TED baseline merged data
-- Usage:
--   psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-ted-baseline.sql

\echo '== QA: TED baseline ==' 
\echo 'tenant_id = :' tenant_id

\echo '== Counts by month (last 12 months) =='
select
  to_char(date_trunc('month', published_at at time zone 'UTC'), 'YYYY-MM') as month_utc,
  count(*) as tenders
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and source = 'TED'
  and published_at >= (now() - interval '12 months')
group by 1
order by 1 desc;

\echo '== Null-rate sanity (TED in current) =='
select
  count(*) as rows,
  sum(case when published_at is null then 1 else 0 end) as published_at_null,
  sum(case when coalesce(data->>'source_notice_id','') = '' then 1 else 0 end) as source_notice_id_null,
  sum(case when coalesce(data->>'title','') = '' then 1 else 0 end) as title_null,
  sum(case when coalesce(data->>'buyer_name','') = '' then 1 else 0 end) as buyer_name_null,
  sum(case when coalesce(data->>'source_url','') = '' then 1 else 0 end) as source_url_null
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and source = 'TED';

\echo '== Duplicate tender IDs in current (should be 0 by PK) =='
select tender_id, count(*) as c
from public."TendersCurrent"
where tenant_id = :'tenant_id'
  and source = 'TED'
group by tender_id
having count(*) > 1
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

\echo '== Sample latest 25 =='
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

\echo '== Connector run snapshots (TED latest 10) =='
select
  started_at,
  status,
  metadata->>'mode' as mode,
  metadata->>'fetched_count' as fetched_count,
  metadata->>'distinct_tenders' as distinct_tenders,
  metadata->>'inserted_count' as inserted_count,
  metadata->>'updated_count' as updated_count,
  metadata->>'noop_count' as noop_count,
  metadata->>'versioned_count' as versioned_count,
  metadata->>'error' as error
from public."ConnectorRuns"
where tenant_id = :'tenant_id'
  and connector_key like 'ted_%'
order by started_at desc
limit 10;
