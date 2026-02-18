-- Verify payload offload integrity + metrics
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v tenant_id='civant_default' -f scripts/verify-payload-offload.sql

\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id civant_default
\endif

\echo '== Payload Offload Verification =='
\echo 'tenant_id=' :tenant_id

-- Offload coverage + average payload size
select 'notices' as table_name,
       count(*) as total_rows,
       sum((raw_object_key is not null)::int) as offloaded_rows,
       round(100.0 * sum((raw_object_key is not null)::int) / nullif(count(*), 0), 2) as offloaded_pct,
       round(avg(payload_bytes)::numeric, 2) as avg_payload_bytes
from public.notices
where tenant_id = :'tenant_id';

select 'TendersCurrent' as table_name,
       count(*) as total_rows,
       sum((raw_object_key is not null)::int) as offloaded_rows,
       round(100.0 * sum((raw_object_key is not null)::int) / nullif(count(*), 0), 2) as offloaded_pct,
       round(avg(payload_bytes)::numeric, 2) as avg_payload_bytes
from public."TendersCurrent"
where tenant_id = :'tenant_id';

select 'canonical_tenders' as table_name,
       count(*) as total_rows,
       sum((raw_object_key is not null)::int) as offloaded_rows,
       round(100.0 * sum((raw_object_key is not null)::int) / nullif(count(*), 0), 2) as offloaded_pct,
       round(avg(payload_bytes)::numeric, 2) as avg_payload_bytes
from public.canonical_tenders
where tenant_id = :'tenant_id';

-- Failure count
select count(*) as payload_offload_failures
from public.payload_offload_failures
where tenant_id = :'tenant_id';

-- Sample hash integrity checks (50 rows each; skip where payload is null)
with sample as (
  select raw_payload, payload_hash_sha256
  from public.notices
  where tenant_id = :'tenant_id'
    and raw_payload is not null
    and payload_hash_sha256 is not null
  order by random()
  limit 50
)
select 'notices' as table_name,
       sum((encode(digest(raw_payload::text, 'sha256'), 'hex') = payload_hash_sha256)::int) as ok_count,
       sum((encode(digest(raw_payload::text, 'sha256'), 'hex') <> payload_hash_sha256)::int) as mismatch_count
from sample;

with sample as (
  select data, payload_hash_sha256
  from public."TendersCurrent"
  where tenant_id = :'tenant_id'
    and data is not null
    and payload_hash_sha256 is not null
  order by random()
  limit 50
)
select 'TendersCurrent' as table_name,
       sum((encode(digest(data::text, 'sha256'), 'hex') = payload_hash_sha256)::int) as ok_count,
       sum((encode(digest(data::text, 'sha256'), 'hex') <> payload_hash_sha256)::int) as mismatch_count
from sample;

with sample as (
  select normalized_json, payload_hash_sha256
  from public.canonical_tenders
  where tenant_id = :'tenant_id'
    and normalized_json is not null
    and payload_hash_sha256 is not null
  order by random()
  limit 50
)
select 'canonical_tenders' as table_name,
       sum((encode(digest(normalized_json::text, 'sha256'), 'hex') = payload_hash_sha256)::int) as ok_count,
       sum((encode(digest(normalized_json::text, 'sha256'), 'hex') <> payload_hash_sha256)::int) as mismatch_count
from sample;

-- Table size metrics (table + toast + indexes)
with rels as (
  select c.oid, n.nspname, c.relname, c.reltoastrelid
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where n.nspname = 'public'
    and c.relname in ('notices', 'TendersCurrent', 'canonical_tenders')
)
select
  relname,
  pg_total_relation_size(oid) as total_bytes,
  pg_relation_size(oid) as table_bytes,
  pg_indexes_size(oid) as index_bytes,
  case when reltoastrelid = 0 then 0 else pg_total_relation_size(reltoastrelid) end as toast_bytes
from rels
order by relname;

-- Search engine check (serving)
select
  coalesce((t.data ? 'engine'), false) as has_engine_marker,
  t.data->>'engine' as engine_mode
from public.search_tenders_ranked(
  :'tenant_id',
  1,
  null, null, null, null,
  null, null, null, null, null
) as t
limit 1;

-- Prediction smoke check
select count(*) as predictions_count
from public.predictions_current
where tenant_id = :'tenant_id';
