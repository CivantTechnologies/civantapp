-- Civant: TendersCurrent Coverage Check
-- Read-only diagnostics comparing canonical_tenders and "TendersCurrent" per source for a tenant.
--
-- Variables (psql -v):
--   tenant_id  (default: civant_default)
--   source     (optional; default: empty = all sources)
--
-- Example:
--   psql "$DATABASE_URL" -v tenant_id=civant_default -v source=PLACSP_ES -f scripts/check-tenderscurrent-coverage.sql

\set ON_ERROR_STOP on
\pset pager off
\timing on

\if :{?statement_timeout}
\else
\set statement_timeout '10min'
\endif

set statement_timeout = :'statement_timeout';

\if :{?tenant_id}
\else
\set tenant_id 'civant_default'
\endif

\if :{?source}
\else
\set source ''
\endif

\echo 'Civant TendersCurrent Coverage Check'
\echo 'tenant_id=' :tenant_id
\echo 'source_filter=' :source
select now() as run_at;

\echo '--- Per-source coverage (canonical_tenders vs TendersCurrent)'
with
params as (
  select
    :'tenant_id'::text as tenant_id,
    nullif(:'source','')::text as source
),
ct as (
  select
    ct.source,
    ct.canonical_id,
    ct.publication_date
  from canonical_tenders ct
  cross join params p
  where ct.tenant_id = p.tenant_id
    and (p.source is null or ct.source = p.source)
),
tc as (
  select
    tc.source,
    tc.tender_id,
    tc.published_at
  from "TendersCurrent" tc
  cross join params p
  where tc.tenant_id = p.tenant_id
    and (p.source is null or tc.source = p.source)
)
select
  coalesce(ct.source, tc.source) as source,
  count(ct.canonical_id) as canonical_rows,
  count(tc.tender_id) as current_rows,
  count(*) filter (where ct.canonical_id is not null and tc.tender_id is not null) as joined_rows,
  count(*) filter (where ct.canonical_id is not null and tc.tender_id is null) as canonical_missing_current,
  count(*) filter (where ct.canonical_id is null and tc.tender_id is not null) as current_missing_canonical,
  min(ct.publication_date) as canonical_min_publication_date,
  max(ct.publication_date) as canonical_max_publication_date,
  min(tc.published_at) as current_min_published_at,
  max(tc.published_at) as current_max_published_at,
  count(*) filter (where tc.tender_id is not null and tc.published_at is null) as current_published_at_null,
  count(*) filter (where ct.canonical_id is not null and ct.canonical_id not like ct.source || ':%') as canonical_id_prefix_mismatch,
  count(*) filter (where tc.tender_id is not null and tc.tender_id not like tc.source || ':%') as tender_id_prefix_mismatch
from ct
full outer join tc
  on tc.tender_id = ct.canonical_id
 and tc.source = ct.source
group by coalesce(ct.source, tc.source)
order by source;

\echo ''
\echo 'Tip: set SOURCE=<SOURCE_ID> to focus on a single source (faster + easier to debug).'
