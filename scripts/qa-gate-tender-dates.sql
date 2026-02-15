-- Civant: Tender Date QA Gate
-- Read-only semantic gate to prevent corrupt mappings from reaching production.
--
-- Validates (per source for a tenant):
-- - publication_date is present
-- - deadline_date is not earlier than publication_date
--
-- Variables (psql -v):
--   tenant_id             (default: civant_default)
--   source                (optional; default: empty = all sources for tenant)
--   max_inversions        (default: 0)
--   max_inversion_pct     (default: 0)
--   max_pub_nulls         (default: 0)
--   max_pub_null_pct      (default: 0)
--   statement_timeout     (default: 0 = no timeout)
--
-- Example (strict):
--   psql "$DATABASE_URL" -v tenant_id=civant_default -v source=PLACSP_ES -f scripts/qa-gate-tender-dates.sql

\set ON_ERROR_STOP on
\pset pager off
\timing on

\if :{?statement_timeout}
\else
\set statement_timeout '0'
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

\if :{?max_inversions}
\else
\set max_inversions '0'
\endif

\if :{?max_inversion_pct}
\else
\set max_inversion_pct '0'
\endif

\if :{?max_pub_nulls}
\else
\set max_pub_nulls '0'
\endif

\if :{?max_pub_null_pct}
\else
\set max_pub_null_pct '0'
\endif

\echo 'Civant Tender Date QA Gate'
\echo 'tenant_id=' :tenant_id
\echo 'source_filter=' :source
\echo 'max_inversions=' :max_inversions
\echo 'max_inversion_pct=' :max_inversion_pct
\echo 'max_pub_nulls=' :max_pub_nulls
\echo 'max_pub_null_pct=' :max_pub_null_pct
select now() as run_at;

drop table if exists tmp_qa_gate_dates;
create temporary table tmp_qa_gate_dates as
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
    ct.publication_date,
    ct.deadline_date
  from canonical_tenders ct
  cross join params p
  where ct.tenant_id = p.tenant_id
    and (p.source is null or ct.source = p.source)
),
ct_stats as (
  select
    source,
    count(*)::bigint as ct_total,
    count(*) filter (where publication_date is null)::bigint as ct_pub_null,
    count(*) filter (where deadline_date is null)::bigint as ct_deadline_null,
    count(*) filter (where publication_date is not null and deadline_date is not null and deadline_date < publication_date)::bigint as ct_inversions
  from ct
  group by source
),
tc as (
  select
    tc.source,
    tc.tender_id,
    case
      when nullif(tc.data->>'publication_date','') is null then null
      when length(tc.data->>'publication_date') < 10 then null
      else left(tc.data->>'publication_date', 10)::date
    end as publication_date,
    case
      when nullif(tc.data->>'deadline_date','') is null then null
      when length(tc.data->>'deadline_date') < 10 then null
      else left(tc.data->>'deadline_date', 10)::date
    end as deadline_date
  from "TendersCurrent" tc
  cross join params p
  where tc.tenant_id = p.tenant_id
    and (p.source is null or tc.source = p.source)
),
tc_stats as (
  select
    source,
    count(*)::bigint as tc_total,
    count(*) filter (where publication_date is null)::bigint as tc_pub_null,
    count(*) filter (where deadline_date is null)::bigint as tc_deadline_null,
    count(*) filter (where publication_date is not null and deadline_date is not null and deadline_date < publication_date)::bigint as tc_inversions
  from tc
  group by source
)
select
  coalesce(c.source, t.source) as source,
  coalesce(c.ct_total, 0) as ct_total,
  coalesce(c.ct_pub_null, 0) as ct_pub_null,
  case
    when coalesce(c.ct_total, 0) = 0 then null
    else (coalesce(c.ct_pub_null, 0)::numeric * 100 / c.ct_total)
  end as ct_pub_null_pct,
  coalesce(c.ct_deadline_null, 0) as ct_deadline_null,
  coalesce(c.ct_inversions, 0) as ct_inversions,
  case
    when coalesce(c.ct_total, 0) = 0 then null
    else (coalesce(c.ct_inversions, 0)::numeric * 100 / c.ct_total)
  end as ct_inversion_pct,
  coalesce(t.tc_total, 0) as tc_total,
  coalesce(t.tc_pub_null, 0) as tc_pub_null,
  case
    when coalesce(t.tc_total, 0) = 0 then null
    else (coalesce(t.tc_pub_null, 0)::numeric * 100 / t.tc_total)
  end as tc_pub_null_pct,
  coalesce(t.tc_deadline_null, 0) as tc_deadline_null,
  coalesce(t.tc_inversions, 0) as tc_inversions,
  case
    when coalesce(t.tc_total, 0) = 0 then null
    else (coalesce(t.tc_inversions, 0)::numeric * 100 / t.tc_total)
  end as tc_inversion_pct
from ct_stats c
full outer join tc_stats t using (source);

\echo '--- Summary (per source)'
select
  source,
  ct_total,
  ct_pub_null,
  round(ct_pub_null_pct, 4) as ct_pub_null_pct,
  ct_deadline_null,
  ct_inversions,
  round(ct_inversion_pct, 4) as ct_inversion_pct,
  tc_total,
  tc_pub_null,
  round(tc_pub_null_pct, 4) as tc_pub_null_pct,
  tc_deadline_null,
  tc_inversions,
  round(tc_inversion_pct, 4) as tc_inversion_pct
from tmp_qa_gate_dates
order by source;

\echo '--- Canonical offenders sample (deadline < publication)'
with
params as (
  select
    :'tenant_id'::text as tenant_id,
    nullif(:'source','')::text as source
)
select
  ct.source,
  ct.canonical_id,
  ct.publication_date,
  ct.deadline_date
from canonical_tenders ct
cross join params p
where ct.tenant_id = p.tenant_id
  and (p.source is null or ct.source = p.source)
  and ct.publication_date is not null
  and ct.deadline_date is not null
  and ct.deadline_date < ct.publication_date
order by ct.publication_date desc, ct.canonical_id
limit 20;

\echo '--- Current offenders sample (deadline < publication)'
with
params as (
  select
    :'tenant_id'::text as tenant_id,
    nullif(:'source','')::text as source
),
tc as (
  select
    tc.source,
    tc.tender_id,
    case
      when nullif(tc.data->>'publication_date','') is null then null
      when length(tc.data->>'publication_date') < 10 then null
      else left(tc.data->>'publication_date', 10)::date
    end as publication_date,
    case
      when nullif(tc.data->>'deadline_date','') is null then null
      when length(tc.data->>'deadline_date') < 10 then null
      else left(tc.data->>'deadline_date', 10)::date
    end as deadline_date
  from "TendersCurrent" tc
  cross join params p
  where tc.tenant_id = p.tenant_id
    and (p.source is null or tc.source = p.source)
)
select
  source,
  tender_id,
  publication_date,
  deadline_date
from tc
where publication_date is not null
  and deadline_date is not null
  and deadline_date < publication_date
order by publication_date desc, tender_id
limit 20;

\echo '--- Gate checks'
\echo 'NOTE: psql variable substitution does not work inside DO $$ blocks; use \\gset + \\if instead.'

select
  count(*)::int as qa_rows,
  (count(*) = 0) as qa_no_rows
from tmp_qa_gate_dates
\gset

\if :qa_no_rows
\echo 'QA gate failed: no rows found for tenant_id=' :tenant_id 'source_filter=' :source
\quit 1
\endif

select
  count(*)::int as qa_bad_canonical_count,
  (count(*) > 0) as qa_bad_canonical
from tmp_qa_gate_dates
where ct_total > 0
  and (
    ct_pub_null > (:'max_pub_nulls')::bigint
    or coalesce(ct_pub_null_pct, 0) > (:'max_pub_null_pct')::numeric
    or ct_inversions > (:'max_inversions')::bigint
    or coalesce(ct_inversion_pct, 0) > (:'max_inversion_pct')::numeric
  )
\gset

select
  count(*)::int as qa_bad_current_count,
  (count(*) > 0) as qa_bad_current
from tmp_qa_gate_dates
where tc_total > 0
  and (
    tc_pub_null > (:'max_pub_nulls')::bigint
    or coalesce(tc_pub_null_pct, 0) > (:'max_pub_null_pct')::numeric
    or tc_inversions > (:'max_inversions')::bigint
    or coalesce(tc_inversion_pct, 0) > (:'max_inversion_pct')::numeric
  )
\gset

\if :qa_bad_canonical
\echo 'QA gate failed: canonical_tenders date checks failed for' :qa_bad_canonical_count 'source(s)'
\quit 1
\endif

\if :qa_bad_current
\echo 'QA gate failed: TendersCurrent date checks failed for' :qa_bad_current_count 'source(s)'
\quit 1
\endif

\echo 'QA gate PASSED.'
