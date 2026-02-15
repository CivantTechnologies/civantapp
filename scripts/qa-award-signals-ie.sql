-- Civant QA Pack: Award-to-Prediction Signals v1 (Ireland only)
-- Tables: award_fact_ie, award_signals_ie
-- Source: ETENDERS_IE

-- 0) Refresh (idempotent)
-- Facts: returns total award rows seen in the canonical source and how many were skipped.
-- select * from refresh_award_fact_ie(null, null);

-- Signals: full recompute/upsert (v1).
-- select * from refresh_award_signals_ie();

-- 0b) Embedded award detection QA (ETENDERS_IE in public."TendersCurrent".data)
-- How many award-events are detected via award_published (parseable DD/MM/YYYY) vs parse failures.
with base as (
  select
    tenant_id,
    data->>'award_published' as award_published
  from public."TendersCurrent"
  where source = 'ETENDERS_IE'
    and tenant_id = 'civant_default'
    and nullif(data->>'award_published','') is not null
),
parsed as (
  select
    award_published,
    civant__parse_date_ddmmyyyy(award_published) as parsed_award_date
  from base
)
select
  count(*) as award_published_nonnull,
  count(*) filter (where parsed_award_date is not null) as award_events_detected_via_award_published,
  count(*) filter (where parsed_award_date is null) as award_published_parse_failures
from parsed;

-- 1) Awards ingested per month (award_date)
select
  date_trunc('month', award_date)::date as month,
  count(*) as awards_ingested
from award_fact_ie
where source_system = 'ETENDERS_IE'
group by 1
order by 1;

-- 2) Skipped awards due to missing award_date (count + percent)
with r as (
  select * from refresh_award_fact_ie(null, null)
)
select
  source_relation,
  total_award_rows,
  skipped_missing_award_date,
  round(100.0 * skipped_missing_award_date / nullif(total_award_rows, 0), 2) as pct_missing_award_date
from r;

-- 3) Data completeness in award_fact_ie
select
  count(*) as rows_total,
  count(*) filter (where supplier_id is null) as supplier_id_null,
  round(100.0 * count(*) filter (where supplier_id is null) / nullif(count(*), 0), 2) as pct_supplier_id_null,
  count(*) filter (where supplier_id is not null) as supplier_id_present,
  round(100.0 * count(*) filter (where supplier_id is not null) / nullif(count(*), 0), 2) as pct_supplier_id_present,
  count(*) filter (where award_value_eur is null) as award_value_null,
  round(100.0 * count(*) filter (where award_value_eur is null) / nullif(count(*), 0), 2) as pct_award_value_null,
  count(*) filter (where cpv_primary is null) as cpv_primary_null,
  round(100.0 * count(*) filter (where cpv_primary is null) / nullif(count(*), 0), 2) as pct_cpv_primary_null
from award_fact_ie
where source_system = 'ETENDERS_IE';

-- 3b) Top 20 buyers by awards_count (facts)
select
  buyer_id,
  count(*) as awards_count
from award_fact_ie
where source_system = 'ETENDERS_IE'
group by 1
order by awards_count desc
limit 20;

-- 3c) Top 20 suppliers by awards_count (facts + dim)
select
  af.supplier_id,
  sd.supplier_name_normalized,
  count(*) as awards_count
from award_fact_ie af
left join ie_supplier_dim sd
  on sd.supplier_id = af.supplier_id
where af.source_system = 'ETENDERS_IE'
  and af.supplier_id is not null
group by 1,2
order by awards_count desc
limit 20;

-- 3d) Distinct raw vs normalized supplier name counts
select
  count(distinct supplier_name_raw) filter (where supplier_name_raw is not null) as distinct_supplier_name_raw,
  count(distinct sd.supplier_name_normalized) filter (where sd.supplier_name_normalized is not null) as distinct_supplier_name_normalized
from award_fact_ie af
left join ie_supplier_dim sd
  on sd.supplier_id = af.supplier_id
where af.source_system = 'ETENDERS_IE';

-- 3e) Top normalized supplier names with the most raw variants (sanity on normalization)
select
  sd.supplier_name_normalized,
  count(distinct af.supplier_name_raw) as raw_variants,
  max(af.supplier_name_raw) as example_raw,
  count(*) as awards_count
from award_fact_ie af
join ie_supplier_dim sd
  on sd.supplier_id = af.supplier_id
where af.source_system = 'ETENDERS_IE'
  and af.supplier_id is not null
  and af.supplier_name_raw is not null
group by 1
having count(distinct af.supplier_name_raw) >= 5
order by raw_variants desc, awards_count desc
limit 50;

-- 4) Awards_count distribution (signals)
select
  awards_count,
  count(*) as buyer_cluster_pairs
from award_signals_ie
group by 1
order by 1;

-- 5) UNKNOWN share in signals + CPV mapping gaps
select
  count(*) as signals_total,
  count(*) filter (where cpv_cluster = 'UNKNOWN') as signals_unknown,
  round(100.0 * count(*) filter (where cpv_cluster = 'UNKNOWN') / nullif(count(*), 0), 2) as pct_unknown
from award_signals_ie;

-- Top cpv_primary values lacking mapping (facts have cpv_primary but cpv_cluster is null)
select
  cpv_primary,
  count(*) as awards_missing_cluster
from award_fact_ie
where source_system = 'ETENDERS_IE'
  and cpv_primary is not null
  and cpv_cluster is null
group by 1
order by awards_missing_cluster desc
limit 50;

-- 6) Top upcoming predicted windows in next 12 months by confidence
select
  buyer_id,
  cpv_cluster,
  last_award_date,
  awards_count,
  expected_next_tender_date,
  expected_window_start,
  expected_window_end,
  renewal_probability,
  switching_probability,
  confidence_score,
  explanations
from award_signals_ie
where expected_window_start <= (current_date + interval '12 months')::date
order by confidence_score desc, expected_next_tender_date asc
limit 50;

-- 7) Sanity checks
select count(*) as bad_windows
from award_signals_ie
where expected_window_start > expected_window_end;

select
  buyer_id,
  cpv_cluster,
  expected_window_start,
  expected_window_end,
  expected_next_tender_date,
  confidence_score,
  explanations
from award_signals_ie
where expected_window_start > expected_window_end
limit 25;
