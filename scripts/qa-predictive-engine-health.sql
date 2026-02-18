\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id civant_default
\endif

\echo '== Predictive Engine V1 Health =='
\echo 'tenant_id=' :tenant_id

\echo ''
\echo '--- 1) Signals Volume + Coverage ---'
with s as (
  select *
  from public.signals
  where tenant_id = :'tenant_id'
)
select
  count(*) as total_signals,
  round(100.0 * avg(case when cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown' then 1 else 0 end), 2) as pct_signals_with_cpv_cluster,
  round(100.0 * avg(case when buyer_entity_id like 'unresolved:%' then 0 else 1 end), 2) as pct_buyer_resolved,
  round(100.0 * avg(case when buyer_entity_id like 'unresolved:%' then 1 else 0 end), 2) as pct_buyer_unresolved
from s;

select signal_type, count(*) as count
from public.signals
where tenant_id = :'tenant_id'
group by 1
order by count desc, signal_type;

select region, count(*) as count
from public.signals
where tenant_id = :'tenant_id'
group by 1
order by count desc, region;

select
  count(*) as canonical_rows,
  count(*) filter (where public.normalize_prediction_region(coalesce(country, normalized_json->>'country', normalized_json->>'country_code'), source) is null) as region_norm_fail_rows,
  round(100.0 * count(*) filter (where public.normalize_prediction_region(coalesce(country, normalized_json->>'country', normalized_json->>'country_code'), source) is null) / nullif(count(*), 0), 2) as pct_region_norm_fail
from public.canonical_tenders
where tenant_id = :'tenant_id';

\echo ''
\echo '--- 2) Mapping Health ---'
select
  count(*) filter (where cpv_codes is not null and cardinality(cpv_codes) > 0) as canonical_with_cpv,
  count(*) filter (
    where cpv_codes is not null and cardinality(cpv_codes) > 0
      and not exists (
        select 1
        from unnest(cpv_codes) code
        join public.cpv_cluster_map m
          on m.tenant_id = ct.tenant_id
         and left(regexp_replace(code, '\\D', '', 'g'), length(m.cpv_prefix)) = m.cpv_prefix
      )
  ) as canonical_cpv_unmapped,
  round(100.0 * count(*) filter (
    where cpv_codes is not null and cardinality(cpv_codes) > 0
      and not exists (
        select 1
        from unnest(cpv_codes) code
        join public.cpv_cluster_map m
          on m.tenant_id = ct.tenant_id
         and left(regexp_replace(code, '\\D', '', 'g'), length(m.cpv_prefix)) = m.cpv_prefix
      )
  ) / nullif(count(*) filter (where cpv_codes is not null and cardinality(cpv_codes) > 0), 0), 2) as pct_cpv_unmapped
from public.canonical_tenders ct
where ct.tenant_id = :'tenant_id';

with cpv_codes as (
  select regexp_replace(code, '\\D', '', 'g') as cpv
  from public.canonical_tenders ct
  cross join unnest(coalesce(ct.cpv_codes, array[]::text[])) as code
  where ct.tenant_id = :'tenant_id'
), cpv_unmapped as (
  select left(cpv, 2) as cpv_prefix
  from cpv_codes c
  where cpv <> ''
    and not exists (
      select 1
      from public.cpv_cluster_map m
      where m.tenant_id = :'tenant_id'
        and left(c.cpv, length(m.cpv_prefix)) = m.cpv_prefix
    )
)
select cpv_prefix, count(*) as occurrences
from cpv_unmapped
where cpv_prefix is not null and cpv_prefix <> ''
group by 1
order by occurrences desc
limit 20;

select
  coalesce((features_json->'resolver'->>'buyer_tier'), 'unknown') as buyer_resolution_tier,
  count(*) as count
from public.signals
where tenant_id = :'tenant_id'
group by 1
order by count desc, buyer_resolution_tier;

select
  coalesce((features_json->'resolver'->>'cluster_tier'), 'unknown') as cluster_mapping_tier,
  count(*) as count
from public.signals
where tenant_id = :'tenant_id'
group by 1
order by count desc, cluster_mapping_tier;

select
  count(*) filter (where buyer_entity_id like 'unresolved:%') as external_unresolved_buyer,
  count(*) filter (where cpv_cluster_id is null or cpv_cluster_id = 'cluster_unknown') as external_unmapped_cluster,
  count(*) as external_total
from public.signals
where tenant_id = :'tenant_id'
  and signal_type in (
    'job_posting',
    'hiring_spike',
    'role_category_detected',
    'budget_approval',
    'budget_increase',
    'grant_awarded',
    'eu_funding_allocation',
    'digital_transformation_program',
    'compliance_mandate',
    'accessibility_regulation',
    'sustainability_requirement',
    'policy_signal'
  );

with buyers as (
  select public.normalize_match_key(coalesce(ct.buyer_name_raw, ct.buyer_name_norm, '')) as buyer_key
  from public.canonical_tenders ct
  where ct.tenant_id = :'tenant_id'
)
select
  count(*) filter (where buyer_key is not null) as canonical_buyer_keys,
  count(*) filter (where buyer_key is not null and ba.raw_buyer_key is not null) as buyer_keys_with_alias,
  round(
    100.0 * count(*) filter (where buyer_key is not null and ba.raw_buyer_key is not null)
      / nullif(count(*) filter (where buyer_key is not null), 0),
    2
  ) as buyer_alias_coverage_pct
from buyers b
left join public.buyer_aliases ba
  on ba.tenant_id = :'tenant_id'
 and ba.raw_buyer_key = b.buyer_key;

\echo ''
\echo '--- 3) Prediction Pipeline Health ---'
with eligible as (
  select distinct buyer_entity_id, cpv_cluster_id, region
  from public.signals
  where tenant_id = :'tenant_id'
    and cpv_cluster_id is not null
    and cpv_cluster_id <> 'cluster_unknown'
), preds as (
  select *
  from public.predictions_current
  where tenant_id = :'tenant_id'
)
select
  (select count(*) from eligible) as eligible_pairs,
  (select count(*) from preds) as predictions_rows,
  round(100.0 * (select count(*) from preds) / nullif((select count(*) from eligible), 0), 2) as predictions_to_eligible_pct;

select confidence_band, count(*) as count
from public.predictions_current
where tenant_id = :'tenant_id'
group by 1
order by count desc, confidence_band;

select
  regexp_replace(coalesce(label, ''), '^Fallback tier\\s*', '') as fallback_tier,
  count(*) as count
from public.prediction_drivers
where tenant_id = :'tenant_id'
  and driver_type = 'fallback_tier'
group by 1
order by fallback_tier;

select narrative, count(*) as count
from public.prediction_drivers
where tenant_id = :'tenant_id'
  and driver_type = 'coverage_penalty'
group by 1
order by count desc, narrative;

\echo ''
\echo '--- 4) Incremental Job Health ---'
select
  run_id,
  run_type,
  status,
  pairs_processed,
  metadata_json->>'computed_prediction_rows' as computed_prediction_rows,
  metadata_json->>'skipped_pairs' as skipped_pairs,
  started_at,
  finished_at,
  error_message
from public.prediction_runs
where tenant_id = :'tenant_id'
order by started_at desc
limit 10;

with last_success as (
  select max(finished_at) as finished_at
  from public.prediction_runs
  where tenant_id = :'tenant_id'
    and status = 'success'
), impacted as (
  select distinct
    s.tenant_id,
    s.buyer_entity_id,
    coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
    coalesce(public.normalize_prediction_region(s.region, s.source), 'IE') as region
  from public.signals s
  cross join last_success ls
  where s.tenant_id = :'tenant_id'
    and s.ingested_at >= coalesce(ls.finished_at, now() - interval '24 hours')
)
select
  (select finished_at from last_success) as last_success_at,
  (select count(*) from impacted) as impacted_pairs_since_last_success,
  (select count(*) from public.signals s cross join last_success ls where s.tenant_id = :'tenant_id' and s.ingested_at >= coalesce(ls.finished_at, now() - interval '24 hours')) as signals_since_last_success;

\echo ''
\echo '--- 5) UI Surface Check (from DB outputs) ---'
select
  count(*) as predictions_with_required_display_fields,
  count(*) filter (where coalesce(buyer_display_name, '') = '' or coalesce(cpv_cluster_label, '') = '') as rows_missing_display_fields
from public.predictions_current
where tenant_id = :'tenant_id';

\echo 'INFO: Confirm UI read path with: rg -n "run_predictive_engine_v1|compute_prediction_for_pair_v1|normalize_all_signals\(" src api'
\echo 'INFO: Confirm Predictions.jsx ordering/filter with: rg -n "predictions_current\.filter|forecast_score|confidence" src/pages/Predictions.jsx'
