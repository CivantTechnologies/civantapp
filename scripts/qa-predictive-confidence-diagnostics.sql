-- Predictive Engine confidence diagnostics (fast, low load)
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v tenant_id='civant_default' -f scripts/qa-predictive-confidence-diagnostics.sql

\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id civant_default
\endif

\echo '== Predictive Confidence Diagnostics =='
\echo 'tenant_id=' :tenant_id
select set_config('civant.tenant_id', :'tenant_id', false);

\echo '\n== 1) Coverage =='
select count(*) as total_predictions
from public.predictions_current
where tenant_id = :'tenant_id';

select count(*) as resolved_predictions
from public.predictions_current
where tenant_id = :'tenant_id'
  and buyer_entity_id not like 'unresolved:%';

select count(*) as unresolved_predictions
from public.predictions_current
where tenant_id = :'tenant_id'
  and buyer_entity_id like 'unresolved:%';

select
  case when buyer_entity_id like 'unresolved:%' then 'unresolved' else 'resolved' end as resolution_bucket,
  min(confidence) as min_confidence,
  max(confidence) as max_confidence,
  round(avg(confidence)::numeric, 2) as avg_confidence
from public.predictions_current
where tenant_id = :'tenant_id'
group by 1
order by 1;

select count(*) as confidence_ge_60
from public.predictions_current
where tenant_id = :'tenant_id'
  and confidence >= 60;

\echo '\n== 1b) PHASE 0: Usable Signal Coverage (mapping vs computation) =='
\echo 'A) award_published usable coverage'
with awards as (
  select *
  from public.signals
  where tenant_id = :'tenant_id'
    and signal_type = 'award_published'
)
select
  count(*) as awards_total,
  sum((buyer_entity_id not like 'unresolved:%')::int) as awards_resolved_buyer,
  round(100.0 * sum((buyer_entity_id not like 'unresolved:%')::int) / nullif(count(*), 0), 2) as awards_resolved_buyer_pct,
  sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int) as awards_with_cpv_cluster,
  round(100.0 * sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int) / nullif(count(*), 0), 2) as awards_with_cpv_cluster_pct,
  sum((upper(region) in ('IE', 'FR', 'ES'))::int) as awards_with_region_iso2,
  round(100.0 * sum((upper(region) in ('IE', 'FR', 'ES'))::int) / nullif(count(*), 0), 2) as awards_with_region_iso2_pct,
  sum((
    buyer_entity_id not like 'unresolved:%'
    and cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown'
    and upper(region) in ('IE', 'FR', 'ES')
  )::int) as awards_usable,
  round(100.0 * sum((
    buyer_entity_id not like 'unresolved:%'
    and cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown'
    and upper(region) in ('IE', 'FR', 'ES')
  )::int) / nullif(count(*), 0), 2) as awards_usable_pct
from awards;

\echo 'B) notice_published usable coverage'
with notices as (
  select *
  from public.signals
  where tenant_id = :'tenant_id'
    and signal_type = 'notice_published'
)
select
  count(*) as notices_total,
  sum((buyer_entity_id not like 'unresolved:%')::int) as notices_resolved_buyer,
  round(100.0 * sum((buyer_entity_id not like 'unresolved:%')::int) / nullif(count(*), 0), 2) as notices_resolved_buyer_pct,
  sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int) as notices_with_cpv_cluster,
  round(100.0 * sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int) / nullif(count(*), 0), 2) as notices_with_cpv_cluster_pct,
  sum((upper(region) in ('IE', 'FR', 'ES'))::int) as notices_with_region_iso2,
  round(100.0 * sum((upper(region) in ('IE', 'FR', 'ES'))::int) / nullif(count(*), 0), 2) as notices_with_region_iso2_pct,
  sum((
    buyer_entity_id not like 'unresolved:%'
    and cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown'
    and upper(region) in ('IE', 'FR', 'ES')
  )::int) as notices_usable,
  round(100.0 * sum((
    buyer_entity_id not like 'unresolved:%'
    and cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown'
    and upper(region) in ('IE', 'FR', 'ES')
  )::int) / nullif(count(*), 0), 2) as notices_usable_pct
from notices;

\echo '\n== 2) Fallback + Evidence (resolved only) =='
\echo 'Fallback tier distribution (predictions_current)'
select fallback_tier, count(*)
from public.predictions_current
where tenant_id = :'tenant_id'
  and buyer_entity_id not like 'unresolved:%'
group by fallback_tier
order by fallback_tier;

\echo 'Fallback tier distribution (prediction_drivers if present)'
do $$
declare
  r record;
  v_tenant text := current_setting('civant.tenant_id');
begin
  if to_regclass('public.prediction_drivers') is null then
    raise notice 'prediction_drivers missing; fallback_tier distribution not available from drivers.';
  else
    raise notice 'prediction_drivers fallback_tier distribution:';
    for r in execute
      'select coalesce(label, ''unknown'') as fallback_tier, count(*) as cnt
       from public.prediction_drivers
       where tenant_id = $1
         and driver_type = ''fallback_tier''
       group by 1
       order by 1'
      using v_tenant
    loop
      raise notice 'fallback_tier=% count=%', r.fallback_tier, r.cnt;
    end loop;
  end if;
end $$;

\echo 'Evidence count distribution (resolved only)'
select evidence_count, count(*)
from public.predictions_current
where tenant_id = :'tenant_id'
  and buyer_entity_id not like 'unresolved:%'
group by evidence_count
order by evidence_count;

\echo 'CPV cluster null/unknown rate (predictions_current)'
select
  count(*) as total_predictions,
  sum((cpv_cluster_id is null or cpv_cluster_id = 'cluster_unknown')::int) as null_or_unknown_cpv,
  round(100.0 * sum((cpv_cluster_id is null or cpv_cluster_id = 'cluster_unknown')::int) / nullif(count(*), 0), 2) as null_or_unknown_pct
from public.predictions_current
where tenant_id = :'tenant_id';

\echo 'C) Top 50 resolved pairs by forecast_score with evidence_count=0'
with candidates as (
  select buyer_entity_id, cpv_cluster_id, region, forecast_score
  from public.predictions_current
  where tenant_id = :'tenant_id'
    and buyer_entity_id not like 'unresolved:%'
    and coalesce(evidence_count, 0) = 0
  order by forecast_score desc nulls last
  limit 50
)
select * from candidates;

\echo 'Missing signal types for top 50 candidates (resolved + evidence_count=0)'
with candidates as (
  select buyer_entity_id, cpv_cluster_id, region, forecast_score
  from public.predictions_current
  where tenant_id = :'tenant_id'
    and buyer_entity_id not like 'unresolved:%'
    and coalesce(evidence_count, 0) = 0
  order by forecast_score desc nulls last
  limit 50
),
agg as (
  select
    s.buyer_entity_id,
    coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
    s.region,
    bool_or(s.signal_type = 'award_published') as has_award,
    bool_or(s.signal_type = 'notice_published') as has_notice,
    bool_or(s.signal_type = 'framework_established') as has_framework,
    bool_or(s.signal_type = 'cancellation') as has_cancellation,
    bool_or(s.signal_type = 'amendment') as has_amendment
  from public.signals s
  join candidates c
    on c.buyer_entity_id = s.buyer_entity_id
   and coalesce(c.cpv_cluster_id, 'cluster_unknown') = coalesce(s.cpv_cluster_id, 'cluster_unknown')
   and c.region = s.region
  where s.tenant_id = :'tenant_id'
  group by 1, 2, 3
)
select
  c.buyer_entity_id,
  c.cpv_cluster_id,
  c.region,
  c.forecast_score,
  array_remove(array[
    case when not coalesce(a.has_award, false) then 'award_published' end,
    case when not coalesce(a.has_notice, false) then 'notice_published' end,
    case when not coalesce(a.has_framework, false) then 'framework_established' end,
    case when not coalesce(a.has_cancellation, false) then 'cancellation' end,
    case when not coalesce(a.has_amendment, false) then 'amendment' end
  ], null) as missing_signal_types
from candidates c
left join agg a
  on a.buyer_entity_id = c.buyer_entity_id
 and a.cpv_cluster_id = coalesce(c.cpv_cluster_id, 'cluster_unknown')
 and a.region = c.region
order by c.forecast_score desc nulls last;

\echo 'Presence of award/notice signals for top 50 candidates'
with candidates as (
  select buyer_entity_id, cpv_cluster_id, region, forecast_score
  from public.predictions_current
  where tenant_id = :'tenant_id'
    and buyer_entity_id not like 'unresolved:%'
    and coalesce(evidence_count, 0) = 0
  order by forecast_score desc nulls last
  limit 50
),
pair_signals as (
  select
    c.buyer_entity_id,
    c.cpv_cluster_id,
    c.region,
    c.forecast_score,
    bool_or(s.signal_type = 'notice_published') as has_notice,
    bool_or(s.signal_type = 'award_published') as has_award
  from candidates c
  left join public.signals s
    on s.tenant_id = :'tenant_id'
   and s.buyer_entity_id = c.buyer_entity_id
   and coalesce(s.cpv_cluster_id, 'cluster_unknown') = coalesce(c.cpv_cluster_id, 'cluster_unknown')
   and s.region = c.region
   and s.signal_type in ('notice_published', 'award_published')
  group by 1, 2, 3, 4
)
select
  count(*) as pairs_total,
  sum((has_notice)::int) as pairs_with_notice,
  sum((has_award)::int) as pairs_with_award,
  sum((has_notice or has_award)::int) as pairs_with_any_signal
from pair_signals;

with candidates as (
  select buyer_entity_id, cpv_cluster_id, region, forecast_score
  from public.predictions_current
  where tenant_id = :'tenant_id'
    and buyer_entity_id not like 'unresolved:%'
    and coalesce(evidence_count, 0) = 0
  order by forecast_score desc nulls last
  limit 50
),
pair_signals as (
  select
    c.buyer_entity_id,
    c.cpv_cluster_id,
    c.region,
    c.forecast_score,
    bool_or(s.signal_type = 'notice_published') as has_notice,
    bool_or(s.signal_type = 'award_published') as has_award
  from candidates c
  left join public.signals s
    on s.tenant_id = :'tenant_id'
   and s.buyer_entity_id = c.buyer_entity_id
   and coalesce(s.cpv_cluster_id, 'cluster_unknown') = coalesce(c.cpv_cluster_id, 'cluster_unknown')
   and s.region = c.region
   and s.signal_type in ('notice_published', 'award_published')
  group by 1, 2, 3, 4
)
select
  buyer_entity_id,
  cpv_cluster_id,
  region,
  forecast_score,
  has_notice,
  has_award
from pair_signals
order by forecast_score desc nulls last;

\echo '\n== 2b) PHASE 2: Evidence model proof (20 resolved predictions sample) =='
with sample_predictions as (
  select
    prediction_id,
    buyer_entity_id,
    cpv_cluster_id,
    region,
    forecast_score,
    confidence,
    coalesce(evidence_count, 0) as evidence_count
  from public.predictions_current
  where tenant_id = :'tenant_id'
    and buyer_entity_id not like 'unresolved:%'
  order by forecast_score desc nulls last, confidence desc
  limit 20
),
signal_presence as (
  select
    sp.prediction_id,
    count(*) filter (
      where s.signal_type in ('notice_published', 'award_published', 'cancellation', 'framework_established', 'amendment')
    ) as relevant_signal_count
  from sample_predictions sp
  left join public.signals s
    on s.tenant_id = :'tenant_id'
   and s.buyer_entity_id = sp.buyer_entity_id
   and coalesce(s.cpv_cluster_id, 'cluster_unknown') = coalesce(sp.cpv_cluster_id, 'cluster_unknown')
   and s.region = sp.region
  group by sp.prediction_id
),
driver_presence as (
  select
    sp.prediction_id,
    count(pd.*) as driver_rows,
    sum((cardinality(coalesce(pd.evidence_refs, '{}'::text[])) > 0)::int) as driver_rows_with_evidence,
    coalesce(sum(cardinality(coalesce(pd.evidence_refs, '{}'::text[]))), 0) as total_driver_evidence_refs
  from sample_predictions sp
  left join public.prediction_drivers pd
    on pd.tenant_id = :'tenant_id'
   and pd.prediction_id = sp.prediction_id
  group by sp.prediction_id
)
select
  sp.prediction_id,
  sp.buyer_entity_id,
  sp.cpv_cluster_id,
  sp.region,
  sp.forecast_score,
  sp.confidence,
  sp.evidence_count,
  coalesce(sig.relevant_signal_count, 0) as relevant_signal_count,
  coalesce(dp.driver_rows, 0) as driver_rows,
  coalesce(dp.driver_rows_with_evidence, 0) as driver_rows_with_evidence,
  coalesce(dp.total_driver_evidence_refs, 0) as total_driver_evidence_refs
from sample_predictions sp
left join signal_presence sig on sig.prediction_id = sp.prediction_id
left join driver_presence dp on dp.prediction_id = sp.prediction_id
order by sp.forecast_score desc nulls last, sp.confidence desc;

\echo 'Evidence isolation summary for same 20 predictions'
with sample_predictions as (
  select
    prediction_id,
    buyer_entity_id,
    cpv_cluster_id,
    region
  from public.predictions_current
  where tenant_id = :'tenant_id'
    and buyer_entity_id not like 'unresolved:%'
  order by forecast_score desc nulls last, confidence desc
  limit 20
),
joined as (
  select
    sp.prediction_id,
    exists (
      select 1
      from public.signals s
      where s.tenant_id = :'tenant_id'
        and s.buyer_entity_id = sp.buyer_entity_id
        and coalesce(s.cpv_cluster_id, 'cluster_unknown') = coalesce(sp.cpv_cluster_id, 'cluster_unknown')
        and s.region = sp.region
        and s.signal_type in ('notice_published', 'award_published', 'cancellation', 'framework_established', 'amendment')
    ) as has_relevant_signals,
    exists (
      select 1
      from public.prediction_drivers pd
      where pd.tenant_id = :'tenant_id'
        and pd.prediction_id = sp.prediction_id
    ) as has_driver_rows,
    exists (
      select 1
      from public.prediction_drivers pd
      where pd.tenant_id = :'tenant_id'
        and pd.prediction_id = sp.prediction_id
        and cardinality(coalesce(pd.evidence_refs, '{}'::text[])) > 0
    ) as has_non_empty_evidence_refs
  from sample_predictions sp
)
select
  count(*) as sample_size,
  sum((has_relevant_signals)::int) as predictions_with_relevant_signals,
  sum((has_driver_rows)::int) as predictions_with_driver_rows,
  sum((has_non_empty_evidence_refs)::int) as predictions_with_non_empty_evidence_refs
from joined;

\echo '\n== 3) Input presence =='
\echo 'buyer_category_stats completeness'
select
  count(*) as total_rows,
  sum((tender_count_24m >= 2)::int) as tender_pairs_ge_2,
  sum((tender_count_24m >= 3)::int) as tender_pairs_ge_3,
  sum((tender_count_24m >= 5)::int) as tender_pairs_ge_5,
  sum((avg_cycle_days is not null)::int) as avg_cycle_present,
  sum((last_award_at is not null)::int) as last_award_present,
  sum((cadence_reliability_score is not null and cadence_reliability_score > 0)::int) as cadence_present,
  sum((data_coverage_score is not null and data_coverage_score > 0)::int) as data_coverage_present
from public.buyer_category_stats
where tenant_id = :'tenant_id';

\echo 'signals by type (tenant)'
select signal_type, count(*) as signals
from public.signals
where tenant_id = :'tenant_id'
group by signal_type
order by signals desc;

\echo 'signals by type + region (tenant)'
select region, signal_type, count(*) as signals
from public.signals
where tenant_id = :'tenant_id'
group by region, signal_type
order by region, signals desc;

\echo 'award signal coverage'
select
  count(*) as awards_total,
  sum((buyer_entity_id not like 'unresolved:%')::int) as awards_with_resolved_buyer,
  sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int) as awards_with_cpv_cluster
from public.signals
where tenant_id = :'tenant_id'
  and signal_type = 'award_published';

select region, count(*) as awards_by_region
from public.signals
where tenant_id = :'tenant_id'
  and signal_type = 'award_published'
group by region
order by region;

\echo 'cpv mapping health (signals)'
select
  count(*) as total_signals,
  sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int) as mapped_signals,
  round(100.0 * sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int) / nullif(count(*), 0), 2) as mapped_pct
from public.signals
where tenant_id = :'tenant_id';

\echo 'Top unmapped CPV codes (if available)'
do $$
declare
  r record;
  v_tenant text := current_setting('civant.tenant_id');
begin
  if to_regclass('public.canonical_tenders') is null then
    raise notice 'canonical_tenders missing; cannot compute unmapped CPV codes.';
    return;
  end if;

  if not exists (
    select 1 from information_schema.columns
    where table_schema = 'public'
      and table_name = 'canonical_tenders'
      and column_name = 'cpv_codes'
  ) then
    raise notice 'canonical_tenders.cpv_codes missing; cannot compute unmapped CPV codes.';
    return;
  end if;

  raise notice 'Top unmapped CPV codes (sampled from canonical_tenders where cluster_unknown):';
  for r in execute
    'with sample as (
       select ct.cpv_codes, ct.title
       from public.canonical_tenders ct
       where ct.tenant_id = $1
         and ct.cpv_codes is not null
       order by coalesce(ct.updated_at, ct.created_at) desc
       limit 20000
     ),
     resolved as (
       select
         unnest(s.cpv_codes) as cpv,
         (public.resolve_cpv_cluster_id($1, s.cpv_codes, coalesce(s.title, ''''))).cpv_cluster_id as cluster_id
       from sample s
     )
     select cpv, count(*) as cnt
     from resolved
     where cluster_id is null or cluster_id = ''cluster_unknown''
     group by cpv
     order by cnt desc
     limit 10'
    using v_tenant
  loop
    raise notice 'cpv=% count=%', r.cpv, r.cnt;
  end loop;
end $$;

\echo '\n== 4) Root cause shortlist =='
do $$
declare
  v_awards bigint := 0;
  v_awards_mapped bigint := 0;
  v_signals_total bigint := 0;
  v_signals_mapped bigint := 0;
  v_stats_total bigint := 0;
  v_stats_avg_cycle bigint := 0;
  v_stats_last_award bigint := 0;
  v_evidence_nonzero bigint := 0;
  v_resolved_predictions bigint := 0;
  v_max_resolved_conf int := 0;
  v_tenant text := current_setting('civant.tenant_id');
begin
  select count(*) into v_resolved_predictions
  from public.predictions_current
  where tenant_id = v_tenant
    and buyer_entity_id not like 'unresolved:%';

  select max(confidence)::int into v_max_resolved_conf
  from public.predictions_current
  where tenant_id = v_tenant
    and buyer_entity_id not like 'unresolved:%';

  select count(*) into v_evidence_nonzero
  from public.predictions_current
  where tenant_id = v_tenant
    and buyer_entity_id not like 'unresolved:%'
    and coalesce(evidence_count, 0) > 0;

  select count(*),
         sum((cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown')::int)
    into v_signals_total, v_signals_mapped
  from public.signals
  where tenant_id = v_tenant;

  select count(*) into v_awards
  from public.signals
  where tenant_id = v_tenant
    and signal_type = 'award_published';

  select count(*) into v_awards_mapped
  from public.signals
  where tenant_id = v_tenant
    and signal_type = 'award_published'
    and buyer_entity_id not like 'unresolved:%'
    and cpv_cluster_id is not null
    and cpv_cluster_id <> 'cluster_unknown';

  select count(*),
         sum((avg_cycle_days is not null)::int),
         sum((last_award_at is not null)::int)
    into v_stats_total, v_stats_avg_cycle, v_stats_last_award
  from public.buyer_category_stats
  where tenant_id = v_tenant;

  if v_awards = 0 then
    raise notice 'ROOT_CAUSE: no award signals present; high confidence unlikely.';
  elsif v_awards_mapped < greatest(1, v_awards / 10) then
    raise notice 'ROOT_CAUSE: award signals are mostly unmapped to buyer+cpv pairs.';
  end if;

  if v_signals_total > 0 and v_signals_mapped::numeric / v_signals_total < 0.5 then
    raise notice 'ROOT_CAUSE: cpv_cluster_id missing for most signals.';
  end if;

  if v_stats_total > 0 and v_stats_avg_cycle::numeric / v_stats_total < 0.3 then
    raise notice 'ROOT_CAUSE: buyer_category_stats lacks avg_cycle_days for most pairs.';
  end if;

  if v_stats_total > 0 and v_stats_last_award::numeric / v_stats_total < 0.3 then
    raise notice 'ROOT_CAUSE: buyer_category_stats lacks last_award_at for most pairs.';
  end if;

  if v_resolved_predictions > 0 and v_evidence_nonzero = 0 then
    raise notice 'ROOT_CAUSE: evidence_count is zero for all resolved predictions.';
  end if;

  if v_max_resolved_conf is not null and v_max_resolved_conf < 60 then
    raise notice 'ROOT_CAUSE: max resolved confidence=% (below 60); check inputs and evidence.', v_max_resolved_conf;
  end if;
end $$;
