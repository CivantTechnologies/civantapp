-- =============================================================================
-- Civant: Buyer category stats cycle minimum gap patch (v1)
-- Migration: 20260223_buyer_category_stats_cycle_min_gap_v1.sql
-- =============================================================================
-- Patch target:
--   public.recompute_buyer_category_stats_v2
--
-- Change:
--   cycle_intervals lower bound reduced from 7 days to 1 day.
--   Same-day dedupe remains unchanged via DISTINCT occurred_at::date.
--
-- Scope:
--   Recompute only IE rows for civant_default where avg_cycle_days is NULL
--   and the (buyer_entity_id, cpv_cluster_id) pair has >= 3 distinct
--   notice_published days in the 15-year lookback window.
-- =============================================================================

create or replace function public.recompute_buyer_category_stats_v2(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_lookback interval default interval '15 years'
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_region text := coalesce(public.normalize_prediction_region(p_region, null), 'IE');
  v_cluster text := coalesce(p_cpv_cluster_id, 'cluster_unknown');
  v_since timestamptz := now() - p_lookback;

  -- Core counts
  v_tender_count integer := 0;
  v_award_count integer := 0;
  v_interval_count integer := 0;

  -- Cycle stats
  v_avg_cycle numeric := null;
  v_std_cycle numeric := null;
  v_median_cycle numeric := null;

  -- Timestamps
  v_last_tender timestamptz;
  v_last_award timestamptz;

  -- Derived scores
  v_seasonality jsonb := '{}'::jsonb;
  v_value_stability numeric := 0.5;
  v_data_coverage numeric := 0;
  v_cadence_reliability numeric := 0;
  v_value_avg numeric := null;
  v_value_std numeric := null;
  v_prev_avg_cycle numeric := null;
  v_prev_std_cycle numeric := null;
  v_prev_data_coverage numeric := null;
  v_prev_cadence numeric := null;
  v_data_coverage_v1 numeric := 0;
  v_cadence_v1 numeric := 0;
  v_cadence_v2 numeric := 0;

  -- Basis tracking
  v_basis text := 'exact';

  -- Coefficient of variation for cadence
  v_cv numeric := null;
begin
  -- =========================================================================
  -- GUARD: Reject unresolved buyers and unknown clusters early
  -- =========================================================================
  if p_buyer_entity_id is null
     or p_buyer_entity_id like 'unresolved:%'
     or v_cluster = 'cluster_unknown'
  then
    return;  -- silently skip; no stats row for junk pairs
  end if;

  select
    s.avg_cycle_days,
    s.std_cycle_days,
    s.data_coverage_score,
    s.cadence_reliability_score
    into
      v_prev_avg_cycle,
      v_prev_std_cycle,
      v_prev_data_coverage,
      v_prev_cadence
  from public.buyer_category_stats s
  where s.tenant_id = p_tenant_id
    and s.buyer_entity_id = p_buyer_entity_id
    and s.cpv_cluster_id = v_cluster
    and s.region = v_region
  limit 1;

  perform public.ensure_prediction_baseline(p_tenant_id);

  -- =========================================================================
  -- CTE CHAIN: Gather signals, compute intervals, seasonality, values
  -- =========================================================================
  with signal_candidates as (
    select s.occurred_at,
           s.signal_type,
           case
             when coalesce(s.features_json->>'estimated_value', '') ~ '^[0-9]+(\.[0-9]+)?$'
               then (s.features_json->>'estimated_value')::numeric
             else null
           end as estimated_value,
           case
             when coalesce(s.cpv_cluster_id, 'cluster_unknown') = v_cluster then 0
             when v_cluster <> 'cluster_unknown'
               and coalesce(s.cpv_cluster_id, 'cluster_unknown') = 'cluster_unknown' then 1
             else 9
           end as match_rank
    from public.signals s
    where s.tenant_id = p_tenant_id
      and s.buyer_entity_id = p_buyer_entity_id
      and upper(trim(s.region)) = upper(trim(v_region))
      and (
        coalesce(s.cpv_cluster_id, 'cluster_unknown') = v_cluster
        or (
          v_cluster <> 'cluster_unknown'
          and coalesce(s.cpv_cluster_id, 'cluster_unknown') = 'cluster_unknown'
        )
      )
      and s.occurred_at >= v_since
      and s.occurred_at <= now()
      and s.signal_type in (
        'notice_published',
        'award_published',
        'framework_established',
        'framework_expiry',
        'cancellation',
        'amendment'
      )
  ),

  -- Exact match = CPV cluster matches directly
  exact_events as (
    select * from signal_candidates where match_rank = 0
  ),

  -- Fallback = exact + signals where CPV was unknown (best-effort)
  fallback_events as (
    select * from signal_candidates where match_rank in (0, 1)
  ),

  -- Decide which event set to use
  stats_basis as (
    select case
      when (
        select count(*)
        from exact_events
        where signal_type in ('notice_published', 'award_published')
      ) >= 2 then 'exact'    -- v2: lowered from 3 to 2 for basis selection
      else 'fallback'
    end as basis
  ),

  -- Select the appropriate event set
  selected_events as (
    select e.occurred_at, e.signal_type, e.estimated_value
    from exact_events e
    join stats_basis b on b.basis = 'exact'
    union all
    select f.occurred_at, f.signal_type, f.estimated_value
    from fallback_events f
    join stats_basis b on b.basis = 'fallback'
  ),

  -- Distinct notice days (deduped by date to avoid same-day double-counting)
  notice_events as (
    select distinct occurred_at::date as event_day
    from selected_events
    where signal_type = 'notice_published'
  ),

  -- Distinct award days
  award_events as (
    select distinct occurred_at::date as event_day
    from selected_events
    where signal_type = 'award_published'
  ),

  -- Notice-to-notice gaps
  notice_gaps as (
    select (event_day - lag(event_day) over (order by event_day))::numeric as gap_days
    from notice_events
  ),

  -- Award-to-next-notice gaps (cross-signal cycle detection)
  award_to_next_notice as (
    select (n.event_day - a.event_day)::numeric as gap_days
    from award_events a
    join lateral (
      select ne.event_day
      from notice_events ne
      where ne.event_day > a.event_day
      order by ne.event_day
      limit 1
    ) n on true
  ),

  -- =========================================================================
  -- FIX 1: Cycle interval cap raised from 730 to 2555 days (7 years)
  -- Public procurement contracts commonly run 3-5 years (1095-1825 days).
  -- Framework agreements can run up to 7 years with extensions.
  -- Minimum gap of 1 day preserves same-day dedupe while retaining next-day intervals.
  -- =========================================================================
  cycle_intervals as (
    select gap_days
    from notice_gaps
    where gap_days between 1 and 2555
    union all
    select gap_days
    from award_to_next_notice
    where gap_days between 1 and 2555
  ),

  -- Seasonality: which months do notices appear?
  seasonality as (
    select extract(month from event_day)::int as month_no,
           count(*)::int as month_count
    from notice_events
    group by 1
  ),

  -- Value data for stability scoring
  values_data as (
    select estimated_value
    from selected_events
    where estimated_value is not null and estimated_value > 0
  )

  -- =========================================================================
  -- AGGREGATE INTO VARIABLES
  -- =========================================================================
  select
    (select basis from stats_basis),
    (select count(*) from notice_events),
    (select count(*) from award_events),
    (select count(*) from cycle_intervals),
    (select avg(gap_days) from cycle_intervals),
    (select stddev_samp(gap_days) from cycle_intervals),
    (select percentile_cont(0.5) within group (order by gap_days) from cycle_intervals),
    (select max(occurred_at) from selected_events where signal_type = 'notice_published'),
    (select max(occurred_at) from selected_events where signal_type = 'award_published'),
    coalesce((
      select jsonb_object_agg(month_no::text, month_count)
      from seasonality
    ), '{}'::jsonb),
    (select avg(estimated_value) from values_data),
    (select stddev_samp(estimated_value) from values_data)
  into
    v_basis,
    v_tender_count,
    v_award_count,
    v_interval_count,
    v_avg_cycle,
    v_std_cycle,
    v_median_cycle,
    v_last_tender,
    v_last_award,
    v_seasonality,
    v_value_avg,
    v_value_std;

  -- =========================================================================
  -- CYCLE STATS: Require 2+ intervals (lowered from 3 in v1)
  -- =========================================================================
  if v_interval_count < 2 then
    -- Keep existing cycle stats for sparse pairs to avoid score regressions.
    v_avg_cycle := v_prev_avg_cycle;
    v_std_cycle := v_prev_std_cycle;
    v_median_cycle := null;
  elsif v_std_cycle is null then
    v_std_cycle := 0;
  end if;

  -- =========================================================================
  -- VALUE STABILITY: How consistent are contract values?
  -- 0.5 = unknown, 1.0 = perfectly stable, 0.0 = wildly variable
  -- =========================================================================
  v_value_stability := case
    when v_value_avg is null or v_value_avg = 0 then 0.5
    else greatest(0, least(1, 1 - (coalesce(v_value_std, 0) / nullif(v_value_avg, 0))))
  end;

  -- =========================================================================
  -- Blend v2 coverage with v1 baseline and protect sparse FR pairs.
  v_data_coverage_v1 := greatest(0, least(1,
    (0.55 * least(1, coalesce(v_tender_count, 0)::numeric / 8.0))
    + (0.25 * least(1, coalesce(v_award_count, 0)::numeric / 6.0))
    + (0.20 * least(1, coalesce(v_interval_count, 0)::numeric / 5.0))
  ));
  v_data_coverage := greatest(0, least(1,
    (0.50 * least(1, coalesce(v_tender_count, 0)::numeric / 10.0))
    + (0.25 * least(1, coalesce(v_award_count, 0)::numeric / 7.0))
    + (0.25 * least(1, coalesce(v_interval_count, 0)::numeric / 5.0))
  ));
  if upper(v_region) = 'FR' then
    v_data_coverage := greatest(v_data_coverage, v_data_coverage_v1);
  end if;

  -- =========================================================================
  -- FIX 2: CADENCE RELIABILITY reworked
  --
  -- V1 formula: 0.70 * (1 - std/avg) + 0.30 * (intervals/8)
  -- Problem: A buyer tendering every 300-400 days has std/avg ~ 0.15,
  --          giving cadence component of 0.70 * 0.85 = 0.595. Reasonable,
  --          but a buyer at 300-600 days (std/avg ~ 0.35) gets 0.70 * 0.65
  --          = 0.455, which is too harsh for still-useful procurement intel.
  --
  -- V2 formula uses a sigmoid-like curve on coefficient of variation:
  --   regularity = 1 / (1 + (CV / 0.5)^2)
  -- This gives:
  --   CV = 0.0  -> 1.0  (perfectly regular)
  --   CV = 0.25 -> 0.80 (very regular, minor variation)
  --   CV = 0.50 -> 0.50 (moderate variation, still useful)
  --   CV = 1.0  -> 0.20 (high variation, low reliability)
  --   CV = 2.0  -> 0.06 (near-random, minimal value)
  --
  -- Weights: 50% regularity, 35% interval depth, 15% recency bonus
  -- =========================================================================
  v_cadence_v1 := greatest(0, least(1,
    case
      when v_avg_cycle is null or v_avg_cycle = 0 then
        case
          when coalesce(v_tender_count, 0) + coalesce(v_award_count, 0) > 0 then 0.15
          else 0
        end
      else
        (0.70 * greatest(0, least(1, 1 - (coalesce(v_std_cycle, v_avg_cycle) / nullif(v_avg_cycle, 0)))))
        + (0.30 * least(1, coalesce(v_interval_count, 0)::numeric / 8.0))
    end
  ));

  if v_avg_cycle is not null and v_avg_cycle > 0 then
    v_cv := coalesce(v_std_cycle, 0) / v_avg_cycle;

    v_cadence_v2 := greatest(0, least(1,
      -- Regularity component (sigmoid on CV)
      (0.50 * (1.0 / (1.0 + power(v_cv / 0.5, 2))))
      -- Interval depth component (more cycles = more confidence)
      + (0.35 * least(1, coalesce(v_interval_count, 0)::numeric / 10.0))
      -- Recency bonus: if last tender was within 2 years, small boost
      + (0.15 * case
          when v_last_tender is not null
               and v_last_tender >= now() - interval '2 years'
          then 1.0
          when v_last_tender is not null
               and v_last_tender >= now() - interval '5 years'
          then 0.5
          else 0.0
        end)
    ));
  else
    -- No cycle data available; assign minimal score if we have any signals
    v_cadence_v2 := case
      when coalesce(v_tender_count, 0) + coalesce(v_award_count, 0) > 0 then 0.10
      else 0
    end;
  end if;

  v_cadence_reliability := greatest(0, least(1, (0.60 * v_cadence_v2) + (0.40 * v_cadence_v1)));
  if upper(v_region) in ('FR', 'IE') then
    v_cadence_reliability := greatest(v_cadence_reliability, v_cadence_v1);
    v_cadence_reliability := greatest(v_cadence_reliability, coalesce(v_prev_cadence, 0));
  end if;
  if v_interval_count < 2 and coalesce(v_tender_count, 0) + coalesce(v_award_count, 0) > 0 then
    v_cadence_reliability := greatest(v_cadence_reliability, 0.15);
  end if;

  -- =========================================================================
  -- Basis validation
  -- =========================================================================
  if v_basis not in ('exact', 'fallback') then
    v_basis := 'fallback';
  end if;

  -- =========================================================================
  -- UPSERT into buyer_category_stats
  -- =========================================================================
  insert into public.buyer_category_stats (
    tenant_id,
    buyer_entity_id,
    cpv_cluster_id,
    region,
    tender_count_24m,
    avg_cycle_days,
    std_cycle_days,
    last_tender_at,
    last_award_at,
    seasonality_vector,
    value_stability_score,
    data_coverage_score,
    cadence_reliability_score,
    updated_at
  )
  values (
    p_tenant_id,
    p_buyer_entity_id,
    v_cluster,
    v_region,
    coalesce(v_tender_count, 0),
    coalesce(v_avg_cycle, v_median_cycle),  -- prefer avg, fallback to median
    v_std_cycle,
    v_last_tender,
    v_last_award,
    coalesce(v_seasonality, '{}'::jsonb),
    greatest(0, least(1, coalesce(v_value_stability, 0.5))),
    greatest(0, least(1, coalesce(v_data_coverage, 0))),
    greatest(0, least(1, coalesce(v_cadence_reliability, 0))),
    now()
  )
  on conflict (tenant_id, buyer_entity_id, cpv_cluster_id, region)
  do update set
    tender_count_24m = excluded.tender_count_24m,
    avg_cycle_days = excluded.avg_cycle_days,
    std_cycle_days = excluded.std_cycle_days,
    last_tender_at = excluded.last_tender_at,
    last_award_at = excluded.last_award_at,
    seasonality_vector = excluded.seasonality_vector,
    value_stability_score = excluded.value_stability_score,
    data_coverage_score = excluded.data_coverage_score,
    cadence_reliability_score = excluded.cadence_reliability_score,
    updated_at = now();
end;
$$;

-- Recompute scoped IE rows directly from buyer_category_stats where cadence is missing.
do $$
declare
  r record;
  v_since timestamptz := now() - interval '15 years';
  v_as_of timestamptz := now();
  v_still_missing integer := 0;
begin
  for r in (
    with eligible_pairs as (
      select
        bcs.tenant_id,
        bcs.buyer_entity_id,
        bcs.cpv_cluster_id,
        bcs.region,
        count(distinct s.occurred_at::date)::int as notice_day_count
      from public.buyer_category_stats bcs
      join public.signals s
        on s.tenant_id = bcs.tenant_id
       and s.buyer_entity_id = bcs.buyer_entity_id
       and s.cpv_cluster_id = bcs.cpv_cluster_id
       and coalesce(public.normalize_prediction_region(s.region, null), 'IE') = bcs.region
      where bcs.tenant_id = 'civant_default'
        and bcs.region = 'IE'
        and bcs.avg_cycle_days is null
        and bcs.buyer_entity_id is not null
        and bcs.buyer_entity_id not like 'unresolved:%'
        and bcs.cpv_cluster_id is not null
        and bcs.cpv_cluster_id <> 'cluster_unknown'
        and s.signal_type = 'notice_published'
        and s.occurred_at >= v_since
        and s.occurred_at <= v_as_of
      group by 1, 2, 3, 4
      having count(distinct s.occurred_at::date) >= 3
    )
    select
      ep.tenant_id,
      ep.buyer_entity_id,
      ep.cpv_cluster_id,
      ep.region
    from eligible_pairs ep
  ) loop
    perform public.recompute_buyer_category_stats_v2(
      r.tenant_id,
      r.buyer_entity_id,
      r.cpv_cluster_id,
      r.region,
      interval '15 years'
    );
  end loop;

  -- Final validation assertion: migration must not leave qualifying IE rows missing avg_cycle_days.
  select count(*)
    into v_still_missing
  from (
    with eligible_pairs as (
      select
        bcs.tenant_id,
        bcs.buyer_entity_id,
        bcs.cpv_cluster_id,
        bcs.region,
        count(distinct s.occurred_at::date)::int as notice_day_count
      from public.buyer_category_stats bcs
      join public.signals s
        on s.tenant_id = bcs.tenant_id
       and s.buyer_entity_id = bcs.buyer_entity_id
       and s.cpv_cluster_id = bcs.cpv_cluster_id
       and coalesce(public.normalize_prediction_region(s.region, null), 'IE') = bcs.region
      where bcs.tenant_id = 'civant_default'
        and bcs.region = 'IE'
        and bcs.avg_cycle_days is null
        and bcs.buyer_entity_id is not null
        and bcs.buyer_entity_id not like 'unresolved:%'
        and bcs.cpv_cluster_id is not null
        and bcs.cpv_cluster_id <> 'cluster_unknown'
        and s.signal_type = 'notice_published'
        and s.occurred_at >= v_since
        and s.occurred_at <= v_as_of
      group by 1, 2, 3, 4
      having count(distinct s.occurred_at::date) >= 3
    )
    select count(*)::int as still_missing
    from eligible_pairs
  ) q;

  if v_still_missing <> 0 then
    raise exception 'buyer_category_stats cycle min-gap patch incomplete: still_missing=%', v_still_missing;
  end if;
end;
$$;

-- =============================================================================
-- Validation SQL (run before + after this migration)
-- =============================================================================
-- Should drop to 0 after recompute:
-- with eligible_pairs as (
--   select
--     bcs.tenant_id,
--     bcs.buyer_entity_id,
--     bcs.cpv_cluster_id,
--     bcs.region,
--     count(distinct s.occurred_at::date)::int as notice_day_count
--   from public.buyer_category_stats bcs
--   join public.signals s
--     on s.tenant_id = bcs.tenant_id
--    and s.buyer_entity_id = bcs.buyer_entity_id
--    and s.cpv_cluster_id = bcs.cpv_cluster_id
--    and coalesce(public.normalize_prediction_region(s.region, null), 'IE') = bcs.region
--   where bcs.tenant_id = 'civant_default'
--     and bcs.region = 'IE'
--     and bcs.avg_cycle_days is null
--     and bcs.buyer_entity_id is not null
--     and bcs.buyer_entity_id not like 'unresolved:%'
--     and bcs.cpv_cluster_id is not null
--     and bcs.cpv_cluster_id <> 'cluster_unknown'
--     and s.signal_type = 'notice_published'
--     and s.occurred_at >= now() - interval '15 years'
--     and s.occurred_at <= now()
--   group by 1, 2, 3, 4
--   having count(distinct s.occurred_at::date) >= 3
-- )
-- select count(*)::int as still_missing
-- from eligible_pairs;
--
-- Final assertion query:
-- with validation as (
--   with eligible_pairs as (
--     select
--       bcs.tenant_id,
--       bcs.buyer_entity_id,
--       bcs.cpv_cluster_id,
--       bcs.region,
--       count(distinct s.occurred_at::date)::int as notice_day_count
--     from public.buyer_category_stats bcs
--     join public.signals s
--       on s.tenant_id = bcs.tenant_id
--      and s.buyer_entity_id = bcs.buyer_entity_id
--      and s.cpv_cluster_id = bcs.cpv_cluster_id
--      and coalesce(public.normalize_prediction_region(s.region, null), 'IE') = bcs.region
--     where bcs.tenant_id = 'civant_default'
--       and bcs.region = 'IE'
--       and bcs.avg_cycle_days is null
--       and bcs.buyer_entity_id is not null
--       and bcs.buyer_entity_id not like 'unresolved:%'
--       and bcs.cpv_cluster_id is not null
--       and bcs.cpv_cluster_id <> 'cluster_unknown'
--       and s.signal_type = 'notice_published'
--       and s.occurred_at >= now() - interval '15 years'
--       and s.occurred_at <= now()
--     group by 1, 2, 3, 4
--     having count(distinct s.occurred_at::date) >= 3
--   )
--   select count(*)::int as still_missing
--   from eligible_pairs
-- )
-- select still_missing, (still_missing = 0) as assert_zero
-- from validation;
