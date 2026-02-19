-- Buyer category stats recompute v1 (deterministic)
-- Fixes stats population using resolved signals (not canonical_tenders buyer ids).

create or replace function public.recompute_buyer_category_stats_v1(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_lookback interval default interval '10 years'
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
  v_tender_count integer := 0;
  v_award_count integer := 0;
  v_interval_count integer := 0;
  v_avg_cycle numeric := null;
  v_std_cycle numeric := null;
  v_last_tender timestamptz;
  v_last_award timestamptz;
  v_seasonality jsonb := '{}'::jsonb;
  v_value_stability numeric := 0.5;
  v_data_coverage numeric := 0;
  v_cadence_reliability numeric := 0;
  v_value_avg numeric := null;
  v_value_std numeric := null;
  v_basis text := 'exact';
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

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
      and s.region = v_region
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
  exact_events as (
    select *
    from signal_candidates
    where match_rank = 0
  ),
  fallback_events as (
    select *
    from signal_candidates
    where match_rank in (0, 1)
  ),
  stats_basis as (
    select case
      when (
        select count(*) from exact_events where signal_type in ('notice_published', 'award_published')
      ) >= 3 then 'exact'
      else 'fallback'
    end as basis
  ),
  selected_events as (
    select e.occurred_at, e.signal_type, e.estimated_value
    from exact_events e
    join stats_basis b on b.basis = 'exact'
    union all
    select f.occurred_at, f.signal_type, f.estimated_value
    from fallback_events f
    join stats_basis b on b.basis = 'fallback'
  ),
  notice_events as (
    select distinct occurred_at::date as event_day
    from selected_events
    where signal_type = 'notice_published'
  ),
  award_events as (
    select distinct occurred_at::date as event_day
    from selected_events
    where signal_type = 'award_published'
  ),
  notice_gaps as (
    select (event_day - lag(event_day) over (order by event_day))::numeric as gap_days
    from notice_events
  ),
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
  cycle_intervals as (
    select gap_days
    from notice_gaps
    where gap_days between 1 and 730
    union all
    select gap_days
    from award_to_next_notice
    where gap_days between 1 and 730
  ),
  seasonality as (
    select extract(month from event_day)::int as month_no,
           count(*)::int as month_count
    from notice_events
    group by 1
  ),
  values_data as (
    select estimated_value
    from selected_events
    where estimated_value is not null and estimated_value > 0
  )
  select
    (select basis from stats_basis),
    (select count(*) from notice_events),
    (select count(*) from award_events),
    (select count(*) from cycle_intervals),
    (select avg(gap_days) from cycle_intervals),
    (select stddev_samp(gap_days) from cycle_intervals),
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
    v_last_tender,
    v_last_award,
    v_seasonality,
    v_value_avg,
    v_value_std;

  -- Require at least 3 deterministic intervals before exposing cadence stats.
  if v_interval_count < 3 then
    v_avg_cycle := null;
    v_std_cycle := null;
  elsif v_std_cycle is null then
    v_std_cycle := 0;
  end if;

  v_value_stability := case
    when v_value_avg is null or v_value_avg = 0 then 0.5
    else greatest(0, least(1, 1 - (coalesce(v_value_std, 0) / nullif(v_value_avg, 0))))
  end;

  v_data_coverage := greatest(0, least(1,
    (0.55 * least(1, coalesce(v_tender_count, 0)::numeric / 8.0))
    + (0.25 * least(1, coalesce(v_award_count, 0)::numeric / 6.0))
    + (0.20 * least(1, coalesce(v_interval_count, 0)::numeric / 5.0))
  ));

  v_cadence_reliability := greatest(0, least(1,
    case
      when v_avg_cycle is null or v_avg_cycle = 0 then
        case when coalesce(v_tender_count, 0) + coalesce(v_award_count, 0) > 0 then 0.15 else 0 end
      else
        (0.70 * greatest(0, least(1, 1 - (coalesce(v_std_cycle, v_avg_cycle) / nullif(v_avg_cycle, 0)))))
        + (0.30 * least(1, coalesce(v_interval_count, 0)::numeric / 8.0))
    end
  ));

  -- Explicit no-op usage of v_basis for migration audit readability and to avoid dead-code cleanup.
  if v_basis not in ('exact', 'fallback') then
    v_basis := 'fallback';
  end if;

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
    v_avg_cycle,
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

create or replace function public.refresh_buyer_category_stats_for_pair(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_as_of timestamptz default now()
)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.recompute_buyer_category_stats_v1(
    p_tenant_id,
    p_buyer_entity_id,
    p_cpv_cluster_id,
    p_region,
    interval '10 years'
  );
end;
$$;
