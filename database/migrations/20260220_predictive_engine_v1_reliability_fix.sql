-- Predictive Engine V1 reliability patch:
-- 1) Improve deterministic buyer resolution coverage.
-- 2) Prevent sparse-but-behavioural procurement pairs from being silently dropped.
-- 3) Add run-level visibility for computed vs skipped pairs.

create or replace function public.resolve_buyer_entity_id(
  p_tenant_id text,
  p_source text,
  p_buyer_name_raw text,
  p_fallback_entity_id text default null
)
returns table (
  buyer_entity_id text,
  is_resolved boolean,
  resolution_tier text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_key text := public.normalize_match_key(p_buyer_name_raw);
  v_source text := upper(coalesce(p_source, 'UNKNOWN'));
  v_entity text;
begin
  if v_key is null then
    if nullif(p_fallback_entity_id, '') is not null then
      return query select p_fallback_entity_id, false, 'fallback_empty_buyer';
    else
      return query select 'unresolved:' || substr(md5('empty-buyer'), 1, 16), false, 'unresolved_empty_buyer';
    end if;
    return;
  end if;

  select ba.canonical_buyer_entity_id
  into v_entity
  from public.buyer_aliases ba
  where ba.tenant_id = p_tenant_id
    and ba.raw_buyer_key = v_key
    and (ba.source = v_source or ba.source = '*')
  order by case when ba.source = v_source then 0 else 1 end, ba.updated_at desc
  limit 1;

  if v_entity is not null then
    return query select v_entity, true, 'buyer_aliases';
    return;
  end if;

  select e.entity_id
  into v_entity
  from public.entities e
  join public.entity_aliases ea
    on ea.entity_id = e.entity_id
   and ea.tenant_id = e.tenant_id
  where e.tenant_id = p_tenant_id
    and lower(coalesce(e.entity_type, '')) = 'buyer'
    and public.normalize_match_key(ea.alias) = v_key
  order by coalesce(ea.confidence, 0) desc, ea.created_at desc
  limit 1;

  if v_entity is not null then
    return query select v_entity, true, 'entity_aliases';
    return;
  end if;

  select e.entity_id
  into v_entity
  from public.entities e
  where e.tenant_id = p_tenant_id
    and lower(coalesce(e.entity_type, '')) = 'buyer'
    and public.normalize_match_key(coalesce(e.canonical_name, '')) = v_key
  order by e.updated_at desc, e.created_at desc
  limit 1;

  if v_entity is not null then
    return query select v_entity, true, 'entities_canonical_name';
    return;
  end if;

  if nullif(p_fallback_entity_id, '') is not null then
    return query select p_fallback_entity_id, false, 'fallback_source';
  else
    return query select 'unresolved:' || substr(md5(v_key), 1, 16), false, 'unresolved';
  end if;
end;
$$;


create or replace function public.compute_prediction_for_pair_v1(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_formula_version text default 'v1.0.0',
  p_as_of timestamptz default now()
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_region text := coalesce(public.normalize_prediction_region(p_region, null), 'IE');
  v_stats public.buyer_category_stats%rowtype;
  v_buyer_stats record;
  v_twin_stats record;
  v_country_cluster record;
  v_country_cycle_direct numeric;
  v_cpv_baseline record;
  v_effective_external numeric := 0;
  v_external_count integer := 0;
  v_non_external_event_count integer := 0;
  v_framework_days numeric := 365;
  v_days_since_last numeric := 365;
  v_expected_days_until numeric := 180;

  v_cycle_proximity numeric := 0;
  v_seasonality_alignment numeric := 0;
  v_framework_expiry_proximity numeric := 0;
  v_authority_cadence_strength numeric := 0;
  v_external_signal_timing_boost numeric := 0;

  v_probability numeric := 0;
  v_historical_depth numeric := 0;
  v_pattern_stability numeric := 0;
  v_data_coverage numeric := 0;
  v_behavioural_reliability numeric := 0;
  v_external_corroboration numeric := 0;
  v_confidence numeric := 0;
  v_confidence_band text := 'Low';
  v_fallback_tier integer := 0;

  v_non_external_support boolean := false;
  v_cycle_score integer := 0;
  v_timing_score integer := 0;
  v_behavioural_score integer := 0;
  v_structural_score integer := 0;
  v_external_score integer := 0;
  v_quality_score integer := 0;
  v_total_score integer := 0;

  v_window_label public.next_window_label;
  v_window_start date;
  v_window_end date;

  v_prediction_id uuid;
  v_driver_count integer := 0;
  v_evidence_count integer := 0;
  v_buyer_name text;
  v_cluster_label text;
  v_has_buyer_entity boolean := false;
  v_behavioural_fingerprint boolean := false;
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

  perform public.refresh_buyer_category_stats_for_pair(
    p_tenant_id,
    p_buyer_entity_id,
    coalesce(p_cpv_cluster_id, 'cluster_unknown'),
    v_region,
    p_as_of
  );

  select *
  into v_stats
  from public.buyer_category_stats s
  where s.tenant_id = p_tenant_id
    and s.buyer_entity_id = p_buyer_entity_id
    and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown')
    and s.region = v_region;

  if not found then
    return null;
  end if;

  -- Fallback tiers.
  if coalesce(v_stats.tender_count_24m, 0) >= 3 and v_stats.avg_cycle_days is not null then
    v_fallback_tier := 0;
  else
    select
      sum(tender_count_24m)::int as tender_count,
      avg(avg_cycle_days)::numeric as avg_cycle,
      avg(cadence_reliability_score)::numeric as cadence
    into v_buyer_stats
    from public.buyer_category_stats s
    where s.tenant_id = p_tenant_id
      and s.buyer_entity_id = p_buyer_entity_id
      and s.region = v_region;

    if coalesce(v_buyer_stats.tender_count, 0) >= 3 and v_buyer_stats.avg_cycle is not null then
      v_fallback_tier := 1;
      select
        avg(s.avg_cycle_days)::numeric as avg_cycle,
        avg(s.cadence_reliability_score)::numeric as cadence
      into v_cpv_baseline
      from public.buyer_category_stats s
      where s.tenant_id = p_tenant_id
        and s.region = v_region
        and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown');

      v_stats.avg_cycle_days := v_buyer_stats.avg_cycle;
      if v_cpv_baseline.avg_cycle is not null then
        v_stats.avg_cycle_days := (0.8 * v_stats.avg_cycle_days) + (0.2 * v_cpv_baseline.avg_cycle);
      end if;
      v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, coalesce(v_buyer_stats.cadence, 0));
    else
      select
        avg(s.avg_cycle_days)::numeric as avg_cycle,
        avg(s.cadence_reliability_score)::numeric as cadence
      into v_twin_stats
      from public.buyer_category_stats s
      where s.tenant_id = p_tenant_id
        and s.region = v_region
        and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown')
        and s.buyer_entity_id <> p_buyer_entity_id
        and s.tender_count_24m between greatest(1, v_stats.tender_count_24m - 3) and (v_stats.tender_count_24m + 3);

      if v_twin_stats.avg_cycle is not null then
        v_fallback_tier := 2;
        v_stats.avg_cycle_days := v_twin_stats.avg_cycle;
        v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, coalesce(v_twin_stats.cadence, 0));
      else
        select
          avg(s.avg_cycle_days)::numeric as avg_cycle,
          avg(s.cadence_reliability_score)::numeric as cadence
        into v_country_cluster
        from public.buyer_category_stats s
        where s.tenant_id = p_tenant_id
          and s.region = v_region
          and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown');

        if v_country_cluster.avg_cycle is not null then
          v_fallback_tier := 3;
          v_stats.avg_cycle_days := v_country_cluster.avg_cycle;
          v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, coalesce(v_country_cluster.cadence, 0));
        else
          select avg(gap_days)::numeric
          into v_country_cycle_direct
          from (
            select
              (event_day - lag(event_day) over (order by event_day))::numeric as gap_days
            from (
              select distinct s.occurred_at::date as event_day
              from public.signals s
              where s.tenant_id = p_tenant_id
                and s.region = v_region
                and coalesce(s.cpv_cluster_id, 'cluster_unknown') = coalesce(p_cpv_cluster_id, 'cluster_unknown')
                and s.signal_type in ('notice_published', 'framework_established', 'amendment')
                and s.occurred_at >= (p_as_of - interval '24 months')
            ) d
          ) x
          where gap_days is not null;

          if v_country_cycle_direct is not null then
            v_fallback_tier := 3;
            v_stats.avg_cycle_days := v_country_cycle_direct;
            v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, 0.25);
          else
            v_fallback_tier := 4;
          end if;
        end if;
      end if;
    end if;
  end if;

  select
    coalesce(max(public.compute_effective_signal_strength(s.signal_type, s.signal_strength, s.occurred_at, p_as_of)), 0),
    count(*)::int
  into v_effective_external, v_external_count
  from public.signals s
  where s.tenant_id = p_tenant_id
    and s.region = v_region
    and s.buyer_entity_id = p_buyer_entity_id
    and (s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown') or s.cpv_cluster_id is null)
    and s.signal_type in (
      'job_posting','hiring_spike','role_category_detected',
      'budget_approval','budget_increase','grant_awarded','eu_funding_allocation',
      'digital_transformation_program','compliance_mandate','accessibility_regulation','sustainability_requirement','policy_signal'
    )
    and s.occurred_at >= (p_as_of - interval '24 months');

  select count(*)::int
  into v_non_external_event_count
  from public.signals s
  where s.tenant_id = p_tenant_id
    and s.region = v_region
    and s.buyer_entity_id = p_buyer_entity_id
    and s.signal_type in (
      'notice_published',
      'award_published',
      'framework_established',
      'framework_expiry',
      'cancellation',
      'amendment'
    )
    and s.occurred_at <= p_as_of;

  v_behavioural_fingerprint := (v_non_external_event_count > 0);

  if v_fallback_tier = 4 and not v_behavioural_fingerprint then
    -- External-only predictions are not allowed without behavioural evidence.
    return null;
  end if;

  if v_fallback_tier = 4 and v_behavioural_fingerprint then
    -- Sparse-but-behavioural pairs should still emit low-confidence predictions.
    v_stats.avg_cycle_days := coalesce(v_stats.avg_cycle_days, 210);
    v_stats.cadence_reliability_score := greatest(coalesce(v_stats.cadence_reliability_score, 0), 0.20);
    v_stats.data_coverage_score := greatest(coalesce(v_stats.data_coverage_score, 0), 0.15);
  end if;

  if v_stats.last_tender_at is not null then
    v_days_since_last := greatest(0, extract(epoch from (p_as_of - v_stats.last_tender_at)) / 86400.0);
  end if;

  if v_stats.avg_cycle_days is not null and v_stats.avg_cycle_days > 0 then
    v_expected_days_until := greatest(0, v_stats.avg_cycle_days - v_days_since_last);
    v_cycle_proximity := greatest(0, least(1, 1 - abs(v_days_since_last - v_stats.avg_cycle_days) / greatest(v_stats.avg_cycle_days, 1)));
  else
    v_expected_days_until := case when v_fallback_tier = 4 then 45 else 180 end;
    v_cycle_proximity := case when v_fallback_tier = 4 then 0.2 else 0.35 end;
  end if;

  v_seasonality_alignment := coalesce(
    (v_stats.seasonality_vector ->> extract(month from p_as_of)::int::text)::numeric
    / nullif((
      select greatest(1, max(value::numeric))
      from jsonb_each_text(coalesce(v_stats.seasonality_vector, '{}'::jsonb))
    ), 0),
    0
  );
  v_seasonality_alignment := greatest(0, least(1, coalesce(v_seasonality_alignment, 0)));

  select coalesce(min(extract(epoch from (s.occurred_at - p_as_of)) / 86400.0), 365)
  into v_framework_days
  from public.signals s
  where s.tenant_id = p_tenant_id
    and s.region = v_region
    and s.buyer_entity_id = p_buyer_entity_id
    and (s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown') or s.cpv_cluster_id is null)
    and s.signal_type = 'framework_expiry'
    and s.occurred_at >= p_as_of;

  v_framework_expiry_proximity := case
    when v_framework_days <= 30 then 1
    when v_framework_days <= 90 then 0.75
    when v_framework_days <= 180 then 0.4
    else 0.1
  end;

  v_authority_cadence_strength := greatest(0, least(1, coalesce(v_stats.cadence_reliability_score, 0)));

  v_external_signal_timing_boost := greatest(0, least(1, coalesce(v_effective_external, 0)));

  v_probability :=
    (0.40 * v_cycle_proximity) +
    (0.15 * v_seasonality_alignment) +
    (0.15 * v_framework_expiry_proximity) +
    (0.15 * v_authority_cadence_strength) +
    (0.15 * v_external_signal_timing_boost);

  -- hard cap and external-only influence cap (+0.15 max absolute already bounded by weight)
  v_probability := least(0.95, greatest(0, v_probability));

  v_historical_depth := greatest(0, least(1, coalesce(v_stats.tender_count_24m, 0)::numeric / 12.0));
  v_pattern_stability := greatest(0, least(1,
    case
      when v_stats.avg_cycle_days is null or v_stats.avg_cycle_days = 0 then 0.2
      else 1 - (coalesce(v_stats.std_cycle_days, v_stats.avg_cycle_days) / nullif(v_stats.avg_cycle_days, 0))
    end
  ));
  v_data_coverage := greatest(0, least(1, coalesce(v_stats.data_coverage_score, 0)));
  v_behavioural_reliability := greatest(0, least(1, coalesce(v_stats.cadence_reliability_score, 0)));
  v_external_corroboration := greatest(0, least(1,
    coalesce(v_effective_external, 0) * case when v_external_count >= 2 then 1 else 0.7 end
  ));

  v_confidence := 100 * (
    (0.30 * v_historical_depth) +
    (0.20 * v_pattern_stability) +
    (0.20 * v_data_coverage) +
    (0.15 * v_behavioural_reliability) +
    (0.15 * v_external_corroboration)
  );

  select exists (
    select 1
    from public.entities e
    where e.tenant_id = p_tenant_id
      and e.entity_id = p_buyer_entity_id
      and lower(coalesce(e.entity_type, '')) = 'buyer'
  )
  into v_has_buyer_entity;

  -- unresolved or fallback buyer ids are capped to Medium confidence
  if p_buyer_entity_id like 'unresolved:%' or not v_has_buyer_entity then
    v_confidence := least(v_confidence, 59);
  end if;

  -- Sparse fallback tiers must remain low/medium confidence.
  if v_fallback_tier >= 3 then
    v_confidence := least(v_confidence, 59);
  end if;

  -- global cap rule
  if not (coalesce(v_stats.tender_count_24m, 0) >= 5 and v_data_coverage > 0.9) then
    v_confidence := least(v_confidence, 95);
  end if;

  v_confidence := greatest(0, least(100, round(v_confidence)));

  v_confidence_band := case
    when v_confidence <= 39 then 'Low'
    when v_confidence <= 59 then 'Medium'
    when v_confidence <= 79 then 'High'
    else 'Very High'
  end;

  select next_window_label, window_start, window_end
    into v_window_label, v_window_start, v_window_end
  from public.resolve_prediction_window(v_expected_days_until, p_as_of::date);

  v_non_external_support := v_behavioural_fingerprint
    and (v_cycle_proximity > 0.2 or v_seasonality_alignment > 0.2 or v_authority_cadence_strength > 0.2 or v_framework_expiry_proximity > 0.2);

  v_cycle_score := round(20 * v_cycle_proximity);
  v_timing_score := round(15 * v_seasonality_alignment);
  v_behavioural_score := round(15 * v_authority_cadence_strength);
  v_structural_score := round(10 * v_framework_expiry_proximity);
  v_external_score := round(20 * v_external_signal_timing_boost);
  if not v_non_external_support then
    v_external_score := least(v_external_score, 18);
  end if;
  v_quality_score := round(20 * v_data_coverage);
  v_total_score := v_cycle_score + v_timing_score + v_behavioural_score + v_structural_score + v_external_score + v_quality_score;
  v_total_score := greatest(0, least(100, v_total_score));

  select coalesce(max(canonical_name), p_buyer_entity_id)
    into v_buyer_name
  from public.entities
  where tenant_id = p_tenant_id
    and entity_id = p_buyer_entity_id;

  select label
    into v_cluster_label
  from public.cpv_cluster c
  where c.tenant_id = p_tenant_id
    and c.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown')
  limit 1;

  insert into public.predictions_current (
    tenant_id,
    buyer_entity_id,
    buyer_display_name,
    cpv_cluster_id,
    cpv_cluster_label,
    region,
    next_window_label,
    expected_window_start,
    expected_window_end,
    probability,
    confidence,
    confidence_band,
    forecast_score,
    fallback_tier,
    drivers_count,
    evidence_count,
    explainability_json,
    last_computed_at
  )
  values (
    p_tenant_id,
    p_buyer_entity_id,
    v_buyer_name,
    coalesce(p_cpv_cluster_id, 'cluster_unknown'),
    coalesce(v_cluster_label, coalesce(p_cpv_cluster_id, 'cluster_unknown')),
    v_region,
    v_window_label,
    v_window_start,
    v_window_end,
    v_probability,
    v_confidence,
    v_confidence_band,
    v_total_score,
    v_fallback_tier,
    0,
    0,
    jsonb_build_object(
      'components', jsonb_build_object(
        'cycle_proximity', v_cycle_proximity,
        'seasonality_alignment', v_seasonality_alignment,
        'framework_expiry_proximity', v_framework_expiry_proximity,
        'authority_cadence_strength', v_authority_cadence_strength,
        'external_signal_timing_boost', v_external_signal_timing_boost
      ),
      'confidence_components', jsonb_build_object(
        'historical_depth', v_historical_depth,
        'pattern_stability', v_pattern_stability,
        'data_coverage', v_data_coverage,
        'behavioural_reliability', v_behavioural_reliability,
        'external_signal_corroboration', v_external_corroboration
      )
    ),
    now()
  )
  on conflict (tenant_id, buyer_entity_id, cpv_cluster_id, region)
  do update set
    buyer_display_name = excluded.buyer_display_name,
    cpv_cluster_label = excluded.cpv_cluster_label,
    next_window_label = excluded.next_window_label,
    expected_window_start = excluded.expected_window_start,
    expected_window_end = excluded.expected_window_end,
    probability = excluded.probability,
    confidence = excluded.confidence,
    confidence_band = excluded.confidence_band,
    forecast_score = excluded.forecast_score,
    fallback_tier = excluded.fallback_tier,
    explainability_json = excluded.explainability_json,
    last_computed_at = now()
  returning prediction_id into v_prediction_id;

  delete from public.prediction_drivers where tenant_id = p_tenant_id and prediction_id = v_prediction_id;

  insert into public.prediction_drivers (
    tenant_id,
    prediction_id,
    driver_type,
    label,
    contribution,
    narrative,
    evidence_refs
  )
  values
    (
      p_tenant_id,
      v_prediction_id,
      'fallback_tier',
      'Fallback tier ' || v_fallback_tier,
      0,
      case v_fallback_tier
        when 0 then 'Primary buyer + CPV cadence available.'
        when 1 then 'Used buyer-level history with CPV baseline adjustment.'
        when 2 then 'Used behavioural twins fallback for cadence.'
        when 3 then 'Used country-level CPV cadence baseline.'
        else 'Sparse fallback with conservative defaults and capped confidence.'
      end,
      array[]::text[]
    ),
    (
      p_tenant_id,
      v_prediction_id,
      'cycle_rhythm',
      'Cycle rhythm signal',
      round((0.40 * v_cycle_proximity)::numeric, 4),
      'Cycle proximity contributes to timing likelihood based on historical cadence.',
      array[]::text[]
    ),
    (
      p_tenant_id,
      v_prediction_id,
      'external_signal',
      'External signal timing boost',
      round((0.15 * v_external_signal_timing_boost)::numeric, 4),
      'Decayed external signals were incorporated with hard influence caps.',
      array[]::text[]
    );

  -- ensure at least one quality/corroboration driver for explainability breadth
  if v_data_coverage < 0.5 then
    insert into public.prediction_drivers (
      tenant_id,
      prediction_id,
      driver_type,
      label,
      contribution,
      narrative,
      evidence_refs
    )
    values (
      p_tenant_id,
      v_prediction_id,
      'coverage_penalty',
      'Coverage quality constraint',
      round((0.2 * (0.5 - v_data_coverage))::numeric, 4),
      'Limited data coverage constrained confidence and score.',
      array[]::text[]
    );
  else
    insert into public.prediction_drivers (
      tenant_id,
      prediction_id,
      driver_type,
      label,
      contribution,
      narrative,
      evidence_refs
    )
    values (
      p_tenant_id,
      v_prediction_id,
      'cadence_pattern',
      'Cadence reliability',
      round((0.15 * v_authority_cadence_strength)::numeric, 4),
      'Cadence stability supports forecast reliability.',
      array[]::text[]
    );
  end if;

  delete from public.prediction_scorecard where prediction_id = v_prediction_id;

  insert into public.prediction_scorecard (
    prediction_id,
    tenant_id,
    cycle_score,
    timing_score,
    behavioural_score,
    structural_score,
    external_signal_score,
    data_quality_score,
    total_score,
    created_at
  )
  values (
    v_prediction_id,
    p_tenant_id,
    v_cycle_score,
    v_timing_score,
    v_behavioural_score,
    v_structural_score,
    v_external_score,
    v_quality_score,
    v_total_score,
    now()
  );

  insert into public.prediction_inputs_snapshot (
    tenant_id,
    prediction_id,
    formula_version,
    features_json,
    computed_at
  )
  values (
    p_tenant_id,
    v_prediction_id,
    p_formula_version,
    jsonb_build_object(
      'pair', jsonb_build_object(
        'tenant_id', p_tenant_id,
        'buyer_entity_id', p_buyer_entity_id,
        'cpv_cluster_id', coalesce(p_cpv_cluster_id, 'cluster_unknown'),
        'region', v_region
      ),
      'fallback_tier', v_fallback_tier,
      'features', jsonb_build_object(
        'cycle_proximity', v_cycle_proximity,
        'seasonality_alignment', v_seasonality_alignment,
        'framework_expiry_proximity', v_framework_expiry_proximity,
        'authority_cadence_strength', v_authority_cadence_strength,
        'external_signal_timing_boost', v_external_signal_timing_boost,
        'historical_depth', v_historical_depth,
        'pattern_stability', v_pattern_stability,
        'data_coverage', v_data_coverage,
        'behavioural_reliability', v_behavioural_reliability,
        'external_signal_corroboration', v_external_corroboration
      ),
      'caps', jsonb_build_object(
        'probability_cap', 0.95,
        'confidence_cap_default', 95,
        'external_probability_max_abs', 0.15,
        'external_score_soft_cap_without_corroboration', 18
      )
    ),
    now()
  );

  select count(*), coalesce(sum(cardinality(evidence_refs)), 0)
    into v_driver_count, v_evidence_count
  from public.prediction_drivers
  where tenant_id = p_tenant_id
    and prediction_id = v_prediction_id;

  update public.predictions_current
  set
    drivers_count = v_driver_count,
    evidence_count = v_evidence_count,
    last_computed_at = now()
  where prediction_id = v_prediction_id;

  return v_prediction_id;
end;
$$;


create or replace function public.run_predictive_engine_v1(
  p_tenant_id text,
  p_run_type text default 'incremental',
  p_formula_version text default 'v1.0.0',
  p_since timestamptz default null,
  p_max_pairs integer default 5000,
  p_normalize_signals boolean default true
)
returns table (
  run_id uuid,
  status text,
  pairs_processed integer,
  error_message text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_run_id uuid;
  v_status text := 'running';
  v_error text := null;
  v_processed integer := 0;
  v_computed integer := 0;
  v_skipped integer := 0;
  v_prediction_id uuid;
  v_last_success timestamptz;
  v_effective_since timestamptz;
  v_norm record;
  p record;
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

  insert into public.prediction_runs (
    tenant_id,
    run_type,
    started_at,
    status,
    metadata_json
  )
  values (
    p_tenant_id,
    case when lower(p_run_type) in ('full', 'signals_only') then lower(p_run_type) else 'incremental' end,
    now(),
    'running',
    jsonb_build_object('formula_version', p_formula_version, 'normalize_signals', p_normalize_signals)
  )
  returning prediction_runs.run_id into v_run_id;

  begin
    if lower(p_run_type) in ('incremental', 'signals_only') then
      select max(finished_at)
      into v_last_success
      from public.prediction_runs
      where tenant_id = p_tenant_id
        and public.prediction_runs.status = 'success'
        and public.prediction_runs.run_type in ('incremental', 'full', 'signals_only');

      v_effective_since := coalesce(p_since, v_last_success, now() - interval '24 hours');
    else
      v_effective_since := coalesce(p_since, now() - interval '24 months');
    end if;

    if p_normalize_signals then
      select *
      into v_norm
      from public.normalize_all_signals(
        p_tenant_id,
        v_effective_since,
        greatest(1000, p_max_pairs * 8)
      );
    end if;

    if lower(p_run_type) = 'signals_only' then
      update public.prediction_runs
      set
        status = 'success',
        pairs_processed = 0,
        finished_at = now(),
        metadata_json = coalesce(metadata_json, '{}'::jsonb)
          || jsonb_build_object(
            'effective_since', v_effective_since,
            'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb),
            'computed_prediction_rows', 0,
            'skipped_pairs', 0
          )
      where public.prediction_runs.run_id = v_run_id;

      return query select v_run_id, 'success'::text, 0::integer, null::text;
      return;
    end if;

    for p in
      with signal_scope as (
        select
          s.tenant_id,
          s.buyer_entity_id,
          coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
          coalesce(public.normalize_prediction_region(s.region, s.source), 'IE') as region,
          s.ingested_at,
          s.occurred_at,
          s.cpv_cluster_id is null or s.cpv_cluster_id = 'cluster_unknown' as is_buyer_level_fallback
        from public.signals s
        where s.tenant_id = p_tenant_id
          and (
            lower(p_run_type) = 'full'
            or s.ingested_at >= v_effective_since
          )
      ),
      direct_pairs as (
        select
          ss.tenant_id,
          ss.buyer_entity_id,
          ss.cpv_cluster_id,
          ss.region,
          max(ss.ingested_at) as last_ingested_at,
          max(ss.occurred_at) as last_occurred_at
        from signal_scope ss
        group by 1, 2, 3, 4
      ),
      buyer_level_pairs as (
        select
          ss.tenant_id,
          ss.buyer_entity_id,
          coalesce(bcs.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
          ss.region,
          max(ss.ingested_at) as last_ingested_at,
          max(ss.occurred_at) as last_occurred_at
        from signal_scope ss
        left join public.buyer_category_stats bcs
          on bcs.tenant_id = ss.tenant_id
         and bcs.buyer_entity_id = ss.buyer_entity_id
         and bcs.region = ss.region
        where ss.is_buyer_level_fallback
        group by 1, 2, 3, 4
      ),
      impacted_union as (
        select * from direct_pairs
        union all
        select * from buyer_level_pairs
      ),
      impacted as (
        select
          tenant_id,
          buyer_entity_id,
          cpv_cluster_id,
          region,
          max(last_ingested_at) as last_ingested_at,
          max(last_occurred_at) as last_occurred_at
        from impacted_union
        group by 1, 2, 3, 4
      )
      select
        tenant_id,
        buyer_entity_id,
        cpv_cluster_id,
        region
      from impacted
      order by
        last_ingested_at desc nulls last,
        last_occurred_at desc nulls last,
        buyer_entity_id,
        cpv_cluster_id,
        region
      limit greatest(1, p_max_pairs)
    loop
      v_prediction_id := public.compute_prediction_for_pair_v1(
        p_tenant_id,
        p.buyer_entity_id,
        p.cpv_cluster_id,
        p.region,
        p_formula_version,
        now()
      );

      if v_prediction_id is null then
        v_skipped := v_skipped + 1;
      else
        v_computed := v_computed + 1;
      end if;

      v_processed := v_processed + 1;
    end loop;

    v_status := 'success';

    update public.prediction_runs
    set
      status = v_status,
      pairs_processed = v_processed,
      finished_at = now(),
      metadata_json = coalesce(metadata_json, '{}'::jsonb)
        || jsonb_build_object(
          'effective_since', v_effective_since,
          'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb),
          'computed_prediction_rows', v_computed,
          'skipped_pairs', v_skipped
        )
    where public.prediction_runs.run_id = v_run_id;

  exception when others then
    v_status := 'failed';
    v_error := sqlerrm;

    update public.prediction_runs
    set
      status = v_status,
      error_message = v_error,
      pairs_processed = v_processed,
      finished_at = now(),
      metadata_json = coalesce(metadata_json, '{}'::jsonb)
        || jsonb_build_object(
          'effective_since', v_effective_since,
          'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb),
          'computed_prediction_rows', v_computed,
          'skipped_pairs', v_skipped
        )
    where public.prediction_runs.run_id = v_run_id;
  end;

  return query select v_run_id, v_status, v_processed, v_error;
end;
$$;
