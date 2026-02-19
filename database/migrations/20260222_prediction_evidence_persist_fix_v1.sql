-- Prediction evidence persistence fix v1 (deterministic)
-- Attaches evidence_refs to prediction_drivers and updates evidence_count.

create or replace function public.collect_prediction_evidence_refs_v1(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_signal_types text[],
  p_max_refs integer default 3
)
returns text[]
language sql
stable
security definer
set search_path = public
as $$
  with filtered as (
    select s.signal_id::text as signal_id,
           s.occurred_at
    from public.signals s
    where s.tenant_id = p_tenant_id
      and s.buyer_entity_id = p_buyer_entity_id
      and public.normalize_prediction_region(s.region, s.source) = coalesce(public.normalize_prediction_region(p_region, null), 'IE')
      and (
        (coalesce(p_cpv_cluster_id, 'cluster_unknown') = 'cluster_unknown' and (s.cpv_cluster_id is null or s.cpv_cluster_id = 'cluster_unknown'))
        or s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown')
      )
      and s.signal_type = any(p_signal_types)
    order by s.occurred_at desc
    limit greatest(1, coalesce(p_max_refs, 3))
  )
  select coalesce(array_agg(signal_id), '{}'::text[]) from filtered;
$$;

create or replace function public.attach_prediction_evidence_v1(
  p_tenant_id text,
  p_prediction_id uuid,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_notice_refs text[] := '{}'::text[];
  v_award_refs text[] := '{}'::text[];
  v_external_refs text[] := '{}'::text[];
  v_cycle_refs text[] := '{}'::text[];
  v_driver_count integer := 0;
  v_evidence_count integer := 0;
begin
  if p_prediction_id is null then
    return;
  end if;

  v_notice_refs := public.collect_prediction_evidence_refs_v1(
    p_tenant_id,
    p_buyer_entity_id,
    p_cpv_cluster_id,
    p_region,
    array['notice_published','framework_established','amendment'],
    3
  );

  v_award_refs := public.collect_prediction_evidence_refs_v1(
    p_tenant_id,
    p_buyer_entity_id,
    p_cpv_cluster_id,
    p_region,
    array['award_published'],
    3
  );

  v_external_refs := public.collect_prediction_evidence_refs_v1(
    p_tenant_id,
    p_buyer_entity_id,
    p_cpv_cluster_id,
    p_region,
    array[
      'job_posting','hiring_spike','role_category_detected',
      'budget_approval','budget_increase','grant_awarded','eu_funding_allocation',
      'digital_transformation_program','compliance_mandate','accessibility_regulation','sustainability_requirement','policy_signal'
    ],
    3
  );

  v_cycle_refs := coalesce(
    array(
      select distinct ref
      from unnest(array_cat(coalesce(v_notice_refs, '{}'::text[]), coalesce(v_award_refs, '{}'::text[]))) as ref
      where ref is not null and btrim(ref) <> ''
      order by ref
    ),
    '{}'::text[]
  );
  v_notice_refs := coalesce(
    array(
      select distinct ref
      from unnest(coalesce(v_notice_refs, '{}'::text[])) as ref
      where ref is not null and btrim(ref) <> ''
      order by ref
    ),
    '{}'::text[]
  );
  v_external_refs := coalesce(
    array(
      select distinct ref
      from unnest(coalesce(v_external_refs, '{}'::text[])) as ref
      where ref is not null and btrim(ref) <> ''
      order by ref
    ),
    '{}'::text[]
  );

  update public.prediction_drivers
  set evidence_refs = v_cycle_refs
  where tenant_id = p_tenant_id
    and prediction_id = p_prediction_id
    and driver_type = 'cycle_rhythm';

  update public.prediction_drivers
  set evidence_refs = coalesce(v_external_refs, '{}'::text[])
  where tenant_id = p_tenant_id
    and prediction_id = p_prediction_id
    and driver_type = 'external_signal';

  update public.prediction_drivers
  set evidence_refs = coalesce(v_notice_refs, '{}'::text[])
  where tenant_id = p_tenant_id
    and prediction_id = p_prediction_id
    and driver_type in ('cadence_pattern', 'coverage_penalty');

  select
    count(*),
    coalesce((
      select count(*)
      from (
        select distinct ref
        from public.prediction_drivers pd2
        cross join lateral unnest(coalesce(pd2.evidence_refs, '{}'::text[])) as ref
        where pd2.tenant_id = p_tenant_id
          and pd2.prediction_id = p_prediction_id
          and ref is not null
          and btrim(ref) <> ''
      ) dedup
    ), 0)
    into v_driver_count, v_evidence_count
  from public.prediction_drivers
  where tenant_id = p_tenant_id
    and prediction_id = p_prediction_id;

  update public.predictions_current
  set
    drivers_count = v_driver_count,
    evidence_count = v_evidence_count,
    last_computed_at = now()
  where tenant_id = p_tenant_id
    and prediction_id = p_prediction_id;
end;
$$;

-- Update scoped batch runner to attach evidence
create or replace function public.run_predictive_engine_v1_scoped(
  p_tenant_id text,
  p_scope_table text default 'pg_temp.prediction_pair_scope',
  p_run_type text default 'full',
  p_formula_version text default 'v1.0.0',
  p_max_pairs integer default 5000,
  p_normalize_signals boolean default false
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
  v_scope_count integer := 0;
  v_norm record;
  v_prediction_id uuid;
  p record;
  v_sql text;
begin
  if p_tenant_id is null or trim(p_tenant_id) = '' then
    raise exception 'tenant_id is required';
  end if;

  if to_regclass(p_scope_table) is null then
    raise exception 'scope table % not found', p_scope_table;
  end if;

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
    jsonb_build_object(
      'formula_version', p_formula_version,
      'normalize_signals', p_normalize_signals,
      'scope_table', p_scope_table,
      'max_pairs', p_max_pairs
    )
  )
  returning prediction_runs.run_id into v_run_id;

  begin
    v_sql := format('select count(*) from %s where tenant_id = $1', p_scope_table);
    execute v_sql into v_scope_count using p_tenant_id;

    if p_normalize_signals then
      select *
      into v_norm
      from public.normalize_all_signals(
        p_tenant_id,
        null,
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
            'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb),
            'scope_count', v_scope_count,
            'computed_prediction_rows', 0,
            'skipped_pairs', 0
          )
      where public.prediction_runs.run_id = v_run_id;

      return query select v_run_id, 'success'::text, 0::integer, null::text;
      return;
    end if;

    v_sql := format(
      'select buyer_entity_id,
              coalesce(cpv_cluster_id, ''cluster_unknown'') as cpv_cluster_id,
              coalesce(region, ''IE'') as region,
              last_ingested_at,
              last_occurred_at
         from %s
        where tenant_id = $1
        order by last_ingested_at desc nulls last, last_occurred_at desc nulls last
        limit $2',
      p_scope_table
    );

    for p in execute v_sql using p_tenant_id, p_max_pairs loop
      v_processed := v_processed + 1;
      begin
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
          perform public.attach_prediction_evidence_v1(
            p_tenant_id,
            v_prediction_id,
            p.buyer_entity_id,
            p.cpv_cluster_id,
            p.region
          );
          v_computed := v_computed + 1;
        end if;
      exception when others then
        v_skipped := v_skipped + 1;
        if v_error is null then
          v_error := sqlerrm;
        end if;
      end;
    end loop;

    v_status := case when v_skipped > 0 then 'partial' else 'success' end;

    update public.prediction_runs
    set
      status = v_status,
      pairs_processed = v_processed,
      finished_at = now(),
      metadata_json = coalesce(metadata_json, '{}'::jsonb)
        || jsonb_build_object(
          'computed_prediction_rows', v_computed,
          'skipped_pairs', v_skipped,
          'scope_count', v_scope_count,
          'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb)
        )
    where public.prediction_runs.run_id = v_run_id;

    return query select v_run_id, v_status, v_processed, v_error;
  exception when others then
    v_error := sqlerrm;
    update public.prediction_runs
    set
      status = 'fail',
      finished_at = now(),
      error_message = v_error,
      metadata_json = coalesce(metadata_json, '{}'::jsonb)
        || jsonb_build_object(
          'computed_prediction_rows', v_computed,
          'skipped_pairs', v_skipped,
          'scope_count', v_scope_count,
          'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb)
        )
    where public.prediction_runs.run_id = v_run_id;

    return query select v_run_id, 'fail'::text, v_processed, v_error;
  end;
end;
$$;

-- Update main runner to attach evidence
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
        perform public.attach_prediction_evidence_v1(
          p_tenant_id,
          v_prediction_id,
          p.buyer_entity_id,
          p.cpv_cluster_id,
          p.region
        );
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
      finished_at = now(),
      error_message = v_error,
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
