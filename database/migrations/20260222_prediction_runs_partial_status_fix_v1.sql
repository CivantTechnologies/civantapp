-- Align scoped runner status with prediction_runs_status_check (no 'partial')

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

    for p in execute
      format(
        'select tenant_id, buyer_entity_id, cpv_cluster_id, region
         from %s
         where tenant_id = $1
         order by last_ingested_at desc nulls last, last_occurred_at desc nulls last, buyer_entity_id, cpv_cluster_id, region
         limit $2',
        p_scope_table
      )
      using p_tenant_id, greatest(1, p_max_pairs)
    loop
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

    v_status := 'success';

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
      status = 'failed',
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

    return query select v_run_id, 'failed'::text, v_processed, v_error;
  end;
end;
$$;
