-- Predictive Engine V1: scoped batch runner helper
-- Enables deterministic batch processing using a temp scope table.

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
        perform public.compute_prediction_for_pair_v1(
          p_tenant_id,
          p.buyer_entity_id,
          p.cpv_cluster_id,
          p.region,
          p_formula_version,
          now()
        );
        v_computed := v_computed + 1;
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
