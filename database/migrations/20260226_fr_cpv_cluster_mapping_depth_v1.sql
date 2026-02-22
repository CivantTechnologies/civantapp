-- Migration: 20260226_fr_cpv_cluster_mapping_depth_v1.sql
-- Goal: reduce remaining FR framework_expiry cluster_unknown rows by adding
--       high-confidence BOAMP descriptor fallback mapping.
--
-- Scope:
--   - Replaces public.sync_fr_award_expiry_signals(text, timestamptz, integer)
--   - Keeps source_event_id stable for in-place upserts.
--
-- Safety:
--   - Tenant-scoped.
--   - Replay-safe function replacement.
--   - Existing resolver remains primary path.
--   - Descriptor fallback is strict:
--       * dominant share >= 90%
--       * support >= 20 rows
--       * split-code fallback requires unanimous inferred cluster
--
-- Rollback:
--   - Re-apply previous function version from:
--       20260226_fr_cpv_cluster_mapping_precision_v1.sql

begin;

create or replace function public.sync_fr_award_expiry_signals(
  p_tenant_id text,
  p_since timestamptz default null,
  p_limit integer default 20000
)
returns table (
  processed_count integer,
  inserted_or_updated integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_processed integer := 0;
  v_upserted integer := 0;
  v_changed_at timestamptz;
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;

  perform public.ensure_prediction_baseline(p_tenant_id);

  for r in
    with descriptor_known as (
      select
        nullif(trim(coalesce(ct.normalized_json->'raw'->>'dc', '')), '') as dc_raw,
        s.cpv_cluster_id
      from public.signals s
      join public.canonical_tenders ct
        on ct.tenant_id = s.tenant_id
       and ct.canonical_id = s.canonical_id
      where s.tenant_id = p_tenant_id
        and s.source = 'AWARD_FACT_FR'
        and s.signal_type = 'framework_expiry'
        and coalesce(s.cpv_cluster_id, '') <> ''
        and s.cpv_cluster_id <> 'cluster_unknown'
        and nullif(trim(coalesce(ct.normalized_json->'raw'->>'dc', '')), '') is not null
    ),
    descriptor_direct_ranked as (
      select
        dc_raw,
        cpv_cluster_id,
        count(*) as n,
        row_number() over (
          partition by dc_raw
          order by count(*) desc, cpv_cluster_id
        ) as rn,
        sum(count(*)) over (partition by dc_raw) as total_n
      from descriptor_known
      group by dc_raw, cpv_cluster_id
    ),
    descriptor_direct_map as (
      select
        dc_raw,
        cpv_cluster_id
      from descriptor_direct_ranked
      where rn = 1
        and total_n >= 20
        and (n::numeric / nullif(total_n, 0)) >= 0.90
    ),
    descriptor_code_ranked as (
      select
        trim(code) as dc_code,
        dk.cpv_cluster_id,
        count(*) as n,
        row_number() over (
          partition by trim(code)
          order by count(*) desc, dk.cpv_cluster_id
        ) as rn,
        sum(count(*)) over (partition by trim(code)) as total_n
      from descriptor_known dk
      cross join lateral regexp_split_to_table(dk.dc_raw, '\s*,\s*') as code
      where trim(code) <> ''
      group by trim(code), dk.cpv_cluster_id
    ),
    descriptor_code_map as (
      select
        dc_code,
        cpv_cluster_id
      from descriptor_code_ranked
      where rn = 1
        and total_n >= 20
        and (n::numeric / nullif(total_n, 0)) >= 0.90
    ),
    descriptor_split_candidates as (
      select
        af2.award_canonical_id,
        dcm.cpv_cluster_id
      from public.award_fact_fr af2
      join public.canonical_tenders ct2
        on ct2.tenant_id = af2.tenant_id
       and ct2.canonical_id = af2.award_canonical_id
      cross join lateral regexp_split_to_table(
        coalesce(ct2.normalized_json->'raw'->>'dc', ''),
        '\s*,\s*'
      ) as code
      join descriptor_code_map dcm
        on dcm.dc_code = trim(code)
      where af2.tenant_id = p_tenant_id
        and af2.award_date is not null
        and af2.end_date is not null
        and trim(code) <> ''
    ),
    descriptor_split_inferred as (
      select
        award_canonical_id,
        case
          when count(distinct cpv_cluster_id) = 1 then min(cpv_cluster_id)
          else null
        end as cpv_cluster_id
      from descriptor_split_candidates
      group by award_canonical_id
    ),
    src as (
      select
        af.tenant_id,
        af.award_canonical_id,
        af.award_date,
        af.end_date,
        af.framework_flag,
        af.duration_months,
        af.award_value_eur,
        af.procedure_type,
        af.cpv_primary,
        af.cpv_cluster as cpv_cluster_raw,
        af.framework_evidence,
        case
          when coalesce(af.framework_evidence->>'fr_end_date_updated_at', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
          then (af.framework_evidence->>'fr_end_date_updated_at')::timestamptz
          else null
        end as fr_updated_at,
        coalesce(
          nullif(ct.buyer_entity_id, ''),
          nullif(af.buyer_id, ''),
          'unresolved:fr:' || split_part(af.award_canonical_id, ':', 2)
        ) as buyer_entity_id,
        coalesce(
          nullif(nullif(resolved.cpv_cluster_id, ''), 'cluster_unknown'),
          nullif(ddm.cpv_cluster_id, ''),
          nullif(dsi.cpv_cluster_id, ''),
          'cluster_unknown'
        ) as cpv_cluster_id,
        case
          when nullif(nullif(resolved.cpv_cluster_id, ''), 'cluster_unknown') is not null
            then 'cpv_resolver'
          when nullif(ddm.cpv_cluster_id, '') is not null then 'boamp_descriptor_direct'
          when nullif(dsi.cpv_cluster_id, '') is not null then 'boamp_descriptor_split_unanimous'
          else 'cluster_unknown'
        end as cpv_cluster_source
      from public.award_fact_fr af
      left join public.canonical_tenders ct
        on ct.tenant_id = af.tenant_id
       and ct.canonical_id = af.award_canonical_id
      left join lateral (
        select r1.cpv_cluster_id
        from public.resolve_cpv_cluster_id(
          af.tenant_id,
          case
            when nullif(af.cpv_primary, '') is null then null
            else array[af.cpv_primary]
          end,
          coalesce(af.cpv_cluster, '')
        ) r1
      ) resolved on true
      left join descriptor_direct_map ddm
        on ddm.dc_raw = nullif(trim(coalesce(ct.normalized_json->'raw'->>'dc', '')), '')
      left join descriptor_split_inferred dsi
        on dsi.award_canonical_id = af.award_canonical_id
      where af.tenant_id = p_tenant_id
        and af.award_date is not null
        and af.end_date is not null
    )
    select *
    from src s
    where (
      p_since is null
      or coalesce(s.fr_updated_at, s.award_date::timestamptz, s.end_date::timestamptz) >= p_since
      or not exists (
        select 1
        from public.signals sig
        where sig.tenant_id = p_tenant_id
          and sig.source = 'AWARD_FACT_FR'
          and sig.source_event_id = s.award_canonical_id || ':fr_expiry'
          and sig.signal_type = 'framework_expiry'
      )
    )
    order by coalesce(s.fr_updated_at, s.award_date::timestamptz, s.end_date::timestamptz) asc
    limit greatest(1, p_limit)
  loop
    v_processed := v_processed + 1;
    v_changed_at := coalesce(
      case
        when coalesce(r.framework_evidence->>'fr_end_date_updated_at', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T'
        then (r.framework_evidence->>'fr_end_date_updated_at')::timestamptz
        else null
      end,
      now()
    );

    perform public.upsert_signal_event(
      p_tenant_id,
      r.buyer_entity_id,
      coalesce(r.cpv_cluster_id, 'cluster_unknown'),
      'FR',
      'framework_expiry',
      case when coalesce(r.framework_flag, false) then 0.95 else 0.78 end,
      r.end_date::timestamptz,
      'AWARD_FACT_FR',
      null,
      r.award_canonical_id,
      r.award_canonical_id || ':fr_expiry',
      jsonb_strip_nulls(
        jsonb_build_object(
          'origin', 'award_fact_fr_end_date',
          'award_canonical_id', r.award_canonical_id,
          'award_date', r.award_date,
          'end_date', r.end_date,
          'duration_months', r.duration_months,
          'award_value_eur', r.award_value_eur,
          'cpv_primary', r.cpv_primary,
          'cpv_cluster_raw', r.cpv_cluster_raw,
          'cpv_cluster_mapped', r.cpv_cluster_id,
          'cpv_cluster_source', r.cpv_cluster_source,
          'procedure_type', r.procedure_type,
          'framework_flag', coalesce(r.framework_flag, false),
          'fr_end_date_source', r.framework_evidence->>'fr_end_date_source',
          'fr_end_date_confidence', r.framework_evidence->>'fr_end_date_confidence',
          'fr_end_date_source_rank', r.framework_evidence->>'fr_end_date_source_rank',
          'fr_end_date_source_name', r.framework_evidence->>'fr_end_date_source_name',
          'fr_end_date_batch_id', r.framework_evidence->>'fr_end_date_batch_id',
          'fr_changed_at', to_char(v_changed_at at time zone 'utc', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
        )
      )
    );

    v_upserted := v_upserted + 1;
  end loop;

  return query select v_processed, v_upserted;
end;
$$;

commit;
