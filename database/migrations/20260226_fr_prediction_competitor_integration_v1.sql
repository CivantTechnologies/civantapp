-- Migration: 20260226_fr_prediction_competitor_integration_v1.sql
-- Goal:
--   1) Include FR awards in competitor intelligence output.
--   2) Include FR awards in renewal signal aggregation.
--   3) Feed FR end-date-derived expiry events into public.signals for predictive scoring.
--
-- Safety:
--   - Additive/replace-only function changes.
--   - Tenant-scoped reads and writes.
--   - Replay-safe via CREATE OR REPLACE.
--
-- Rollback:
--   - Re-apply prior function versions:
--       public.get_competitor_intelligence(text, text)
--       public.get_renewal_signals(int, numeric)
--       public.normalize_all_signals(text, timestamptz, integer)
--   - drop function if exists public.sync_fr_award_expiry_signals(text, timestamptz, integer);

begin;

drop function if exists get_renewal_signals(integer, numeric);
drop function if exists get_renewal_signals(integer, numeric, text);

create or replace function get_renewal_signals(
    p_months_ahead int default 18,
    p_min_value_eur numeric default 0,
    p_tenant_id text default null
)
returns jsonb
language plpgsql
security definer
set statement_timeout = '60s'
as $fn$
declare
    v_result jsonb;
    v_cutoff_date date := current_date + (p_months_ahead || ' months')::interval;
    v_now date := current_date;
    v_tenant_id text := coalesce(nullif(trim(p_tenant_id), ''), public.current_tenant_id(), 'civant_default');
begin

with all_awards as (
    select supplier_name_raw, buyer_id, buyer_name_raw, cpv_cluster,
           'IE' as country, award_value_eur, start_date, end_date,
           duration_months, coalesce(framework_flag, false) as framework_flag
    from award_fact_ie
    where tenant_id = v_tenant_id
      and end_date is not null
    union all
    select supplier_name_raw, buyer_id, buyer_name_raw, cpv_cluster,
           'FR' as country, award_value_eur, start_date, end_date,
           duration_months, coalesce(framework_flag, false)
    from award_fact_fr
    where tenant_id = v_tenant_id
      and end_date is not null
    union all
    select supplier_name_raw, buyer_id, buyer_name_raw, cpv_cluster,
           'ES' as country, award_value_eur, start_date, end_date,
           duration_months, coalesce(framework_flag, false)
    from award_fact_es
    where tenant_id = v_tenant_id
      and end_date is not null
),
expiring as (
    select * from all_awards
    where end_date between v_now and v_cutoff_date
      and (p_min_value_eur = 0 or coalesce(award_value_eur, 0) >= p_min_value_eur)
),
bcc as (
    select
        buyer_id,
        coalesce(buyer_name_raw, buyer_id) as buyer_name,
        cpv_cluster,
        country,
        count(*) as total_contracts,
        count(*) filter (where end_date between v_now and v_cutoff_date) as expiring_count,
        avg(duration_months) filter (where duration_months > 0) as avg_dur,
        stddev(duration_months) filter (where duration_months > 0) as std_dur,
        max(end_date) filter (where end_date between v_now and v_cutoff_date) as latest_end_date,
        sum(award_value_eur) as total_val,
        avg(award_value_eur) filter (where award_value_eur > 0) as avg_val,
        count(distinct supplier_name_raw) as dist_suppliers,
        array_agg(distinct supplier_name_raw order by supplier_name_raw)
            filter (where supplier_name_raw is not null) as suppliers,
        bool_or(framework_flag) as has_fw
    from all_awards
    where buyer_id is not null and cpv_cluster is not null
    group by buyer_id, buyer_name_raw, cpv_cluster, country
    having count(*) filter (where end_date between v_now and v_cutoff_date) > 0
),
rs as (
    select *,
        case when has_fw then latest_end_date - interval '9 months'
             else latest_end_date - interval '4 months' end as pred_date,
        case when has_fw then 'framework_expiry'
             when total_contracts >= 3 and std_dur is not null and std_dur < 6 then 'repeat_buyer_cycle'
             else 'contract_renewal' end as sig_type,
        case when has_fw and total_contracts >= 2 then 0.92
             when total_contracts >= 3 and std_dur is not null and std_dur < 6 then 0.88
             when total_contracts >= 2 then 0.75
             else 0.62 end as confidence,
        (latest_end_date - v_now) as days_exp,
        case when (latest_end_date - v_now) < 90 then 'imminent'
             when (latest_end_date - v_now) < 180 then 'upcoming'
             when (latest_end_date - v_now) < 365 then 'horizon'
             else 'distant' end as urgency
    from bcc
),
ed as (
    select e.buyer_id, e.cpv_cluster, e.country,
        jsonb_agg(jsonb_build_object(
            'supplier', e.supplier_name_raw,
            'value_eur', e.award_value_eur,
            'start_date', e.start_date,
            'end_date', e.end_date,
            'duration_months', e.duration_months,
            'is_framework', e.framework_flag
        ) order by e.end_date) as contracts
    from expiring e
    group by e.buyer_id, e.cpv_cluster, e.country
)
select jsonb_build_object(
    'success', true,
    'tenant_id', v_tenant_id,
    'generated_at', now(),
    'window_months', p_months_ahead,
    'total_signals', (select count(*) from rs),
    'signals', coalesce((
        select jsonb_agg(s order by (s->>'confidence')::numeric desc, (s->>'days_until_expiry')::int asc)
        from (
            select jsonb_build_object(
                'signal_type', r.sig_type,
                'buyer_id', r.buyer_id,
                'buyer_name', r.buyer_name,
                'cpv_cluster', r.cpv_cluster,
                'country', r.country,
                'confidence', r.confidence,
                'urgency', r.urgency,
                'days_until_expiry', r.days_exp,
                'predicted_tender_date', r.pred_date,
                'latest_contract_end', r.latest_end_date,
                'total_contracts', r.total_contracts,
                'expiring_count', r.expiring_count,
                'avg_duration_months', round(coalesce(r.avg_dur, 0)::numeric, 1),
                'total_value_eur', round(coalesce(r.total_val, 0)::numeric, 2),
                'avg_value_eur', round(coalesce(r.avg_val, 0)::numeric, 2),
                'distinct_suppliers', r.dist_suppliers,
                'incumbent_suppliers', r.suppliers,
                'has_frameworks', r.has_fw,
                'expiring_contracts', e.contracts
            ) as s
            from rs r
            left join ed e on e.buyer_id = r.buyer_id
                and e.cpv_cluster = r.cpv_cluster
                and e.country = r.country
            limit 5000
        ) sub
    ), '[]'::jsonb),
    'summary', jsonb_build_object(
        'imminent', (select count(*) from rs where urgency = 'imminent'),
        'upcoming', (select count(*) from rs where urgency = 'upcoming'),
        'horizon', (select count(*) from rs where urgency = 'horizon'),
        'distant', (select count(*) from rs where urgency = 'distant'),
        'framework_expiries', (select count(*) from rs where sig_type = 'framework_expiry'),
        'repeat_cycles', (select count(*) from rs where sig_type = 'repeat_buyer_cycle'),
        'contract_renewals', (select count(*) from rs where sig_type = 'contract_renewal'),
        'total_value_at_risk', (select round(coalesce(sum(total_val), 0)::numeric, 2) from rs),
        'avg_confidence', (select round(avg(confidence)::numeric, 3) from rs),
        'by_country', jsonb_build_object(
            'IE', (select count(*) from rs where country = 'IE'),
            'FR', (select count(*) from rs where country = 'FR'),
            'ES', (select count(*) from rs where country = 'ES')
        )
    )
) into v_result;

return v_result;
end;
$fn$;

create or replace function get_competitor_intelligence(
  p_tenant_id text,
  p_search_term text
)
returns jsonb
language plpgsql
stable
as $$
declare
  v_search text;
  v_result jsonb;
  v_summary jsonb;
  v_contracts jsonb;
  v_renewals jsonb;
  v_buyers jsonb;
  v_categories jsonb;
  v_trend jsonb;
  v_award_count int;
begin
  v_search := '%' || lower(trim(p_search_term)) || '%';

  select count(*) into v_award_count
  from (
    select supplier_name_raw
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select supplier_name_raw
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  ) s;

  if v_award_count = 0 then
    return jsonb_build_object(
      'success', false,
      'message', 'No awards found for "' || p_search_term || '"',
      'search_term', p_search_term
    );
  end if;

  with all_awards as (
    select
      'IE'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  )
  select jsonb_build_object(
    'total_awards', count(*),
    'total_value_eur', round(coalesce(sum(award_value_eur), 0)::numeric, 0),
    'distinct_buyers', count(distinct buyer_id),
    'distinct_clusters', count(distinct cpv_cluster) filter (where cpv_cluster is not null),
    'active_contracts', count(*) filter (where end_date > now()),
    'expiring_3m', count(*) filter (where end_date between now() and now() + interval '3 months'),
    'expiring_6m', count(*) filter (where end_date between now() and now() + interval '6 months'),
    'expiring_12m', count(*) filter (where end_date between now() and now() + interval '12 months'),
    'first_award', min(award_date),
    'last_award', max(award_date),
    'years_active', extract(year from age(max(award_date), min(award_date)))::int + 1,
    'has_frameworks', count(*) filter (where framework_flag = true),
    'avg_contract_value_eur', round(coalesce(avg(award_value_eur) filter (where award_value_eur > 0), 0)::numeric, 0),
    'max_contract_value_eur', round(coalesce(max(award_value_eur), 0)::numeric, 0),
    'countries', jsonb_build_object(
      'IE', count(*) filter (where country = 'IE'),
      'FR', count(*) filter (where country = 'FR')
    )
  ) into v_summary
  from all_awards;

  with all_awards as (
    select
      'IE'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  )
  select coalesce(jsonb_agg(row_to_json(r)::jsonb order by r.end_date asc), '[]'::jsonb)
  into v_renewals
  from (
    select
      a.country,
      a.buyer_id,
      coalesce(ct.buyer_name_raw, a.buyer_name_raw, a.buyer_id) as buyer_name,
      round(a.award_value_eur::numeric, 0) as value_eur,
      a.award_date,
      a.end_date,
      a.cpv_cluster,
      a.duration_months,
      a.framework_flag,
      a.procedure_type,
      (
        select count(*)
        from (
          select buyer_id, supplier_name_raw from award_fact_ie where tenant_id = p_tenant_id
          union all
          select buyer_id, supplier_name_raw from award_fact_fr where tenant_id = p_tenant_id
        ) a2
        where a2.buyer_id = a.buyer_id
          and lower(coalesce(a2.supplier_name_raw, '')) like v_search
      ) as repeat_wins,
      case
        when a.end_date <= now() + interval '3 months' then 'imminent'
        when a.end_date <= now() + interval '6 months' then 'upcoming'
        else 'horizon'
      end as window_class,
      (a.end_date - current_date) as days_until_expiry
    from all_awards a
    left join lateral (
      select ct2.buyer_name_raw
      from canonical_tenders ct2
      where ct2.canonical_id = a.award_canonical_id
        and ct2.tenant_id = a.tenant_id
      limit 1
    ) ct on true
    where a.end_date between now() and now() + interval '12 months'
      and a.award_value_eur is not null
    order by a.end_date asc
    limit 50
  ) r;

  with all_awards as (
    select
      'IE'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  )
  select coalesce(jsonb_agg(row_to_json(b)::jsonb order by b.total_value desc), '[]'::jsonb)
  into v_buyers
  from (
    select
      a.buyer_id,
      coalesce(max(ct.buyer_name_raw), max(a.buyer_name_raw), a.buyer_id) as buyer_name,
      count(*) as award_count,
      round(sum(a.award_value_eur)::numeric, 0) as total_value,
      min(a.award_date) as first_award,
      max(a.award_date) as last_award,
      count(*) filter (where a.end_date > now()) as active_contracts,
      case
        when count(*) >= 5 then 'strong'
        when count(*) >= 3 then 'moderate'
        else 'emerging'
      end as relationship_strength
    from all_awards a
    left join canonical_tenders ct
      on ct.tenant_id = a.tenant_id
     and ct.canonical_id = a.award_canonical_id
    group by a.buyer_id
    order by sum(a.award_value_eur) desc nulls last
    limit 20
  ) b;

  with all_awards as (
    select
      'IE'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  )
  select coalesce(jsonb_agg(row_to_json(c)::jsonb order by c.total_value desc), '[]'::jsonb)
  into v_categories
  from (
    select
      coalesce(cpv_cluster, 'Unknown') as cluster,
      count(*) as award_count,
      round(sum(award_value_eur)::numeric, 0) as total_value,
      count(distinct buyer_id) as distinct_buyers,
      count(*) filter (where end_date > now()) as active_contracts
    from all_awards
    group by coalesce(cpv_cluster, 'Unknown')
    order by sum(award_value_eur) desc nulls last
  ) c;

  with all_awards as (
    select
      'IE'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  )
  select coalesce(jsonb_agg(row_to_json(t)::jsonb order by t.year asc), '[]'::jsonb)
  into v_trend
  from (
    select
      extract(year from award_date)::int as year,
      count(*) as awards,
      round(sum(award_value_eur)::numeric, 0) as total_value,
      count(distinct buyer_id) as distinct_buyers
    from all_awards
    where award_date is not null
    group by extract(year from award_date)::int
    order by year asc
  ) t;

  with all_awards as (
    select
      'IE'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id,
      award_canonical_id,
      buyer_id,
      buyer_name_raw,
      supplier_name_raw,
      award_value_eur,
      award_date,
      end_date,
      cpv_cluster,
      cpv_primary,
      duration_months,
      framework_flag,
      procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  )
  select coalesce(jsonb_agg(row_to_json(rc)::jsonb), '[]'::jsonb)
  into v_contracts
  from (
    select
      a.country,
      a.buyer_id,
      coalesce(ct.buyer_name_raw, a.buyer_name_raw, a.buyer_id) as buyer_name,
      round(a.award_value_eur::numeric, 0) as value_eur,
      a.award_date,
      a.end_date,
      a.cpv_cluster,
      a.cpv_primary,
      a.duration_months,
      a.framework_flag,
      a.procedure_type,
      left(a.supplier_name_raw, 200) as supplier_raw
    from all_awards a
    left join lateral (
      select ct2.buyer_name_raw
      from canonical_tenders ct2
      where ct2.canonical_id = a.award_canonical_id
        and ct2.tenant_id = a.tenant_id
      limit 1
    ) ct on true
    order by a.award_date desc
    limit 20
  ) rc;

  v_result := jsonb_build_object(
    'success', true,
    'search_term', p_search_term,
    'summary', v_summary,
    'renewal_opportunities', v_renewals,
    'buyer_relationships', v_buyers,
    'category_breakdown', v_categories,
    'yearly_trend', v_trend,
    'recent_contracts', v_contracts
  );

  return v_result;
end;
$$;

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
    with src as (
      select
        af.tenant_id,
        af.award_canonical_id,
        af.award_date,
        af.end_date,
        af.framework_flag,
        af.duration_months,
        af.award_value_eur,
        af.cpv_cluster as cpv_cluster_raw,
        af.procedure_type,
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
        'cluster_unknown'::text as cpv_cluster_id
      from public.award_fact_fr af
      left join public.canonical_tenders ct
        on ct.tenant_id = af.tenant_id
       and ct.canonical_id = af.award_canonical_id
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
          'cpv_cluster_raw', r.cpv_cluster_raw,
          'procedure_type', r.procedure_type,
          'framework_flag', coalesce(r.framework_flag, false),
          'fr_end_date_source', r.framework_evidence->>'fr_end_date_source',
          'fr_end_date_confidence', r.framework_evidence->>'fr_end_date_confidence',
          'fr_end_date_source_rank', r.framework_evidence->>'fr_end_date_source_rank',
          'fr_end_date_source_name', r.framework_evidence->>'fr_end_date_source_name',
          'fr_end_date_batch_id', r.framework_evidence->>'fr_end_date_batch_id',
          'fr_changed_at', to_char(v_changed_at at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
        )
      )
    );

    v_upserted := v_upserted + 1;
  end loop;

  return query select v_processed, v_upserted;
end;
$$;

create or replace function public.normalize_all_signals(
  p_tenant_id text,
  p_since timestamptz default null,
  p_limit integer default 20000
)
returns table (
  procurement_processed integer,
  procurement_upserted integer,
  external_processed integer,
  external_upserted integer,
  unresolved_buyers integer,
  null_cluster_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  p_rec record;
  e_rec record;
  f_rec record;
begin
  select * into p_rec from public.normalize_procurement_signals(p_tenant_id, p_since, p_limit);
  select * into e_rec from public.normalize_external_signals(p_tenant_id, p_since, p_limit);
  select * into f_rec from public.sync_fr_award_expiry_signals(p_tenant_id, p_since, p_limit);

  return query
  select
    coalesce(p_rec.processed_count, 0) + coalesce(f_rec.processed_count, 0),
    coalesce(p_rec.inserted_or_updated, 0) + coalesce(f_rec.inserted_or_updated, 0),
    coalesce(e_rec.processed_count, 0),
    coalesce(e_rec.inserted_or_updated, 0),
    coalesce(p_rec.unresolved_buyers, 0) + coalesce(e_rec.unresolved_buyers, 0),
    coalesce(p_rec.null_cluster_count, 0) + coalesce(e_rec.null_cluster_count, 0);
end;
$$;

commit;
