-- =============================================================================
-- Civant: Rate limiting on expensive RPCs
-- Migration: 20260301200000_rpc_rate_limiting_v1.sql
-- =============================================================================
--
-- DEPENDS ON: 20260301100000_rate_limit_fn_v1.sql (check_rate_limit function)
--
-- CHANGES:
--   1. get_competitor_intelligence — adds caller auth + tenant ownership check
--      and rate limit (30 calls / 60 s per user). Changed STABLE → VOLATILE.
--   2. get_tenant_predictions — adds rate limit (20 calls / 60 s per user).
--      Changed STABLE → VOLATILE.
--
-- Both functions previously had no per-user throttle. With parallel competitor
-- prefetch (Promise.allSettled, up to ~10 concurrent) a 30/60s window is
-- generous for normal use while blocking abusive polling.
--
-- ROLLBACK:
--   Re-run 20260226_fr_prediction_competitor_integration_v1.sql to restore
--   get_competitor_intelligence without rate limit.
--   Re-run 20260225_company_profiles_predictions_rpc_hardening_v1.sql to
--   restore get_tenant_predictions without rate limit.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. get_competitor_intelligence
--    - Added: caller auth + tenant ownership check (defense-in-depth on top of RLS)
--    - Added: check_rate_limit('get_competitor_intelligence', 30, 60)
--    - Changed: STABLE → VOLATILE (function now writes to api_rate_limits)
--    - Body otherwise identical to 20260226_fr_prediction_competitor_integration_v1.sql
-- ---------------------------------------------------------------------------
drop function if exists public.get_competitor_intelligence(text, text);

create or replace function public.get_competitor_intelligence(
  p_tenant_id  text,
  p_search_term text
)
returns jsonb
language plpgsql
volatile
set search_path = public
as $$
declare
  v_caller_role   text := coalesce(auth.role(), '');
  v_caller_tenant text;
  v_search        text;
  v_result        jsonb;
  v_summary       jsonb;
  v_contracts     jsonb;
  v_renewals      jsonb;
  v_buyers        jsonb;
  v_categories    jsonb;
  v_trend         jsonb;
  v_award_count   int;
begin
  -- -------------------------------------------------------------------------
  -- Auth + tenant ownership (skip for service_role pipeline calls)
  -- -------------------------------------------------------------------------
  if v_caller_role is distinct from 'service_role' then
    if auth.uid() is null then
      raise exception 'authenticated user required' using errcode = 'P0001';
    end if;

    select lower(u.tenant_id)
      into v_caller_tenant
    from public.users u
    where u.id = auth.uid()::text;

    if v_caller_tenant is null then
      raise exception 'no tenant mapping for authenticated user' using errcode = 'P0001';
    end if;

    if v_caller_tenant <> lower(trim(coalesce(p_tenant_id, ''))) then
      raise exception 'cross-tenant access denied' using errcode = '42501';
    end if;
  end if;

  -- -------------------------------------------------------------------------
  -- Rate limit: 30 calls per 60 seconds per user
  -- -------------------------------------------------------------------------
  perform public.check_rate_limit('get_competitor_intelligence', 30, 60);

  -- -------------------------------------------------------------------------
  -- Main logic (unchanged from 20260226_fr_prediction_competitor_integration)
  -- -------------------------------------------------------------------------
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
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
  )
  select jsonb_build_object(
    'total_awards',           count(*),
    'total_value_eur',        round(coalesce(sum(award_value_eur), 0)::numeric, 0),
    'distinct_buyers',        count(distinct buyer_id),
    'distinct_clusters',      count(distinct cpv_cluster) filter (where cpv_cluster is not null),
    'active_contracts',       count(*) filter (where end_date > now()),
    'expiring_3m',            count(*) filter (where end_date between now() and now() + interval '3 months'),
    'expiring_6m',            count(*) filter (where end_date between now() and now() + interval '6 months'),
    'expiring_12m',           count(*) filter (where end_date between now() and now() + interval '12 months'),
    'first_award',            min(award_date),
    'last_award',             max(award_date),
    'years_active',           extract(year from age(max(award_date), min(award_date)))::int + 1,
    'has_frameworks',         count(*) filter (where framework_flag = true),
    'avg_contract_value_eur', round(coalesce(avg(award_value_eur) filter (where award_value_eur > 0), 0)::numeric, 0),
    'max_contract_value_eur', round(coalesce(max(award_value_eur), 0)::numeric, 0),
    'countries',              jsonb_build_object(
                                'IE', count(*) filter (where country = 'IE'),
                                'FR', count(*) filter (where country = 'FR')
                              )
  ) into v_summary
  from all_awards;

  with all_awards as (
    select
      'IE'::text as country,
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
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
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
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
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
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
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
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
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id
      and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select
      'FR'::text as country,
      tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
      supplier_name_raw, award_value_eur, award_date, end_date,
      cpv_cluster, cpv_primary, duration_months, framework_flag, procedure_type
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
    'success',              true,
    'search_term',          p_search_term,
    'summary',              v_summary,
    'renewal_opportunities', v_renewals,
    'buyer_relationships',  v_buyers,
    'category_breakdown',   v_categories,
    'yearly_trend',         v_trend,
    'recent_contracts',     v_contracts
  );

  return v_result;
end;
$$;

revoke all on function public.get_competitor_intelligence(text, text) from public;
grant execute on function public.get_competitor_intelligence(text, text) to authenticated;
grant execute on function public.get_competitor_intelligence(text, text) to service_role;

-- ---------------------------------------------------------------------------
-- 2. get_tenant_predictions
--    - Added: check_rate_limit('get_tenant_predictions', 20, 60)
--    - Changed: STABLE → VOLATILE (function now writes to api_rate_limits)
--    - Body otherwise identical to 20260225_company_profiles_predictions_rpc_hardening_v1.sql
-- ---------------------------------------------------------------------------
drop function if exists public.get_tenant_predictions(text);

create or replace function public.get_tenant_predictions(p_tenant_id text)
returns table (
  id                   text,
  buyer_id             text,
  category             text,
  cpv_family           text,
  time_window          text,
  probability          numeric,
  confidence           numeric,
  confidence_breakdown jsonb,
  top_drivers          jsonb,
  evidence             jsonb,
  model_version        text,
  generated_at         timestamptz,
  tenant_id            text,
  predicted_tender_date date,
  signal_type          text,
  renewal_source       jsonb,
  urgency              text,
  buyer_name           text,
  country              text,
  total_value_eur      numeric
)
language plpgsql
volatile
security definer
set search_path = public
as $$
declare
  v_req_tenant   text := lower(nullif(trim(coalesce(p_tenant_id, '')), ''));
  v_caller_role  text := coalesce(auth.role(), nullif(current_setting('request.jwt.claim.role', true), ''));
  v_caller_tenant text;
begin
  if v_req_tenant is null then
    raise exception 'tenant_id is required';
  end if;

  if v_caller_role is distinct from 'service_role' then
    if auth.uid() is null then
      raise exception 'authenticated user required';
    end if;

    select lower(u.tenant_id)
      into v_caller_tenant
    from public.users u
    where u.id = auth.uid()::text;

    if v_caller_tenant is null then
      raise exception 'no tenant mapping for authenticated user';
    end if;

    if v_caller_tenant <> v_req_tenant then
      raise exception 'cross-tenant access denied';
    end if;

    -- Rate limit: 20 calls per 60 seconds per authenticated user
    perform public.check_rate_limit('get_tenant_predictions', 20, 60);
  end if;

  return query
  select
    p.id,
    p.buyer_id,
    p.category,
    p.cpv_family,
    p.time_window,
    p.probability,
    p.confidence,
    p.confidence_breakdown,
    p.top_drivers,
    p.evidence,
    p.model_version,
    p.generated_at,
    p.tenant_id,
    p.predicted_tender_date,
    p.signal_type,
    p.renewal_source,
    p.urgency,
    p.buyer_name,
    p.country,
    p.total_value_eur
  from public.predictions p
  where p.tenant_id = v_req_tenant
  order by
    case lower(coalesce(p.urgency, ''))
      when 'overdue'  then 0
      when 'imminent' then 1
      when 'upcoming' then 2
      when 'horizon'  then 3
      when 'distant'  then 4
      else 5
    end,
    p.predicted_tender_date asc nulls last,
    p.generated_at desc
  limit 500;
end;
$$;

revoke all on function public.get_tenant_predictions(text) from public;
grant execute on function public.get_tenant_predictions(text) to authenticated;
grant execute on function public.get_tenant_predictions(text) to service_role;
