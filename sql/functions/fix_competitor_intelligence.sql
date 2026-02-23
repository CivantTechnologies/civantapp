CREATE OR REPLACE FUNCTION public.get_competitor_intelligence(p_tenant_id text, p_search_term text)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE
 SET statement_timeout TO '30s'
AS $function$
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
    union all
    select supplier_name_raw
    from award_fact_es
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

  -- SUMMARY
  with all_awards as (
    select 'IE'::text as country, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'FR'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'ES'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_es
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
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
      'FR', count(*) filter (where country = 'FR'),
      'ES', count(*) filter (where country = 'ES')
    )
  ) into v_summary
  from all_awards;

  -- RENEWALS
  with all_awards as (
    select 'IE'::text as country, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'FR'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'ES'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_es
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
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
          union all
          select buyer_id, supplier_name_raw from award_fact_es where tenant_id = p_tenant_id
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

  -- BUYERS
  with all_awards as (
    select 'IE'::text as country, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'FR'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'ES'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_es
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
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

  -- CATEGORIES
  with all_awards as (
    select 'IE'::text as country, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'FR'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'ES'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_es
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
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

  -- TREND
  with all_awards as (
    select 'IE'::text as country, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'FR'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'ES'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_es
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
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

  -- RECENT CONTRACTS
  with all_awards as (
    select 'IE'::text as country, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_ie
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'FR'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_fr
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
    union all
    select 'ES'::text, tenant_id, award_canonical_id, buyer_id, buyer_name_raw,
           supplier_name_raw, award_value_eur, award_date, end_date, cpv_cluster, cpv_primary,
           duration_months, framework_flag, procedure_type
    from award_fact_es
    where tenant_id = p_tenant_id and lower(coalesce(supplier_name_raw, '')) like v_search
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
$function$;
