-- =============================================================================
-- Civant: Update get_competitor_intelligence — optimised v2
-- Migration: 20260301420000_get_competitor_intelligence_v2.sql
-- =============================================================================
--
-- CHANGES:
--   - Short-term matching (<=4 chars) uses word-boundary regex instead of LIKE
--   - Longer terms still use LIKE '%term%'
--   - Includes SET search_path = public (SECURITY DEFINER best practice)
-- =============================================================================

DROP FUNCTION IF EXISTS get_competitor_intelligence(text, text);

CREATE FUNCTION get_competitor_intelligence(p_tenant_id text, p_search_term text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v_result jsonb; v_search text; v_cnt int; v_term text;
BEGIN
  v_term := lower(trim(p_search_term));

  IF length(v_term) <= 4 THEN
    v_search := v_term;
  ELSE
    v_search := '%' || v_term || '%';
  END IF;

  SELECT count(*) INTO v_cnt FROM (
    SELECT 1 FROM award_fact_ie WHERE tenant_id=p_tenant_id
      AND CASE WHEN length(v_term) <= 4
        THEN lower(supplier_name_raw) ~ ('(^|[\s,|/\-])' || v_term || '($|[\s,|/\-])')
        ELSE lower(supplier_name_raw) LIKE v_search END
    UNION ALL
    SELECT 1 FROM award_fact_fr WHERE tenant_id=p_tenant_id
      AND CASE WHEN length(v_term) <= 4
        THEN lower(supplier_name_raw) ~ ('(^|[\s,|/\-])' || v_term || '($|[\s,|/\-])')
        ELSE lower(supplier_name_raw) LIKE v_search END
    UNION ALL
    SELECT 1 FROM award_fact_es WHERE tenant_id=p_tenant_id
      AND CASE WHEN length(v_term) <= 4
        THEN lower(supplier_name_raw) ~ ('(^|[\s,|/\-])' || v_term || '($|[\s,|/\-])')
        ELSE lower(supplier_name_raw) LIKE v_search END
  ) combined;

  IF v_cnt=0 THEN RETURN jsonb_build_object('success',false,'message','No awards found for: '||p_search_term); END IF;

  WITH base AS (
    SELECT a.tenant_id, a.source_system, a.award_canonical_id, a.buyer_id, a.supplier_id,
           a.award_date, a.award_value_eur, a.cpv_primary, a.cpv_cluster, a.procedure_type,
           a.framework_flag, a.duration_months, a.start_date, a.end_date,
           a.supplier_name_raw, 'IE' as country,
           coalesce(ct.buyer_name_raw, a.buyer_id) as buyer_name
    FROM award_fact_ie a
    LEFT JOIN canonical_tenders ct ON ct.canonical_id=a.award_canonical_id AND ct.tenant_id=a.tenant_id
    WHERE a.tenant_id=p_tenant_id
      AND CASE WHEN length(v_term) <= 4
        THEN lower(a.supplier_name_raw) ~ ('(^|[\s,|/\-])' || v_term || '($|[\s,|/\-])')
        ELSE lower(a.supplier_name_raw) LIKE v_search END
    UNION ALL
    SELECT a.tenant_id, a.source_system, a.award_canonical_id, a.buyer_id, a.supplier_id,
           a.award_date, a.award_value_eur, a.cpv_primary, a.cpv_cluster, a.procedure_type,
           a.framework_flag, a.duration_months, a.start_date, a.end_date,
           a.supplier_name_raw, 'FR' as country,
           coalesce(a.buyer_name_raw, a.buyer_id) as buyer_name
    FROM award_fact_fr a
    WHERE a.tenant_id=p_tenant_id
      AND CASE WHEN length(v_term) <= 4
        THEN lower(a.supplier_name_raw) ~ ('(^|[\s,|/\-])' || v_term || '($|[\s,|/\-])')
        ELSE lower(a.supplier_name_raw) LIKE v_search END
    UNION ALL
    SELECT a.tenant_id, a.source_system, a.award_canonical_id, a.buyer_id, a.supplier_id,
           a.award_date, a.award_value_eur, a.cpv_primary, a.cpv_cluster, a.procedure_type,
           a.framework_flag, a.duration_months, a.start_date, a.end_date,
           a.supplier_name_raw, 'ES' as country,
           coalesce(a.buyer_name_raw, a.buyer_id) as buyer_name
    FROM award_fact_es a
    WHERE a.tenant_id=p_tenant_id
      AND CASE WHEN length(v_term) <= 4
        THEN lower(a.supplier_name_raw) ~ ('(^|[\s,|/\-])' || v_term || '($|[\s,|/\-])')
        ELSE lower(a.supplier_name_raw) LIKE v_search END
  ),
  tnames_raw AS (
    SELECT trim(u.nm) as tname, count(*) as cnt
    FROM base b,
    LATERAL unnest(
      CASE WHEN b.supplier_name_raw LIKE '%|%'
        THEN string_to_array(b.supplier_name_raw, '|')
        WHEN b.supplier_name_raw LIKE '%,%'
        THEN string_to_array(b.supplier_name_raw, ',')
        ELSE ARRAY[b.supplier_name_raw]
      END
    ) AS u(nm)
    WHERE CASE WHEN length(v_term) <= 4
      THEN lower(trim(u.nm)) ~ ('(^|[\s,|/\-])' || v_term || '($|[\s,|/\-])')
      ELSE lower(trim(u.nm)) LIKE v_search END
    GROUP BY trim(u.nm)
    ORDER BY count(*) DESC
  ),
  summary AS (
    SELECT jsonb_build_object(
      'total_awards', count(*),
      'total_value_eur', sum(award_value_eur),
      'distinct_buyers', count(DISTINCT buyer_id),
      'active_contracts', count(*) FILTER (WHERE end_date > now()),
      'expiring_3m', count(*) FILTER (WHERE end_date BETWEEN now() AND now()+interval '3 months'),
      'expiring_6m', count(*) FILTER (WHERE end_date BETWEEN now() AND now()+interval '6 months'),
      'expiring_12m', count(*) FILTER (WHERE end_date BETWEEN now() AND now()+interval '12 months'),
      'years_active', GREATEST(1, extract(year FROM age(max(award_date),min(award_date)))::int),
      'has_frameworks', count(*) FILTER (WHERE framework_flag),
      'first_award', min(award_date),
      'last_award', max(award_date),
      'distinct_clusters', count(DISTINCT cpv_cluster),
      'avg_contract_value_eur', CASE WHEN count(*) FILTER (WHERE award_value_eur>0)>0
        THEN (sum(award_value_eur) FILTER (WHERE award_value_eur>0) / count(*) FILTER (WHERE award_value_eur>0))::bigint ELSE 0 END,
      'max_contract_value_eur', max(award_value_eur),
      'countries', (SELECT jsonb_agg(DISTINCT country) FROM base)
    ) as data FROM base
  ),
  renewals AS (
    SELECT coalesce(jsonb_agg(r ORDER BY r->>'days_until_expiry'), '[]'::jsonb) as data FROM (
      SELECT jsonb_build_object(
        'buyer_name', buyer_name, 'buyer_id', buyer_id, 'value_eur', award_value_eur,
        'cpv_cluster', cpv_cluster, 'end_date', end_date::text,
        'days_until_expiry', extract(day FROM end_date - now())::int,
        'framework_flag', framework_flag,
        'repeat_wins', (SELECT count(*) FROM base b2 WHERE b2.buyer_id = base.buyer_id),
        'window_class', CASE
          WHEN end_date < now()+interval '3 months' THEN 'imminent'
          WHEN end_date < now()+interval '6 months' THEN 'upcoming'
          ELSE 'horizon' END,
        'country', country
      ) as r FROM base WHERE end_date BETWEEN now() AND now()+interval '12 months'
    ) sub
  ),
  buyers AS (
    SELECT coalesce(jsonb_agg(b ORDER BY b->>'total_value' DESC NULLS LAST), '[]'::jsonb) as data FROM (
      SELECT jsonb_build_object(
        'buyer_name', buyer_name, 'buyer_id', buyer_id,
        'award_count', count(*), 'total_value', sum(award_value_eur),
        'first_award', min(award_date)::text, 'last_award', max(award_date)::text,
        'active_contracts', count(*) FILTER (WHERE end_date > now()),
        'relationship_strength', CASE WHEN count(*)>=3 THEN 'strong' WHEN count(*)>=2 THEN 'moderate' ELSE 'emerging' END,
        'country', (array_agg(DISTINCT country))[1]
      ) as b FROM base GROUP BY buyer_name, buyer_id ORDER BY sum(award_value_eur) DESC NULLS LAST LIMIT 20
    ) sub
  ),
  categories AS (
    SELECT coalesce(jsonb_agg(c ORDER BY c->>'total_value' DESC NULLS LAST), '[]'::jsonb) as data FROM (
      SELECT jsonb_build_object(
        'cluster', cpv_cluster, 'award_count', count(*), 'total_value', sum(award_value_eur),
        'distinct_buyers', count(DISTINCT buyer_id),
        'active_contracts', count(*) FILTER (WHERE end_date > now())
      ) as c FROM base GROUP BY cpv_cluster ORDER BY sum(award_value_eur) DESC NULLS LAST
    ) sub
  ),
  yearly AS (
    SELECT coalesce(jsonb_agg(y ORDER BY y->>'year'), '[]'::jsonb) as data FROM (
      SELECT jsonb_build_object(
        'year', extract(year FROM award_date)::int, 'awards', count(*),
        'total_value', sum(award_value_eur), 'distinct_buyers', count(DISTINCT buyer_id)
      ) as y FROM base WHERE extract(year FROM award_date) >= 2010
      GROUP BY extract(year FROM award_date)::int ORDER BY extract(year FROM award_date)::int
    ) sub
  ),
  recent AS (
    SELECT coalesce(jsonb_agg(r), '[]'::jsonb) as data FROM (
      SELECT jsonb_build_object(
        'buyer_name', buyer_name, 'buyer_id', buyer_id, 'value_eur', award_value_eur,
        'cpv_cluster', cpv_cluster, 'cpv_primary', cpv_primary,
        'award_date', award_date::text,
        'end_date', CASE WHEN end_date IS NOT NULL THEN end_date::text ELSE null END,
        'framework_flag', framework_flag, 'procedure_type', procedure_type,
        'duration_months', duration_months, 'supplier_raw', supplier_name_raw,
        'country', country
      ) as r FROM base ORDER BY award_date DESC LIMIT 20
    ) sub
  ),
  tnames AS (
    SELECT coalesce(jsonb_agg(jsonb_build_object('name', tname, 'award_count', cnt)), '[]'::jsonb) as data
    FROM tnames_raw
  )
  SELECT jsonb_build_object(
    'success', true, 'company_name', p_search_term, 'found_tenders', v_cnt,
    'summary', s.data, 'renewal_opportunities', rn.data, 'buyer_relationships', bu.data,
    'category_breakdown', ca.data, 'yearly_trend', yr.data, 'recent_contracts', rc.data,
    'trading_names', tn.data
  ) INTO v_result
  FROM summary s
  CROSS JOIN renewals rn CROSS JOIN buyers bu CROSS JOIN categories ca
  CROSS JOIN yearly yr CROSS JOIN recent rc CROSS JOIN tnames tn;

  RETURN v_result;
END;
$$;
