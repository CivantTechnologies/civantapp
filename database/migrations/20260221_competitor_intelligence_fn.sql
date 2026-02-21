-- =============================================================================
-- Civant: Competitor Intelligence Database Function
-- Migration: 20260221_competitor_intelligence_fn.sql
-- =============================================================================
--
-- PURPOSE: Single database function that returns all data needed for the
-- competitor intelligence dashboard. Takes a company name search term and
-- returns: summary stats, contract history, renewal opportunities, buyer
-- relationships, category breakdown, and yearly trend.
--
-- USAGE:
--   SELECT * FROM get_competitor_intelligence('civant_default', 'BAM');
--   SELECT * FROM get_competitor_intelligence('civant_default', 'Accenture');
--
-- ROLLBACK:
--   DROP FUNCTION IF EXISTS get_competitor_intelligence(text, text);
--   DROP TYPE IF EXISTS competitor_renewal_opportunity;
--   DROP TYPE IF EXISTS competitor_buyer_relationship;
--   DROP TYPE IF EXISTS competitor_category_breakdown;
--   DROP TYPE IF EXISTS competitor_yearly_trend;
--   DROP TYPE IF EXISTS competitor_contract_record;
-- =============================================================================

-- Drop existing types/function if re-running
DROP FUNCTION IF EXISTS get_competitor_intelligence(text, text);
DROP TYPE IF EXISTS competitor_intelligence_result;

-- =========================================================================
-- Main function: returns JSON with all competitor data
-- =========================================================================
CREATE OR REPLACE FUNCTION get_competitor_intelligence(
  p_tenant_id text,
  p_search_term text
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_search text;
  v_result jsonb;
  v_summary jsonb;
  v_contracts jsonb;
  v_renewals jsonb;
  v_buyers jsonb;
  v_categories jsonb;
  v_trend jsonb;
  v_award_count int;
BEGIN
  -- Normalize search term
  v_search := '%' || lower(trim(p_search_term)) || '%';

  -- Quick check: does this competitor exist?
  SELECT count(*) INTO v_award_count
  FROM award_fact_ie
  WHERE tenant_id = p_tenant_id
    AND lower(supplier_name_raw) LIKE v_search;

  IF v_award_count = 0 THEN
    RETURN jsonb_build_object(
      'success', false,
      'message', 'No awards found for "' || p_search_term || '"',
      'search_term', p_search_term
    );
  END IF;

  -- =====================================================================
  -- SUMMARY STATS
  -- =====================================================================
  SELECT jsonb_build_object(
    'total_awards', count(*),
    'total_value_eur', round(coalesce(sum(award_value_eur), 0)::numeric, 0),
    'distinct_buyers', count(DISTINCT buyer_id),
    'distinct_clusters', count(DISTINCT cpv_cluster) FILTER (WHERE cpv_cluster IS NOT NULL),
    'active_contracts', count(*) FILTER (WHERE end_date > now()),
    'expiring_3m', count(*) FILTER (WHERE end_date BETWEEN now() AND now() + interval '3 months'),
    'expiring_6m', count(*) FILTER (WHERE end_date BETWEEN now() AND now() + interval '6 months'),
    'expiring_12m', count(*) FILTER (WHERE end_date BETWEEN now() AND now() + interval '12 months'),
    'first_award', min(award_date),
    'last_award', max(award_date),
    'years_active', extract(year from age(max(award_date), min(award_date)))::int + 1,
    'has_frameworks', count(*) FILTER (WHERE framework_flag = true),
    'avg_contract_value_eur', round(coalesce(avg(award_value_eur) FILTER (WHERE award_value_eur > 0), 0)::numeric, 0),
    'max_contract_value_eur', round(coalesce(max(award_value_eur), 0)::numeric, 0)
  ) INTO v_summary
  FROM award_fact_ie
  WHERE tenant_id = p_tenant_id
    AND lower(supplier_name_raw) LIKE v_search;

  -- =====================================================================
  -- RENEWAL OPPORTUNITIES (expiring in next 12 months)
  -- =====================================================================
  SELECT coalesce(jsonb_agg(row_to_json(r)::jsonb ORDER BY r.end_date ASC), '[]'::jsonb)
  INTO v_renewals
  FROM (
    SELECT
      a.buyer_id,
      coalesce(ct.buyer_name_raw, a.buyer_id) as buyer_name,
      round(a.award_value_eur::numeric, 0) as value_eur,
      a.award_date,
      a.end_date,
      a.cpv_cluster,
      a.duration_months,
      a.framework_flag,
      a.procedure_type,
      -- Incumbent strength: how many times has this supplier won from this buyer?
      (
        SELECT count(*)
        FROM award_fact_ie a2
        WHERE a2.tenant_id = p_tenant_id
          AND a2.buyer_id = a.buyer_id
          AND lower(a2.supplier_name_raw) LIKE v_search
      ) as repeat_wins,
      -- Opportunity window classification
      CASE
        WHEN a.end_date <= now() + interval '3 months' THEN 'imminent'
        WHEN a.end_date <= now() + interval '6 months' THEN 'upcoming'
        ELSE 'horizon'
      END as window_class,
      -- Days until expiry
      (a.end_date - current_date) as days_until_expiry
    FROM award_fact_ie a
    LEFT JOIN LATERAL (
      SELECT ct2.buyer_name_raw
      FROM canonical_tenders ct2
      WHERE ct2.canonical_id = a.award_canonical_id
        AND ct2.tenant_id = a.tenant_id
      LIMIT 1
    ) ct ON true
    WHERE a.tenant_id = p_tenant_id
      AND lower(a.supplier_name_raw) LIKE v_search
      AND a.end_date BETWEEN now() AND now() + interval '12 months'
      AND a.award_value_eur IS NOT NULL
    ORDER BY a.end_date ASC
    LIMIT 50
  ) r;

  -- =====================================================================
  -- TOP BUYER RELATIONSHIPS
  -- =====================================================================
  SELECT coalesce(jsonb_agg(row_to_json(b)::jsonb ORDER BY b.total_value DESC), '[]'::jsonb)
  INTO v_buyers
  FROM (
    SELECT
      a.buyer_id,
      coalesce(
        (SELECT ct2.buyer_name_raw FROM canonical_tenders ct2 
         WHERE ct2.canonical_id = (
           SELECT a3.award_canonical_id FROM award_fact_ie a3 
           WHERE a3.tenant_id = p_tenant_id AND a3.buyer_id = a.buyer_id 
           LIMIT 1
         ) AND ct2.tenant_id = p_tenant_id LIMIT 1),
        a.buyer_id
      ) as buyer_name,
      count(*) as award_count,
      round(sum(a.award_value_eur)::numeric, 0) as total_value,
      min(a.award_date) as first_award,
      max(a.award_date) as last_award,
      count(*) FILTER (WHERE a.end_date > now()) as active_contracts,
      CASE
        WHEN count(*) >= 5 THEN 'strong'
        WHEN count(*) >= 3 THEN 'moderate'
        ELSE 'emerging'
      END as relationship_strength
    FROM award_fact_ie a
    WHERE a.tenant_id = p_tenant_id
      AND lower(a.supplier_name_raw) LIKE v_search
    GROUP BY a.buyer_id
    ORDER BY sum(a.award_value_eur) DESC NULLS LAST
    LIMIT 20
  ) b;

  -- =====================================================================
  -- CATEGORY BREAKDOWN
  -- =====================================================================
  SELECT coalesce(jsonb_agg(row_to_json(c)::jsonb ORDER BY c.total_value DESC), '[]'::jsonb)
  INTO v_categories
  FROM (
    SELECT
      coalesce(cpv_cluster, 'Unknown') as cluster,
      count(*) as award_count,
      round(sum(award_value_eur)::numeric, 0) as total_value,
      count(DISTINCT buyer_id) as distinct_buyers,
      count(*) FILTER (WHERE end_date > now()) as active_contracts
    FROM award_fact_ie
    WHERE tenant_id = p_tenant_id
      AND lower(supplier_name_raw) LIKE v_search
    GROUP BY coalesce(cpv_cluster, 'Unknown')
    ORDER BY sum(award_value_eur) DESC NULLS LAST
  ) c;

  -- =====================================================================
  -- YEARLY TREND
  -- =====================================================================
  SELECT coalesce(jsonb_agg(row_to_json(t)::jsonb ORDER BY t.year ASC), '[]'::jsonb)
  INTO v_trend
  FROM (
    SELECT
      extract(year from award_date)::int as year,
      count(*) as awards,
      round(sum(award_value_eur)::numeric, 0) as total_value,
      count(DISTINCT buyer_id) as distinct_buyers
    FROM award_fact_ie
    WHERE tenant_id = p_tenant_id
      AND lower(supplier_name_raw) LIKE v_search
      AND award_date IS NOT NULL
    GROUP BY extract(year from award_date)::int
    ORDER BY year ASC
  ) t;

  -- =====================================================================
  -- RECENT CONTRACTS (last 20)
  -- =====================================================================
  SELECT coalesce(jsonb_agg(row_to_json(rc)::jsonb), '[]'::jsonb)
  INTO v_contracts
  FROM (
    SELECT
      a.buyer_id,
      coalesce(ct.buyer_name_raw, a.buyer_id) as buyer_name,
      round(a.award_value_eur::numeric, 0) as value_eur,
      a.award_date,
      a.end_date,
      a.cpv_cluster,
      a.cpv_primary,
      a.duration_months,
      a.framework_flag,
      a.procedure_type,
      left(a.supplier_name_raw, 200) as supplier_raw
    FROM award_fact_ie a
    LEFT JOIN LATERAL (
      SELECT ct2.buyer_name_raw
      FROM canonical_tenders ct2
      WHERE ct2.canonical_id = a.award_canonical_id
        AND ct2.tenant_id = a.tenant_id
      LIMIT 1
    ) ct ON true
    WHERE a.tenant_id = p_tenant_id
      AND lower(a.supplier_name_raw) LIKE v_search
    ORDER BY a.award_date DESC
    LIMIT 20
  ) rc;

  -- =====================================================================
  -- ASSEMBLE RESULT
  -- =====================================================================
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

  RETURN v_result;
END;
$$;

-- =========================================================================
-- VALIDATION
-- =========================================================================

-- Quick test:
-- SELECT get_competitor_intelligence('civant_default', 'BAM');
-- SELECT get_competitor_intelligence('civant_default', 'Accenture');
-- SELECT get_competitor_intelligence('civant_default', 'Sisk');
