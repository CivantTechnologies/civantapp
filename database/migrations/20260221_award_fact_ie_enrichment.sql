-- =============================================================================
-- Civant: Award Fact IE Data Enrichment
-- Migration: 20260221_award_fact_ie_enrichment.sql
-- =============================================================================
--
-- PURPOSE: Enrich award_fact_ie with two computed fields:
--   1) cpv_cluster — mapped from cpv_primary using cpv_cluster_map prefix lookup
--   2) end_date — computed from award_date + duration_months
--
-- SCOPE: tenant_id = 'civant_default', award_fact_ie only
-- SAFETY: Only fills NULL/empty values. Idempotent.
--
-- ROLLBACK:
--   UPDATE award_fact_ie SET cpv_cluster = NULL WHERE tenant_id = 'civant_default';
--   UPDATE award_fact_ie SET end_date = NULL WHERE tenant_id = 'civant_default';
-- =============================================================================

-- =========================================================================
-- STEP 1: Fill cpv_cluster from cpv_primary using prefix map
-- =========================================================================
-- Match the first 2 digits of cpv_primary against cpv_cluster_map.
-- If multiple clusters match (shouldn't happen with 2-digit prefix), pick highest weight.

UPDATE award_fact_ie a
SET cpv_cluster = sub.cpv_cluster_id
FROM (
  SELECT DISTINCT ON (a2.award_canonical_id)
    a2.award_canonical_id,
    a2.tenant_id,
    m.cpv_cluster_id
  FROM award_fact_ie a2
  JOIN cpv_cluster_map m
    ON m.tenant_id = a2.tenant_id
   AND m.cpv_prefix = left(a2.cpv_primary, 2)
  WHERE a2.tenant_id = 'civant_default'
    AND a2.cpv_primary IS NOT NULL
    AND length(trim(a2.cpv_primary)) >= 2
    AND (a2.cpv_cluster IS NULL OR a2.cpv_cluster = '')
  ORDER BY a2.award_canonical_id, m.weight DESC
) sub
WHERE a.award_canonical_id = sub.award_canonical_id
  AND a.tenant_id = sub.tenant_id;

-- =========================================================================
-- STEP 2: Compute end_date from award_date + duration_months
-- =========================================================================
-- Only where both award_date and duration_months exist and end_date is null.

UPDATE award_fact_ie
SET end_date = award_date + (duration_months || ' months')::interval
WHERE tenant_id = 'civant_default'
  AND award_date IS NOT NULL
  AND duration_months IS NOT NULL
  AND duration_months > 0
  AND end_date IS NULL;

-- =========================================================================
-- VALIDATION QUERIES
-- =========================================================================

-- 1) CPV cluster coverage after enrichment:
-- SELECT
--   count(*) as total,
--   count(*) FILTER (WHERE cpv_cluster IS NOT NULL AND cpv_cluster != '') as has_cluster,
--   count(*) FILTER (WHERE cpv_cluster IS NULL OR cpv_cluster = '') as no_cluster
-- FROM award_fact_ie WHERE tenant_id = 'civant_default';

-- 2) End date coverage after enrichment:
-- SELECT
--   count(*) as total,
--   count(*) FILTER (WHERE end_date IS NOT NULL) as has_end_date,
--   count(*) FILTER (WHERE duration_months IS NOT NULL AND duration_months > 0) as has_duration,
--   count(*) FILTER (WHERE end_date IS NOT NULL AND end_date > now()) as still_active
-- FROM award_fact_ie WHERE tenant_id = 'civant_default';

-- 3) Cluster distribution:
-- SELECT cpv_cluster, count(*) as cnt
-- FROM award_fact_ie WHERE tenant_id = 'civant_default' AND cpv_cluster IS NOT NULL
-- GROUP BY cpv_cluster ORDER BY cnt DESC;

-- 4) Contracts expiring in next 12 months (renewal opportunities):
-- SELECT
--   left(supplier_name_raw, 60) as supplier,
--   buyer_id,
--   award_value_eur,
--   end_date,
--   cpv_cluster
-- FROM award_fact_ie
-- WHERE tenant_id = 'civant_default'
--   AND end_date BETWEEN now() AND now() + interval '12 months'
--   AND supplier_name_raw IS NOT NULL AND trim(supplier_name_raw) != ''
-- ORDER BY award_value_eur DESC NULLS LAST
-- LIMIT 15;
