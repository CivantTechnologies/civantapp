-- =============================================================================
-- Civant: Grant-awarded dominant cluster mapping (MVP)
-- Migration: 20260223_grant_awarded_dominant_cluster_map_v1.sql
-- =============================================================================
-- Goal:
--   Map grant_awarded signals with NULL/cluster_unknown CPV to a deterministic,
--   explainable dominant CPV cluster per buyer+region.
--
-- Scope (MVP): tenant_id = 'civant_default' only.
-- - Only signal_type = 'grant_awarded'
-- - Only cpv_cluster_id IS NULL OR cpv_cluster_id = 'cluster_unknown'
-- - Only last 24 months (engine-aligned window)
-- - Region uses normalize_prediction_region(signal.region, signal.source)
-- - Skip unresolved/null buyer ids
--
-- Dominant cluster rule:
-- - Highest tender_count_24m for (tenant_id, buyer_entity_id, region)
-- - Dominant share must be >= 40% of buyer total tenders in that region
-- - Deterministic tie-break: cpv_cluster_id ASC
--
-- Idempotency:
-- - Update target set is only unknown/null CPV grant rows.
-- - Once mapped, rows leave the target set.
--
-- Rollback (best-effort):
-- update public.signals
-- set
--   cpv_cluster_id = 'cluster_unknown',
--   features_json = coalesce(features_json, '{}'::jsonb) - 'cpv_mapping'
-- where tenant_id = 'civant_default'
--   and signal_type = 'grant_awarded'
--   and occurred_at >= now() - interval '24 months'
--   and coalesce(features_json->'cpv_mapping'->>'method', '') = 'dominant_cluster_v1';
-- =============================================================================

with dominant_candidates as (
  select
    bcs.tenant_id,
    bcs.buyer_entity_id,
    bcs.region,
    bcs.cpv_cluster_id,
    coalesce(bcs.tender_count_24m, 0)::int as tender_count_24m,
    sum(coalesce(bcs.tender_count_24m, 0)::int) over (
      partition by bcs.tenant_id, bcs.buyer_entity_id, bcs.region
    )::int as buyer_total_tenders,
    row_number() over (
      partition by bcs.tenant_id, bcs.buyer_entity_id, bcs.region
      order by coalesce(bcs.tender_count_24m, 0) desc, bcs.cpv_cluster_id asc
    ) as rn
  from public.buyer_category_stats bcs
  where bcs.tenant_id = 'civant_default'
    and bcs.cpv_cluster_id is not null
    and bcs.cpv_cluster_id <> 'cluster_unknown'
),
dominant_cluster as (
  select
    dc.tenant_id,
    dc.buyer_entity_id,
    dc.region,
    dc.cpv_cluster_id,
    dc.tender_count_24m,
    dc.buyer_total_tenders,
    (dc.tender_count_24m::numeric / nullif(dc.buyer_total_tenders, 0)::numeric) as dominant_cluster_pct
  from dominant_candidates dc
  where dc.rn = 1
    and dc.buyer_total_tenders > 0
    and (dc.tender_count_24m::numeric / dc.buyer_total_tenders::numeric) >= 0.40
),
target_signals as (
  select
    s.signal_id,
    s.tenant_id,
    s.buyer_entity_id,
    coalesce(public.normalize_prediction_region(s.region, s.source), 'IE') as normalized_region
  from public.signals s
  where s.tenant_id = 'civant_default'
    and s.signal_type = 'grant_awarded'
    and s.occurred_at >= now() - interval '24 months'
    and (s.cpv_cluster_id is null or s.cpv_cluster_id = 'cluster_unknown')
    and s.buyer_entity_id is not null
    and s.buyer_entity_id not like 'unresolved:%'
)
update public.signals s
set
  cpv_cluster_id = dom.cpv_cluster_id,
  features_json = coalesce(s.features_json, '{}'::jsonb)
    || jsonb_build_object(
      'cpv_mapping',
      jsonb_build_object(
        'method', 'dominant_cluster_v1',
        'dominant_cluster_id', dom.cpv_cluster_id,
        'dominant_cluster_pct', round(dom.dominant_cluster_pct, 4),
        'mapped_at', now()
      )
    )
from target_signals t
join dominant_cluster dom
  on dom.tenant_id = t.tenant_id
 and dom.buyer_entity_id = t.buyer_entity_id
 and dom.region = t.normalized_region
where s.signal_id = t.signal_id
  and (s.cpv_cluster_id is null or s.cpv_cluster_id = 'cluster_unknown');

-- =============================================================================
-- Validation Queries
-- =============================================================================
-- 1) Before/after unknown cluster count for grants in 24-month window
-- select
--   count(*) as total_grants,
--   count(*) filter (where cpv_cluster_id is null) as still_null,
--   count(*) filter (where cpv_cluster_id='cluster_unknown') as still_unknown,
--   count(*) filter (where cpv_cluster_id is not null and cpv_cluster_id<>'cluster_unknown') as now_mapped
-- from signals
-- where tenant_id='civant_default'
--   and signal_type='grant_awarded'
--   and occurred_at >= now() - interval '24 months';
--
-- 2) External influence coverage after rerun
-- select
--   count(*) filter (where s.external_signal_score > 0) as with_external,
--   count(*) as total,
--   round(100.0 * count(*) filter (where s.external_signal_score > 0) / nullif(count(*),0), 2) as pct
-- from prediction_scorecard s
-- join predictions_current p
--   on s.tenant_id=p.tenant_id and s.prediction_id=p.prediction_id
-- where p.tenant_id='civant_default';
