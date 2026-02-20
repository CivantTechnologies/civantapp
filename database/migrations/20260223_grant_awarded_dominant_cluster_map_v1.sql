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
-- - Derived from public.signals notice_published counts over 15 years
-- - Highest notice_published count for (tenant_id, buyer_entity_id, region)
-- - Dominant share must be >= 40% of buyer total notice_published in that region
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
    s.tenant_id,
    s.buyer_entity_id,
    coalesce(public.normalize_prediction_region(s.region, s.source), 'IE') as region,
    s.cpv_cluster_id,
    count(*)::int as notice_count,
    sum(count(*)) over (
      partition by s.tenant_id, s.buyer_entity_id, coalesce(public.normalize_prediction_region(s.region, s.source), 'IE')
    )::int as buyer_total_notice_count,
    row_number() over (
      partition by s.tenant_id, s.buyer_entity_id, coalesce(public.normalize_prediction_region(s.region, s.source), 'IE')
      order by count(*) desc, s.cpv_cluster_id asc
    ) as rn
  from public.signals s
  where s.tenant_id = 'civant_default'
    and s.signal_type = 'notice_published'
    and s.occurred_at >= now() - interval '15 years'
    and s.occurred_at <= now()
    and s.buyer_entity_id is not null
    and s.buyer_entity_id not like 'unresolved:%'
    and s.cpv_cluster_id is not null
    and s.cpv_cluster_id <> 'cluster_unknown'
  group by 1, 2, 3, 4
),
dominant_cluster as (
  select
    dc.tenant_id,
    dc.buyer_entity_id,
    dc.region,
    dc.cpv_cluster_id,
    dc.notice_count,
    dc.buyer_total_notice_count,
    (dc.notice_count::numeric / nullif(dc.buyer_total_notice_count, 0)::numeric) as dominant_cluster_pct
  from dominant_candidates dc
  where dc.rn = 1
    and dc.buyer_total_notice_count > 0
    and (dc.notice_count::numeric / dc.buyer_total_notice_count::numeric) >= 0.40
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
-- 2) Before/after now_mapped delta (material increase check)
-- -- before migration:
-- -- create temp table _grant_map_before as
-- -- select
-- --   count(*) as total_grants,
-- --   count(*) filter (where cpv_cluster_id is not null and cpv_cluster_id<>'cluster_unknown') as now_mapped
-- -- from signals
-- -- where tenant_id='civant_default'
-- --   and signal_type='grant_awarded'
-- --   and occurred_at >= now() - interval '24 months';
-- --
-- -- after migration:
-- with after_stats as (
--   select
--     count(*) as total_grants,
--     count(*) filter (where cpv_cluster_id is not null and cpv_cluster_id<>'cluster_unknown') as now_mapped
--   from signals
--   where tenant_id='civant_default'
--     and signal_type='grant_awarded'
--     and occurred_at >= now() - interval '24 months'
-- )
-- select
--   b.now_mapped as before_now_mapped,
--   a.now_mapped as after_now_mapped,
--   (a.now_mapped - b.now_mapped) as now_mapped_delta,
--   round(100.0 * b.now_mapped / nullif(b.total_grants, 0), 2) as before_mapped_pct,
--   round(100.0 * a.now_mapped / nullif(a.total_grants, 0), 2) as after_mapped_pct,
--   round(
--     (100.0 * a.now_mapped / nullif(a.total_grants, 0))
--     - (100.0 * b.now_mapped / nullif(b.total_grants, 0)),
--     2
--   ) as mapped_delta_pp
-- from _grant_map_before b
-- cross join after_stats a;
--
-- 3) External influence coverage after rerun
-- select
--   count(*) filter (where s.external_signal_score > 0) as with_external,
--   count(*) as total,
--   round(100.0 * count(*) filter (where s.external_signal_score > 0) / nullif(count(*),0), 2) as pct
-- from prediction_scorecard s
-- join predictions_current p
--   on s.tenant_id=p.tenant_id and s.prediction_id=p.prediction_id
-- where p.tenant_id='civant_default';
