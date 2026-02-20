-- =============================================================================
-- Civant: Grant-awarded dominant cluster mapping (top-cluster v2)
-- Migration: 20260224_grant_awarded_dominant_cluster_map_v2.sql
-- =============================================================================
-- Goal:
--   Map grant_awarded signals with NULL/cluster_unknown CPV to each buyer's top
--   observed CPV cluster per region, then scale signal_strength by dominance
--   share to avoid over-boosting diversified buyers.
--
-- Scope (MVP): tenant_id = 'civant_default' only.
-- - Only signal_type = 'grant_awarded'
-- - Only cpv_cluster_id IS NULL OR cpv_cluster_id = 'cluster_unknown'
-- - Only last 24 months (engine-aligned window)
-- - Dominance source: notice_published signals over 15 years
-- - Excludes cluster_unknown from dominant cluster candidates
-- - Region uses normalize_prediction_region(signal.region, signal.source)
-- - Skip unresolved/null buyer ids
--
-- Mapping rule:
-- - Always map to the buyer's top cpv_cluster_id in that region (deterministic
--   tie-break: cpv_cluster_id ASC), without requiring a minimum share threshold.
-- - Persist cpv_mapping metadata:
--   method, dominant_cluster_id, dominant_cluster_pct, confidence_band, mapped_at.
-- - confidence_band is derived from dominant_cluster_pct:
--   High >= 0.70, Medium >= 0.40, Low < 0.40.
--
-- signal_strength scaling:
-- - Multiply existing signal_strength by dominant_cluster_pct.
-- - Clamp multiplier to [0.10, 1.0].
-- - Clamp final signal_strength to [0, 1].
--
-- Idempotency:
-- - Target set is only unknown/null CPV grant rows.
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
--   and coalesce(features_json->'cpv_mapping'->>'method', '') = 'dominant_cluster_v2_top_cluster';
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
    (dc.notice_count::numeric / nullif(dc.buyer_total_notice_count, 0)::numeric) as dominant_cluster_pct,
    case
      when (dc.notice_count::numeric / nullif(dc.buyer_total_notice_count, 0)::numeric) >= 0.70 then 'high'
      when (dc.notice_count::numeric / nullif(dc.buyer_total_notice_count, 0)::numeric) >= 0.40 then 'medium'
      else 'low'
    end as confidence_band
  from dominant_candidates dc
  where dc.rn = 1
    and dc.buyer_total_notice_count > 0
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
  signal_strength = greatest(
    0::numeric,
    least(
      1::numeric,
      coalesce(s.signal_strength, 0.5) * greatest(0.10::numeric, least(1::numeric, dom.dominant_cluster_pct))
    )
  ),
  features_json = coalesce(s.features_json, '{}'::jsonb)
    || jsonb_build_object(
      'cpv_mapping',
      jsonb_build_object(
        'method', 'dominant_cluster_v2_top_cluster',
        'dominant_cluster_id', dom.cpv_cluster_id,
        'dominant_cluster_pct', round(dom.dominant_cluster_pct, 4),
        'confidence_band', dom.confidence_band,
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
-- 1) Before migration (capture baseline):
-- create temp table _grant_map_before as
-- select
--   count(*) as total_grants,
--   count(*) filter (where cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown') as now_mapped
-- from public.signals
-- where tenant_id = 'civant_default'
--   and signal_type = 'grant_awarded'
--   and occurred_at >= now() - interval '24 months';
--
-- 2) After migration (material mapped increase check):
-- with after_stats as (
--   select
--     count(*) as total_grants,
--     count(*) filter (where cpv_cluster_id is not null and cpv_cluster_id <> 'cluster_unknown') as now_mapped
--   from public.signals
--   where tenant_id = 'civant_default'
--     and signal_type = 'grant_awarded'
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
-- 3) Coverage impact after rerunning incremental engine:
-- select
--   count(*) filter (where s.external_signal_score > 0) as with_external,
--   count(*) as total,
--   round(100.0 * count(*) filter (where s.external_signal_score > 0) / nullif(count(*), 0), 2) as pct
-- from public.prediction_scorecard s
-- join public.predictions_current p
--   on p.tenant_id = s.tenant_id
--  and p.prediction_id = s.prediction_id
-- where p.tenant_id = 'civant_default';
