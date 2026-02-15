-- Civant QA Pack: External Signal Rollup IE v1
-- Table: public.external_signal_rollup_ie

\pset pager off
\timing on

-- Configure tenant for QA run
\if :{?tenant_id}
\else
\set tenant_id 'civant_default'
\endif

-- -------------------------------------------------------------------------------------
-- Idempotence check guidance
-- - Run refresh twice.
-- - Second run should report 0 inserted/updated/deleted unless the source table changed.
-- -------------------------------------------------------------------------------------

select * from public.refresh_external_signal_rollup_ie(:'tenant_id'::text);
select * from public.refresh_external_signal_rollup_ie(:'tenant_id'::text);

-- Row counts per tenant
select
  tenant_id,
  count(*) as buyer_rows
from public.external_signal_rollup_ie
group by 1
order by buyer_rows desc, tenant_id asc;

-- Top 50 buyers by combined_external_strength_90d
select
  buyer_key,
  buyer_id,
  buyer_name_normalized,
  combined_external_strength_90d,
  hiring_last_date,
  hiring_count_30d,
  hiring_count_90d,
  hiring_max_strength_90d,
  funding_last_date,
  funding_count_30d,
  funding_count_90d,
  funding_max_strength_90d,
  refreshed_at
from public.external_signal_rollup_ie
where tenant_id = :'tenant_id'
order by combined_external_strength_90d desc, buyer_key asc
limit 50;

-- UNKNOWN share (buyer_key = 'UNKNOWN')
select
  count(*) as total_buyers,
  count(*) filter (where buyer_key = 'UNKNOWN') as unknown_buyers,
  round(100.0 * count(*) filter (where buyer_key = 'UNKNOWN') / nullif(count(*), 0), 2) as pct_unknown
from public.external_signal_rollup_ie
where tenant_id = :'tenant_id';

-- -------------------------------------------------------------------------------------
-- Manual test spot checks
-- Expectation (visual verification):
-- - Office of Public Works: hiring_last_date + hiring_count_30d/90d reflect the inserted hiring test rows
-- - HSE: funding_last_date + funding_count_30d/90d reflect the inserted funding test rows
--
-- Note: buyer_key is coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN').
-- For manual inserts without buyer_id, buyer_key should match buyer_name_normalized.
-- -------------------------------------------------------------------------------------

with target_keys as (
  select :'tenant_id'::text as tenant_id, 'office of public works'::text as buyer_key
  union all
  select :'tenant_id'::text as tenant_id, 'health service executive hse'::text as buyer_key
),
raw as (
  select
    es.tenant_id,
    coalesce(es.buyer_id, es.buyer_name_normalized, 'UNKNOWN') as buyer_key,

    max(es.signal_date) filter (where es.signal_type = 'hiring') as hiring_last_date_raw,
    count(*) filter (where es.signal_type = 'hiring' and es.signal_date >= (current_date - interval '30 days')::date)::int as hiring_count_30d_raw,
    count(*) filter (where es.signal_type = 'hiring' and es.signal_date >= (current_date - interval '90 days')::date)::int as hiring_count_90d_raw,

    max(es.signal_date) filter (where es.signal_type = 'funding') as funding_last_date_raw,
    count(*) filter (where es.signal_type = 'funding' and es.signal_date >= (current_date - interval '30 days')::date)::int as funding_count_30d_raw,
    count(*) filter (where es.signal_type = 'funding' and es.signal_date >= (current_date - interval '90 days')::date)::int as funding_count_90d_raw
  from public.external_signals_ie es
  where es.tenant_id = :'tenant_id'
    and coalesce(es.buyer_id, es.buyer_name_normalized, 'UNKNOWN') in (
      'office of public works',
      'health service executive hse'
    )
  group by 1,2
)
select
  tk.buyer_key,

  r.hiring_last_date as hiring_last_date_rollup,
  raw.hiring_last_date_raw,
  r.hiring_count_30d as hiring_count_30d_rollup,
  raw.hiring_count_30d_raw,
  r.hiring_count_90d as hiring_count_90d_rollup,
  raw.hiring_count_90d_raw,

  r.funding_last_date as funding_last_date_rollup,
  raw.funding_last_date_raw,
  r.funding_count_30d as funding_count_30d_rollup,
  raw.funding_count_30d_raw,
  r.funding_count_90d as funding_count_90d_rollup,
  raw.funding_count_90d_raw,

  r.combined_external_strength_90d,
  r.explainability->'evidence'->'hiring' as hiring_evidence_top3_90d,
  r.explainability->'evidence'->'funding' as funding_evidence_top3_90d
from target_keys tk
left join public.external_signal_rollup_ie r
  on r.tenant_id = tk.tenant_id
 and r.buyer_key = tk.buyer_key
left join raw
  on raw.tenant_id = tk.tenant_id
 and raw.buyer_key = tk.buyer_key
order by tk.buyer_key;

-- Raw evidence rows (last 120 days) for those buyers, for human inspection
select
  es.signal_type,
  es.signal_date,
  es.signal_strength,
  es.signal_source,
  es.source_event_id,
  es.evidence_url,
  left(es.evidence_text, 160) as evidence_text_preview
from public.external_signals_ie es
where es.tenant_id = :'tenant_id'
  and coalesce(es.buyer_id, es.buyer_name_normalized, 'UNKNOWN') in (
    'office of public works',
    'health service executive hse'
  )
  and es.signal_date >= (current_date - interval '120 days')::date
order by es.signal_date desc, es.signal_type asc, es.signal_strength desc;
