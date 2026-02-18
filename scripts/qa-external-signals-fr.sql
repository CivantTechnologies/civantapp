-- Civant QA Pack: External Signals FR v1
-- Table: public.external_signals_fr

\pset pager off
\timing on

\if :{?tenant_id}
\else
\set tenant_id 'civant_default'
\endif

select signal_type, count(*) as rows
from public.external_signals_fr
where tenant_id = :'tenant_id'
group by 1
order by rows desc;

select signal_source, count(*) as rows
from public.external_signals_fr
where tenant_id = :'tenant_id'
group by 1
order by rows desc, signal_source;

select
  source_event_id,
  count(*) as dupes
from public.external_signals_fr
where tenant_id = :'tenant_id'
group by 1
having count(*) > 1
order by dupes desc, source_event_id
limit 50;

select
  signal_date,
  signal_source,
  left(evidence_text, 120) as evidence_preview,
  evidence_url,
  extracted_entities
from public.external_signals_fr
where tenant_id = :'tenant_id'
  and signal_date >= (current_date - interval '30 days')::date
order by signal_date desc, captured_at desc
limit 200;

select * from public.refresh_external_signal_rollup_fr(:'tenant_id'::text);

select
  buyer_key,
  funding_count_30d,
  funding_count_90d,
  combined_external_strength_90d,
  refreshed_at
from public.external_signal_rollup_fr
where tenant_id = :'tenant_id'
order by combined_external_strength_90d desc nulls last
limit 50;
