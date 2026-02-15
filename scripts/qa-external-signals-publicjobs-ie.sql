-- Civant QA Pack: External Signals IE (Hiring via PublicJobs)
-- Connector: publicjobs_ie
-- Table: public.external_signals_ie

\pset pager off
\timing on

\if :{?tenant_id}
\else
\set tenant_id 'civant_default'
\endif

-- Counts by source (hiring only)
select
  signal_source,
  count(*) as rows,
  min(signal_date) as min_signal_date,
  max(signal_date) as max_signal_date
from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_type = 'hiring'
  and signal_source = 'publicjobs_ie'
group by 1
order by rows desc;

-- Last 30 days sample
select
  id,
  tenant_id,
  signal_type,
  signal_source,
  source_event_id,
  signal_date,
  signal_strength,
  buyer_name_normalized,
  evidence_url,
  left(evidence_text, 160) as evidence_text_preview,
  extracted_entities
from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_type = 'hiring'
  and signal_source = 'publicjobs_ie'
  and signal_date >= (current_date - interval '30 days')::date
order by signal_date desc, captured_at desc
limit 200;

-- Duplicate check on (tenant_id, signal_source, source_event_id)
select
  tenant_id,
  signal_source,
  source_event_id,
  count(*) as rows
from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_source = 'publicjobs_ie'
group by 1,2,3
having count(*) > 1
order by rows desc
limit 50;

-- Missing evidence / IDs (should be zero)
select
  count(*) filter (where evidence_url is null or evidence_url = '') as missing_evidence_url,
  count(*) filter (where source_event_id is null or source_event_id = '') as missing_source_event_id
from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_type = 'hiring'
  and signal_source = 'publicjobs_ie';
