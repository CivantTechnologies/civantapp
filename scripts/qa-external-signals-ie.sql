-- Civant QA Pack: External Signals IE v1
-- Table: public.external_signals_ie

\pset pager off
\timing on

-- Configure tenant for QA run
\if :{?tenant_id}
\else
\set tenant_id 'civant_default'
\endif

-- Counts by signal_type
select
  signal_type,
  count(*) as rows
from public.external_signals_ie
where tenant_id = :'tenant_id'
group by 1
order by rows desc;

-- Counts by signal_source
select
  signal_source,
  count(*) as rows
from public.external_signals_ie
where tenant_id = :'tenant_id'
group by 1
order by rows desc, signal_source asc;

-- Last 30 days signals
select
  id,
  tenant_id,
  buyer_id,
  buyer_name_normalized,
  signal_type,
  signal_source,
  source_event_id,
  signal_date,
  signal_strength,
  evidence_url,
  left(evidence_text, 120) as evidence_text_preview,
  extracted_entities
from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_date >= (current_date - interval '30 days')::date
order by signal_date desc, captured_at desc
limit 200;

-- Top buyers (prefer buyer_id; else normalized name)
select
  coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN') as buyer_key,
  count(*) as rows,
  min(signal_date) as first_signal_date,
  max(signal_date) as last_signal_date
from public.external_signals_ie
where tenant_id = :'tenant_id'
group by 1
order by rows desc
limit 50;

-- -------------------------------------------------------------------------------------
-- Repeatable example inserts (upsert) + select back
-- -------------------------------------------------------------------------------------

select public.upsert_external_signal_ie(
  :'tenant_id'::text,
  'hiring'::text,
  'qa_manual'::text,
  'qa_example_hiring_1'::text,
  current_date,
  'https://example.com/ie/hiring/1'::text,
  'Example hiring evidence snippet (QA).',
  null,
  'Office of Public Works',
  0.650,
  jsonb_build_object('job_title','Senior Engineer','location','Dublin'),
  jsonb_build_object('raw','example')
) as example_hiring_id;

select public.upsert_external_signal_ie(
  :'tenant_id'::text,
  'funding'::text,
  'qa_manual'::text,
  'qa_example_funding_1'::text,
  current_date,
  'https://example.com/ie/funding/1'::text,
  'Example funding evidence snippet (QA).',
  null,
  'Health Service Executive (HSE)',
  0.800,
  jsonb_build_object('amount_eur', 2500000, 'program','Digital modernisation'),
  jsonb_build_object('raw','example')
) as example_funding_id;

select
  id,
  tenant_id,
  buyer_id,
  buyer_name_raw,
  buyer_name_normalized,
  signal_type,
  signal_source,
  source_event_id,
  signal_date,
  signal_strength,
  evidence_url,
  evidence_text,
  extracted_entities
from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_source = 'qa_manual'
  and source_event_id in ('qa_example_hiring_1','qa_example_funding_1')
order by source_event_id;

-- Cleanup so QA is repeatable
delete from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_source = 'qa_manual'
  and source_event_id in ('qa_example_hiring_1','qa_example_funding_1');

-- Sanity: ensure cleanup worked
select
  count(*) as remaining_example_rows
from public.external_signals_ie
where tenant_id = :'tenant_id'
  and signal_source = 'qa_manual'
  and source_event_id in ('qa_example_hiring_1','qa_example_funding_1');
