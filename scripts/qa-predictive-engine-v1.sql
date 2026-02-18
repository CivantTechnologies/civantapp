\set ON_ERROR_STOP on
\if :{?tenant_id}
\else
\set tenant_id qa_predictive_v1
\endif

\echo '== QA Predictive Engine V1 =='
\echo 'tenant_id=' :tenant_id

begin;

select public.ensure_prediction_baseline(:'tenant_id');

-- Clean tenant-scoped predictive fixtures in-transaction.
delete from public.prediction_inputs_snapshot where tenant_id = :'tenant_id';
delete from public.prediction_extensions where tenant_id = :'tenant_id';
delete from public.prediction_scorecard where tenant_id = :'tenant_id';
delete from public.prediction_drivers where tenant_id = :'tenant_id';
delete from public.predictions_current where tenant_id = :'tenant_id';
delete from public.buyer_category_stats where tenant_id = :'tenant_id';
delete from public.prediction_runs where tenant_id = :'tenant_id';
delete from public.signals where tenant_id = :'tenant_id';
delete from public.external_signal_raw where tenant_id = :'tenant_id';
delete from public.buyer_aliases where tenant_id = :'tenant_id' and source = 'QA_TEST';

insert into public.entities (entity_id, tenant_id, entity_type, canonical_name, country, metadata, created_at, updated_at)
values
  ('qa_predictive_v1_buyer_ie', :'tenant_id', 'buyer', 'QA Buyer IE', 'IE', '{}'::jsonb, now(), now()),
  ('qa_predictive_v1_buyer_ie_ext_only', :'tenant_id', 'buyer', 'QA Buyer IE ExtOnly', 'IE', '{}'::jsonb, now(), now())
on conflict (entity_id) do nothing;

insert into public.buyer_aliases (tenant_id, raw_buyer_key, source, canonical_buyer_entity_id, resolution_confidence)
values
  (:'tenant_id', public.normalize_match_key('QA Buyer IE'), 'QA_TEST', 'qa_predictive_v1_buyer_ie', 1.0)
on conflict (tenant_id, raw_buyer_key, source) do update
set canonical_buyer_entity_id = excluded.canonical_buyer_entity_id,
    resolution_confidence = excluded.resolution_confidence,
    updated_at = now();

-- Baseline procurement history (non-external drivers present).
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'notice_published', 0.80, now() - interval '210 days', 'QA_TEST',
  'https://example.com/qa/notice/1', null, 'qa_notice_1',
  jsonb_build_object('estimated_value', 500000)
);
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'notice_published', 0.82, now() - interval '140 days', 'QA_TEST',
  'https://example.com/qa/notice/2', null, 'qa_notice_2',
  jsonb_build_object('estimated_value', 550000)
);
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'notice_published', 0.84, now() - interval '70 days', 'QA_TEST',
  'https://example.com/qa/notice/3', null, 'qa_notice_3',
  jsonb_build_object('estimated_value', 530000)
);
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'award_published', 0.90, now() - interval '45 days', 'QA_TEST',
  'https://example.com/qa/award/1', null, 'qa_award_1',
  jsonb_build_object('estimated_value', 500000)
);
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'framework_expiry', 0.85, now() + interval '35 days', 'QA_TEST',
  'https://example.com/qa/framework/1', null, 'qa_framework_expiry_1',
  '{}'::jsonb
);

-- External signals (decayed + corroborated).
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'job_posting', 1.00, now() - interval '45 days', 'QA_TEST',
  'https://example.com/qa/signal/job', null, 'qa_signal_job_1',
  '{}'::jsonb
);
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'budget_increase', 0.90, now() - interval '10 days', 'QA_TEST',
  'https://example.com/qa/signal/budget', null, 'qa_signal_budget_1',
  '{}'::jsonb
);

-- External-only pair (must not produce prediction because no behavioural fingerprint).
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie_ext_only', 'cluster_digital', 'IE',
  'job_posting', 0.95, now() - interval '2 days', 'QA_TEST',
  'https://example.com/qa/signal/extonly', null, 'qa_signal_extonly_1',
  '{}'::jsonb
);

-- Full run (no normalization needed; we inserted normalized signals directly).
select *
from public.run_predictive_engine_v1(
  :'tenant_id',
  'full',
  'v1.qa',
  now() - interval '24 months',
  500,
  false
);

-- Incremental smoke path.
select public.upsert_signal_event(
  :'tenant_id', 'qa_predictive_v1_buyer_ie', 'cluster_digital', 'IE',
  'amendment', 0.75, now() - interval '5 minutes', 'QA_TEST',
  'https://example.com/qa/notice/amend', null, 'qa_notice_amend_1',
  '{}'::jsonb
);

select *
from public.run_predictive_engine_v1(
  :'tenant_id',
  'incremental',
  'v1.qa',
  now() - interval '1 hour',
  500,
  false
);

-- Metrics: buyer resolution and confidence distribution
\echo '== QA Metrics: Buyer resolution =='
select count(distinct case when buyer_entity_id like 'unresolved:%' then buyer_entity_id end) as unresolved_buyer_keys
from public.signals
where tenant_id = :'tenant_id';

select count(*) as buyer_aliases_count
from public.buyer_aliases
where tenant_id = :'tenant_id';

select
  count(*) as total_signals,
  sum((buyer_entity_id not like 'unresolved:%')::int) as resolved_signals,
  round(100.0 * sum((buyer_entity_id not like 'unresolved:%')::int) / nullif(count(*), 0), 2) as resolved_pct
from public.signals
where tenant_id = :'tenant_id';

select confidence_band, count(*)
from public.predictions_current
where tenant_id = :'tenant_id'
group by confidence_band
order by count(*) desc;

select count(*) as confidence_ge_60
from public.predictions_current
where tenant_id = :'tenant_id'
  and confidence >= 60;

-- Assertions

select set_config('app.qa_tenant_id', :'tenant_id', true);

do $$
declare
  v_tenant text := current_setting('app.qa_tenant_id', true);
  v_pred_count int;
  v_driver_min int;
  v_dupes int;
  v_decay_45 numeric;
  v_decay_120 numeric;
  v_extonly uuid;
  v_extonly_confidence int;
  v_extonly_tier int;
  v_incremental_success int;
  v_latest_full_computed int;
begin
  select count(*) into v_pred_count
  from public.predictions_current
  where tenant_id = v_tenant;

  if v_pred_count < 1 then
    raise exception 'FAIL: expected >=1 prediction row for QA tenant';
  end if;

  select min(drivers_count) into v_driver_min
  from public.predictions_current
  where tenant_id = v_tenant;

  if coalesce(v_driver_min, 0) < 2 then
    raise exception 'FAIL: expected every prediction to have at least 2 drivers';
  end if;

  select count(*) - count(distinct prediction_id) into v_dupes
  from public.predictions_current
  where tenant_id = v_tenant;

  if coalesce(v_dupes, 0) <> 0 then
    raise exception 'FAIL: duplicate prediction rows detected in predictions_current';
  end if;

  select public.compute_effective_signal_strength('job_posting', 1.0, now() - interval '45 days', now()) into v_decay_45;
  select public.compute_effective_signal_strength('job_posting', 1.0, now() - interval '120 days', now()) into v_decay_120;

  if not (v_decay_45 < 0.40 and v_decay_45 > 0.30) then
    raise exception 'FAIL: unexpected decay at 45 days: %', v_decay_45;
  end if;

  if not (v_decay_120 < v_decay_45) then
    raise exception 'FAIL: decay monotonicity violated (120d >= 45d): % >= %', v_decay_120, v_decay_45;
  end if;

  select public.compute_prediction_for_pair_v1(
    v_tenant,
    'qa_predictive_v1_buyer_ie_ext_only',
    'cluster_digital',
    'IE',
    'v1.qa',
    now()
  ) into v_extonly;

  if v_extonly is not null then
    select confidence, fallback_tier
      into v_extonly_confidence, v_extonly_tier
    from public.predictions_current
    where prediction_id = v_extonly
      and tenant_id = v_tenant;

    if coalesce(v_extonly_tier, 0) < 3 then
      raise exception 'FAIL: sparse buyer prediction should be fallback tier >=3, got %', v_extonly_tier;
    end if;

    if coalesce(v_extonly_confidence, 100) > 59 then
      raise exception 'FAIL: sparse buyer prediction should be capped to Medium confidence, got %', v_extonly_confidence;
    end if;
  end if;

  select count(*) into v_incremental_success
  from public.prediction_runs
  where tenant_id = v_tenant
    and run_type = 'incremental'
    and status = 'success';

  if v_incremental_success < 1 then
    raise exception 'FAIL: incremental run did not complete successfully';
  end if;

  select coalesce((metadata_json->>'computed_prediction_rows')::int, 0)
  into v_latest_full_computed
  from public.prediction_runs
  where tenant_id = v_tenant
    and run_type = 'full'
    and status = 'success'
  order by started_at desc
  limit 1;

  if coalesce(v_latest_full_computed, 0) < 1 then
    raise exception 'FAIL: run metadata shows zero computed prediction rows';
  end if;
end $$;

select
  tenant_id,
  run_type,
  status,
  pairs_processed,
  started_at,
  finished_at
from public.prediction_runs
where tenant_id = :'tenant_id'
order by started_at desc
limit 5;

select
  tenant_id,
  region,
  confidence_band,
  count(*) as prediction_count
from public.predictions_current
where tenant_id = :'tenant_id'
group by 1, 2, 3
order by 1, 2, 3;

\echo 'PASS: predictive QA assertions passed'
rollback;
