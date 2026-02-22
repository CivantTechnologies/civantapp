-- Migration: 20260226_fr_award_enrichment_gate_v1.sql
-- Goal: introduce a fail-closed FR award enrichment gate (raw -> stage -> reject/metrics -> merge).
-- Safety:
--   - Additive only.
--   - Replay-safe (CREATE IF NOT EXISTS / CREATE OR REPLACE).
--   - Tenant-scoped merge only; never touches canonical tables.
-- Rollback (object-level):
--   drop function if exists public.civant_fr_enrichment_gate_and_merge(text, text, text, boolean);
--   drop table if exists public.fr_award_enrichment_run_metrics;
--   drop table if exists public.fr_award_enrichment_rejects;
--   drop table if exists public.fr_award_enrichment_stage;
--   drop table if exists public.fr_award_enrichment_raw;
--   drop table if exists public.fr_award_enrichment_batches;

begin;

create table if not exists public.fr_award_enrichment_batches (
  tenant_id text not null,
  batch_id text not null,
  source_name text not null,
  run_mode text not null default 'apply' check (run_mode in ('apply', 'dry_run')),
  status text not null default 'running' check (status in ('running', 'completed', 'failed')),
  input_rows integer not null default 0,
  notes jsonb not null default '{}'::jsonb,
  started_at timestamptz not null default now(),
  finished_at timestamptz null,
  created_at timestamptz not null default now(),
  primary key (tenant_id, batch_id)
);

create index if not exists idx_fr_award_enrichment_batches_source_started
  on public.fr_award_enrichment_batches (source_name, started_at desc);

create table if not exists public.fr_award_enrichment_raw (
  id bigserial primary key,
  tenant_id text not null,
  batch_id text not null,
  source_name text not null,
  source_row_hash text not null,
  payload jsonb not null,
  ingested_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  constraint fk_fr_award_enrichment_raw_batch
    foreign key (tenant_id, batch_id)
    references public.fr_award_enrichment_batches (tenant_id, batch_id)
    on delete cascade,
  constraint uq_fr_award_enrichment_raw_hash
    unique (tenant_id, batch_id, source_name, source_row_hash)
);

create index if not exists idx_fr_award_enrichment_raw_batch
  on public.fr_award_enrichment_raw (tenant_id, batch_id, source_name);

create table if not exists public.fr_award_enrichment_stage (
  id bigserial primary key,
  tenant_id text not null,
  batch_id text not null,
  source_name text not null,
  award_canonical_id text not null,
  source_notice_id text null,
  award_date date null,
  duration_months integer null,
  start_date_candidate date null,
  end_date_candidate date null,
  source_method text not null,
  source_confidence text not null default 'medium' check (source_confidence in ('high', 'medium', 'low')),
  source_rank integer not null default 50,
  evidence_url text null,
  source_file text null,
  raw_payload jsonb null,
  quality_status text not null default 'pending' check (quality_status in ('pending', 'accepted', 'rejected')),
  quality_issues jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  constraint fk_fr_award_enrichment_stage_batch
    foreign key (tenant_id, batch_id)
    references public.fr_award_enrichment_batches (tenant_id, batch_id)
    on delete cascade
);

create index if not exists idx_fr_award_enrichment_stage_batch
  on public.fr_award_enrichment_stage (tenant_id, batch_id, source_name, quality_status);

create index if not exists idx_fr_award_enrichment_stage_award_id
  on public.fr_award_enrichment_stage (tenant_id, award_canonical_id);

create table if not exists public.fr_award_enrichment_rejects (
  id bigserial primary key,
  stage_row_id bigint not null unique,
  tenant_id text not null,
  batch_id text not null,
  source_name text not null,
  award_canonical_id text null,
  source_notice_id text null,
  reject_reason text not null,
  reject_details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint fk_fr_award_enrichment_rejects_stage
    foreign key (stage_row_id)
    references public.fr_award_enrichment_stage (id)
    on delete cascade
);

create index if not exists idx_fr_award_enrichment_rejects_batch
  on public.fr_award_enrichment_rejects (tenant_id, batch_id, source_name, reject_reason);

create table if not exists public.fr_award_enrichment_run_metrics (
  id bigserial primary key,
  tenant_id text not null,
  batch_id text not null,
  source_name text not null,
  metric_name text not null,
  metric_value numeric not null,
  metric_payload jsonb not null default '{}'::jsonb,
  captured_at timestamptz not null default now(),
  constraint uq_fr_award_enrichment_run_metric
    unique (tenant_id, batch_id, source_name, metric_name),
  constraint fk_fr_award_enrichment_metrics_batch
    foreign key (tenant_id, batch_id)
    references public.fr_award_enrichment_batches (tenant_id, batch_id)
    on delete cascade
);

create index if not exists idx_fr_award_enrichment_metrics_batch
  on public.fr_award_enrichment_run_metrics (tenant_id, batch_id, source_name);

create or replace function public.civant_fr_enrichment_gate_and_merge(
  p_tenant_id text,
  p_batch_id text,
  p_source_name text,
  p_apply boolean default true
)
returns table (
  input_rows integer,
  accepted_rows integer,
  rejected_rows integer,
  duplicate_rows integer,
  candidate_rows integer,
  updated_rows integer
)
language plpgsql
set search_path = public
as $$
declare
  v_input_rows integer := 0;
  v_accepted_rows integer := 0;
  v_rejected_rows integer := 0;
  v_duplicate_rows integer := 0;
  v_candidate_rows integer := 0;
  v_updated_rows integer := 0;
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;
  if coalesce(trim(p_batch_id), '') = '' then
    raise exception 'p_batch_id is required';
  end if;
  if coalesce(trim(p_source_name), '') = '' then
    raise exception 'p_source_name is required';
  end if;

  insert into public.fr_award_enrichment_batches (
    tenant_id,
    batch_id,
    source_name,
    run_mode,
    status
  )
  values (
    p_tenant_id,
    p_batch_id,
    p_source_name,
    case when p_apply then 'apply' else 'dry_run' end,
    'running'
  )
  on conflict (tenant_id, batch_id) do update
    set source_name = excluded.source_name,
        run_mode = excluded.run_mode,
        status = 'running',
        finished_at = null;

  select count(*)
    into v_input_rows
  from public.fr_award_enrichment_stage s
  where s.tenant_id = p_tenant_id
    and s.batch_id = p_batch_id
    and s.source_name = p_source_name;

  with assessed as (
    select
      s.id,
      array_remove(array[
        case when coalesce(trim(s.award_canonical_id), '') = '' then 'missing_award_canonical_id' end,
        case when s.award_date is null then 'missing_award_date' end,
        case when s.duration_months is null then 'missing_duration_months' end,
        case when s.duration_months is not null and (s.duration_months < 1 or s.duration_months > 240) then 'duration_out_of_range' end,
        case when s.end_date_candidate is null then 'missing_end_date_candidate' end,
        case when s.end_date_candidate is not null and s.award_date is not null and s.end_date_candidate <= s.award_date then 'non_positive_term' end
      ]::text[], null) as issues
    from public.fr_award_enrichment_stage s
    where s.tenant_id = p_tenant_id
      and s.batch_id = p_batch_id
      and s.source_name = p_source_name
      and s.quality_status = 'pending'
  ),
  applied as (
    update public.fr_award_enrichment_stage s
       set quality_status = case when coalesce(array_length(a.issues, 1), 0) = 0 then 'accepted' else 'rejected' end,
           quality_issues = case
             when coalesce(array_length(a.issues, 1), 0) = 0 then '[]'::jsonb
             else to_jsonb(a.issues)
           end
      from assessed a
     where s.id = a.id
     returning s.id
  )
  select count(*) into v_accepted_rows
  from public.fr_award_enrichment_stage s
  where s.tenant_id = p_tenant_id
    and s.batch_id = p_batch_id
    and s.source_name = p_source_name
    and s.quality_status = 'accepted';

  with ranked as (
    select
      s.id,
      row_number() over (
        partition by s.award_canonical_id
        order by s.source_rank desc, s.id desc
      ) as rn
    from public.fr_award_enrichment_stage s
    where s.tenant_id = p_tenant_id
      and s.batch_id = p_batch_id
      and s.source_name = p_source_name
      and s.quality_status = 'accepted'
  ),
  demoted as (
    update public.fr_award_enrichment_stage s
       set quality_status = 'rejected',
           quality_issues = coalesce(s.quality_issues, '[]'::jsonb) || '["duplicate_lower_rank"]'::jsonb
      from ranked r
     where s.id = r.id
       and r.rn > 1
     returning s.id
  )
  select count(*) into v_duplicate_rows from demoted;

  select count(*)
    into v_accepted_rows
  from public.fr_award_enrichment_stage s
  where s.tenant_id = p_tenant_id
    and s.batch_id = p_batch_id
    and s.source_name = p_source_name
    and s.quality_status = 'accepted';

  select count(*)
    into v_rejected_rows
  from public.fr_award_enrichment_stage s
  where s.tenant_id = p_tenant_id
    and s.batch_id = p_batch_id
    and s.source_name = p_source_name
    and s.quality_status = 'rejected';

  insert into public.fr_award_enrichment_rejects (
    stage_row_id,
    tenant_id,
    batch_id,
    source_name,
    award_canonical_id,
    source_notice_id,
    reject_reason,
    reject_details
  )
  select
    s.id,
    s.tenant_id,
    s.batch_id,
    s.source_name,
    s.award_canonical_id,
    s.source_notice_id,
    coalesce(s.quality_issues->>0, 'validation_failed'),
    jsonb_build_object(
      'quality_issues', s.quality_issues,
      'source_method', s.source_method,
      'source_confidence', s.source_confidence,
      'source_rank', s.source_rank
    )
  from public.fr_award_enrichment_stage s
  where s.tenant_id = p_tenant_id
    and s.batch_id = p_batch_id
    and s.source_name = p_source_name
    and s.quality_status = 'rejected'
  on conflict (stage_row_id) do update
    set reject_reason = excluded.reject_reason,
        reject_details = excluded.reject_details,
        created_at = now();

  with candidates as (
    select
      s.id,
      s.award_canonical_id,
      s.source_notice_id,
      s.award_date,
      s.duration_months,
      s.end_date_candidate,
      s.source_method,
      s.source_confidence,
      s.source_rank,
      s.evidence_url,
      s.source_file,
      s.raw_payload,
      af.award_date as award_date_existing
    from public.fr_award_enrichment_stage s
    join public.award_fact_fr af
      on af.tenant_id = s.tenant_id
     and af.award_canonical_id = s.award_canonical_id
    where s.tenant_id = p_tenant_id
      and s.batch_id = p_batch_id
      and s.source_name = p_source_name
      and s.quality_status = 'accepted'
      and af.end_date is null
      and af.award_date is not null
      and s.end_date_candidate is not null
      and s.end_date_candidate > af.award_date
      and s.duration_months between 1 and 240
      and (s.award_date is null or s.award_date = af.award_date)
  )
  select count(*) into v_candidate_rows from candidates;

  if p_apply then
    with candidates as (
      select
        s.id,
        s.award_canonical_id,
        s.source_notice_id,
        s.duration_months,
        s.end_date_candidate,
        s.source_method,
        s.source_confidence,
        s.source_rank,
        s.evidence_url,
        s.source_file,
        s.raw_payload,
        af.award_date as award_date_existing
      from public.fr_award_enrichment_stage s
      join public.award_fact_fr af
        on af.tenant_id = s.tenant_id
       and af.award_canonical_id = s.award_canonical_id
      where s.tenant_id = p_tenant_id
        and s.batch_id = p_batch_id
        and s.source_name = p_source_name
        and s.quality_status = 'accepted'
        and af.end_date is null
        and af.award_date is not null
        and s.end_date_candidate is not null
        and s.end_date_candidate > af.award_date
        and s.duration_months between 1 and 240
        and (s.award_date is null or s.award_date = af.award_date)
    ),
    updated as (
      update public.award_fact_fr af
         set end_date = c.end_date_candidate,
             duration_months = coalesce(af.duration_months, c.duration_months),
             framework_evidence = jsonb_strip_nulls(
               coalesce(af.framework_evidence, '{}'::jsonb) ||
               jsonb_build_object(
                 'fr_end_date_source', c.source_method,
                 'fr_end_date_confidence', c.source_confidence,
                 'fr_end_date_source_rank', c.source_rank,
                 'fr_end_date_source_name', p_source_name,
                 'fr_end_date_batch_id', p_batch_id,
                 'fr_end_date_source_notice_id', c.source_notice_id,
                 'fr_end_date_source_file', c.source_file,
                 'fr_end_date_evidence_url', c.evidence_url,
                 'fr_end_date_updated_at', to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
               )
             )
        from candidates c
       where af.tenant_id = p_tenant_id
         and af.award_canonical_id = c.award_canonical_id
         and af.end_date is null
      returning 1
    )
    select count(*) into v_updated_rows from updated;
  else
    v_updated_rows := 0;
  end if;

  insert into public.fr_award_enrichment_run_metrics (
    tenant_id,
    batch_id,
    source_name,
    metric_name,
    metric_value,
    metric_payload
  )
  values
    (p_tenant_id, p_batch_id, p_source_name, 'input_rows', v_input_rows, jsonb_build_object('apply', p_apply)),
    (p_tenant_id, p_batch_id, p_source_name, 'accepted_rows', v_accepted_rows, jsonb_build_object('apply', p_apply)),
    (p_tenant_id, p_batch_id, p_source_name, 'rejected_rows', v_rejected_rows, jsonb_build_object('apply', p_apply)),
    (p_tenant_id, p_batch_id, p_source_name, 'duplicate_rows', v_duplicate_rows, jsonb_build_object('apply', p_apply)),
    (p_tenant_id, p_batch_id, p_source_name, 'candidate_rows', v_candidate_rows, jsonb_build_object('apply', p_apply)),
    (p_tenant_id, p_batch_id, p_source_name, 'updated_rows', v_updated_rows, jsonb_build_object('apply', p_apply))
  on conflict (tenant_id, batch_id, source_name, metric_name) do update
    set metric_value = excluded.metric_value,
        metric_payload = excluded.metric_payload,
        captured_at = now();

  update public.fr_award_enrichment_batches b
     set status = 'completed',
         input_rows = v_input_rows,
         finished_at = now(),
         notes = coalesce(b.notes, '{}'::jsonb) || jsonb_build_object(
           'accepted_rows', v_accepted_rows,
           'rejected_rows', v_rejected_rows,
           'duplicate_rows', v_duplicate_rows,
           'candidate_rows', v_candidate_rows,
           'updated_rows', v_updated_rows,
           'apply', p_apply
         )
   where b.tenant_id = p_tenant_id
     and b.batch_id = p_batch_id;

  return query
  select
    v_input_rows,
    v_accepted_rows,
    v_rejected_rows,
    v_duplicate_rows,
    v_candidate_rows,
    v_updated_rows;
end;
$$;

commit;
