-- Migration: 20260227_prediction_closed_loop_cycle_report_v1.sql
-- Goal:
--   Implement a closed-loop prediction lifecycle:
--     prediction -> reality notice -> canonical-linked outcome -> next prediction,
--   and provide a buyer/category cycle+trend report from 2012 onward.
--
-- Safety:
--   - Additive tables/functions/indexes/policies only.
--   - Tenant-scoped reads/writes.
--   - Canonical tables are read-only in data-path (no canonical data mutation).
--
-- Rollback (manual):
--   - Drop trigger trg_prediction_reconcile_on_canonical_link
--   - Drop functions introduced in this file
--   - Drop tables:
--       prediction_reconciliation_candidates
--       prediction_reconciliation_log
--       prediction_cycle_history
--       prediction_lifecycle

begin;

create table if not exists public.prediction_lifecycle (
  lifecycle_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  prediction_id uuid,
  buyer_entity_id text not null,
  cpv_cluster_id text not null,
  region text not null check (region in ('IE', 'FR', 'ES')),
  status text not null default 'Draft' check (status in ('Draft', 'Published', 'Monitoring', 'Hit', 'Miss', 'Withdrawn', 'NeedsReview')),
  contract_status text not null default 'Active' check (contract_status in ('Active', 'EndingSoon', 'Extended', 'RetenderInProgress', 'Replaced')),
  predicted_window_start date not null,
  predicted_window_end date not null,
  grace_days integer not null default 30 check (grace_days between 0 and 180),
  forecast_probability numeric(6,5) not null default 0 check (forecast_probability >= 0 and forecast_probability <= 1),
  confidence_score numeric(6,2) not null default 0 check (confidence_score >= 0 and confidence_score <= 100),
  evidence_count integer not null default 0,
  evidence_refs jsonb not null default '[]'::jsonb,
  top_drivers jsonb not null default '[]'::jsonb,
  model_version text not null default 'v1.0.0',
  source_prediction_json jsonb not null default '{}'::jsonb,
  linked_notice_id uuid,
  linked_canonical_id text,
  matching_score numeric(6,3),
  matching_reasons jsonb not null default '{}'::jsonb,
  hit_at timestamptz,
  miss_at timestamptz,
  withdrawn_at timestamptz,
  needs_review_at timestamptz,
  last_reconciled_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint prediction_lifecycle_window_check check (predicted_window_end >= predicted_window_start),
  constraint prediction_lifecycle_unique_window unique (tenant_id, buyer_entity_id, cpv_cluster_id, region, predicted_window_start, predicted_window_end, model_version),
  constraint prediction_lifecycle_cluster_fk
    foreign key (tenant_id, cpv_cluster_id)
    references public.cpv_cluster(tenant_id, cpv_cluster_id)
    on delete restrict,
  constraint prediction_lifecycle_prediction_fk
    foreign key (prediction_id)
    references public.predictions_current(prediction_id)
    on delete set null,
  constraint prediction_lifecycle_canonical_fk
    foreign key (linked_canonical_id)
    references public.canonical_tenders(canonical_id)
    on delete set null,
  constraint prediction_lifecycle_notice_fk
    foreign key (linked_notice_id)
    references public.notices(notice_id)
    on delete set null
);

create index if not exists idx_prediction_lifecycle_tenant_status_updated
  on public.prediction_lifecycle (tenant_id, status, updated_at desc);

create index if not exists idx_prediction_lifecycle_tenant_pair_status
  on public.prediction_lifecycle (tenant_id, buyer_entity_id, cpv_cluster_id, region, status);

create index if not exists idx_prediction_lifecycle_tenant_window
  on public.prediction_lifecycle (tenant_id, predicted_window_end, grace_days, status);

create table if not exists public.prediction_cycle_history (
  history_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  lifecycle_id uuid not null references public.prediction_lifecycle(lifecycle_id) on delete cascade,
  buyer_entity_id text not null,
  cpv_cluster_id text not null,
  region text not null check (region in ('IE', 'FR', 'ES')),
  canonical_id text,
  notice_id uuid,
  outcome_status text not null check (outcome_status in ('Hit', 'Miss', 'NeedsReview', 'Withdrawn')),
  publication_date date,
  predicted_window_start date,
  predicted_window_end date,
  in_window boolean,
  match_score numeric(6,3),
  source_refs jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint prediction_cycle_history_canonical_fk
    foreign key (canonical_id)
    references public.canonical_tenders(canonical_id)
    on delete set null,
  constraint prediction_cycle_history_notice_fk
    foreign key (notice_id)
    references public.notices(notice_id)
    on delete set null
);

create index if not exists idx_prediction_cycle_history_tenant_pair
  on public.prediction_cycle_history (tenant_id, buyer_entity_id, cpv_cluster_id, region, created_at desc);

create table if not exists public.prediction_reconciliation_candidates (
  candidate_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  lifecycle_id uuid not null references public.prediction_lifecycle(lifecycle_id) on delete cascade,
  canonical_id text,
  notice_id uuid,
  match_score numeric(6,3) not null,
  within_window boolean not null default false,
  within_grace boolean not null default false,
  reasons jsonb not null default '{}'::jsonb,
  status text not null default 'pending' check (status in ('pending', 'approved', 'rejected', 'superseded', 'auto_hit')),
  reviewed_by text,
  reviewed_at timestamptz,
  review_notes text,
  created_at timestamptz not null default now(),
  constraint prediction_reconciliation_candidates_canonical_fk
    foreign key (canonical_id)
    references public.canonical_tenders(canonical_id)
    on delete set null,
  constraint prediction_reconciliation_candidates_notice_fk
    foreign key (notice_id)
    references public.notices(notice_id)
    on delete set null
);

create index if not exists idx_prediction_reconciliation_candidates_tenant_status
  on public.prediction_reconciliation_candidates (tenant_id, status, created_at desc);

create index if not exists idx_prediction_reconciliation_candidates_lifecycle
  on public.prediction_reconciliation_candidates (tenant_id, lifecycle_id, created_at desc);

create table if not exists public.prediction_reconciliation_log (
  log_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  lifecycle_id uuid,
  canonical_id text,
  notice_id uuid,
  event_type text not null check (event_type in ('notice_seen', 'hit', 'miss', 'needs_review', 'withdrawn', 'status_update')),
  match_score numeric(6,3),
  reasons jsonb not null default '{}'::jsonb,
  decision text,
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint prediction_reconciliation_log_lifecycle_fk
    foreign key (lifecycle_id)
    references public.prediction_lifecycle(lifecycle_id)
    on delete set null,
  constraint prediction_reconciliation_log_canonical_fk
    foreign key (canonical_id)
    references public.canonical_tenders(canonical_id)
    on delete set null,
  constraint prediction_reconciliation_log_notice_fk
    foreign key (notice_id)
    references public.notices(notice_id)
    on delete set null
);

create index if not exists idx_prediction_reconciliation_log_tenant_created
  on public.prediction_reconciliation_log (tenant_id, created_at desc);

create index if not exists idx_prediction_reconciliation_log_tenant_lifecycle
  on public.prediction_reconciliation_log (tenant_id, lifecycle_id, created_at desc);

create index if not exists idx_canonical_tenders_tenant_buyer_entity_pub
  on public.canonical_tenders (tenant_id, buyer_entity_id, publication_date desc);

create index if not exists idx_canonical_notice_links_tenant_canonical_primary_linked
  on public.canonical_notice_links (tenant_id, canonical_id, is_primary, linked_at desc);

create or replace function public.set_prediction_lifecycle_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists trg_prediction_lifecycle_updated_at on public.prediction_lifecycle;
create trigger trg_prediction_lifecycle_updated_at
before update on public.prediction_lifecycle
for each row
execute function public.set_prediction_lifecycle_updated_at();

create or replace function public.resolve_report_cpv_cluster_id(
  p_tenant_id text,
  p_cpv_cluster_id text default null,
  p_category text default null
)
returns text
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_cluster text;
begin
  v_cluster := nullif(lower(trim(coalesce(p_cpv_cluster_id, ''))), '');
  if v_cluster is not null then
    return v_cluster;
  end if;

  if nullif(trim(coalesce(p_category, '')), '') is not null then
    select c.cpv_cluster_id
      into v_cluster
    from public.cpv_cluster c
    where c.tenant_id = p_tenant_id
      and (
        lower(c.cpv_cluster_id) = lower(trim(p_category))
        or lower(c.label) = lower(trim(p_category))
      )
    order by case when lower(c.cpv_cluster_id) = lower(trim(p_category)) then 0 else 1 end
    limit 1;
  end if;

  return coalesce(v_cluster, 'cluster_unknown');
end;
$$;

create or replace function public.get_buyer_category_timeline_rows(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text default null,
  p_category text default null,
  p_region text default null,
  p_date_from date default date '2012-01-01'
)
returns table (
  notice_id text,
  award_id text,
  contract_id text,
  publication_date date,
  award_date date,
  contract_start_date date,
  contract_end_date date,
  extension_allowed text,
  supplier text,
  source_refs jsonb
)
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_cluster_id text;
  v_region text;
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;
  if coalesce(trim(p_buyer_entity_id), '') = '' then
    raise exception 'p_buyer_entity_id is required';
  end if;

  v_cluster_id := public.resolve_report_cpv_cluster_id(p_tenant_id, p_cpv_cluster_id, p_category);
  v_region := case
    when nullif(trim(coalesce(p_region, '')), '') is null then null
    else public.normalize_prediction_region(p_region, null)
  end;

  return query
  with notice_scope as (
    select
      ct.canonical_id,
      ct.source,
      coalesce(ct.source_url, ct.primary_source_url) as source_url,
      ct.country,
      ct.title,
      ct.status,
      ct.normalized_json,
      ct.publication_date,
      coalesce(resolved.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id
    from public.canonical_tenders ct
    left join lateral (
      select r.cpv_cluster_id
      from public.resolve_cpv_cluster_id(
        p_tenant_id,
        case when ct.cpv_codes is null or array_length(ct.cpv_codes, 1) = 0 then null else ct.cpv_codes end,
        coalesce(ct.category, '')
      ) r
    ) resolved on true
    where ct.tenant_id = p_tenant_id
      and ct.buyer_entity_id = p_buyer_entity_id
      and coalesce(ct.publication_date, date '1900-01-01') >= p_date_from
      and (v_region is null or public.normalize_prediction_region(coalesce(ct.country, ''), ct.source) = v_region)
      and coalesce(resolved.cpv_cluster_id, 'cluster_unknown') = v_cluster_id
  ),
  awards as (
    select
      af.award_canonical_id as canonical_id,
      'IE'::text as region,
      af.award_date,
      af.start_date,
      af.end_date,
      af.framework_flag,
      af.supplier_name_raw,
      af.framework_evidence
    from public.award_fact_ie af
    where af.tenant_id = p_tenant_id
    union all
    select
      af.award_canonical_id,
      'FR'::text,
      af.award_date,
      af.start_date,
      af.end_date,
      af.framework_flag,
      af.supplier_name_raw,
      af.framework_evidence
    from public.award_fact_fr af
    where af.tenant_id = p_tenant_id
    union all
    select
      af.award_canonical_id,
      'ES'::text,
      af.award_date,
      af.start_date,
      af.end_date,
      af.framework_flag,
      af.supplier_name_raw,
      af.framework_evidence
    from public.award_fact_es af
    where af.tenant_id = p_tenant_id
  ),
  rows as (
    select
      n.canonical_id as notice_id,
      a.canonical_id as award_id,
      coalesce(a.canonical_id, n.canonical_id) as contract_id,
      n.publication_date,
      a.award_date,
      a.start_date as contract_start_date,
      a.end_date as contract_end_date,
      case
        when lower(coalesce(
          n.normalized_json->>'extension_allowed',
          n.normalized_json->>'extensions_allowed',
          n.normalized_json->>'renewal_possible',
          a.framework_evidence->>'extension_allowed',
          ''
        )) in ('true', 'yes', '1', 'y') then 'yes'
        when lower(coalesce(
          n.normalized_json->>'extension_allowed',
          n.normalized_json->>'extensions_allowed',
          n.normalized_json->>'renewal_possible',
          a.framework_evidence->>'extension_allowed',
          ''
        )) in ('false', 'no', '0', 'n') then 'no'
        else 'unknown'
      end as extension_allowed,
      coalesce(
        nullif(a.supplier_name_raw, ''),
        nullif(n.normalized_json->>'supplier_name', ''),
        nullif(n.normalized_json->>'supplier', ''),
        nullif(n.normalized_json->>'winner_name', ''),
        nullif(n.normalized_json->>'contractor', '')
      ) as supplier,
      jsonb_strip_nulls(jsonb_build_object(
        'source', n.source,
        'source_url', n.source_url,
        'canonical_id', n.canonical_id,
        'country', n.country,
        'title', n.title,
        'status', n.status
      )) as source_refs
    from notice_scope n
    left join awards a
      on a.canonical_id = n.canonical_id
     and (v_region is null or a.region = v_region)
  )
  select
    r.notice_id,
    r.award_id,
    r.contract_id,
    r.publication_date,
    r.award_date,
    r.contract_start_date,
    r.contract_end_date,
    r.extension_allowed,
    r.supplier,
    r.source_refs
  from rows r
  order by coalesce(r.publication_date, r.award_date, r.contract_start_date, r.contract_end_date) asc nulls last, r.notice_id;
end;
$$;

create or replace function public.upsert_prediction_lifecycle_for_pair(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_status text default 'Monitoring',
  p_model_version text default 'v1.0.0'
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_row record;
  v_lifecycle_id uuid;
  v_status text := coalesce(nullif(trim(p_status), ''), 'Monitoring');
  v_model_version text := coalesce(nullif(trim(p_model_version), ''), 'v1.0.0');
  v_region text := coalesce(public.normalize_prediction_region(p_region, null), 'IE');
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;
  if coalesce(trim(p_buyer_entity_id), '') = '' then
    raise exception 'p_buyer_entity_id is required';
  end if;
  if coalesce(trim(p_cpv_cluster_id), '') = '' then
    raise exception 'p_cpv_cluster_id is required';
  end if;

  if v_status not in ('Draft', 'Published', 'Monitoring', 'Hit', 'Miss', 'Withdrawn', 'NeedsReview') then
    raise exception 'invalid status: %', v_status;
  end if;

  select
    pc.prediction_id,
    pc.tenant_id,
    pc.buyer_entity_id,
    pc.cpv_cluster_id,
    pc.region,
    pc.expected_window_start,
    pc.expected_window_end,
    pc.probability,
    pc.confidence,
    pc.explainability_json,
    coalesce(pis.formula_version, v_model_version) as formula_version,
    coalesce((
      select jsonb_agg(ref)
      from (
        select distinct trim(e) as ref
        from public.prediction_drivers d
        cross join lateral unnest(coalesce(d.evidence_refs, '{}'::text[])) as e
        where d.tenant_id = pc.tenant_id
          and d.prediction_id = pc.prediction_id
          and trim(e) <> ''
        limit 50
      ) refs
    ), '[]'::jsonb) as evidence_refs_json,
    coalesce((
      select jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'driver_type', d.driver_type,
        'label', d.label,
        'contribution', d.contribution,
        'narrative', d.narrative,
        'evidence_refs', coalesce(to_jsonb(d.evidence_refs), '[]'::jsonb)
      )) order by d.contribution desc)
      from (
        select d.*
        from public.prediction_drivers d
        where d.tenant_id = pc.tenant_id
          and d.prediction_id = pc.prediction_id
        order by d.contribution desc
        limit 5
      ) d
    ), '[]'::jsonb) as top_drivers_json,
    coalesce((
      select count(*)
      from public.prediction_drivers d
      where d.tenant_id = pc.tenant_id
        and d.prediction_id = pc.prediction_id
    ), 0) as evidence_count
  into v_row
  from public.predictions_current pc
  left join lateral (
    select i.formula_version
    from public.prediction_inputs_snapshot i
    where i.tenant_id = pc.tenant_id
      and i.prediction_id = pc.prediction_id
    order by i.computed_at desc
    limit 1
  ) pis on true
  where pc.tenant_id = p_tenant_id
    and pc.buyer_entity_id = p_buyer_entity_id
    and pc.cpv_cluster_id = p_cpv_cluster_id
    and pc.region = v_region
  limit 1;

  if not found then
    return null;
  end if;

  insert into public.prediction_lifecycle (
    tenant_id,
    prediction_id,
    buyer_entity_id,
    cpv_cluster_id,
    region,
    status,
    contract_status,
    predicted_window_start,
    predicted_window_end,
    forecast_probability,
    confidence_score,
    evidence_count,
    evidence_refs,
    top_drivers,
    model_version,
    source_prediction_json
  )
  values (
    v_row.tenant_id,
    v_row.prediction_id,
    v_row.buyer_entity_id,
    v_row.cpv_cluster_id,
    v_row.region,
    v_status,
    case
      when v_row.expected_window_end < current_date then 'RetenderInProgress'
      when v_row.expected_window_end <= current_date + 90 then 'EndingSoon'
      else 'Active'
    end,
    v_row.expected_window_start,
    v_row.expected_window_end,
    coalesce(v_row.probability, 0),
    coalesce(v_row.confidence, 0),
    coalesce(v_row.evidence_count, 0),
    coalesce(v_row.evidence_refs_json, '[]'::jsonb),
    coalesce(v_row.top_drivers_json, '[]'::jsonb),
    coalesce(v_row.formula_version, v_model_version),
    jsonb_strip_nulls(jsonb_build_object(
      'prediction_id', v_row.prediction_id,
      'explainability', coalesce(v_row.explainability_json, '{}'::jsonb),
      'seeded_from', 'predictions_current'
    ))
  )
  on conflict (tenant_id, buyer_entity_id, cpv_cluster_id, region, predicted_window_start, predicted_window_end, model_version)
  do update set
    prediction_id = excluded.prediction_id,
    forecast_probability = excluded.forecast_probability,
    confidence_score = excluded.confidence_score,
    evidence_count = excluded.evidence_count,
    evidence_refs = excluded.evidence_refs,
    top_drivers = excluded.top_drivers,
    source_prediction_json = excluded.source_prediction_json,
    contract_status = case
      when public.prediction_lifecycle.status in ('Hit', 'Miss', 'Withdrawn') then public.prediction_lifecycle.contract_status
      else excluded.contract_status
    end,
    status = case
      when public.prediction_lifecycle.status in ('Hit', 'Miss', 'Withdrawn') then public.prediction_lifecycle.status
      else excluded.status
    end,
    updated_at = now()
  returning lifecycle_id into v_lifecycle_id;

  return v_lifecycle_id;
end;
$$;

create or replace function public.bootstrap_prediction_lifecycle_from_current(
  p_tenant_id text,
  p_status text default 'Monitoring',
  p_model_version text default 'v1.0.0',
  p_limit integer default 5000
)
returns table (
  processed_count integer,
  upserted_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_processed integer := 0;
  v_upserted integer := 0;
  v_id uuid;
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;

  for r in
    select
      pc.buyer_entity_id,
      pc.cpv_cluster_id,
      pc.region
    from public.predictions_current pc
    where pc.tenant_id = p_tenant_id
    order by pc.forecast_score desc, pc.last_computed_at desc
    limit greatest(1, p_limit)
  loop
    v_processed := v_processed + 1;
    v_id := public.upsert_prediction_lifecycle_for_pair(
      p_tenant_id,
      r.buyer_entity_id,
      r.cpv_cluster_id,
      r.region,
      p_status,
      p_model_version
    );

    if v_id is not null then
      v_upserted := v_upserted + 1;
    end if;
  end loop;

  return query select v_processed, v_upserted;
end;
$$;

create or replace function public.record_prediction_reconciliation_log(
  p_tenant_id text,
  p_lifecycle_id uuid,
  p_canonical_id text,
  p_notice_id uuid,
  p_event_type text,
  p_match_score numeric,
  p_reasons jsonb default '{}'::jsonb,
  p_decision text default null,
  p_details jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_log_id uuid;
begin
  insert into public.prediction_reconciliation_log (
    tenant_id,
    lifecycle_id,
    canonical_id,
    notice_id,
    event_type,
    match_score,
    reasons,
    decision,
    details
  )
  values (
    p_tenant_id,
    p_lifecycle_id,
    p_canonical_id,
    p_notice_id,
    p_event_type,
    p_match_score,
    coalesce(p_reasons, '{}'::jsonb),
    p_decision,
    coalesce(p_details, '{}'::jsonb)
  )
  returning log_id into v_log_id;

  return v_log_id;
end;
$$;

create or replace function public.reconcile_prediction_for_canonical_notice(
  p_tenant_id text,
  p_canonical_id text,
  p_notice_id uuid default null,
  p_grace_days integer default 30,
  p_model_version text default 'v1.0.0'
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_notice record;
  v_best record;
  v_has_best boolean := false;
  v_candidate record;
  v_reasons jsonb := '{}'::jsonb;
  v_outcome text := 'no_match';
  v_scanned integer := 0;
  v_reviewed integer := 0;
  v_hit_threshold numeric := 0.80;
  v_review_threshold numeric := 0.60;
  v_notice_type text := '';
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;
  if coalesce(trim(p_canonical_id), '') = '' then
    raise exception 'p_canonical_id is required';
  end if;

  select
    ct.canonical_id,
    ct.buyer_entity_id,
    ct.publication_date,
    coalesce(nt.notice_id, p_notice_id) as notice_id,
    coalesce(nt.source_url, ct.source_url, ct.primary_source_url) as source_url,
    lower(coalesce(
      ct.normalized_json->>'notice_type',
      nt.status,
      ct.status,
      ''
    )) as notice_type,
    coalesce(resolved.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
    coalesce(public.normalize_prediction_region(coalesce(ct.country, ''), ct.source), 'IE') as region,
    ct.title
  into v_notice
  from public.canonical_tenders ct
  left join lateral (
    select r.cpv_cluster_id
    from public.resolve_cpv_cluster_id(
      p_tenant_id,
      case when ct.cpv_codes is null or array_length(ct.cpv_codes, 1) = 0 then null else ct.cpv_codes end,
      coalesce(ct.category, '')
    ) r
  ) resolved on true
  left join public.canonical_notice_links cnl
    on cnl.tenant_id = ct.tenant_id
   and cnl.canonical_id = ct.canonical_id
   and cnl.is_primary = true
  left join public.notices nt
    on nt.tenant_id = ct.tenant_id
   and nt.notice_id = coalesce(p_notice_id, cnl.notice_id)
  where ct.tenant_id = p_tenant_id
    and ct.canonical_id = p_canonical_id
  order by cnl.linked_at desc nulls last
  limit 1;

  if not found then
    return jsonb_build_object(
      'success', false,
      'reason', 'canonical_not_found',
      'canonical_id', p_canonical_id
    );
  end if;

  if v_notice.buyer_entity_id is null or v_notice.publication_date is null then
    return jsonb_build_object(
      'success', false,
      'reason', 'insufficient_notice_context',
      'canonical_id', p_canonical_id,
      'buyer_entity_id', v_notice.buyer_entity_id,
      'publication_date', v_notice.publication_date
    );
  end if;

  v_notice_type := coalesce(v_notice.notice_type, '');

  for v_candidate in
    with candidates as (
      select
        pl.lifecycle_id,
        pl.buyer_entity_id,
        pl.cpv_cluster_id,
        pl.region,
        pl.predicted_window_start,
        pl.predicted_window_end,
        pl.grace_days,
        pl.status,
        case
          when v_notice.publication_date between pl.predicted_window_start and pl.predicted_window_end then true
          else false
        end as within_window,
        case
          when v_notice.publication_date between
               (pl.predicted_window_start - greatest(0, coalesce(pl.grace_days, p_grace_days)))
               and (pl.predicted_window_end + greatest(0, coalesce(pl.grace_days, p_grace_days)))
          then true
          else false
        end as within_grace,
        greatest(
          0,
          least(
            1,
            (
              case when pl.buyer_entity_id = v_notice.buyer_entity_id then 0.55 else 0 end +
              case
                when pl.cpv_cluster_id = v_notice.cpv_cluster_id then 0.25
                when pl.cpv_cluster_id = 'cluster_unknown' then 0.05
                else 0
              end +
              case when v_notice.publication_date between pl.predicted_window_start and pl.predicted_window_end then 0.20 else 0 end +
              case
                when v_notice.publication_date between
                     (pl.predicted_window_start - greatest(0, coalesce(pl.grace_days, p_grace_days)))
                     and (pl.predicted_window_end + greatest(0, coalesce(pl.grace_days, p_grace_days)))
                     and not (v_notice.publication_date between pl.predicted_window_start and pl.predicted_window_end)
                then 0.10
                else 0
              end
            )
          )
        )::numeric(6,3) as match_score,
        abs(
          v_notice.publication_date -
          (
            pl.predicted_window_start + ((pl.predicted_window_end - pl.predicted_window_start) / 2)
          )
        ) as days_from_mid
      from public.prediction_lifecycle pl
      where pl.tenant_id = p_tenant_id
        and pl.buyer_entity_id = v_notice.buyer_entity_id
        and pl.region = v_notice.region
        and pl.status in ('Draft', 'Published', 'Monitoring', 'NeedsReview')
        and (
          pl.cpv_cluster_id = v_notice.cpv_cluster_id
          or (v_notice.cpv_cluster_id = 'cluster_unknown' and pl.cpv_cluster_id = 'cluster_unknown')
        )
    )
    select *
    from candidates
    order by match_score desc, days_from_mid asc, predicted_window_start desc
    limit 5
  loop
    v_scanned := v_scanned + 1;

    if not v_has_best then
      v_best := v_candidate;
      v_has_best := true;
    end if;

    if v_candidate.match_score >= v_review_threshold then
      insert into public.prediction_reconciliation_candidates (
        tenant_id,
        lifecycle_id,
        canonical_id,
        notice_id,
        match_score,
        within_window,
        within_grace,
        reasons,
        status
      )
      select
        p_tenant_id,
        v_candidate.lifecycle_id,
        v_notice.canonical_id,
        v_notice.notice_id,
        v_candidate.match_score,
        v_candidate.within_window,
        v_candidate.within_grace,
        jsonb_build_object(
          'buyer_match', true,
          'cluster_match', (v_candidate.cpv_cluster_id = v_notice.cpv_cluster_id),
          'within_window', v_candidate.within_window,
          'within_grace', v_candidate.within_grace,
          'publication_date', v_notice.publication_date,
          'predicted_window_start', v_candidate.predicted_window_start,
          'predicted_window_end', v_candidate.predicted_window_end,
          'days_from_mid', v_candidate.days_from_mid
        ),
        case when v_candidate.within_window and v_candidate.match_score >= v_hit_threshold then 'auto_hit' else 'pending' end
      where not exists (
        select 1
        from public.prediction_reconciliation_candidates c
        where c.tenant_id = p_tenant_id
          and c.lifecycle_id = v_candidate.lifecycle_id
          and c.canonical_id = v_notice.canonical_id
          and coalesce(c.notice_id, '00000000-0000-0000-0000-000000000000'::uuid)
            = coalesce(v_notice.notice_id, '00000000-0000-0000-0000-000000000000'::uuid)
          and c.status in ('pending', 'auto_hit')
      );
      v_reviewed := v_reviewed + 1;
    end if;
  end loop;

  if not v_has_best then
    return jsonb_build_object(
      'success', true,
      'outcome', 'no_open_prediction',
      'canonical_id', v_notice.canonical_id,
      'notice_id', v_notice.notice_id,
      'buyer_entity_id', v_notice.buyer_entity_id,
      'cpv_cluster_id', v_notice.cpv_cluster_id,
      'region', v_notice.region
    );
  end if;

  v_reasons := jsonb_build_object(
    'buyer_match', true,
    'cluster_match', (v_best.cpv_cluster_id = v_notice.cpv_cluster_id),
    'within_window', v_best.within_window,
    'within_grace', v_best.within_grace,
    'publication_date', v_notice.publication_date,
    'predicted_window_start', v_best.predicted_window_start,
    'predicted_window_end', v_best.predicted_window_end,
    'days_from_mid', v_best.days_from_mid,
    'notice_title', coalesce(v_notice.title, ''),
    'notice_type', v_notice_type
  );

  if v_best.within_window and v_best.match_score >= v_hit_threshold then
    update public.prediction_lifecycle pl
    set
      status = 'Hit',
      contract_status = case
        when v_notice_type like '%award%' then 'Replaced'
        when v_notice_type ~ '(extend|extension|prolong|amend|avenant)' then 'Extended'
        else 'RetenderInProgress'
      end,
      linked_notice_id = v_notice.notice_id,
      linked_canonical_id = v_notice.canonical_id,
      matching_score = v_best.match_score,
      matching_reasons = v_reasons,
      hit_at = now(),
      last_reconciled_at = now(),
      updated_at = now()
    where pl.lifecycle_id = v_best.lifecycle_id
      and pl.tenant_id = p_tenant_id;

    insert into public.prediction_cycle_history (
      tenant_id,
      lifecycle_id,
      buyer_entity_id,
      cpv_cluster_id,
      region,
      canonical_id,
      notice_id,
      outcome_status,
      publication_date,
      predicted_window_start,
      predicted_window_end,
      in_window,
      match_score,
      source_refs
    )
    values (
      p_tenant_id,
      v_best.lifecycle_id,
      v_best.buyer_entity_id,
      v_best.cpv_cluster_id,
      v_best.region,
      v_notice.canonical_id,
      v_notice.notice_id,
      'Hit',
      v_notice.publication_date,
      v_best.predicted_window_start,
      v_best.predicted_window_end,
      true,
      v_best.match_score,
      jsonb_strip_nulls(jsonb_build_object(
        'source_url', v_notice.source_url,
        'canonical_id', v_notice.canonical_id,
        'notice_id', v_notice.notice_id
      ))
    );

    perform public.record_prediction_reconciliation_log(
      p_tenant_id,
      v_best.lifecycle_id,
      v_notice.canonical_id,
      v_notice.notice_id,
      'hit',
      v_best.match_score,
      v_reasons,
      'auto',
      jsonb_build_object('trigger', 'canonical_notice_link')
    );

    perform public.refresh_buyer_category_stats_for_pair(
      p_tenant_id,
      v_best.buyer_entity_id,
      v_best.cpv_cluster_id,
      v_best.region,
      now()
    );

    perform public.compute_prediction_for_pair_v1(
      p_tenant_id,
      v_best.buyer_entity_id,
      v_best.cpv_cluster_id,
      v_best.region,
      coalesce(nullif(trim(p_model_version), ''), 'v1.0.0'),
      now()
    );

    perform public.upsert_prediction_lifecycle_for_pair(
      p_tenant_id,
      v_best.buyer_entity_id,
      v_best.cpv_cluster_id,
      v_best.region,
      'Draft',
      coalesce(nullif(trim(p_model_version), ''), 'v1.0.0')
    );

    v_outcome := 'hit';
  elsif v_best.within_grace and v_best.match_score >= v_review_threshold then
    update public.prediction_lifecycle pl
    set
      status = 'NeedsReview',
      needs_review_at = now(),
      matching_score = v_best.match_score,
      matching_reasons = v_reasons,
      last_reconciled_at = now(),
      updated_at = now()
    where pl.lifecycle_id = v_best.lifecycle_id
      and pl.tenant_id = p_tenant_id
      and pl.status in ('Draft', 'Published', 'Monitoring', 'NeedsReview');

    perform public.record_prediction_reconciliation_log(
      p_tenant_id,
      v_best.lifecycle_id,
      v_notice.canonical_id,
      v_notice.notice_id,
      'needs_review',
      v_best.match_score,
      v_reasons,
      'auto',
      jsonb_build_object('trigger', 'canonical_notice_link')
    );

    insert into public.prediction_cycle_history (
      tenant_id,
      lifecycle_id,
      buyer_entity_id,
      cpv_cluster_id,
      region,
      canonical_id,
      notice_id,
      outcome_status,
      publication_date,
      predicted_window_start,
      predicted_window_end,
      in_window,
      match_score,
      source_refs
    )
    values (
      p_tenant_id,
      v_best.lifecycle_id,
      v_best.buyer_entity_id,
      v_best.cpv_cluster_id,
      v_best.region,
      v_notice.canonical_id,
      v_notice.notice_id,
      'NeedsReview',
      v_notice.publication_date,
      v_best.predicted_window_start,
      v_best.predicted_window_end,
      v_best.within_window,
      v_best.match_score,
      jsonb_strip_nulls(jsonb_build_object(
        'source_url', v_notice.source_url,
        'canonical_id', v_notice.canonical_id,
        'notice_id', v_notice.notice_id
      ))
    );

    v_outcome := 'needs_review';
  else
    perform public.record_prediction_reconciliation_log(
      p_tenant_id,
      v_best.lifecycle_id,
      v_notice.canonical_id,
      v_notice.notice_id,
      'notice_seen',
      v_best.match_score,
      v_reasons,
      'auto',
      jsonb_build_object('trigger', 'canonical_notice_link', 'outcome', 'no_match')
    );

    v_outcome := 'no_match';
  end if;

  return jsonb_build_object(
    'success', true,
    'outcome', v_outcome,
    'canonical_id', v_notice.canonical_id,
    'notice_id', v_notice.notice_id,
    'buyer_entity_id', v_notice.buyer_entity_id,
    'cpv_cluster_id', v_notice.cpv_cluster_id,
    'region', v_notice.region,
    'best_lifecycle_id', v_best.lifecycle_id,
    'best_match_score', v_best.match_score,
    'scanned_candidates', v_scanned,
    'queued_candidates', v_reviewed
  );
end;
$$;

create or replace function public.resolve_prediction_reconciliation_candidate(
  p_tenant_id text,
  p_candidate_id uuid,
  p_decision text,
  p_reviewed_by text default null,
  p_review_notes text default null
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_candidate record;
  v_open_pending integer := 0;
  v_decision text := lower(trim(coalesce(p_decision, '')));
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;
  if p_candidate_id is null then
    raise exception 'p_candidate_id is required';
  end if;
  if v_decision not in ('approve', 'reject') then
    raise exception 'p_decision must be approve|reject';
  end if;

  select c.*,
         pl.buyer_entity_id,
         pl.cpv_cluster_id,
         pl.region,
         pl.predicted_window_start,
         pl.predicted_window_end
  into v_candidate
  from public.prediction_reconciliation_candidates c
  join public.prediction_lifecycle pl
    on pl.lifecycle_id = c.lifecycle_id
   and pl.tenant_id = c.tenant_id
  where c.tenant_id = p_tenant_id
    and c.candidate_id = p_candidate_id
  limit 1;

  if not found then
    return jsonb_build_object('success', false, 'reason', 'candidate_not_found');
  end if;

  if v_decision = 'approve' then
    update public.prediction_reconciliation_candidates
    set
      status = 'approved',
      reviewed_by = coalesce(nullif(trim(coalesce(p_reviewed_by, '')), ''), 'admin'),
      reviewed_at = now(),
      review_notes = p_review_notes
    where tenant_id = p_tenant_id
      and candidate_id = p_candidate_id;

    update public.prediction_lifecycle
    set
      status = 'Hit',
      contract_status = 'RetenderInProgress',
      linked_canonical_id = v_candidate.canonical_id,
      linked_notice_id = v_candidate.notice_id,
      matching_score = v_candidate.match_score,
      matching_reasons = coalesce(v_candidate.reasons, '{}'::jsonb),
      hit_at = coalesce(hit_at, now()),
      last_reconciled_at = now(),
      updated_at = now()
    where tenant_id = p_tenant_id
      and lifecycle_id = v_candidate.lifecycle_id;

    update public.prediction_reconciliation_candidates
    set status = 'superseded'
    where tenant_id = p_tenant_id
      and lifecycle_id = v_candidate.lifecycle_id
      and candidate_id <> p_candidate_id
      and status = 'pending';

    perform public.record_prediction_reconciliation_log(
      p_tenant_id,
      v_candidate.lifecycle_id,
      v_candidate.canonical_id,
      v_candidate.notice_id,
      'hit',
      v_candidate.match_score,
      coalesce(v_candidate.reasons, '{}'::jsonb),
      'manual_approve',
      jsonb_build_object('review_notes', p_review_notes)
    );

    return jsonb_build_object(
      'success', true,
      'outcome', 'approved_hit',
      'lifecycle_id', v_candidate.lifecycle_id,
      'candidate_id', p_candidate_id
    );
  end if;

  update public.prediction_reconciliation_candidates
  set
    status = 'rejected',
    reviewed_by = coalesce(nullif(trim(coalesce(p_reviewed_by, '')), ''), 'admin'),
    reviewed_at = now(),
    review_notes = p_review_notes
  where tenant_id = p_tenant_id
    and candidate_id = p_candidate_id;

  select count(*)
    into v_open_pending
  from public.prediction_reconciliation_candidates c
  where c.tenant_id = p_tenant_id
    and c.lifecycle_id = v_candidate.lifecycle_id
    and c.status = 'pending';

  if v_open_pending = 0 then
    update public.prediction_lifecycle
    set
      status = case when status = 'NeedsReview' then 'Monitoring' else status end,
      last_reconciled_at = now(),
      updated_at = now()
    where tenant_id = p_tenant_id
      and lifecycle_id = v_candidate.lifecycle_id;
  end if;

  perform public.record_prediction_reconciliation_log(
    p_tenant_id,
    v_candidate.lifecycle_id,
    v_candidate.canonical_id,
    v_candidate.notice_id,
    'status_update',
    v_candidate.match_score,
    coalesce(v_candidate.reasons, '{}'::jsonb),
    'manual_reject',
    jsonb_build_object('review_notes', p_review_notes)
  );

  return jsonb_build_object(
    'success', true,
    'outcome', 'rejected',
    'lifecycle_id', v_candidate.lifecycle_id,
    'candidate_id', p_candidate_id,
    'remaining_pending', v_open_pending
  );
end;
$$;

create or replace function public.evaluate_prediction_lifecycle_misses(
  p_tenant_id text,
  p_as_of timestamptz default now(),
  p_default_grace_days integer default 30
)
returns table (
  marked_miss integer,
  still_open integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_marked integer := 0;
  v_open integer := 0;
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;

  with to_miss as (
    update public.prediction_lifecycle pl
    set
      status = 'Miss',
      miss_at = coalesce(pl.miss_at, now()),
      contract_status = case
        when pl.contract_status = 'Active' then 'EndingSoon'
        else pl.contract_status
      end,
      matching_reasons = coalesce(pl.matching_reasons, '{}'::jsonb)
        || jsonb_build_object(
          'miss_reason', 'window_expired_no_match',
          'evaluated_as_of', (p_as_of at time zone 'utc')
        ),
      updated_at = now()
    where pl.tenant_id = p_tenant_id
      and pl.status in ('Draft', 'Published', 'Monitoring', 'NeedsReview')
      and pl.linked_canonical_id is null
      and (pl.predicted_window_end + greatest(0, coalesce(pl.grace_days, p_default_grace_days))) < p_as_of::date
    returning pl.lifecycle_id, pl.buyer_entity_id, pl.cpv_cluster_id, pl.region, pl.predicted_window_start, pl.predicted_window_end
  ),
  log_rows as (
    insert into public.prediction_reconciliation_log (
      tenant_id,
      lifecycle_id,
      event_type,
      decision,
      details
    )
    select
      p_tenant_id,
      t.lifecycle_id,
      'miss',
      'auto',
      jsonb_build_object(
        'reason', 'window_expired_no_match',
        'evaluated_as_of', (p_as_of at time zone 'utc'),
        'predicted_window_start', t.predicted_window_start,
        'predicted_window_end', t.predicted_window_end
      )
    from to_miss t
    returning 1
  )
  select count(*) into v_marked from to_miss;

  select count(*)
    into v_open
  from public.prediction_lifecycle pl
  where pl.tenant_id = p_tenant_id
    and pl.status in ('Draft', 'Published', 'Monitoring', 'NeedsReview');

  return query select v_marked, v_open;
end;
$$;

create or replace function public.get_buyer_category_cycle_trend_report(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text default null,
  p_category text default null,
  p_region text default null,
  p_date_from date default date '2012-01-01'
)
returns jsonb
language plpgsql
stable
security definer
set search_path = public
as $$
declare
  v_cluster_id text;
  v_region text;
  v_report jsonb;
begin
  if coalesce(trim(p_tenant_id), '') = '' then
    raise exception 'p_tenant_id is required';
  end if;
  if coalesce(trim(p_buyer_entity_id), '') = '' then
    raise exception 'p_buyer_entity_id is required';
  end if;

  v_cluster_id := public.resolve_report_cpv_cluster_id(p_tenant_id, p_cpv_cluster_id, p_category);
  v_region := case
    when nullif(trim(coalesce(p_region, '')), '') is null then null
    else public.normalize_prediction_region(p_region, null)
  end;

  with timeline as (
    select *
    from public.get_buyer_category_timeline_rows(
      p_tenant_id,
      p_buyer_entity_id,
      v_cluster_id,
      null,
      v_region,
      p_date_from
    )
  ),
  notices as (
    select distinct
      t.notice_id,
      t.publication_date
    from timeline t
    where t.publication_date is not null
  ),
  awards as (
    select distinct
      coalesce(t.award_id, t.contract_id) as award_id,
      t.award_date
    from timeline t
    where t.award_date is not null
  ),
  cycle_pairs as (
    select
      a.award_id,
      a.award_date,
      (
        select min(n.publication_date)
        from notices n
        where n.publication_date > a.award_date
      ) as next_publication_date
    from awards a
  ),
  cycle_values as (
    select
      c.award_id,
      c.award_date,
      c.next_publication_date,
      (c.next_publication_date - c.award_date) as cycle_days,
      row_number() over (order by c.award_date) as seq
    from cycle_pairs c
    where c.next_publication_date is not null
  ),
  cycle_metrics as (
    select
      count(*)::integer as cycle_count,
      round(avg(cycle_days)::numeric, 2) as avg_cycle_days,
      round(percentile_cont(0.5) within group (order by cycle_days)::numeric, 2) as median_cycle_days,
      round(var_samp(cycle_days)::numeric, 2) as variance_cycle_days,
      round(stddev_samp(cycle_days)::numeric, 2) as stddev_cycle_days,
      round(regr_slope(cycle_days::numeric, seq::numeric)::numeric, 4) as trend_slope_days_per_cycle
    from cycle_values
  ),
  notice_scope as (
    select
      ct.canonical_id,
      lower(coalesce(ct.normalized_json->>'notice_type', ct.status, '')) as notice_type,
      lower(coalesce(ct.title, '')) as title,
      case when coalesce(resolved.cpv_cluster_id, 'cluster_unknown') = v_cluster_id then true else false end as cpv_match,
      coalesce(public.normalize_prediction_region(coalesce(ct.country, ''), ct.source), 'IE') as region
    from public.canonical_tenders ct
    left join lateral (
      select r.cpv_cluster_id
      from public.resolve_cpv_cluster_id(
        p_tenant_id,
        case when ct.cpv_codes is null or array_length(ct.cpv_codes, 1) = 0 then null else ct.cpv_codes end,
        coalesce(ct.category, '')
      ) r
    ) resolved on true
    where ct.tenant_id = p_tenant_id
      and ct.buyer_entity_id = p_buyer_entity_id
      and coalesce(ct.publication_date, date '1900-01-01') >= p_date_from
      and (v_region is null or public.normalize_prediction_region(coalesce(ct.country, ''), ct.source) = v_region)
  ),
  leading_indicators as (
    select
      count(*) filter (
        where n.cpv_match
          and (
            n.notice_type like '%amend%'
            or n.notice_type like '%modif%'
            or n.title ~ '(amend|modif|avenant)'
          )
      )::integer as modifications_count,
      count(*) filter (
        where n.cpv_match
          and n.title ~ '(support|assistance|maintenance|helpdesk|managed service|technical support)'
      )::integer as support_tenders_count,
      count(*) filter (
        where n.cpv_match
          and (
            n.notice_type like '%extend%'
            or n.notice_type like '%renew%'
            or n.title ~ '(extension|prolongation|renewal|renouvellement|prorogation|avenant)'
          )
      )::integer as extensions_count
    from notice_scope n
  ),
  lifecycle_pred as (
    select
      pl.prediction_id,
      pl.predicted_window_start,
      pl.predicted_window_end,
      round(pl.confidence_score, 2) as confidence_score,
      pl.evidence_refs,
      pl.model_version,
      pl.status,
      pl.top_drivers
    from public.prediction_lifecycle pl
    where pl.tenant_id = p_tenant_id
      and pl.buyer_entity_id = p_buyer_entity_id
      and pl.cpv_cluster_id = v_cluster_id
      and (v_region is null or pl.region = v_region)
      and pl.status in ('Draft', 'Published', 'Monitoring', 'NeedsReview')
    order by pl.updated_at desc, pl.created_at desc
    limit 1
  ),
  current_pred as (
    select
      pc.prediction_id,
      pc.expected_window_start as predicted_window_start,
      pc.expected_window_end as predicted_window_end,
      pc.confidence::numeric(6,2) as confidence_score,
      '[]'::jsonb as evidence_refs,
      coalesce(i.formula_version, 'v1.0.0') as model_version,
      'Monitoring'::text as status,
      '[]'::jsonb as top_drivers
    from public.predictions_current pc
    left join lateral (
      select i.formula_version
      from public.prediction_inputs_snapshot i
      where i.tenant_id = pc.tenant_id
        and i.prediction_id = pc.prediction_id
      order by i.computed_at desc
      limit 1
    ) i on true
    where pc.tenant_id = p_tenant_id
      and pc.buyer_entity_id = p_buyer_entity_id
      and pc.cpv_cluster_id = v_cluster_id
      and (v_region is null or pc.region = v_region)
    order by pc.last_computed_at desc
    limit 1
  ),
  chosen_pred as (
    select * from lifecycle_pred
    union all
    select * from current_pred where not exists (select 1 from lifecycle_pred)
    limit 1
  ),
  chosen_pred_drivers as (
    select
      coalesce((
        select jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
          'label', d.label,
          'contribution', d.contribution,
          'narrative', d.narrative,
          'evidence_refs', coalesce(to_jsonb(d.evidence_refs), '[]'::jsonb)
        )))
        from (
          select d.*
          from public.prediction_drivers d
          join chosen_pred cp on cp.prediction_id = d.prediction_id
          where d.tenant_id = p_tenant_id
          order by d.contribution desc
          limit 5
        ) d
      ), coalesce((select cp.top_drivers from chosen_pred cp), '[]'::jsonb)) as top_drivers,
      coalesce((
        select jsonb_agg(distinct ref)
        from (
          select jsonb_array_elements_text(coalesce(cp.evidence_refs, '[]'::jsonb)) as ref
          from chosen_pred cp
          union all
          select trim(e) as ref
          from public.prediction_drivers d
          join chosen_pred cp on cp.prediction_id = d.prediction_id
          cross join lateral unnest(coalesce(d.evidence_refs, '{}'::text[])) as e
          where d.tenant_id = p_tenant_id
            and trim(e) <> ''
        ) x
      ), '[]'::jsonb) as evidence_links
  )
  select jsonb_build_object(
    'success', true,
    'tenant_id', p_tenant_id,
    'buyer_entity_id', p_buyer_entity_id,
    'cpv_cluster_id', v_cluster_id,
    'region', v_region,
    'date_from', p_date_from,
    'timeline', coalesce((
      select jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'notice_id', t.notice_id,
        'award_id', t.award_id,
        'contract_id', t.contract_id,
        'publication_date', t.publication_date,
        'award_date', t.award_date,
        'contract_start_date', t.contract_start_date,
        'contract_end_date', t.contract_end_date,
        'extension_allowed', t.extension_allowed,
        'supplier', t.supplier,
        'source_refs', t.source_refs
      )) order by coalesce(t.publication_date, t.award_date, t.contract_start_date, t.contract_end_date), t.notice_id)
      from timeline t
    ), '[]'::jsonb),
    'cycle_metrics', jsonb_build_object(
      'avg_cycle_days', coalesce((select avg_cycle_days from cycle_metrics), 0),
      'median_cycle_days', coalesce((select median_cycle_days from cycle_metrics), 0),
      'variance_cycle_days', coalesce((select variance_cycle_days from cycle_metrics), 0),
      'stddev_cycle_days', coalesce((select stddev_cycle_days from cycle_metrics), 0),
      'cycle_count', coalesce((select cycle_count from cycle_metrics), 0),
      'trend_slope_days_per_cycle', coalesce((select trend_slope_days_per_cycle from cycle_metrics), 0),
      'trend_direction', case
        when coalesce((select trend_slope_days_per_cycle from cycle_metrics), 0) > 0.2 then 'lengthening'
        when coalesce((select trend_slope_days_per_cycle from cycle_metrics), 0) < -0.2 then 'shortening'
        else 'stable'
      end,
      'cycle_confidence_score', coalesce((
        select round(
          greatest(
            0,
            least(
              100,
              (
                least(1.0, coalesce(cycle_count, 0)::numeric / 12.0)
                *
                (1 - least(1.0, coalesce(stddev_cycle_days, 0) / nullif(greatest(avg_cycle_days, 1), 0)))
              ) * 100
            )
          )
        , 2)
        from cycle_metrics
      ), 0),
      'leading_indicators', jsonb_build_object(
        'modifications_count', coalesce((select modifications_count from leading_indicators), 0),
        'support_tenders_count', coalesce((select support_tenders_count from leading_indicators), 0),
        'extensions_count', coalesce((select extensions_count from leading_indicators), 0)
      )
    ),
    'next_prediction_window', coalesce((
      select jsonb_build_object(
        'predicted_window_start', cp.predicted_window_start,
        'predicted_window_end', cp.predicted_window_end,
        'confidence_score', cp.confidence_score,
        'status', cp.status,
        'model_version', cp.model_version,
        'top_drivers', cpd.top_drivers,
        'evidence_links', cpd.evidence_links
      )
      from chosen_pred cp
      cross join chosen_pred_drivers cpd
      limit 1
    ), jsonb_build_object(
      'predicted_window_start', null,
      'predicted_window_end', null,
      'confidence_score', 0,
      'status', 'unavailable',
      'model_version', null,
      'top_drivers', '[]'::jsonb,
      'evidence_links', '[]'::jsonb
    ))
  )
  into v_report;

  return v_report;
end;
$$;

create or replace function public.trg_reconcile_prediction_on_canonical_link()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  begin
    if new.is_primary is true
       and coalesce(trim(new.tenant_id), '') <> ''
       and coalesce(trim(new.canonical_id), '') <> '' then
      perform public.reconcile_prediction_for_canonical_notice(
        new.tenant_id,
        new.canonical_id,
        new.notice_id,
        30,
        'v1.0.0'
      );
    end if;
  exception when others then
    -- Never block ingestion/linking due to reconciliation failures.
    raise notice 'prediction reconciliation trigger skipped for canonical_id=% error=%', new.canonical_id, sqlerrm;
  end;

  return new;
end;
$$;

drop trigger if exists trg_prediction_reconcile_on_canonical_link on public.canonical_notice_links;
create trigger trg_prediction_reconcile_on_canonical_link
after insert or update of canonical_id, notice_id, is_primary, match_score
on public.canonical_notice_links
for each row
execute function public.trg_reconcile_prediction_on_canonical_link();

-- RLS + tenant policies for new tables.
do $$
declare
  t text;
begin
  foreach t in array array[
    'prediction_lifecycle',
    'prediction_cycle_history',
    'prediction_reconciliation_candidates',
    'prediction_reconciliation_log'
  ]
  loop
    execute format('alter table public.%I enable row level security', t);
    execute format('alter table public.%I force row level security', t);

    execute format('drop policy if exists %I_select on public.%I', t || '_tenant', t);
    execute format('drop policy if exists %I_insert on public.%I', t || '_tenant', t);
    execute format('drop policy if exists %I_update on public.%I', t || '_tenant', t);
    execute format('drop policy if exists %I_delete on public.%I', t || '_tenant', t);

    execute format(
      'create policy %I_select on public.%I for select to authenticated using (tenant_id = public.current_tenant_id())',
      t || '_tenant',
      t
    );

    execute format(
      'create policy %I_insert on public.%I for insert to authenticated with check (tenant_id = public.current_tenant_id())',
      t || '_tenant',
      t
    );

    execute format(
      'create policy %I_update on public.%I for update to authenticated using (tenant_id = public.current_tenant_id()) with check (tenant_id = public.current_tenant_id())',
      t || '_tenant',
      t
    );

    execute format(
      'create policy %I_delete on public.%I for delete to authenticated using (tenant_id = public.current_tenant_id())',
      t || '_tenant',
      t
    );
  end loop;
end $$;

revoke all on function public.resolve_report_cpv_cluster_id(text, text, text) from public;
revoke all on function public.get_buyer_category_timeline_rows(text, text, text, text, text, date) from public;
revoke all on function public.get_buyer_category_cycle_trend_report(text, text, text, text, text, date) from public;
revoke all on function public.upsert_prediction_lifecycle_for_pair(text, text, text, text, text, text) from public;
revoke all on function public.bootstrap_prediction_lifecycle_from_current(text, text, text, integer) from public;
revoke all on function public.reconcile_prediction_for_canonical_notice(text, text, uuid, integer, text) from public;
revoke all on function public.resolve_prediction_reconciliation_candidate(text, uuid, text, text, text) from public;
revoke all on function public.evaluate_prediction_lifecycle_misses(text, timestamptz, integer) from public;

grant execute on function public.resolve_report_cpv_cluster_id(text, text, text) to authenticated, service_role;
grant execute on function public.get_buyer_category_timeline_rows(text, text, text, text, text, date) to authenticated, service_role;
grant execute on function public.get_buyer_category_cycle_trend_report(text, text, text, text, text, date) to authenticated, service_role;
grant execute on function public.upsert_prediction_lifecycle_for_pair(text, text, text, text, text, text) to service_role;
grant execute on function public.bootstrap_prediction_lifecycle_from_current(text, text, text, integer) to service_role;
grant execute on function public.reconcile_prediction_for_canonical_notice(text, text, uuid, integer, text) to service_role;
grant execute on function public.resolve_prediction_reconciliation_candidate(text, uuid, text, text, text) to service_role;
grant execute on function public.evaluate_prediction_lifecycle_misses(text, timestamptz, integer) to service_role;

commit;
