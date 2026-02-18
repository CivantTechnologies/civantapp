-- Civant Predictive Engine V1 (deterministic, explainable, tenant-safe)
-- Hybrid architecture: DB schema + deterministic SQL helpers + scheduled job entrypoints.

create extension if not exists pgcrypto;

-- -----------------------------
-- ENUMS
-- -----------------------------
do $$
begin
  if not exists (
    select 1
    from pg_type t
    join pg_namespace n on n.oid = t.typnamespace
    where n.nspname = 'public'
      and t.typname = 'next_window_label'
  ) then
    create type public.next_window_label as enum ('next_30', 'm1_3', 'm3_6', 'm6_12');
  end if;
end $$;

-- -----------------------------
-- MAPPING TABLES
-- -----------------------------
create table if not exists public.cpv_cluster (
  tenant_id text not null,
  cpv_cluster_id text not null,
  label text not null,
  description text,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, cpv_cluster_id)
);

create index if not exists idx_cpv_cluster_tenant_active
  on public.cpv_cluster (tenant_id, is_active, cpv_cluster_id);

create table if not exists public.cpv_cluster_map (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  cpv_prefix text not null,
  cpv_cluster_id text not null,
  weight numeric(4,3) not null default 1.0 check (weight >= 0 and weight <= 1),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, cpv_prefix, cpv_cluster_id),
  constraint fk_cpv_cluster_map_cluster
    foreign key (tenant_id, cpv_cluster_id)
    references public.cpv_cluster(tenant_id, cpv_cluster_id)
    on delete cascade
);

create index if not exists idx_cpv_cluster_map_tenant_prefix
  on public.cpv_cluster_map (tenant_id, cpv_prefix);

create table if not exists public.buyer_aliases (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  raw_buyer_key text not null,
  source text not null,
  canonical_buyer_entity_id text not null,
  resolution_confidence numeric(4,3) not null default 1.0 check (resolution_confidence >= 0 and resolution_confidence <= 1),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, raw_buyer_key, source)
);

create index if not exists idx_buyer_aliases_lookup
  on public.buyer_aliases (tenant_id, source, raw_buyer_key);

create table if not exists public.external_signal_raw (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  source text not null,
  source_event_id text not null,
  buyer_name_raw text,
  signal_type_raw text,
  signal_strength numeric(4,3) check (signal_strength >= 0 and signal_strength <= 1),
  occurred_at timestamptz,
  source_url text,
  payload_json jsonb not null default '{}'::jsonb,
  ingested_at timestamptz not null default now(),
  unique (tenant_id, source, source_event_id)
);

create index if not exists idx_external_signal_raw_tenant_ingested
  on public.external_signal_raw (tenant_id, ingested_at desc);

create table if not exists public.signal_mappings (
  mapping_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  source text,
  signal_type text,
  raw_buyer_key text,
  raw_keyword text,
  canonical_buyer_entity_id text,
  cpv_cluster_id text,
  region text,
  strength_multiplier numeric(4,3) not null default 1.0 check (strength_multiplier >= 0 and strength_multiplier <= 2),
  priority integer not null default 100,
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint fk_signal_mappings_cluster
    foreign key (tenant_id, cpv_cluster_id)
    references public.cpv_cluster(tenant_id, cpv_cluster_id)
    on delete set null
);

create index if not exists idx_signal_mappings_lookup
  on public.signal_mappings (tenant_id, is_active, priority, source, signal_type, raw_buyer_key);

create table if not exists public.external_signal_to_cpv (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  role_keyword text not null,
  cpv_cluster_id text not null,
  weight numeric(4,3) not null default 1.0 check (weight >= 0 and weight <= 1),
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, role_keyword, cpv_cluster_id),
  constraint fk_external_signal_to_cpv_cluster
    foreign key (tenant_id, cpv_cluster_id)
    references public.cpv_cluster(tenant_id, cpv_cluster_id)
    on delete cascade
);

create index if not exists idx_external_signal_to_cpv_lookup
  on public.external_signal_to_cpv (tenant_id, is_active, role_keyword);

-- -----------------------------
-- CORE PREDICTIVE TABLES
-- -----------------------------
create table if not exists public.signals (
  signal_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  buyer_entity_id text not null,
  cpv_cluster_id text,
  region text not null check (region in ('IE', 'FR', 'ES')),
  signal_type text not null,
  signal_strength numeric(4,3) not null check (signal_strength >= 0 and signal_strength <= 1),
  occurred_at timestamptz not null,
  ingested_at timestamptz not null default now(),
  source text not null,
  source_url text,
  canonical_id text,
  source_event_id text not null,
  features_json jsonb not null default '{}'::jsonb,
  constraint fk_signals_cluster
    foreign key (tenant_id, cpv_cluster_id)
    references public.cpv_cluster(tenant_id, cpv_cluster_id)
    on delete set null
);

create unique index if not exists uq_signals_source_event
  on public.signals (tenant_id, source, source_event_id, signal_type);

create index if not exists idx_signals_tenant_ingested
  on public.signals (tenant_id, ingested_at desc);
create index if not exists idx_signals_tenant_pair
  on public.signals (tenant_id, buyer_entity_id, cpv_cluster_id, region);
create index if not exists idx_signals_type
  on public.signals (signal_type);

create index if not exists idx_canonical_tenders_tenant_updated_coalesce
  on public.canonical_tenders (tenant_id, (coalesce(updated_at, created_at)));

update public.signals
set source_event_id = md5(
  coalesce(tenant_id, '') || '|' ||
  coalesce(source, '') || '|' ||
  coalesce(signal_type, '') || '|' ||
  coalesce(buyer_entity_id, '') || '|' ||
  coalesce(cpv_cluster_id, '') || '|' ||
  coalesce(region, '') || '|' ||
  coalesce(occurred_at::text, '') || '|' ||
  coalesce(canonical_id, '') || '|' ||
  coalesce(source_url, '')
)
where source_event_id is null;

with ranked as (
  select
    signal_id,
    row_number() over (
      partition by tenant_id, source, source_event_id, signal_type
      order by ingested_at desc, signal_id desc
    ) as rn
  from public.signals
)
delete from public.signals s
using ranked r
where s.signal_id = r.signal_id
  and r.rn > 1;

drop index if exists uq_signals_source_event;
create unique index if not exists uq_signals_source_event
  on public.signals (tenant_id, source, source_event_id, signal_type);

alter table public.signals
  alter column source_event_id set not null;

create table if not exists public.buyer_category_stats (
  tenant_id text not null,
  buyer_entity_id text not null,
  cpv_cluster_id text not null,
  region text not null check (region in ('IE', 'FR', 'ES')),
  tender_count_24m integer not null default 0,
  avg_cycle_days numeric(10,2),
  std_cycle_days numeric(10,2),
  last_tender_at timestamptz,
  last_award_at timestamptz,
  seasonality_vector jsonb not null default '{}'::jsonb,
  value_stability_score numeric(4,3) not null default 0 check (value_stability_score >= 0 and value_stability_score <= 1),
  data_coverage_score numeric(4,3) not null default 0 check (data_coverage_score >= 0 and data_coverage_score <= 1),
  cadence_reliability_score numeric(4,3) not null default 0 check (cadence_reliability_score >= 0 and cadence_reliability_score <= 1),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, buyer_entity_id, cpv_cluster_id, region),
  constraint fk_buyer_category_stats_cluster
    foreign key (tenant_id, cpv_cluster_id)
    references public.cpv_cluster(tenant_id, cpv_cluster_id)
    on delete cascade
);

create index if not exists idx_buyer_category_stats_updated
  on public.buyer_category_stats (tenant_id, updated_at desc);

create table if not exists public.predictions_current (
  prediction_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  buyer_entity_id text not null,
  buyer_display_name text,
  cpv_cluster_id text not null,
  cpv_cluster_label text,
  region text not null check (region in ('IE', 'FR', 'ES')),
  next_window_label public.next_window_label not null,
  expected_window_start date not null,
  expected_window_end date not null,
  probability numeric(4,3) not null check (probability >= 0 and probability <= 1),
  confidence integer not null check (confidence >= 0 and confidence <= 100),
  confidence_band text not null check (confidence_band in ('Low', 'Medium', 'High', 'Very High')),
  forecast_score integer not null check (forecast_score >= 0 and forecast_score <= 100),
  fallback_tier integer not null default 0 check (fallback_tier between 0 and 4),
  drivers_count integer not null default 0,
  evidence_count integer not null default 0,
  explainability_json jsonb not null default '{}'::jsonb,
  last_computed_at timestamptz not null default now(),
  unique (tenant_id, buyer_entity_id, cpv_cluster_id, region),
  constraint fk_predictions_current_cluster
    foreign key (tenant_id, cpv_cluster_id)
    references public.cpv_cluster(tenant_id, cpv_cluster_id)
    on delete restrict
);

create index if not exists idx_predictions_current_tenant_score
  on public.predictions_current (tenant_id, forecast_score desc, probability desc);
create index if not exists idx_predictions_current_tenant_window
  on public.predictions_current (tenant_id, next_window_label, expected_window_start);

create table if not exists public.prediction_drivers (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  prediction_id uuid not null references public.predictions_current(prediction_id) on delete cascade,
  driver_type text not null,
  label text not null,
  contribution numeric(6,3) not null,
  narrative text not null,
  evidence_refs text[] not null default '{}'::text[],
  created_at timestamptz not null default now()
);

create index if not exists idx_prediction_drivers_prediction
  on public.prediction_drivers (tenant_id, prediction_id, created_at desc);

create table if not exists public.prediction_scorecard (
  prediction_id uuid primary key references public.predictions_current(prediction_id) on delete cascade,
  tenant_id text not null,
  cycle_score integer not null check (cycle_score between 0 and 20),
  timing_score integer not null check (timing_score between 0 and 15),
  behavioural_score integer not null check (behavioural_score between 0 and 15),
  structural_score integer not null check (structural_score between 0 and 10),
  external_signal_score integer not null check (external_signal_score between 0 and 20),
  data_quality_score integer not null check (data_quality_score between 0 and 20),
  total_score integer not null check (total_score between 0 and 100),
  created_at timestamptz not null default now()
);

create index if not exists idx_prediction_scorecard_tenant
  on public.prediction_scorecard (tenant_id, total_score desc);

create table if not exists public.prediction_runs (
  run_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  run_type text not null check (run_type in ('incremental', 'full', 'signals_only')),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  pairs_processed integer not null default 0,
  status text not null check (status in ('running', 'success', 'failed')),
  error_message text,
  metadata_json jsonb not null default '{}'::jsonb
);

create index if not exists idx_prediction_runs_tenant_started
  on public.prediction_runs (tenant_id, started_at desc);

create table if not exists public.prediction_extensions (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  prediction_id uuid not null references public.predictions_current(prediction_id) on delete cascade,
  extension_type text not null,
  payload_json jsonb not null default '{}'::jsonb,
  model_version text,
  citations text[] not null default '{}'::text[],
  created_at timestamptz not null default now()
);

create index if not exists idx_prediction_extensions_prediction
  on public.prediction_extensions (tenant_id, prediction_id, created_at desc);

create table if not exists public.prediction_inputs_snapshot (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  prediction_id uuid not null references public.predictions_current(prediction_id) on delete cascade,
  formula_version text not null,
  features_json jsonb not null,
  computed_at timestamptz not null default now()
);

create index if not exists idx_prediction_inputs_snapshot_prediction
  on public.prediction_inputs_snapshot (tenant_id, prediction_id, computed_at desc);

-- -----------------------------
-- DEFAULT TAXONOMY SEED
-- -----------------------------
insert into public.cpv_cluster (tenant_id, cpv_cluster_id, label, description)
values
  ('civant_default', 'cluster_unknown', 'Unknown / Unmapped', 'Fallback cluster when deterministic mapping is unavailable'),
  ('civant_default', 'cluster_digital', 'Digital & ICT', 'Software, systems, telecoms, cybersecurity, platforms'),
  ('civant_default', 'cluster_construction', 'Construction', 'Civil works and built environment'),
  ('civant_default', 'cluster_facilities', 'Facilities & Maintenance', 'Operational services and maintenance'),
  ('civant_default', 'cluster_professional_services', 'Professional Services', 'Consulting, legal, advisory, studies')
on conflict (tenant_id, cpv_cluster_id) do nothing;

insert into public.cpv_cluster_map (tenant_id, cpv_prefix, cpv_cluster_id, weight)
values
  ('civant_default', '48', 'cluster_digital', 1.0),
  ('civant_default', '72', 'cluster_digital', 0.9),
  ('civant_default', '30', 'cluster_digital', 0.8),
  ('civant_default', '32', 'cluster_digital', 0.8),
  ('civant_default', '45', 'cluster_construction', 1.0),
  ('civant_default', '50', 'cluster_facilities', 0.8),
  ('civant_default', '71', 'cluster_professional_services', 0.8),
  ('civant_default', '79', 'cluster_professional_services', 0.8)
on conflict (tenant_id, cpv_prefix, cpv_cluster_id) do nothing;

insert into public.external_signal_to_cpv (tenant_id, role_keyword, cpv_cluster_id, weight)
values
  ('civant_default', 'digital', 'cluster_digital', 0.9),
  ('civant_default', 'ict', 'cluster_digital', 0.9),
  ('civant_default', 'platform', 'cluster_digital', 0.85),
  ('civant_default', 'system', 'cluster_digital', 0.75),
  ('civant_default', 'construction', 'cluster_construction', 0.8),
  ('civant_default', 'infrastructure', 'cluster_construction', 0.8),
  ('civant_default', 'maintenance', 'cluster_facilities', 0.75),
  ('civant_default', 'consulting', 'cluster_professional_services', 0.75)
on conflict (tenant_id, role_keyword, cpv_cluster_id) do nothing;

-- -----------------------------
-- HELPER FUNCTIONS
-- -----------------------------
create or replace function public.normalize_match_key(p_input text)
returns text
language sql
immutable
as $$
  select nullif(trim(regexp_replace(lower(coalesce(p_input, '')), '[^a-z0-9]+', ' ', 'g')), '')
$$;

create or replace function public.normalize_prediction_region(
  p_region text,
  p_source text default null
)
returns text
language sql
immutable
as $$
  select case
    when upper(coalesce(p_region, '')) in ('IE', 'IRL', 'IRE', 'IRELAND') then 'IE'
    when upper(coalesce(p_region, '')) in ('FR', 'FRA', 'FRANCE') then 'FR'
    when upper(coalesce(p_region, '')) in ('ES', 'ESP', 'ESPAÑA', 'ESPANA', 'SPAIN') then 'ES'
    when upper(coalesce(p_source, '')) like '%ETENDERS_IE%' then 'IE'
    when upper(coalesce(p_source, '')) like '%BOAMP_FR%' then 'FR'
    when upper(coalesce(p_source, '')) like '%PLACSP_ES%' then 'ES'
    when upper(coalesce(p_source, '')) like '%TED%' and upper(coalesce(p_region, '')) = 'IRL' then 'IE'
    when upper(coalesce(p_source, '')) like '%TED%' and upper(coalesce(p_region, '')) = 'FRA' then 'FR'
    when upper(coalesce(p_source, '')) like '%TED%' and upper(coalesce(p_region, '')) = 'ESP' then 'ES'
    else null
  end
$$;

create or replace function public.ensure_prediction_baseline(
  p_tenant_id text
)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  insert into public.cpv_cluster (tenant_id, cpv_cluster_id, label, description)
  values
    (p_tenant_id, 'cluster_unknown', 'Unknown / Unmapped', 'Fallback cluster when deterministic mapping is unavailable'),
    (p_tenant_id, 'cluster_digital', 'Digital & ICT', 'Software, systems, telecoms, cybersecurity, platforms'),
    (p_tenant_id, 'cluster_construction', 'Construction', 'Civil works and built environment'),
    (p_tenant_id, 'cluster_facilities', 'Facilities & Maintenance', 'Operational services and maintenance'),
    (p_tenant_id, 'cluster_professional_services', 'Professional Services', 'Consulting, legal, advisory, studies')
  on conflict (tenant_id, cpv_cluster_id) do nothing;
end;
$$;

create or replace function public.prediction_signal_half_life_days(p_signal_type text)
returns numeric
language sql
immutable
as $$
  select case
    when lower(coalesce(p_signal_type, '')) in ('job_posting') then 45
    when lower(coalesce(p_signal_type, '')) in ('hiring_spike', 'role_category_detected') then 30
    when lower(coalesce(p_signal_type, '')) in ('budget_approval', 'budget_increase') then 120
    when lower(coalesce(p_signal_type, '')) in ('grant_awarded', 'eu_funding_allocation') then 180
    when lower(coalesce(p_signal_type, '')) in ('digital_transformation_program', 'compliance_mandate', 'accessibility_regulation', 'sustainability_requirement', 'policy_signal') then 365
    else 90
  end
$$;

create or replace function public.compute_effective_signal_strength(
  p_signal_type text,
  p_signal_strength numeric,
  p_occurred_at timestamptz,
  p_as_of timestamptz default now()
)
returns numeric
language sql
stable
as $$
  select greatest(
    0,
    least(
      1,
      coalesce(p_signal_strength, 0)
      * exp(
          - greatest(0, extract(epoch from (coalesce(p_as_of, now()) - coalesce(p_occurred_at, p_as_of))) / 86400.0)
            / nullif(public.prediction_signal_half_life_days(p_signal_type), 0)
        )
    )
  )
$$;

create or replace function public.resolve_prediction_window(
  p_days_until numeric,
  p_as_of date default current_date
)
returns table (
  next_window_label public.next_window_label,
  window_start date,
  window_end date
)
language plpgsql
stable
as $$
declare
  v_days numeric := greatest(0, coalesce(p_days_until, 0));
  v_start date := coalesce(p_as_of, current_date);
begin
  if v_days <= 30 then
    return query select 'next_30'::public.next_window_label, v_start, v_start + 30;
  elsif v_days <= 90 then
    return query select 'm1_3'::public.next_window_label, v_start + 31, v_start + 90;
  elsif v_days <= 180 then
    return query select 'm3_6'::public.next_window_label, v_start + 91, v_start + 180;
  else
    return query select 'm6_12'::public.next_window_label, v_start + 181, v_start + 365;
  end if;
end;
$$;

create or replace function public.resolve_buyer_entity_id(
  p_tenant_id text,
  p_source text,
  p_buyer_name_raw text,
  p_fallback_entity_id text default null
)
returns table (
  buyer_entity_id text,
  is_resolved boolean,
  resolution_tier text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_key text := public.normalize_match_key(p_buyer_name_raw);
  v_source text := upper(coalesce(p_source, 'UNKNOWN'));
  v_entity text;
begin
  if v_key is null then
    if nullif(p_fallback_entity_id, '') is not null then
      return query select p_fallback_entity_id, false, 'fallback_empty_buyer';
    else
      return query select 'unresolved:' || substr(md5('empty-buyer'), 1, 16), false, 'unresolved_empty_buyer';
    end if;
    return;
  end if;

  select ba.canonical_buyer_entity_id
  into v_entity
  from public.buyer_aliases ba
  where ba.tenant_id = p_tenant_id
    and ba.raw_buyer_key = v_key
    and (ba.source = v_source or ba.source = '*')
  order by case when ba.source = v_source then 0 else 1 end, ba.updated_at desc
  limit 1;

  if v_entity is not null then
    return query select v_entity, true, 'buyer_aliases';
    return;
  end if;

  select e.entity_id
  into v_entity
  from public.entities e
  join public.entity_aliases ea
    on ea.entity_id = e.entity_id
   and ea.tenant_id = e.tenant_id
  where e.tenant_id = p_tenant_id
    and lower(coalesce(e.entity_type, '')) = 'buyer'
    and public.normalize_match_key(ea.alias) = v_key
  order by coalesce(ea.confidence, 0) desc, ea.created_at desc
  limit 1;

  if v_entity is not null then
    return query select v_entity, true, 'entity_aliases';
    return;
  end if;

  if nullif(p_fallback_entity_id, '') is not null then
    return query select p_fallback_entity_id, false, 'fallback_source';
  else
    return query select 'unresolved:' || substr(md5(v_key), 1, 16), false, 'unresolved';
  end if;
end;
$$;

create or replace function public.resolve_cpv_cluster_id(
  p_tenant_id text,
  p_cpv_codes text[],
  p_text_hint text default null
)
returns table (
  cpv_cluster_id text,
  mapping_weight numeric,
  mapping_tier text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_code text;
  v_cluster text;
  v_weight numeric;
  v_hint text := lower(coalesce(p_text_hint, ''));
begin
  if p_cpv_codes is not null then
    foreach v_code in array p_cpv_codes loop
      if v_code is null then
        continue;
      end if;

      select cpm.cpv_cluster_id, cpm.weight
      into v_cluster, v_weight
      from public.cpv_cluster_map cpm
      where cpm.tenant_id = p_tenant_id
        and left(regexp_replace(v_code, '\\D', '', 'g'), length(cpm.cpv_prefix)) = cpm.cpv_prefix
      order by length(cpm.cpv_prefix) desc, cpm.weight desc
      limit 1;

      if v_cluster is not null then
        return query select v_cluster, coalesce(v_weight, 1.0), 'cpv_prefix';
        return;
      end if;
    end loop;
  end if;

  if v_hint <> '' then
    select esc.cpv_cluster_id, esc.weight
    into v_cluster, v_weight
    from public.external_signal_to_cpv esc
    where esc.tenant_id = p_tenant_id
      and esc.is_active
      and v_hint like ('%' || lower(esc.role_keyword) || '%')
    order by esc.weight desc, length(esc.role_keyword) desc
    limit 1;

    if v_cluster is not null then
      return query select v_cluster, coalesce(v_weight, 0.6), 'keyword';
      return;
    end if;
  end if;

  return query select 'cluster_unknown', 0.2::numeric, 'fallback_unknown';
end;
$$;

create or replace function public.infer_signal_type_from_procurement(
  p_notice_type text,
  p_status text
)
returns text
language sql
immutable
as $$
  select case
    when lower(coalesce(p_notice_type, '')) like '%award%' or lower(coalesce(p_status, '')) in ('award', 'awarded', 'res', 'adj') then 'award_published'
    when lower(coalesce(p_notice_type, '')) like '%framework%' then 'framework_established'
    when lower(coalesce(p_notice_type, '')) like '%cancel%' or lower(coalesce(p_status, '')) in ('cancelled', 'canceled', 'can', 'des') then 'cancellation'
    when lower(coalesce(p_notice_type, '')) like '%amend%' then 'amendment'
    else 'notice_published'
  end
$$;

create or replace function public.upsert_signal_event(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_signal_type text,
  p_signal_strength numeric,
  p_occurred_at timestamptz,
  p_source text,
  p_source_url text,
  p_canonical_id text,
  p_source_event_id text,
  p_features_json jsonb
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_signal_id uuid;
  v_region text := upper(coalesce(p_region, ''));
  v_event_id text := nullif(trim(coalesce(p_source_event_id, '')), '');
begin
  v_region := coalesce(public.normalize_prediction_region(p_region, p_source), 'IE');

  if v_event_id is null then
    v_event_id := md5(
      coalesce(p_tenant_id, '') || '|' ||
      coalesce(p_source, '') || '|' ||
      coalesce(p_signal_type, '') || '|' ||
      coalesce(p_buyer_entity_id, '') || '|' ||
      coalesce(p_cpv_cluster_id, '') || '|' ||
      coalesce(p_occurred_at::text, '') || '|' ||
      coalesce(p_canonical_id, '')
    );
  end if;

  insert into public.signals (
    tenant_id,
    buyer_entity_id,
    cpv_cluster_id,
    region,
    signal_type,
    signal_strength,
    occurred_at,
    source,
    source_url,
    canonical_id,
    source_event_id,
    features_json,
    ingested_at
  )
  values (
    p_tenant_id,
    p_buyer_entity_id,
    nullif(p_cpv_cluster_id, ''),
    v_region,
    p_signal_type,
    greatest(0, least(1, coalesce(p_signal_strength, 0))),
    coalesce(p_occurred_at, now()),
    upper(coalesce(p_source, 'UNKNOWN')),
    p_source_url,
    p_canonical_id,
    v_event_id,
    coalesce(p_features_json, '{}'::jsonb),
    now()
  )
  on conflict (tenant_id, source, source_event_id, signal_type)
  do update set
    buyer_entity_id = excluded.buyer_entity_id,
    cpv_cluster_id = excluded.cpv_cluster_id,
    region = excluded.region,
    signal_strength = excluded.signal_strength,
    occurred_at = excluded.occurred_at,
    source_url = excluded.source_url,
    canonical_id = excluded.canonical_id,
    features_json = excluded.features_json,
    ingested_at = now()
  returning signal_id into v_signal_id;

  return v_signal_id;
end;
$$;

-- -----------------------------
-- SIGNAL NORMALIZATION PIPELINE
-- -----------------------------
create or replace function public.normalize_procurement_signals(
  p_tenant_id text,
  p_since timestamptz default null,
  p_limit integer default 20000
)
returns table (
  processed_count integer,
  inserted_or_updated integer,
  unresolved_buyers integer,
  null_cluster_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_processed integer := 0;
  v_upserted integer := 0;
  v_unresolved integer := 0;
  v_null_cluster integer := 0;
  v_buyer_id text;
  v_buyer_resolved boolean;
  v_buyer_tier text;
  v_cluster_id text;
  v_cluster_weight numeric;
  v_cluster_tier text;
  v_region text;
  v_signal_type text;
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

  for r in
    select
      ct.canonical_id,
      ct.source,
      coalesce(ct.source_url, (to_jsonb(ct)->>'primary_source_url')) as source_url,
      coalesce(ct.country, ct.normalized_json->>'country', ct.normalized_json->>'country_code') as country,
      ct.title,
      ct.buyer_entity_id as buyer_id_existing,
      ct.buyer_name_raw,
      ct.buyer_name_norm,
      ct.cpv_codes,
      ct.publication_date,
      ct.updated_at,
      coalesce((to_jsonb(ct)->>'status'), ct.normalized_json->>'status') as status,
      coalesce((to_jsonb(ct)->>'notice_type'), ct.normalized_json->>'notice_type') as notice_type,
      ct.estimated_value,
      ct.normalized_json
    from public.canonical_tenders ct
    where ct.tenant_id = p_tenant_id
      and (p_since is null or coalesce(ct.updated_at, ct.created_at) >= p_since)
    order by coalesce(ct.updated_at, ct.created_at) asc
    limit greatest(1, p_limit)
  loop
    v_processed := v_processed + 1;

    select buyer_entity_id, is_resolved, resolution_tier
      into v_buyer_id, v_buyer_resolved, v_buyer_tier
    from public.resolve_buyer_entity_id(
      p_tenant_id,
      r.source,
      coalesce(r.buyer_name_raw, r.buyer_name_norm, ''),
      r.buyer_id_existing
    );

    if not coalesce(v_buyer_resolved, false) then
      v_unresolved := v_unresolved + 1;
    end if;

    select cpv_cluster_id, mapping_weight, mapping_tier
      into v_cluster_id, v_cluster_weight, v_cluster_tier
    from public.resolve_cpv_cluster_id(
      p_tenant_id,
      r.cpv_codes,
      coalesce(r.title, '')
    );

    if v_cluster_id is null or v_cluster_id = 'cluster_unknown' then
      v_null_cluster := v_null_cluster + 1;
    end if;

    v_region := coalesce(public.normalize_prediction_region(r.country, r.source), 'IE');

    v_signal_type := public.infer_signal_type_from_procurement(r.notice_type, r.status);

    perform public.upsert_signal_event(
      p_tenant_id,
      v_buyer_id,
      coalesce(v_cluster_id, 'cluster_unknown'),
      v_region,
      v_signal_type,
      case when v_signal_type = 'award_published' then 0.95 else 0.75 end,
      coalesce(r.publication_date::timestamptz, r.updated_at, now()),
      coalesce(r.source, 'CANONICAL'),
      r.source_url,
      r.canonical_id,
      r.canonical_id || ':' || v_signal_type,
      jsonb_build_object(
        'resolver', jsonb_build_object(
          'buyer_tier', coalesce(v_buyer_tier, 'unknown'),
          'cluster_tier', coalesce(v_cluster_tier, 'unknown'),
          'cluster_weight', coalesce(v_cluster_weight, 0)
        ),
        'estimated_value', r.estimated_value,
        'notice_type', r.notice_type,
        'status', r.status
      )
    );

    v_upserted := v_upserted + 1;
  end loop;

  return query select v_processed, v_upserted, v_unresolved, v_null_cluster;
end;
$$;

create or replace function public.normalize_external_signals(
  p_tenant_id text,
  p_since timestamptz default null,
  p_limit integer default 20000
)
returns table (
  processed_count integer,
  inserted_or_updated integer,
  unresolved_buyers integer,
  null_cluster_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_processed integer := 0;
  v_upserted integer := 0;
  v_unresolved integer := 0;
  v_null_cluster integer := 0;
  v_buyer_id text;
  v_buyer_resolved boolean;
  v_buyer_tier text;
  v_cluster_id text;
  v_cluster_weight numeric;
  v_cluster_tier text;
  v_signal_type text;
  v_region text;
  v_strength numeric;
  v_map_buyer_entity_id text;
  v_map_cpv_cluster_id text;
  v_map_region text;
  v_map_strength_multiplier numeric := 1;
  v_text_hint text;
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

  for r in
    with unified as (
      select tenant_id, 'EXTERNAL_IE'::text as source, signal_source, source_event_id, buyer_id, buyer_name_raw, buyer_name_normalized,
             signal_type, signal_strength, signal_date::timestamptz as occurred_at, captured_at, evidence_url, evidence_text, extracted_entities, raw_payload, country_code
      from public.external_signals_ie
      where tenant_id = p_tenant_id
        and (p_since is null or captured_at >= p_since)
      union all
      select tenant_id, 'EXTERNAL_FR'::text as source, signal_source, source_event_id, buyer_id, buyer_name_raw, buyer_name_normalized,
             signal_type, signal_strength, signal_date::timestamptz as occurred_at, captured_at, evidence_url, evidence_text, extracted_entities, raw_payload, country_code
      from public.external_signals_fr
      where tenant_id = p_tenant_id
        and (p_since is null or captured_at >= p_since)
      union all
      select tenant_id, 'EXTERNAL_ES'::text as source, signal_source, source_event_id, buyer_id, buyer_name_raw, buyer_name_normalized,
             signal_type, signal_strength, signal_date::timestamptz as occurred_at, captured_at, evidence_url, evidence_text, extracted_entities, raw_payload, country_code
      from public.external_signals_es
      where tenant_id = p_tenant_id
        and (p_since is null or captured_at >= p_since)
    )
    select *
    from unified
    order by captured_at asc
    limit greatest(1, p_limit)
  loop
    v_processed := v_processed + 1;

    insert into public.external_signal_raw (
      tenant_id,
      source,
      source_event_id,
      buyer_name_raw,
      signal_type_raw,
      signal_strength,
      occurred_at,
      source_url,
      payload_json,
      ingested_at
    )
    values (
      p_tenant_id,
      coalesce(r.signal_source, r.source),
      r.source_event_id,
      coalesce(r.buyer_name_raw, r.buyer_name_normalized),
      r.signal_type,
      r.signal_strength,
      coalesce(r.occurred_at, r.captured_at, now()),
      r.evidence_url,
      coalesce(r.raw_payload, '{}'::jsonb),
      now()
    )
    on conflict (tenant_id, source, source_event_id)
    do update set
      buyer_name_raw = excluded.buyer_name_raw,
      signal_type_raw = excluded.signal_type_raw,
      signal_strength = excluded.signal_strength,
      occurred_at = excluded.occurred_at,
      source_url = excluded.source_url,
      payload_json = excluded.payload_json,
      ingested_at = now();

    select buyer_entity_id, is_resolved, resolution_tier
      into v_buyer_id, v_buyer_resolved, v_buyer_tier
    from public.resolve_buyer_entity_id(
      p_tenant_id,
      coalesce(r.signal_source, r.source),
      coalesce(r.buyer_name_raw, r.buyer_name_normalized),
      nullif(r.buyer_id, '')
    );

    if not coalesce(v_buyer_resolved, false) then
      v_unresolved := v_unresolved + 1;
    end if;

    v_text_hint := lower(
      coalesce(r.evidence_text, '') || ' ' ||
      coalesce(r.extracted_entities::text, '')
    );

    select cpv_cluster_id, mapping_weight, mapping_tier
      into v_cluster_id, v_cluster_weight, v_cluster_tier
    from public.resolve_cpv_cluster_id(p_tenant_id, null, v_text_hint);

    if v_cluster_id is null or v_cluster_id = 'cluster_unknown' then
      v_null_cluster := v_null_cluster + 1;
    end if;

    v_map_buyer_entity_id := null;
    v_map_cpv_cluster_id := null;
    v_map_region := null;
    v_map_strength_multiplier := 1;

    select sm.canonical_buyer_entity_id,
           sm.cpv_cluster_id,
           sm.region,
           sm.strength_multiplier
      into v_map_buyer_entity_id,
           v_map_cpv_cluster_id,
           v_map_region,
           v_map_strength_multiplier
    from public.signal_mappings sm
    where sm.tenant_id = p_tenant_id
      and sm.is_active
      and (sm.source is null or upper(sm.source) = upper(coalesce(r.signal_source, r.source)))
      and (sm.signal_type is null or lower(sm.signal_type) = lower(coalesce(r.signal_type, '')))
      and (
        sm.raw_buyer_key is null
        or sm.raw_buyer_key = public.normalize_match_key(coalesce(r.buyer_name_raw, r.buyer_name_normalized))
      )
      and (
        sm.raw_keyword is null
        or v_text_hint like ('%' || lower(sm.raw_keyword) || '%')
      )
    order by sm.priority asc, sm.updated_at desc
    limit 1;

    if v_map_buyer_entity_id is not null then
      v_buyer_id := v_map_buyer_entity_id;
      v_buyer_resolved := true;
      v_buyer_tier := 'signal_mappings';
    end if;

    if v_map_cpv_cluster_id is not null then
      v_cluster_id := v_map_cpv_cluster_id;
      v_cluster_tier := 'signal_mappings';
    end if;

    v_region := coalesce(
      public.normalize_prediction_region(v_map_region, coalesce(r.signal_source, r.source)),
      public.normalize_prediction_region(r.country_code, coalesce(r.signal_source, r.source)),
      'IE'
    );

    v_signal_type := case
      when lower(coalesce(r.signal_type, '')) = 'hiring' then 'job_posting'
      when lower(coalesce(r.signal_type, '')) = 'funding' then 'grant_awarded'
      else lower(coalesce(r.signal_type, 'external_signal'))
    end;

    v_strength := greatest(0, least(1, coalesce(r.signal_strength, 0.5) * coalesce(v_map_strength_multiplier, 1)));

    perform public.upsert_signal_event(
      p_tenant_id,
      v_buyer_id,
      coalesce(v_cluster_id, 'cluster_unknown'),
      v_region,
      v_signal_type,
      v_strength,
      coalesce(r.occurred_at, r.captured_at, now()),
      coalesce(r.signal_source, r.source),
      r.evidence_url,
      null,
      r.source_event_id,
      jsonb_build_object(
        'resolver', jsonb_build_object(
          'buyer_tier', coalesce(v_buyer_tier, 'unknown'),
          'cluster_tier', coalesce(v_cluster_tier, 'unknown'),
          'cluster_weight', coalesce(v_cluster_weight, 0)
        ),
        'source_signal_type', r.signal_type,
        'evidence_text', r.evidence_text,
        'extracted_entities', r.extracted_entities
      )
    );

    v_upserted := v_upserted + 1;
  end loop;

  return query select v_processed, v_upserted, v_unresolved, v_null_cluster;
end;
$$;

create or replace function public.normalize_all_signals(
  p_tenant_id text,
  p_since timestamptz default null,
  p_limit integer default 20000
)
returns table (
  procurement_processed integer,
  procurement_upserted integer,
  external_processed integer,
  external_upserted integer,
  unresolved_buyers integer,
  null_cluster_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  p_rec record;
  e_rec record;
begin
  select * into p_rec from public.normalize_procurement_signals(p_tenant_id, p_since, p_limit);
  select * into e_rec from public.normalize_external_signals(p_tenant_id, p_since, p_limit);

  return query
  select
    coalesce(p_rec.processed_count, 0),
    coalesce(p_rec.inserted_or_updated, 0),
    coalesce(e_rec.processed_count, 0),
    coalesce(e_rec.inserted_or_updated, 0),
    coalesce(p_rec.unresolved_buyers, 0) + coalesce(e_rec.unresolved_buyers, 0),
    coalesce(p_rec.null_cluster_count, 0) + coalesce(e_rec.null_cluster_count, 0);
end;
$$;

-- -----------------------------
-- STATS + PREDICTION COMPUTE
-- -----------------------------
create or replace function public.refresh_buyer_category_stats_for_pair(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_as_of timestamptz default now()
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_region text := coalesce(public.normalize_prediction_region(p_region, null), 'IE');
  v_tender_count integer := 0;
  v_avg_cycle numeric := null;
  v_std_cycle numeric := null;
  v_last_tender timestamptz;
  v_last_award timestamptz;
  v_seasonality jsonb := '{}'::jsonb;
  v_value_stability numeric := 0;
  v_data_coverage numeric := 0;
  v_cadence_reliability numeric := 0;
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

  with proc_events as (
    select s.occurred_at,
           s.signal_type,
           nullif((s.features_json->>'estimated_value')::numeric, null) as estimated_value
    from public.signals s
    where s.tenant_id = p_tenant_id
      and s.buyer_entity_id = p_buyer_entity_id
      and coalesce(s.cpv_cluster_id, 'cluster_unknown') = coalesce(p_cpv_cluster_id, 'cluster_unknown')
      and s.region = v_region
      and s.occurred_at >= (p_as_of - interval '24 months')
      and s.signal_type in ('notice_published', 'award_published', 'framework_established', 'framework_expiry', 'cancellation', 'amendment')
  ),
  notice_events as (
    select distinct occurred_at::date as event_day
    from proc_events
    where signal_type in ('notice_published', 'framework_established', 'amendment')
  ),
  notice_gaps as (
    select
      event_day,
      lag(event_day) over (order by event_day) as prev_day
    from notice_events
  ),
  seasonality as (
    select extract(month from event_day)::int as month_no,
           count(*)::int as month_count
    from notice_events
    group by 1
  ),
  values_data as (
    select estimated_value
    from proc_events
    where estimated_value is not null and estimated_value > 0
  )
  select
    (select count(*) from notice_events),
    (select avg((event_day - prev_day)::numeric) from notice_gaps where prev_day is not null),
    (select stddev_samp((event_day - prev_day)::numeric) from notice_gaps where prev_day is not null),
    (select max(occurred_at) from proc_events where signal_type in ('notice_published', 'framework_established', 'amendment')),
    (select max(occurred_at) from proc_events where signal_type = 'award_published'),
    coalesce((
      select jsonb_object_agg(month_no::text, month_count)
      from seasonality
    ), '{}'::jsonb),
    coalesce((
      select case
        when avg(estimated_value) is null or avg(estimated_value) = 0 then 0.5
        else greatest(0, least(1, 1 - (coalesce(stddev_samp(estimated_value), 0) / nullif(avg(estimated_value), 0))))
      end
      from values_data
    ), 0.5),
    greatest(0, least(1,
      (coalesce((select count(*) from notice_events), 0)::numeric / 12.0)
    )),
    greatest(0, least(1,
      case
        when (select avg((event_day - prev_day)::numeric) from notice_gaps where prev_day is not null) is null then 0.2
        when (select avg((event_day - prev_day)::numeric) from notice_gaps where prev_day is not null) = 0 then 0.2
        else 1 - (
          coalesce((select stddev_samp((event_day - prev_day)::numeric) from notice_gaps where prev_day is not null), 0)
          / nullif((select avg((event_day - prev_day)::numeric) from notice_gaps where prev_day is not null), 0)
        )
      end
    ))
  into
    v_tender_count,
    v_avg_cycle,
    v_std_cycle,
    v_last_tender,
    v_last_award,
    v_seasonality,
    v_value_stability,
    v_data_coverage,
    v_cadence_reliability;

  insert into public.buyer_category_stats (
    tenant_id,
    buyer_entity_id,
    cpv_cluster_id,
    region,
    tender_count_24m,
    avg_cycle_days,
    std_cycle_days,
    last_tender_at,
    last_award_at,
    seasonality_vector,
    value_stability_score,
    data_coverage_score,
    cadence_reliability_score,
    updated_at
  )
  values (
    p_tenant_id,
    p_buyer_entity_id,
    coalesce(p_cpv_cluster_id, 'cluster_unknown'),
    v_region,
    coalesce(v_tender_count, 0),
    v_avg_cycle,
    v_std_cycle,
    v_last_tender,
    v_last_award,
    coalesce(v_seasonality, '{}'::jsonb),
    greatest(0, least(1, coalesce(v_value_stability, 0))),
    greatest(0, least(1, coalesce(v_data_coverage, 0))),
    greatest(0, least(1, coalesce(v_cadence_reliability, 0))),
    now()
  )
  on conflict (tenant_id, buyer_entity_id, cpv_cluster_id, region)
  do update set
    tender_count_24m = excluded.tender_count_24m,
    avg_cycle_days = excluded.avg_cycle_days,
    std_cycle_days = excluded.std_cycle_days,
    last_tender_at = excluded.last_tender_at,
    last_award_at = excluded.last_award_at,
    seasonality_vector = excluded.seasonality_vector,
    value_stability_score = excluded.value_stability_score,
    data_coverage_score = excluded.data_coverage_score,
    cadence_reliability_score = excluded.cadence_reliability_score,
    updated_at = now();
end;
$$;

create or replace function public.compute_prediction_for_pair_v1(
  p_tenant_id text,
  p_buyer_entity_id text,
  p_cpv_cluster_id text,
  p_region text,
  p_formula_version text default 'v1.0.0',
  p_as_of timestamptz default now()
)
returns uuid
language plpgsql
security definer
set search_path = public
as $$
declare
  v_region text := coalesce(public.normalize_prediction_region(p_region, null), 'IE');
  v_stats public.buyer_category_stats%rowtype;
  v_buyer_stats record;
  v_twin_stats record;
  v_country_cluster record;
  v_country_cycle_direct numeric;
  v_cpv_baseline record;
  v_effective_external numeric := 0;
  v_external_count integer := 0;
  v_non_external_event_count integer := 0;
  v_framework_days numeric := 365;
  v_days_since_last numeric := 365;
  v_expected_days_until numeric := 180;

  v_cycle_proximity numeric := 0;
  v_seasonality_alignment numeric := 0;
  v_framework_expiry_proximity numeric := 0;
  v_authority_cadence_strength numeric := 0;
  v_external_signal_timing_boost numeric := 0;

  v_probability numeric := 0;
  v_historical_depth numeric := 0;
  v_pattern_stability numeric := 0;
  v_data_coverage numeric := 0;
  v_behavioural_reliability numeric := 0;
  v_external_corroboration numeric := 0;
  v_confidence numeric := 0;
  v_confidence_band text := 'Low';
  v_fallback_tier integer := 0;

  v_non_external_support boolean := false;
  v_cycle_score integer := 0;
  v_timing_score integer := 0;
  v_behavioural_score integer := 0;
  v_structural_score integer := 0;
  v_external_score integer := 0;
  v_quality_score integer := 0;
  v_total_score integer := 0;

  v_window_label public.next_window_label;
  v_window_start date;
  v_window_end date;

  v_prediction_id uuid;
  v_driver_count integer := 0;
  v_evidence_count integer := 0;
  v_buyer_name text;
  v_cluster_label text;
  v_has_buyer_entity boolean := false;
  v_behavioural_fingerprint boolean := false;
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

  perform public.refresh_buyer_category_stats_for_pair(
    p_tenant_id,
    p_buyer_entity_id,
    coalesce(p_cpv_cluster_id, 'cluster_unknown'),
    v_region,
    p_as_of
  );

  select *
  into v_stats
  from public.buyer_category_stats s
  where s.tenant_id = p_tenant_id
    and s.buyer_entity_id = p_buyer_entity_id
    and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown')
    and s.region = v_region;

  if not found then
    return null;
  end if;

  -- Fallback tiers.
  if coalesce(v_stats.tender_count_24m, 0) >= 3 and v_stats.avg_cycle_days is not null then
    v_fallback_tier := 0;
  else
    select
      sum(tender_count_24m)::int as tender_count,
      avg(avg_cycle_days)::numeric as avg_cycle,
      avg(cadence_reliability_score)::numeric as cadence
    into v_buyer_stats
    from public.buyer_category_stats s
    where s.tenant_id = p_tenant_id
      and s.buyer_entity_id = p_buyer_entity_id
      and s.region = v_region;

    if coalesce(v_buyer_stats.tender_count, 0) >= 3 and v_buyer_stats.avg_cycle is not null then
      v_fallback_tier := 1;
      select
        avg(s.avg_cycle_days)::numeric as avg_cycle,
        avg(s.cadence_reliability_score)::numeric as cadence
      into v_cpv_baseline
      from public.buyer_category_stats s
      where s.tenant_id = p_tenant_id
        and s.region = v_region
        and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown');

      v_stats.avg_cycle_days := v_buyer_stats.avg_cycle;
      if v_cpv_baseline.avg_cycle is not null then
        v_stats.avg_cycle_days := (0.8 * v_stats.avg_cycle_days) + (0.2 * v_cpv_baseline.avg_cycle);
      end if;
      v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, coalesce(v_buyer_stats.cadence, 0));
    else
      select
        avg(s.avg_cycle_days)::numeric as avg_cycle,
        avg(s.cadence_reliability_score)::numeric as cadence
      into v_twin_stats
      from public.buyer_category_stats s
      where s.tenant_id = p_tenant_id
        and s.region = v_region
        and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown')
        and s.buyer_entity_id <> p_buyer_entity_id
        and s.tender_count_24m between greatest(1, v_stats.tender_count_24m - 3) and (v_stats.tender_count_24m + 3);

      if v_twin_stats.avg_cycle is not null then
        v_fallback_tier := 2;
        v_stats.avg_cycle_days := v_twin_stats.avg_cycle;
        v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, coalesce(v_twin_stats.cadence, 0));
      else
        select
          avg(s.avg_cycle_days)::numeric as avg_cycle,
          avg(s.cadence_reliability_score)::numeric as cadence
        into v_country_cluster
        from public.buyer_category_stats s
        where s.tenant_id = p_tenant_id
          and s.region = v_region
          and s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown');

        if v_country_cluster.avg_cycle is not null then
          v_fallback_tier := 3;
          v_stats.avg_cycle_days := v_country_cluster.avg_cycle;
          v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, coalesce(v_country_cluster.cadence, 0));
        else
          select avg(gap_days)::numeric
          into v_country_cycle_direct
          from (
            select
              (event_day - lag(event_day) over (order by event_day))::numeric as gap_days
            from (
              select distinct s.occurred_at::date as event_day
              from public.signals s
              where s.tenant_id = p_tenant_id
                and s.region = v_region
                and coalesce(s.cpv_cluster_id, 'cluster_unknown') = coalesce(p_cpv_cluster_id, 'cluster_unknown')
                and s.signal_type in ('notice_published', 'framework_established', 'amendment')
                and s.occurred_at >= (p_as_of - interval '24 months')
            ) d
          ) x
          where gap_days is not null;

          if v_country_cycle_direct is not null then
            v_fallback_tier := 3;
            v_stats.avg_cycle_days := v_country_cycle_direct;
            v_stats.cadence_reliability_score := greatest(v_stats.cadence_reliability_score, 0.25);
          else
            v_fallback_tier := 4;
          end if;
        end if;
      end if;
    end if;
  end if;

  select
    coalesce(max(public.compute_effective_signal_strength(s.signal_type, s.signal_strength, s.occurred_at, p_as_of)), 0),
    count(*)::int
  into v_effective_external, v_external_count
  from public.signals s
  where s.tenant_id = p_tenant_id
    and s.region = v_region
    and s.buyer_entity_id = p_buyer_entity_id
    and (s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown') or s.cpv_cluster_id is null)
    and s.signal_type in (
      'job_posting','hiring_spike','role_category_detected',
      'budget_approval','budget_increase','grant_awarded','eu_funding_allocation',
      'digital_transformation_program','compliance_mandate','accessibility_regulation','sustainability_requirement','policy_signal'
    )
    and s.occurred_at >= (p_as_of - interval '24 months');

  select count(*)::int
  into v_non_external_event_count
  from public.signals s
  where s.tenant_id = p_tenant_id
    and s.region = v_region
    and s.buyer_entity_id = p_buyer_entity_id
    and s.signal_type in (
      'notice_published',
      'award_published',
      'framework_established',
      'framework_expiry',
      'cancellation',
      'amendment'
    )
    and s.occurred_at >= (p_as_of - interval '24 months');

  v_behavioural_fingerprint := (v_non_external_event_count > 0);

  if v_fallback_tier = 4 and coalesce(v_effective_external, 0) < 0.8 then
    -- external-only fallback is not allowed under low effective strength.
    return null;
  end if;

  if v_fallback_tier = 4 and not v_behavioural_fingerprint then
    -- external-only fallback requires behaviour support.
    return null;
  end if;

  if v_stats.last_tender_at is not null then
    v_days_since_last := greatest(0, extract(epoch from (p_as_of - v_stats.last_tender_at)) / 86400.0);
  end if;

  if v_stats.avg_cycle_days is not null and v_stats.avg_cycle_days > 0 then
    v_expected_days_until := greatest(0, v_stats.avg_cycle_days - v_days_since_last);
    v_cycle_proximity := greatest(0, least(1, 1 - abs(v_days_since_last - v_stats.avg_cycle_days) / greatest(v_stats.avg_cycle_days, 1)));
  else
    v_expected_days_until := case when v_fallback_tier = 4 then 45 else 180 end;
    v_cycle_proximity := case when v_fallback_tier = 4 then 0.2 else 0.35 end;
  end if;

  v_seasonality_alignment := coalesce(
    (v_stats.seasonality_vector ->> extract(month from p_as_of)::int::text)::numeric
    / nullif((
      select greatest(1, max(value::numeric))
      from jsonb_each_text(coalesce(v_stats.seasonality_vector, '{}'::jsonb))
    ), 0),
    0
  );
  v_seasonality_alignment := greatest(0, least(1, coalesce(v_seasonality_alignment, 0)));

  select coalesce(min(extract(epoch from (s.occurred_at - p_as_of)) / 86400.0), 365)
  into v_framework_days
  from public.signals s
  where s.tenant_id = p_tenant_id
    and s.region = v_region
    and s.buyer_entity_id = p_buyer_entity_id
    and (s.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown') or s.cpv_cluster_id is null)
    and s.signal_type = 'framework_expiry'
    and s.occurred_at >= p_as_of;

  v_framework_expiry_proximity := case
    when v_framework_days <= 30 then 1
    when v_framework_days <= 90 then 0.75
    when v_framework_days <= 180 then 0.4
    else 0.1
  end;

  v_authority_cadence_strength := greatest(0, least(1, coalesce(v_stats.cadence_reliability_score, 0)));

  v_external_signal_timing_boost := greatest(0, least(1, coalesce(v_effective_external, 0)));

  v_probability :=
    (0.40 * v_cycle_proximity) +
    (0.15 * v_seasonality_alignment) +
    (0.15 * v_framework_expiry_proximity) +
    (0.15 * v_authority_cadence_strength) +
    (0.15 * v_external_signal_timing_boost);

  -- hard cap and external-only influence cap (+0.15 max absolute already bounded by weight)
  v_probability := least(0.95, greatest(0, v_probability));

  v_historical_depth := greatest(0, least(1, coalesce(v_stats.tender_count_24m, 0)::numeric / 12.0));
  v_pattern_stability := greatest(0, least(1,
    case
      when v_stats.avg_cycle_days is null or v_stats.avg_cycle_days = 0 then 0.2
      else 1 - (coalesce(v_stats.std_cycle_days, v_stats.avg_cycle_days) / nullif(v_stats.avg_cycle_days, 0))
    end
  ));
  v_data_coverage := greatest(0, least(1, coalesce(v_stats.data_coverage_score, 0)));
  v_behavioural_reliability := greatest(0, least(1, coalesce(v_stats.cadence_reliability_score, 0)));
  v_external_corroboration := greatest(0, least(1,
    coalesce(v_effective_external, 0) * case when v_external_count >= 2 then 1 else 0.7 end
  ));

  v_confidence := 100 * (
    (0.30 * v_historical_depth) +
    (0.20 * v_pattern_stability) +
    (0.20 * v_data_coverage) +
    (0.15 * v_behavioural_reliability) +
    (0.15 * v_external_corroboration)
  );

  select exists (
    select 1
    from public.entities e
    where e.tenant_id = p_tenant_id
      and e.entity_id = p_buyer_entity_id
      and lower(coalesce(e.entity_type, '')) = 'buyer'
  )
  into v_has_buyer_entity;

  -- unresolved or fallback buyer ids are capped to Medium confidence
  if p_buyer_entity_id like 'unresolved:%' or not v_has_buyer_entity then
    v_confidence := least(v_confidence, 59);
  end if;

  -- Sparse fallback tiers must remain low/medium confidence.
  if v_fallback_tier >= 3 then
    v_confidence := least(v_confidence, 59);
  end if;

  -- global cap rule
  if not (coalesce(v_stats.tender_count_24m, 0) >= 5 and v_data_coverage > 0.9) then
    v_confidence := least(v_confidence, 95);
  end if;

  v_confidence := greatest(0, least(100, round(v_confidence)));

  v_confidence_band := case
    when v_confidence <= 39 then 'Low'
    when v_confidence <= 59 then 'Medium'
    when v_confidence <= 79 then 'High'
    else 'Very High'
  end;

  select next_window_label, window_start, window_end
    into v_window_label, v_window_start, v_window_end
  from public.resolve_prediction_window(v_expected_days_until, p_as_of::date);

  v_non_external_support := v_behavioural_fingerprint
    and (v_cycle_proximity > 0.2 or v_seasonality_alignment > 0.2 or v_authority_cadence_strength > 0.2 or v_framework_expiry_proximity > 0.2);

  v_cycle_score := round(20 * v_cycle_proximity);
  v_timing_score := round(15 * v_seasonality_alignment);
  v_behavioural_score := round(15 * v_authority_cadence_strength);
  v_structural_score := round(10 * v_framework_expiry_proximity);
  v_external_score := round(20 * v_external_signal_timing_boost);
  if not v_non_external_support then
    v_external_score := least(v_external_score, 18);
  end if;
  v_quality_score := round(20 * v_data_coverage);
  v_total_score := v_cycle_score + v_timing_score + v_behavioural_score + v_structural_score + v_external_score + v_quality_score;
  v_total_score := greatest(0, least(100, v_total_score));

  select coalesce(max(canonical_name), p_buyer_entity_id)
    into v_buyer_name
  from public.entities
  where tenant_id = p_tenant_id
    and entity_id = p_buyer_entity_id;

  select label
    into v_cluster_label
  from public.cpv_cluster c
  where c.tenant_id = p_tenant_id
    and c.cpv_cluster_id = coalesce(p_cpv_cluster_id, 'cluster_unknown')
  limit 1;

  insert into public.predictions_current (
    tenant_id,
    buyer_entity_id,
    buyer_display_name,
    cpv_cluster_id,
    cpv_cluster_label,
    region,
    next_window_label,
    expected_window_start,
    expected_window_end,
    probability,
    confidence,
    confidence_band,
    forecast_score,
    fallback_tier,
    drivers_count,
    evidence_count,
    explainability_json,
    last_computed_at
  )
  values (
    p_tenant_id,
    p_buyer_entity_id,
    v_buyer_name,
    coalesce(p_cpv_cluster_id, 'cluster_unknown'),
    coalesce(v_cluster_label, coalesce(p_cpv_cluster_id, 'cluster_unknown')),
    v_region,
    v_window_label,
    v_window_start,
    v_window_end,
    v_probability,
    v_confidence,
    v_confidence_band,
    v_total_score,
    v_fallback_tier,
    0,
    0,
    jsonb_build_object(
      'components', jsonb_build_object(
        'cycle_proximity', v_cycle_proximity,
        'seasonality_alignment', v_seasonality_alignment,
        'framework_expiry_proximity', v_framework_expiry_proximity,
        'authority_cadence_strength', v_authority_cadence_strength,
        'external_signal_timing_boost', v_external_signal_timing_boost
      ),
      'confidence_components', jsonb_build_object(
        'historical_depth', v_historical_depth,
        'pattern_stability', v_pattern_stability,
        'data_coverage', v_data_coverage,
        'behavioural_reliability', v_behavioural_reliability,
        'external_signal_corroboration', v_external_corroboration
      )
    ),
    now()
  )
  on conflict (tenant_id, buyer_entity_id, cpv_cluster_id, region)
  do update set
    buyer_display_name = excluded.buyer_display_name,
    cpv_cluster_label = excluded.cpv_cluster_label,
    next_window_label = excluded.next_window_label,
    expected_window_start = excluded.expected_window_start,
    expected_window_end = excluded.expected_window_end,
    probability = excluded.probability,
    confidence = excluded.confidence,
    confidence_band = excluded.confidence_band,
    forecast_score = excluded.forecast_score,
    fallback_tier = excluded.fallback_tier,
    explainability_json = excluded.explainability_json,
    last_computed_at = now()
  returning prediction_id into v_prediction_id;

  delete from public.prediction_drivers where tenant_id = p_tenant_id and prediction_id = v_prediction_id;

  insert into public.prediction_drivers (
    tenant_id,
    prediction_id,
    driver_type,
    label,
    contribution,
    narrative,
    evidence_refs
  )
  values
    (
      p_tenant_id,
      v_prediction_id,
      'fallback_tier',
      'Fallback tier ' || v_fallback_tier,
      0,
      case v_fallback_tier
        when 0 then 'Primary buyer + CPV cadence available.'
        when 1 then 'Used buyer-level history with CPV baseline adjustment.'
        when 2 then 'Used behavioural twins fallback for cadence.'
        when 3 then 'Used country-level CPV cadence baseline.'
        else 'External-signal-only fallback (high effective strength) with capped confidence.'
      end,
      array[]::text[]
    ),
    (
      p_tenant_id,
      v_prediction_id,
      'cycle_rhythm',
      'Cycle rhythm signal',
      round((0.40 * v_cycle_proximity)::numeric, 4),
      'Cycle proximity contributes to timing likelihood based on historical cadence.',
      array[]::text[]
    ),
    (
      p_tenant_id,
      v_prediction_id,
      'external_signal',
      'External signal timing boost',
      round((0.15 * v_external_signal_timing_boost)::numeric, 4),
      'Decayed external signals were incorporated with hard influence caps.',
      array[]::text[]
    );

  -- ensure at least one quality/corroboration driver for explainability breadth
  if v_data_coverage < 0.5 then
    insert into public.prediction_drivers (
      tenant_id,
      prediction_id,
      driver_type,
      label,
      contribution,
      narrative,
      evidence_refs
    )
    values (
      p_tenant_id,
      v_prediction_id,
      'coverage_penalty',
      'Coverage quality constraint',
      round((0.2 * (0.5 - v_data_coverage))::numeric, 4),
      'Limited data coverage constrained confidence and score.',
      array[]::text[]
    );
  else
    insert into public.prediction_drivers (
      tenant_id,
      prediction_id,
      driver_type,
      label,
      contribution,
      narrative,
      evidence_refs
    )
    values (
      p_tenant_id,
      v_prediction_id,
      'cadence_pattern',
      'Cadence reliability',
      round((0.15 * v_authority_cadence_strength)::numeric, 4),
      'Cadence stability supports forecast reliability.',
      array[]::text[]
    );
  end if;

  delete from public.prediction_scorecard where prediction_id = v_prediction_id;

  insert into public.prediction_scorecard (
    prediction_id,
    tenant_id,
    cycle_score,
    timing_score,
    behavioural_score,
    structural_score,
    external_signal_score,
    data_quality_score,
    total_score,
    created_at
  )
  values (
    v_prediction_id,
    p_tenant_id,
    v_cycle_score,
    v_timing_score,
    v_behavioural_score,
    v_structural_score,
    v_external_score,
    v_quality_score,
    v_total_score,
    now()
  );

  insert into public.prediction_inputs_snapshot (
    tenant_id,
    prediction_id,
    formula_version,
    features_json,
    computed_at
  )
  values (
    p_tenant_id,
    v_prediction_id,
    p_formula_version,
    jsonb_build_object(
      'pair', jsonb_build_object(
        'tenant_id', p_tenant_id,
        'buyer_entity_id', p_buyer_entity_id,
        'cpv_cluster_id', coalesce(p_cpv_cluster_id, 'cluster_unknown'),
        'region', v_region
      ),
      'fallback_tier', v_fallback_tier,
      'features', jsonb_build_object(
        'cycle_proximity', v_cycle_proximity,
        'seasonality_alignment', v_seasonality_alignment,
        'framework_expiry_proximity', v_framework_expiry_proximity,
        'authority_cadence_strength', v_authority_cadence_strength,
        'external_signal_timing_boost', v_external_signal_timing_boost,
        'historical_depth', v_historical_depth,
        'pattern_stability', v_pattern_stability,
        'data_coverage', v_data_coverage,
        'behavioural_reliability', v_behavioural_reliability,
        'external_signal_corroboration', v_external_corroboration
      ),
      'caps', jsonb_build_object(
        'probability_cap', 0.95,
        'confidence_cap_default', 95,
        'external_probability_max_abs', 0.15,
        'external_score_soft_cap_without_corroboration', 18
      )
    ),
    now()
  );

  select count(*), coalesce(sum(cardinality(evidence_refs)), 0)
    into v_driver_count, v_evidence_count
  from public.prediction_drivers
  where tenant_id = p_tenant_id
    and prediction_id = v_prediction_id;

  update public.predictions_current
  set
    drivers_count = v_driver_count,
    evidence_count = v_evidence_count,
    last_computed_at = now()
  where prediction_id = v_prediction_id;

  return v_prediction_id;
end;
$$;

create or replace function public.run_predictive_engine_v1(
  p_tenant_id text,
  p_run_type text default 'incremental',
  p_formula_version text default 'v1.0.0',
  p_since timestamptz default null,
  p_max_pairs integer default 5000,
  p_normalize_signals boolean default true
)
returns table (
  run_id uuid,
  status text,
  pairs_processed integer,
  error_message text
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_run_id uuid;
  v_status text := 'running';
  v_error text := null;
  v_processed integer := 0;
  v_last_success timestamptz;
  v_effective_since timestamptz;
  v_norm record;
  p record;
begin
  perform public.ensure_prediction_baseline(p_tenant_id);

  insert into public.prediction_runs (
    tenant_id,
    run_type,
    started_at,
    status,
    metadata_json
  )
  values (
    p_tenant_id,
    case when lower(p_run_type) in ('full', 'signals_only') then lower(p_run_type) else 'incremental' end,
    now(),
    'running',
    jsonb_build_object('formula_version', p_formula_version, 'normalize_signals', p_normalize_signals)
  )
  returning prediction_runs.run_id into v_run_id;

  begin
    if lower(p_run_type) in ('incremental', 'signals_only') then
      select max(finished_at)
      into v_last_success
      from public.prediction_runs
      where tenant_id = p_tenant_id
        and public.prediction_runs.status = 'success'
        and public.prediction_runs.run_type in ('incremental', 'full', 'signals_only');

      v_effective_since := coalesce(p_since, v_last_success, now() - interval '24 hours');
    else
      v_effective_since := coalesce(p_since, now() - interval '24 months');
    end if;

    if p_normalize_signals then
      select *
      into v_norm
      from public.normalize_all_signals(
        p_tenant_id,
        v_effective_since,
        greatest(1000, p_max_pairs * 8)
      );
    end if;

    if lower(p_run_type) = 'signals_only' then
      update public.prediction_runs
      set
        status = 'success',
        pairs_processed = 0,
        finished_at = now(),
        metadata_json = coalesce(metadata_json, '{}'::jsonb)
          || jsonb_build_object(
            'effective_since', v_effective_since,
            'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb)
          )
      where public.prediction_runs.run_id = v_run_id;

      return query select v_run_id, 'success'::text, 0::integer, null::text;
      return;
    end if;

    for p in
      with signal_scope as (
        select
          s.tenant_id,
          s.buyer_entity_id,
          coalesce(s.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
          coalesce(public.normalize_prediction_region(s.region, s.source), 'IE') as region,
          s.cpv_cluster_id is null or s.cpv_cluster_id = 'cluster_unknown' as is_buyer_level_fallback
        from public.signals s
        where s.tenant_id = p_tenant_id
          and (
            lower(p_run_type) = 'full'
            or s.ingested_at >= v_effective_since
          )
      ),
      direct_pairs as (
        select distinct
          ss.tenant_id,
          ss.buyer_entity_id,
          ss.cpv_cluster_id,
          ss.region
        from signal_scope ss
      ),
      buyer_level_pairs as (
        select distinct
          ss.tenant_id,
          ss.buyer_entity_id,
          coalesce(bcs.cpv_cluster_id, 'cluster_unknown') as cpv_cluster_id,
          ss.region
        from signal_scope ss
        left join public.buyer_category_stats bcs
          on bcs.tenant_id = ss.tenant_id
         and bcs.buyer_entity_id = ss.buyer_entity_id
         and bcs.region = ss.region
        where ss.is_buyer_level_fallback
      ),
      impacted as (
        select * from direct_pairs
        union
        select * from buyer_level_pairs
      )
      select *
      from impacted
      order by buyer_entity_id, cpv_cluster_id, region
      limit greatest(1, p_max_pairs)
    loop
      perform public.compute_prediction_for_pair_v1(
        p_tenant_id,
        p.buyer_entity_id,
        p.cpv_cluster_id,
        p.region,
        p_formula_version,
        now()
      );

      v_processed := v_processed + 1;
    end loop;

    v_status := 'success';

    update public.prediction_runs
    set
      status = v_status,
      pairs_processed = v_processed,
      finished_at = now(),
      metadata_json = coalesce(metadata_json, '{}'::jsonb)
        || jsonb_build_object(
          'effective_since', v_effective_since,
          'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb)
        )
    where public.prediction_runs.run_id = v_run_id;

  exception when others then
    v_status := 'failed';
    v_error := sqlerrm;

    update public.prediction_runs
    set
      status = v_status,
      error_message = v_error,
      pairs_processed = v_processed,
      finished_at = now(),
      metadata_json = coalesce(metadata_json, '{}'::jsonb)
        || jsonb_build_object(
          'effective_since', v_effective_since,
          'normalize', coalesce(to_jsonb(v_norm), '{}'::jsonb)
        )
    where public.prediction_runs.run_id = v_run_id;
  end;

  return query select v_run_id, v_status, v_processed, v_error;
end;
$$;

-- -----------------------------
-- RLS
-- -----------------------------
do $$
declare
  t text;
begin
  foreach t in array array[
    'cpv_cluster',
    'cpv_cluster_map',
    'buyer_aliases',
    'external_signal_raw',
    'signal_mappings',
    'external_signal_to_cpv',
    'signals',
    'buyer_category_stats',
    'predictions_current',
    'prediction_drivers',
    'prediction_scorecard',
    'prediction_runs',
    'prediction_extensions',
    'prediction_inputs_snapshot'
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
