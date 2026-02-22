-- Stabilization: codify company profile schema + prediction serving RPC
-- Date: 2026-02-25
-- Scope:
-- 1) Bring company_profiles under migrations with strict tenant isolation
-- 2) Codify renewal-enriched predictions columns used by UI/pipeline
-- 3) Add tenant-verified get_tenant_predictions RPC

begin;

-- -------------------------------------------------------------------
-- company_profiles (idempotent + additive)
-- -------------------------------------------------------------------
create table if not exists public.company_profiles (
  tenant_id text primary key references public.tenants(id) on delete cascade,
  company_name text,
  company_description text,
  company_size text,
  country_hq text,
  website text,
  logo_url text,
  year_established integer,
  target_buyer_types text[] not null default '{}'::text[],
  target_cpv_clusters text[] not null default '{}'::text[],
  target_countries text[] not null default '{}'::text[],
  target_regions jsonb not null default '[]'::jsonb,
  contract_size_min_eur numeric,
  contract_size_max_eur numeric,
  does_frameworks boolean not null default false,
  years_in_procurement integer,
  annual_bid_volume text,
  known_competitors text[] not null default '{}'::text[],
  key_products_services text[] not null default '{}'::text[],
  billing_company_name text,
  billing_address_line1 text,
  billing_address_line2 text,
  billing_city text,
  billing_postcode text,
  billing_country text,
  billing_vat_number text,
  billing_email text,
  plan_type text not null default 'free',
  stripe_customer_id text,
  stripe_subscription_id text,
  onboarding_step integer not null default 0,
  onboarding_completed boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

alter table public.company_profiles add column if not exists company_name text;
alter table public.company_profiles add column if not exists company_description text;
alter table public.company_profiles add column if not exists company_size text;
alter table public.company_profiles add column if not exists country_hq text;
alter table public.company_profiles add column if not exists website text;
alter table public.company_profiles add column if not exists logo_url text;
alter table public.company_profiles add column if not exists year_established integer;
alter table public.company_profiles add column if not exists target_buyer_types text[];
alter table public.company_profiles add column if not exists target_cpv_clusters text[];
alter table public.company_profiles add column if not exists target_countries text[];
alter table public.company_profiles add column if not exists target_regions jsonb;
alter table public.company_profiles add column if not exists contract_size_min_eur numeric;
alter table public.company_profiles add column if not exists contract_size_max_eur numeric;
alter table public.company_profiles add column if not exists does_frameworks boolean;
alter table public.company_profiles add column if not exists years_in_procurement integer;
alter table public.company_profiles add column if not exists annual_bid_volume text;
alter table public.company_profiles add column if not exists known_competitors text[];
alter table public.company_profiles add column if not exists key_products_services text[];
alter table public.company_profiles add column if not exists billing_company_name text;
alter table public.company_profiles add column if not exists billing_address_line1 text;
alter table public.company_profiles add column if not exists billing_address_line2 text;
alter table public.company_profiles add column if not exists billing_city text;
alter table public.company_profiles add column if not exists billing_postcode text;
alter table public.company_profiles add column if not exists billing_country text;
alter table public.company_profiles add column if not exists billing_vat_number text;
alter table public.company_profiles add column if not exists billing_email text;
alter table public.company_profiles add column if not exists plan_type text;
alter table public.company_profiles add column if not exists stripe_customer_id text;
alter table public.company_profiles add column if not exists stripe_subscription_id text;
alter table public.company_profiles add column if not exists onboarding_step integer;
alter table public.company_profiles add column if not exists onboarding_completed boolean;
alter table public.company_profiles add column if not exists created_at timestamptz;
alter table public.company_profiles add column if not exists updated_at timestamptz;

update public.company_profiles
set
  target_buyer_types = coalesce(target_buyer_types, '{}'::text[]),
  target_cpv_clusters = coalesce(target_cpv_clusters, '{}'::text[]),
  target_countries = coalesce(target_countries, '{}'::text[]),
  target_regions = coalesce(target_regions, '[]'::jsonb),
  known_competitors = coalesce(known_competitors, '{}'::text[]),
  key_products_services = coalesce(key_products_services, '{}'::text[]),
  does_frameworks = coalesce(does_frameworks, false),
  plan_type = coalesce(nullif(plan_type, ''), 'free'),
  onboarding_step = coalesce(onboarding_step, 0),
  onboarding_completed = coalesce(onboarding_completed, false),
  created_at = coalesce(created_at, now()),
  updated_at = coalesce(updated_at, now())
where true;

alter table public.company_profiles alter column target_buyer_types set default '{}'::text[];
alter table public.company_profiles alter column target_buyer_types set not null;
alter table public.company_profiles alter column target_cpv_clusters set default '{}'::text[];
alter table public.company_profiles alter column target_cpv_clusters set not null;
alter table public.company_profiles alter column target_countries set default '{}'::text[];
alter table public.company_profiles alter column target_countries set not null;
alter table public.company_profiles alter column target_regions set default '[]'::jsonb;
alter table public.company_profiles alter column target_regions set not null;
alter table public.company_profiles alter column known_competitors set default '{}'::text[];
alter table public.company_profiles alter column known_competitors set not null;
alter table public.company_profiles alter column key_products_services set default '{}'::text[];
alter table public.company_profiles alter column key_products_services set not null;
alter table public.company_profiles alter column does_frameworks set default false;
alter table public.company_profiles alter column does_frameworks set not null;
alter table public.company_profiles alter column plan_type set default 'free';
alter table public.company_profiles alter column plan_type set not null;
alter table public.company_profiles alter column onboarding_step set default 0;
alter table public.company_profiles alter column onboarding_step set not null;
alter table public.company_profiles alter column onboarding_completed set default false;
alter table public.company_profiles alter column onboarding_completed set not null;
alter table public.company_profiles alter column created_at set default now();
alter table public.company_profiles alter column created_at set not null;
alter table public.company_profiles alter column updated_at set default now();
alter table public.company_profiles alter column updated_at set not null;

create index if not exists idx_company_profiles_plan_type on public.company_profiles (plan_type);
create index if not exists idx_company_profiles_onboarding_completed on public.company_profiles (onboarding_completed);

alter table public.company_profiles enable row level security;
alter table public.company_profiles force row level security;

-- Remove any permissive or legacy policies before applying strict tenant policies.
do $$
declare
  p record;
begin
  for p in
    select policyname
    from pg_policies
    where schemaname = 'public'
      and tablename = 'company_profiles'
  loop
    execute format('drop policy if exists %I on public.company_profiles', p.policyname);
  end loop;
end $$;

create policy company_profiles_tenant_select
on public.company_profiles
for select
to authenticated
using (
  tenant_id = (
    select u.tenant_id
    from public.users u
    where u.id = auth.uid()::text
  )
);

create policy company_profiles_tenant_insert
on public.company_profiles
for insert
to authenticated
with check (
  tenant_id = (
    select u.tenant_id
    from public.users u
    where u.id = auth.uid()::text
  )
);

create policy company_profiles_tenant_update
on public.company_profiles
for update
to authenticated
using (
  tenant_id = (
    select u.tenant_id
    from public.users u
    where u.id = auth.uid()::text
  )
)
with check (
  tenant_id = (
    select u.tenant_id
    from public.users u
    where u.id = auth.uid()::text
  )
);

create policy company_profiles_tenant_delete
on public.company_profiles
for delete
to authenticated
using (
  tenant_id = (
    select u.tenant_id
    from public.users u
    where u.id = auth.uid()::text
  )
);

-- -------------------------------------------------------------------
-- predictions renewal enrichment columns (idempotent + additive)
-- -------------------------------------------------------------------
alter table if exists public.predictions add column if not exists predicted_tender_date date;
alter table if exists public.predictions add column if not exists signal_type text;
alter table if exists public.predictions add column if not exists renewal_source jsonb not null default '{}'::jsonb;
alter table if exists public.predictions add column if not exists urgency text;
alter table if exists public.predictions add column if not exists buyer_name text;
alter table if exists public.predictions add column if not exists country text;
alter table if exists public.predictions add column if not exists total_value_eur numeric;

create index if not exists idx_predictions_tenant_predicted_tender_date
  on public.predictions (tenant_id, predicted_tender_date);
create index if not exists idx_predictions_tenant_signal_type
  on public.predictions (tenant_id, signal_type);
create index if not exists idx_predictions_tenant_urgency
  on public.predictions (tenant_id, urgency);
create index if not exists idx_predictions_tenant_country
  on public.predictions (tenant_id, country);

-- -------------------------------------------------------------------
-- get_tenant_predictions RPC (tenant-scoped, replay-safe)
-- -------------------------------------------------------------------
drop function if exists public.get_tenant_predictions(text);

create or replace function public.get_tenant_predictions(p_tenant_id text)
returns table (
  id text,
  buyer_id text,
  category text,
  cpv_family text,
  time_window text,
  probability numeric,
  confidence numeric,
  confidence_breakdown jsonb,
  top_drivers jsonb,
  evidence jsonb,
  model_version text,
  generated_at timestamptz,
  tenant_id text,
  predicted_tender_date date,
  signal_type text,
  renewal_source jsonb,
  urgency text,
  buyer_name text,
  country text,
  total_value_eur numeric
)
language plpgsql
security definer
set search_path = public
stable
as $$
declare
  v_req_tenant text := lower(nullif(trim(coalesce(p_tenant_id, '')), ''));
  v_caller_role text := coalesce(auth.role(), nullif(current_setting('request.jwt.claim.role', true), ''));
  v_caller_tenant text;
begin
  if v_req_tenant is null then
    raise exception 'tenant_id is required';
  end if;

  if v_caller_role is distinct from 'service_role' then
    if auth.uid() is null then
      raise exception 'authenticated user required';
    end if;

    select lower(u.tenant_id)
      into v_caller_tenant
    from public.users u
    where u.id = auth.uid()::text;

    if v_caller_tenant is null then
      raise exception 'no tenant mapping for authenticated user';
    end if;

    if v_caller_tenant <> v_req_tenant then
      raise exception 'cross-tenant access denied';
    end if;
  end if;

  return query
  select
    p.id,
    p.buyer_id,
    p.category,
    p.cpv_family,
    p.time_window,
    p.probability,
    p.confidence,
    p.confidence_breakdown,
    p.top_drivers,
    p.evidence,
    p.model_version,
    p.generated_at,
    p.tenant_id,
    p.predicted_tender_date,
    p.signal_type,
    p.renewal_source,
    p.urgency,
    p.buyer_name,
    p.country,
    p.total_value_eur
  from public.predictions p
  where p.tenant_id = v_req_tenant
  order by
    case lower(coalesce(p.urgency, ''))
      when 'overdue' then 0
      when 'imminent' then 1
      when 'upcoming' then 2
      when 'horizon' then 3
      when 'distant' then 4
      else 5
    end,
    p.predicted_tender_date asc nulls last,
    p.generated_at desc
  limit 500;
end;
$$;

revoke all on function public.get_tenant_predictions(text) from public;
grant execute on function public.get_tenant_predictions(text) to authenticated;
grant execute on function public.get_tenant_predictions(text) to service_role;

commit;

-- Rollback plan (manual):
-- 1) drop function if exists public.get_tenant_predictions(text);
-- 2) alter table public.predictions drop column if exists predicted_tender_date,
--    drop column if exists signal_type,
--    drop column if exists renewal_source,
--    drop column if exists urgency,
--    drop column if exists buyer_name,
--    drop column if exists country,
--    drop column if exists total_value_eur;
-- 3) drop table if exists public.company_profiles;
