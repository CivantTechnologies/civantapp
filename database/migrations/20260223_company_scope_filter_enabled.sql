-- Company scope mode toggle (idempotent)
-- Adds per-tenant setting for FILTER vs RANK/HIGHLIGHT behavior in Search + Forecast.

alter table public.company_profiles
  add column if not exists company_scope_filter_enabled boolean;

alter table public.company_profiles
  alter column company_scope_filter_enabled set default true;

update public.company_profiles
set company_scope_filter_enabled = true
where company_scope_filter_enabled is null;

alter table public.company_profiles
  alter column company_scope_filter_enabled set not null;
