-- Migration: 20260225_ie_supplier_raw_cleaning_guardrail_v1.sql
-- Goal: normalize whitespace/control chars in IE supplier raw-name fields.
-- Scope:
--   - Future writes: enforce cleanup on award_fact_ie.supplier_name_raw and
--     ie_supplier_dim.supplier_name_raw_example via triggers.
--   - Existing rows: one-time cleanup backfill.
-- Rollback plan:
--   - Drop triggers:
--       drop trigger if exists trg_award_fact_ie_clean_supplier_name_raw on public.award_fact_ie;
--       drop trigger if exists trg_ie_supplier_dim_clean_supplier_name_raw_example on public.ie_supplier_dim;
--   - Drop trigger functions:
--       drop function if exists public.civant_ie__sanitize_award_fact_ie_supplier_name();
--       drop function if exists public.civant_ie__sanitize_ie_supplier_dim_raw_example();
--   - Keep civant_ie__clean_supplier_name_raw(text) (safe utility) or drop if desired.

begin;

create or replace function public.civant_ie__clean_supplier_name_raw(p_name text)
returns text
language sql
immutable
as $$
  with
  s0 as (
    select coalesce(p_name, '') as s
  ),
  s1 as (
    -- Replace control chars with spaces first.
    select regexp_replace(s, '[[:cntrl:]]+', ' ', 'g') as s
    from s0
  ),
  s2 as (
    -- Collapse all whitespace runs and trim.
    select trim(regexp_replace(s, '[[:space:]]+', ' ', 'g')) as s
    from s1
  )
  select nullif(s, '')
  from s2;
$$;

create or replace function public.civant_ie__sanitize_award_fact_ie_supplier_name()
returns trigger
language plpgsql
as $$
begin
  new.supplier_name_raw := public.civant_ie__clean_supplier_name_raw(new.supplier_name_raw);
  return new;
end;
$$;

drop trigger if exists trg_award_fact_ie_clean_supplier_name_raw on public.award_fact_ie;
create trigger trg_award_fact_ie_clean_supplier_name_raw
before insert or update of supplier_name_raw
on public.award_fact_ie
for each row
execute function public.civant_ie__sanitize_award_fact_ie_supplier_name();

create or replace function public.civant_ie__sanitize_ie_supplier_dim_raw_example()
returns trigger
language plpgsql
as $$
begin
  new.supplier_name_raw_example := coalesce(
    public.civant_ie__clean_supplier_name_raw(new.supplier_name_raw_example),
    new.supplier_name_raw_example
  );
  return new;
end;
$$;

drop trigger if exists trg_ie_supplier_dim_clean_supplier_name_raw_example on public.ie_supplier_dim;
create trigger trg_ie_supplier_dim_clean_supplier_name_raw_example
before insert or update of supplier_name_raw_example
on public.ie_supplier_dim
for each row
execute function public.civant_ie__sanitize_ie_supplier_dim_raw_example();

-- Backfill existing rows (idempotent and replay-safe).
update public.award_fact_ie
set supplier_name_raw = public.civant_ie__clean_supplier_name_raw(supplier_name_raw)
where supplier_name_raw is distinct from public.civant_ie__clean_supplier_name_raw(supplier_name_raw);

update public.ie_supplier_dim
set supplier_name_raw_example = public.civant_ie__clean_supplier_name_raw(supplier_name_raw_example),
    updated_at = now()
where public.civant_ie__clean_supplier_name_raw(supplier_name_raw_example) is not null
  and supplier_name_raw_example is distinct from public.civant_ie__clean_supplier_name_raw(supplier_name_raw_example);

commit;
