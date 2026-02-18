-- Buyer alias backfill v1 (deterministic, tenant-safe)
-- Purpose: map deterministic buyer names to existing buyer entities only.
-- Guard: defaults to civant_default unless civant.buyer_alias_backfill_tenant is set.

create extension if not exists pgcrypto;

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

create or replace function public.normalize_buyer_name_v1(p_input text)
returns text
language sql
immutable
as $$
  select public.normalize_match_key(p_input)
$$;

do $$
declare
  v_tenant text := coalesce(nullif(current_setting('civant.buyer_alias_backfill_tenant', true), ''), 'civant_default');
  v_inserted_canonical bigint := 0;
  v_inserted_external bigint := 0;
begin
  if v_tenant is null or v_tenant = '' then
    raise exception 'buyer_alias_backfill: tenant id is required';
  end if;

  if to_regclass('public.entities') is null then
    raise exception 'buyer_alias_backfill: missing table public.entities';
  end if;

  -- Procurement path: canonical_tenders buyer names.
  with buyer_entities as (
    select
      e.tenant_id,
      e.entity_id,
      public.normalize_buyer_name_v1(e.canonical_name) as norm_name
    from public.entities e
    where e.tenant_id = v_tenant
      and lower(coalesce(e.entity_type, '')) = 'buyer'
      and public.normalize_buyer_name_v1(e.canonical_name) is not null
  ),
  canonical_candidates as (
    select
      ct.tenant_id,
      upper(coalesce(ct.source, 'CANONICAL')) as source,
      public.normalize_buyer_name_v1(coalesce(ct.buyer_name_raw, ct.buyer_name_norm, '')) as norm_key
    from public.canonical_tenders ct
    where ct.tenant_id = v_tenant
      and coalesce(ct.buyer_name_raw, ct.buyer_name_norm, '') <> ''
  ),
  canonical_matches as (
    select distinct
      cc.tenant_id,
      cc.source,
      cc.norm_key as raw_buyer_key,
      be.entity_id as canonical_buyer_entity_id
    from canonical_candidates cc
    join buyer_entities be
      on be.tenant_id = cc.tenant_id
     and be.norm_name = cc.norm_key
    where cc.norm_key is not null
  )
  insert into public.buyer_aliases (
    tenant_id,
    raw_buyer_key,
    source,
    canonical_buyer_entity_id,
    resolution_confidence,
    created_at,
    updated_at
  )
  select
    cm.tenant_id,
    cm.raw_buyer_key,
    cm.source,
    cm.canonical_buyer_entity_id,
    1.0,
    now(),
    now()
  from canonical_matches cm
  on conflict (tenant_id, raw_buyer_key, source) do nothing;

  get diagnostics v_inserted_canonical = row_count;
  raise notice 'buyer_alias_backfill: inserted % canonical aliases for tenant=%', v_inserted_canonical, v_tenant;

  -- External path: external_signal_raw buyer names (if present).
  if to_regclass('public.external_signal_raw') is not null then
    with buyer_entities as (
      select
        e.tenant_id,
        e.entity_id,
        public.normalize_buyer_name_v1(e.canonical_name) as norm_name
      from public.entities e
      where e.tenant_id = v_tenant
        and lower(coalesce(e.entity_type, '')) = 'buyer'
        and public.normalize_buyer_name_v1(e.canonical_name) is not null
    ),
    external_candidates as (
      select
        esr.tenant_id,
        upper(coalesce(esr.source, 'EXTERNAL')) as source,
        public.normalize_buyer_name_v1(coalesce(esr.buyer_name_raw, '')) as norm_key
      from public.external_signal_raw esr
      where esr.tenant_id = v_tenant
        and coalesce(esr.buyer_name_raw, '') <> ''
    ),
    external_matches as (
      select distinct
        ec.tenant_id,
        ec.source,
        ec.norm_key as raw_buyer_key,
        be.entity_id as canonical_buyer_entity_id
      from external_candidates ec
      join buyer_entities be
        on be.tenant_id = ec.tenant_id
       and be.norm_name = ec.norm_key
      where ec.norm_key is not null
    )
    insert into public.buyer_aliases (
      tenant_id,
      raw_buyer_key,
      source,
      canonical_buyer_entity_id,
      resolution_confidence,
      created_at,
      updated_at
    )
    select
      em.tenant_id,
      em.raw_buyer_key,
      em.source,
      em.canonical_buyer_entity_id,
      1.0,
      now(),
      now()
    from external_matches em
    on conflict (tenant_id, raw_buyer_key, source) do nothing;

    get diagnostics v_inserted_external = row_count;
    raise notice 'buyer_alias_backfill: inserted % external aliases for tenant=%', v_inserted_external, v_tenant;
  else
    raise notice 'buyer_alias_backfill: external_signal_raw missing; skipping external alias backfill';
  end if;
end $$;

