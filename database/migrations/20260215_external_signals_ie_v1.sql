-- Civant: External Signals Foundation v1 (Ireland only)
-- Additive: creates a clean external signals table + helper functions + RLS.
-- No canonical tables are modified.

begin;

-- gen_random_uuid()
create extension if not exists pgcrypto;

create table if not exists public.external_signals_ie (
  id uuid primary key default gen_random_uuid(),

  tenant_id text not null,
  buyer_id text null,
  buyer_name_raw text null,
  buyer_name_normalized text null,

  signal_type text not null,
  signal_source text not null,
  source_event_id text not null,

  signal_date date not null,
  captured_at timestamptz not null default now(),

  signal_strength numeric(4,3) not null default 0.500,

  evidence_url text not null,
  evidence_text text null,

  extracted_entities jsonb not null default '{}'::jsonb,
  raw_payload jsonb not null default '{}'::jsonb,

  country_code text not null default 'IE',

  constraint external_signals_ie_source_uniq unique (tenant_id, signal_source, source_event_id),

  constraint external_signals_ie_signal_type_chk
    check (signal_type in ('hiring','funding')),

  constraint external_signals_ie_signal_strength_chk
    check (signal_strength >= 0 and signal_strength <= 1),

  constraint external_signals_ie_evidence_text_len_chk
    check (evidence_text is null or length(evidence_text) <= 800),

  constraint external_signals_ie_country_chk
    check (country_code = 'IE')
);

-- Hot-path indexes (query-oriented)
create index if not exists idx_external_signals_ie_tenant_buyer_type_date
  on public.external_signals_ie (tenant_id, buyer_id, signal_type, signal_date desc);

create index if not exists idx_external_signals_ie_tenant_type_date
  on public.external_signals_ie (tenant_id, signal_type, signal_date desc);

create index if not exists idx_external_signals_ie_tenant_buyer_name_norm_when_no_id
  on public.external_signals_ie (tenant_id, buyer_name_normalized)
  where buyer_id is null and buyer_name_normalized is not null;

-- =====================================================================================
-- Helpers
-- =====================================================================================

-- Tenant context helper.
-- Civant already uses public.current_tenant_id() for multi-tenant RLS; this wrapper also
-- supports local/admin scripts that set app.tenant_id.
create or replace function public.civant_current_tenant_id()
returns text
language sql
stable
as $$
  select coalesce(nullif(current_setting('app.tenant_id', true), ''), public.current_tenant_id());
$$;

-- Minimal buyer name normalization (not entity resolution):
-- - lowercase
-- - trim
-- - remove punctuation (non-alphanum -> space)
-- - collapse whitespace
create or replace function public.civant_ie__normalize_buyer_name(p_name text)
returns text
language sql
immutable
as $$
  with
  s0 as (
    select lower(trim(coalesce(p_name,''))) as s
  ),
  s1 as (
    select regexp_replace(s, '[^a-z0-9]+', ' ', 'g') as s from s0
  ),
  s2 as (
    select trim(regexp_replace(s, '[[:space:]]+', ' ', 'g')) as s from s1
  )
  select nullif(s, '') from s2;
$$;

-- Idempotent upsert for connectors (replay-safe).
create or replace function public.upsert_external_signal_ie(
  p_tenant_id text,
  p_signal_type text,
  p_signal_source text,
  p_source_event_id text,
  p_signal_date date,
  p_evidence_url text,
  p_evidence_text text default null,
  p_buyer_id text default null,
  p_buyer_name_raw text default null,
  p_signal_strength numeric default 0.5,
  p_extracted_entities jsonb default '{}'::jsonb,
  p_raw_payload jsonb default '{}'::jsonb
)
returns uuid
language plpgsql
as $$
declare
  v_buyer_name_normalized text;
  v_id uuid;
begin
  if p_buyer_id is null then
    v_buyer_name_normalized := public.civant_ie__normalize_buyer_name(p_buyer_name_raw);
  else
    v_buyer_name_normalized := null;
  end if;

  with upserted as (
    insert into public.external_signals_ie (
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
      extracted_entities,
      raw_payload,
      country_code
    )
    values (
      p_tenant_id,
      p_buyer_id,
      p_buyer_name_raw,
      v_buyer_name_normalized,
      p_signal_type,
      p_signal_source,
      p_source_event_id,
      p_signal_date,
      p_signal_strength,
      p_evidence_url,
      p_evidence_text,
      coalesce(p_extracted_entities, '{}'::jsonb),
      coalesce(p_raw_payload, '{}'::jsonb),
      'IE'
    )
    on conflict (tenant_id, signal_source, source_event_id) do update set
      buyer_id = excluded.buyer_id,
      buyer_name_raw = excluded.buyer_name_raw,
      buyer_name_normalized = excluded.buyer_name_normalized,
      signal_type = excluded.signal_type,
      signal_date = excluded.signal_date,
      signal_strength = excluded.signal_strength,
      evidence_url = excluded.evidence_url,
      evidence_text = excluded.evidence_text,
      extracted_entities = excluded.extracted_entities,
      raw_payload = excluded.raw_payload,
      country_code = excluded.country_code,
      captured_at = excluded.captured_at
    where
      public.external_signals_ie.buyer_id is distinct from excluded.buyer_id
      or public.external_signals_ie.buyer_name_raw is distinct from excluded.buyer_name_raw
      or public.external_signals_ie.buyer_name_normalized is distinct from excluded.buyer_name_normalized
      or public.external_signals_ie.signal_type is distinct from excluded.signal_type
      or public.external_signals_ie.signal_date is distinct from excluded.signal_date
      or public.external_signals_ie.signal_strength is distinct from excluded.signal_strength
      or public.external_signals_ie.evidence_url is distinct from excluded.evidence_url
      or public.external_signals_ie.evidence_text is distinct from excluded.evidence_text
      or public.external_signals_ie.extracted_entities is distinct from excluded.extracted_entities
      or public.external_signals_ie.raw_payload is distinct from excluded.raw_payload
      or public.external_signals_ie.country_code is distinct from excluded.country_code
    returning id
  )
  select coalesce(
    (select id from upserted),
    (select es.id
     from public.external_signals_ie es
     where es.tenant_id = p_tenant_id
       and es.signal_source = p_signal_source
       and es.source_event_id = p_source_event_id
     limit 1)
  )
  into v_id;

  return v_id;
end;
$$;

-- =====================================================================================
-- RLS (tenant isolation)
-- =====================================================================================

alter table public.external_signals_ie enable row level security;
alter table public.external_signals_ie force row level security;

drop policy if exists external_signals_ie_tenant_select on public.external_signals_ie;
drop policy if exists external_signals_ie_tenant_insert on public.external_signals_ie;
drop policy if exists external_signals_ie_tenant_update on public.external_signals_ie;
drop policy if exists external_signals_ie_tenant_delete on public.external_signals_ie;

create policy external_signals_ie_tenant_select
on public.external_signals_ie
for select
to authenticated
using (tenant_id = public.civant_current_tenant_id());

create policy external_signals_ie_tenant_insert
on public.external_signals_ie
for insert
to authenticated
with check (tenant_id = public.civant_current_tenant_id());

create policy external_signals_ie_tenant_update
on public.external_signals_ie
for update
to authenticated
using (tenant_id = public.civant_current_tenant_id())
with check (tenant_id = public.civant_current_tenant_id());

create policy external_signals_ie_tenant_delete
on public.external_signals_ie
for delete
to authenticated
using (tenant_id = public.civant_current_tenant_id());

commit;
