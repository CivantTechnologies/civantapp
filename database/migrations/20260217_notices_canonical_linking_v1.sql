-- Canonical notice linking layer: schema foundation
-- Adds source notice storage, canonical notice links, and canonical enrichment columns.

create extension if not exists pgcrypto;
create extension if not exists pg_trgm;

create table if not exists public.notices (
  notice_id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  source text not null,
  source_notice_id text not null,
  notice_version_hash text not null,
  source_url text,
  country text,
  title text,
  buyer_name_raw text,
  buyer_name_norm text,
  cpv_codes text[] not null default '{}'::text[],
  publication_date date,
  deadline_date date,
  status text,
  raw_payload jsonb not null default '{}'::jsonb,
  parsed_payload jsonb not null default '{}'::jsonb,
  ingested_at timestamptz not null default now()
);

create unique index if not exists idx_notices_tenant_source_notice_version
  on public.notices (tenant_id, source, source_notice_id, notice_version_hash);

create index if not exists idx_notices_tenant_source_notice
  on public.notices (tenant_id, source, source_notice_id);

create index if not exists idx_notices_tenant_publication_date_desc
  on public.notices (tenant_id, publication_date desc);

create index if not exists idx_notices_tenant_deadline_date
  on public.notices (tenant_id, deadline_date);

create index if not exists idx_notices_raw_payload_gin
  on public.notices using gin (raw_payload);

create table if not exists public.canonical_notice_links (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  canonical_id text not null references public.canonical_tenders(canonical_id) on delete cascade,
  notice_id uuid not null references public.notices(notice_id) on delete cascade,
  link_tier text not null default 'hard',
  match_score numeric not null default 1.0,
  match_features jsonb not null default '{}'::jsonb,
  is_primary boolean not null default false,
  linked_at timestamptz not null default now(),
  unique (tenant_id, notice_id)
);

create index if not exists idx_canonical_notice_links_tenant_canonical
  on public.canonical_notice_links (tenant_id, canonical_id);

create index if not exists idx_canonical_notice_links_tenant_linked_at
  on public.canonical_notice_links (tenant_id, linked_at desc);

create table if not exists public.reconciliation_queue (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  notice_id uuid not null,
  candidate_canonical_id text,
  score numeric,
  reasons jsonb not null default '{}'::jsonb,
  status text not null default 'pending',
  created_at timestamptz not null default now()
);

do $$
begin
  if to_regclass('public.reconciliation_queue') is not null then
    -- Backward-compatible alignment for environments where reconciliation_queue already existed
    -- with a different shape from earlier pipeline migrations.
    alter table public.reconciliation_queue add column if not exists notice_id uuid;
    alter table public.reconciliation_queue add column if not exists candidate_canonical_id text;
    alter table public.reconciliation_queue add column if not exists score numeric;
    alter table public.reconciliation_queue add column if not exists reasons jsonb not null default '{}'::jsonb;
    alter table public.reconciliation_queue add column if not exists status text not null default 'pending';
    alter table public.reconciliation_queue add column if not exists created_at timestamptz not null default now();

    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'reconciliation_queue'
        and column_name = 'candidate_json'
    ) then
      alter table public.reconciliation_queue alter column candidate_json set default '{}'::jsonb;
    end if;

    -- Ensure id has a default generator regardless of whether id is uuid or text in legacy DBs.
    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'reconciliation_queue'
        and column_name = 'id'
        and data_type = 'uuid'
    ) then
      alter table public.reconciliation_queue alter column id set default gen_random_uuid();
    elsif exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = 'reconciliation_queue'
        and column_name = 'id'
        and data_type in ('text', 'character varying')
    ) then
      alter table public.reconciliation_queue alter column id set default (gen_random_uuid()::text);
    end if;
  end if;
end $$;

create index if not exists idx_reconciliation_queue_tenant_status_created
  on public.reconciliation_queue (tenant_id, status, created_at desc);

create index if not exists idx_reconciliation_queue_tenant_notice
  on public.reconciliation_queue (tenant_id, notice_id);

create index if not exists idx_reconciliation_queue_tenant_candidate
  on public.reconciliation_queue (tenant_id, candidate_canonical_id);

alter table public.canonical_tenders
  add column if not exists country text,
  add column if not exists buyer_name_raw text,
  add column if not exists buyer_name_norm text,
  add column if not exists title_norm text,
  add column if not exists status text,
  add column if not exists is_open boolean not null default true,
  add column if not exists first_seen_at timestamptz,
  add column if not exists last_seen_at timestamptz,
  add column if not exists notice_count integer not null default 0,
  add column if not exists verification_state text,
  add column if not exists verification_sources text[] not null default '{}'::text[],
  add column if not exists has_ted boolean not null default false,
  add column if not exists ted_notice_ids text[] not null default '{}'::text[],
  add column if not exists coverage_status text not null default 'national_only',
  add column if not exists verification_level text not null default 'unverified';

create index if not exists idx_canonical_tenders_tenant_publication_date_desc
  on public.canonical_tenders (tenant_id, publication_date desc);

create index if not exists idx_canonical_tenders_tenant_deadline_date
  on public.canonical_tenders (tenant_id, deadline_date);

create index if not exists idx_canonical_tenders_tenant_is_open_deadline
  on public.canonical_tenders (tenant_id, is_open, deadline_date);

create index if not exists idx_canonical_tenders_tenant_last_seen_at_desc
  on public.canonical_tenders (tenant_id, last_seen_at desc);

create index if not exists idx_canonical_tenders_tenant_source
  on public.canonical_tenders (tenant_id, source);

create index if not exists idx_canonical_tenders_tenant_external_id
  on public.canonical_tenders (tenant_id, external_id);

create index if not exists idx_canonical_tenders_tenant_buyer_name_norm
  on public.canonical_tenders (tenant_id, buyer_name_norm);

create index if not exists idx_canonical_tenders_cpv_codes_gin
  on public.canonical_tenders using gin (cpv_codes);

create index if not exists idx_canonical_tenders_verification_sources_gin
  on public.canonical_tenders using gin (verification_sources);

create index if not exists idx_canonical_tenders_title_norm_trgm
  on public.canonical_tenders using gin (title_norm gin_trgm_ops);

create index if not exists idx_canonical_tenders_buyer_norm_trgm
  on public.canonical_tenders using gin (buyer_name_norm gin_trgm_ops);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_notice_links_link_tier'
      and conrelid = 'public.canonical_notice_links'::regclass
  ) then
    alter table public.canonical_notice_links
      add constraint ck_canonical_notice_links_link_tier
      check (link_tier in ('hard', 'strong', 'fuzzy', 'seeded'));
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_tenders_coverage_status'
      and conrelid = 'public.canonical_tenders'::regclass
  ) then
    alter table public.canonical_tenders
      add constraint ck_canonical_tenders_coverage_status
      check (coverage_status in ('national_only', 'ted_only', 'linked'));
  end if;
end $$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_tenders_verification_level'
      and conrelid = 'public.canonical_tenders'::regclass
  ) then
    alter table public.canonical_tenders
      add constraint ck_canonical_tenders_verification_level
      check (verification_level in ('unverified', 'partially_verified', 'verified'));
  end if;
end $$;

alter table public.notices enable row level security;
alter table public.notices force row level security;

alter table public.canonical_notice_links enable row level security;
alter table public.canonical_notice_links force row level security;

alter table public.reconciliation_queue enable row level security;
alter table public.reconciliation_queue force row level security;

drop policy if exists notices_tenant_select on public.notices;
drop policy if exists notices_tenant_insert on public.notices;
drop policy if exists notices_tenant_update on public.notices;
drop policy if exists notices_tenant_delete on public.notices;

create policy notices_tenant_select
on public.notices
for select
to authenticated
using (tenant_id = public.current_tenant_id());

create policy notices_tenant_insert
on public.notices
for insert
to authenticated
with check (tenant_id = public.current_tenant_id());

create policy notices_tenant_update
on public.notices
for update
to authenticated
using (tenant_id = public.current_tenant_id())
with check (tenant_id = public.current_tenant_id());

create policy notices_tenant_delete
on public.notices
for delete
to authenticated
using (tenant_id = public.current_tenant_id());

drop policy if exists canonical_notice_links_tenant_select on public.canonical_notice_links;
drop policy if exists canonical_notice_links_tenant_insert on public.canonical_notice_links;
drop policy if exists canonical_notice_links_tenant_update on public.canonical_notice_links;
drop policy if exists canonical_notice_links_tenant_delete on public.canonical_notice_links;

create policy canonical_notice_links_tenant_select
on public.canonical_notice_links
for select
to authenticated
using (tenant_id = public.current_tenant_id());

create policy canonical_notice_links_tenant_insert
on public.canonical_notice_links
for insert
to authenticated
with check (tenant_id = public.current_tenant_id());

create policy canonical_notice_links_tenant_update
on public.canonical_notice_links
for update
to authenticated
using (tenant_id = public.current_tenant_id())
with check (tenant_id = public.current_tenant_id());

create policy canonical_notice_links_tenant_delete
on public.canonical_notice_links
for delete
to authenticated
using (tenant_id = public.current_tenant_id());

drop policy if exists reconciliation_queue_tenant_select on public.reconciliation_queue;
drop policy if exists reconciliation_queue_tenant_insert on public.reconciliation_queue;
drop policy if exists reconciliation_queue_tenant_update on public.reconciliation_queue;
drop policy if exists reconciliation_queue_tenant_delete on public.reconciliation_queue;

create policy reconciliation_queue_tenant_select
on public.reconciliation_queue
for select
to authenticated
using (tenant_id = public.current_tenant_id());

create policy reconciliation_queue_tenant_insert
on public.reconciliation_queue
for insert
to authenticated
with check (tenant_id = public.current_tenant_id());

create policy reconciliation_queue_tenant_update
on public.reconciliation_queue
for update
to authenticated
using (tenant_id = public.current_tenant_id())
with check (tenant_id = public.current_tenant_id());

create policy reconciliation_queue_tenant_delete
on public.reconciliation_queue
for delete
to authenticated
using (tenant_id = public.current_tenant_id());
