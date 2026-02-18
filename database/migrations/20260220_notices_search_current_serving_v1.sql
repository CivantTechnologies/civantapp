-- Stop-the-bleeding: thin serving table for tender search (no raw JSON search)
-- NOTE: Do NOT drop indexes concurrently in migrations; see scripts/stop-bleeding-drop-indexes.sh

create extension if not exists pgcrypto;

create table if not exists public.notices_search_current (
  tenant_id text not null,
  canonical_id text not null,
  notice_id uuid,

  -- Primary display + filters
  source text,
  source_url text,
  region text, -- V1 region = ISO country code
  title text,
  title_norm text,
  buyer_entity_id text,
  buyer_name_raw text,
  buyer_name_norm text,
  cpv_codes text[] not null default '{}'::text[],
  cpv_cluster_id text,
  publication_date date,
  deadline_date date,
  status text,
  is_open boolean,
  estimated_value numeric,
  value_band text,

  -- Canonical verification badges
  coverage_status text,
  verification_level text,
  verification_sources text[] not null default '{}'::text[],
  has_ted boolean not null default false,
  ted_notice_ids text[] not null default '{}'::text[],
  notice_count integer not null default 1,

  -- Search text (bounded; avoids jsonb scans)
  short_summary text,
  search_text text,

  -- Change tracking
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  updated_at timestamptz,
  source_updated_at timestamptz,
  last_updated_at timestamptz not null default now(),

  -- Optional pointer to external raw payload storage (future stop-the-bleeding step)
  raw_object_key text,
  payload_hash text,

  primary key (tenant_id, canonical_id)
);

create unique index if not exists idx_notices_search_current_tenant_notice_unique
  on public.notices_search_current (tenant_id, notice_id)
  where notice_id is not null;

create index if not exists idx_notices_search_current_tenant_last_seen_desc
  on public.notices_search_current (tenant_id, last_seen_at desc nulls last, canonical_id);

create index if not exists idx_notices_search_current_tenant_deadline_date
  on public.notices_search_current (tenant_id, deadline_date);

create index if not exists idx_notices_search_current_tenant_region
  on public.notices_search_current (tenant_id, region);

create index if not exists idx_notices_search_current_tenant_source
  on public.notices_search_current (tenant_id, source);

create index if not exists idx_notices_search_current_tenant_source_updated
  on public.notices_search_current (tenant_id, source_updated_at desc nulls last);

-- -----------------------------
-- Backfill checkpoint (resumable)
-- -----------------------------
create table if not exists public.notices_search_backfill_state (
  tenant_id text primary key,
  cursor_updated_at timestamptz,
  cursor_canonical_id text,
  total_rows bigint,
  processed_rows bigint not null default 0,
  started_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz,
  last_error text
);

-- -----------------------------
-- Upsert helpers
-- -----------------------------
create or replace function public.refresh_notices_search_current_row(
  p_tenant_id text,
  p_canonical_id text
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_tenant_id text := lower(trim(coalesce(p_tenant_id, '')));
  v_canonical_id text := trim(coalesce(p_canonical_id, ''));
begin
  if v_tenant_id = '' or v_canonical_id = '' then
    return;
  end if;

  insert into public.notices_search_current (
    tenant_id,
    canonical_id,
    notice_id,
    source,
    source_url,
    region,
    title,
    title_norm,
    buyer_entity_id,
    buyer_name_raw,
    buyer_name_norm,
    cpv_codes,
    cpv_cluster_id,
    publication_date,
    deadline_date,
    status,
    is_open,
    estimated_value,
    value_band,
    coverage_status,
    verification_level,
    verification_sources,
    has_ted,
    ted_notice_ids,
    notice_count,
    short_summary,
    search_text,
    first_seen_at,
    last_seen_at,
    updated_at,
    source_updated_at,
    last_updated_at,
    payload_hash
  )
  select
    ct.tenant_id,
    ct.canonical_id,
    pn.notice_id,
    ct.source,
    coalesce(pn.source_url, ct.source_url) as source_url,
    ct.country as region,
    ct.title,
    coalesce(nullif(ct.title_norm, ''), public.normalize_match_text(ct.title)) as title_norm,
    ct.buyer_entity_id,
    ct.buyer_name_raw,
    coalesce(nullif(ct.buyer_name_norm, ''), public.normalize_match_text(ct.buyer_name_raw)) as buyer_name_norm,
    coalesce(ct.cpv_codes, '{}'::text[]),
    null::text as cpv_cluster_id,
    ct.publication_date,
    ct.deadline_date,
    ct.status,
    ct.is_open,
    ct.estimated_value,
    null::text as value_band,
    ct.coverage_status,
    ct.verification_level,
    coalesce(ct.verification_sources, '{}'::text[]),
    coalesce(ct.has_ted, false),
    coalesce(ct.ted_notice_ids, '{}'::text[]),
    greatest(coalesce(ct.notice_count, 1), 1),
    left(coalesce(
      ct.normalized_json->>'summary',
      ct.normalized_json->>'short_description',
      ct.normalized_json->>'description',
      ''
    ), 800) as short_summary,
    left(trim(concat_ws(' ',
      coalesce(ct.title, ''),
      coalesce(ct.buyer_name_raw, ''),
      coalesce(
        ct.normalized_json->>'summary',
        ct.normalized_json->>'short_description',
        ct.normalized_json->>'description',
        ''
      )
    )), 4000) as search_text,
    ct.first_seen_at,
    ct.last_seen_at,
    ct.updated_at,
    greatest(
      coalesce(ct.updated_at, ct.created_at),
      coalesce(pn.ingested_at, '-infinity'::timestamptz)
    ) as source_updated_at,
    now() as last_updated_at,
    encode(digest(convert_to(coalesce(ct.canonical_id, '') || '|' || coalesce(pn.notice_version_hash, ''), 'UTF8'), 'sha256'), 'hex') as payload_hash
  from public.canonical_tenders ct
  left join lateral (
    select
      n.notice_id,
      n.source_url,
      n.ingested_at,
      n.notice_version_hash
    from public.canonical_notice_links l
    join public.notices n
      on n.notice_id = l.notice_id
     and n.tenant_id = l.tenant_id
    where l.tenant_id = ct.tenant_id
      and l.canonical_id = ct.canonical_id
    order by l.is_primary desc, l.linked_at desc
    limit 1
  ) pn on true
  where ct.tenant_id = v_tenant_id
    and ct.canonical_id = v_canonical_id
  on conflict (tenant_id, canonical_id) do update set
    notice_id = excluded.notice_id,
    source = excluded.source,
    source_url = excluded.source_url,
    region = excluded.region,
    title = excluded.title,
    title_norm = excluded.title_norm,
    buyer_entity_id = excluded.buyer_entity_id,
    buyer_name_raw = excluded.buyer_name_raw,
    buyer_name_norm = excluded.buyer_name_norm,
    cpv_codes = excluded.cpv_codes,
    cpv_cluster_id = excluded.cpv_cluster_id,
    publication_date = excluded.publication_date,
    deadline_date = excluded.deadline_date,
    status = excluded.status,
    is_open = excluded.is_open,
    estimated_value = excluded.estimated_value,
    value_band = excluded.value_band,
    coverage_status = excluded.coverage_status,
    verification_level = excluded.verification_level,
    verification_sources = excluded.verification_sources,
    has_ted = excluded.has_ted,
    ted_notice_ids = excluded.ted_notice_ids,
    notice_count = excluded.notice_count,
    short_summary = excluded.short_summary,
    search_text = excluded.search_text,
    first_seen_at = excluded.first_seen_at,
    last_seen_at = excluded.last_seen_at,
    updated_at = excluded.updated_at,
    source_updated_at = excluded.source_updated_at,
    last_updated_at = excluded.last_updated_at,
    payload_hash = excluded.payload_hash;
end;
$$;

create or replace function public.backfill_notices_search_current_batch(
  p_tenant_id text,
  p_batch_size integer default 2000,
  p_cursor_updated_at timestamptz default null,
  p_cursor_canonical_id text default null
)
returns table (
  processed integer,
  inserted integer,
  updated integer,
  next_cursor_updated_at timestamptz,
  next_cursor_canonical_id text,
  done boolean
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_tenant_id text := lower(trim(coalesce(p_tenant_id, '')));
  v_batch_size integer := greatest(1, least(coalesce(p_batch_size, 2000), 20000));
begin
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  with candidates as (
    select ct.tenant_id, ct.canonical_id, ct.updated_at
    from public.canonical_tenders ct
    where ct.tenant_id = v_tenant_id
      and (
        p_cursor_updated_at is null
        or (ct.updated_at, ct.canonical_id) < (p_cursor_updated_at, coalesce(p_cursor_canonical_id, ''))
      )
    order by ct.updated_at desc, ct.canonical_id desc
    limit v_batch_size
  ),
  upserted as (
    insert into public.notices_search_current (
      tenant_id,
      canonical_id,
      notice_id,
      source,
      source_url,
      region,
      title,
      title_norm,
      buyer_entity_id,
      buyer_name_raw,
      buyer_name_norm,
      cpv_codes,
      cpv_cluster_id,
      publication_date,
      deadline_date,
      status,
      is_open,
      estimated_value,
      value_band,
      coverage_status,
      verification_level,
      verification_sources,
      has_ted,
      ted_notice_ids,
      notice_count,
      short_summary,
      search_text,
      first_seen_at,
      last_seen_at,
      updated_at,
      source_updated_at,
      last_updated_at,
      payload_hash
    )
    select
      ct.tenant_id,
      ct.canonical_id,
      pn.notice_id,
      ct.source,
      coalesce(pn.source_url, ct.source_url) as source_url,
      ct.country as region,
      ct.title,
      coalesce(nullif(ct.title_norm, ''), public.normalize_match_text(ct.title)) as title_norm,
      ct.buyer_entity_id,
      ct.buyer_name_raw,
      coalesce(nullif(ct.buyer_name_norm, ''), public.normalize_match_text(ct.buyer_name_raw)) as buyer_name_norm,
      coalesce(ct.cpv_codes, '{}'::text[]),
      null::text as cpv_cluster_id,
      ct.publication_date,
      ct.deadline_date,
      ct.status,
      ct.is_open,
      ct.estimated_value,
      null::text as value_band,
      ct.coverage_status,
      ct.verification_level,
      coalesce(ct.verification_sources, '{}'::text[]),
      coalesce(ct.has_ted, false),
      coalesce(ct.ted_notice_ids, '{}'::text[]),
      greatest(coalesce(ct.notice_count, 1), 1),
      left(coalesce(
        ct.normalized_json->>'summary',
        ct.normalized_json->>'short_description',
        ct.normalized_json->>'description',
        ''
      ), 800) as short_summary,
      left(trim(concat_ws(' ',
        coalesce(ct.title, ''),
        coalesce(ct.buyer_name_raw, ''),
        coalesce(
          ct.normalized_json->>'summary',
          ct.normalized_json->>'short_description',
          ct.normalized_json->>'description',
          ''
        )
      )), 4000) as search_text,
      ct.first_seen_at,
      ct.last_seen_at,
      ct.updated_at,
      greatest(
        coalesce(ct.updated_at, ct.created_at),
        coalesce(pn.ingested_at, '-infinity'::timestamptz)
      ) as source_updated_at,
      now() as last_updated_at,
      encode(digest(convert_to(coalesce(ct.canonical_id, '') || '|' || coalesce(pn.notice_version_hash, ''), 'UTF8'), 'sha256'), 'hex') as payload_hash
    from candidates c
    join public.canonical_tenders ct
      on ct.tenant_id = c.tenant_id
     and ct.canonical_id = c.canonical_id
    left join lateral (
      select
        n.notice_id,
        n.source_url,
        n.ingested_at,
        n.notice_version_hash
      from public.canonical_notice_links l
      join public.notices n
        on n.notice_id = l.notice_id
       and n.tenant_id = l.tenant_id
      where l.tenant_id = ct.tenant_id
        and l.canonical_id = ct.canonical_id
      order by l.is_primary desc, l.linked_at desc
      limit 1
    ) pn on true
    on conflict (tenant_id, canonical_id) do update set
      notice_id = excluded.notice_id,
      source = excluded.source,
      source_url = excluded.source_url,
      region = excluded.region,
      title = excluded.title,
      title_norm = excluded.title_norm,
      buyer_entity_id = excluded.buyer_entity_id,
      buyer_name_raw = excluded.buyer_name_raw,
      buyer_name_norm = excluded.buyer_name_norm,
      cpv_codes = excluded.cpv_codes,
      cpv_cluster_id = excluded.cpv_cluster_id,
      publication_date = excluded.publication_date,
      deadline_date = excluded.deadline_date,
      status = excluded.status,
      is_open = excluded.is_open,
      estimated_value = excluded.estimated_value,
      value_band = excluded.value_band,
      coverage_status = excluded.coverage_status,
      verification_level = excluded.verification_level,
      verification_sources = excluded.verification_sources,
      has_ted = excluded.has_ted,
      ted_notice_ids = excluded.ted_notice_ids,
      notice_count = excluded.notice_count,
      short_summary = excluded.short_summary,
      search_text = excluded.search_text,
      first_seen_at = excluded.first_seen_at,
      last_seen_at = excluded.last_seen_at,
      updated_at = excluded.updated_at,
      source_updated_at = excluded.source_updated_at,
      last_updated_at = excluded.last_updated_at,
      payload_hash = excluded.payload_hash
    returning (xmax = 0) as inserted_row
  ),
  stats as (
    select
      (select count(*) from candidates)::int as processed,
      (select count(*) from upserted where inserted_row)::int as inserted,
      (select count(*) from upserted where not inserted_row)::int as updated,
      (select max(updated_at) from candidates) as max_updated_at,
      (select canonical_id from candidates order by updated_at asc, canonical_id asc limit 1) as min_canonical_id,
      (select canonical_id from candidates order by updated_at desc, canonical_id desc limit 1) as max_canonical_id,
      (select updated_at from candidates order by updated_at asc, canonical_id asc limit 1) as min_updated_at
  )
  select
    s.processed,
    s.inserted,
    s.updated,
    s.min_updated_at as next_cursor_updated_at,
    s.min_canonical_id as next_cursor_canonical_id,
    (s.processed = 0) as done
  from stats s;
end;
$$;

-- -----------------------------
-- Incremental sync triggers
-- -----------------------------
create or replace function public.trg_refresh_notices_search_current_from_canonical()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.refresh_notices_search_current_row(new.tenant_id, new.canonical_id);
  return new;
end;
$$;

drop trigger if exists trg_canonical_tenders_refresh_notices_search_current on public.canonical_tenders;
create trigger trg_canonical_tenders_refresh_notices_search_current
after insert or update on public.canonical_tenders
for each row
execute function public.trg_refresh_notices_search_current_from_canonical();

create or replace function public.trg_refresh_notices_search_current_from_links()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_tenant_id text := coalesce(new.tenant_id, old.tenant_id);
  v_canonical_id text := coalesce(new.canonical_id, old.canonical_id);
begin
  perform public.refresh_notices_search_current_row(v_tenant_id, v_canonical_id);
  return coalesce(new, old);
end;
$$;

drop trigger if exists trg_canonical_notice_links_refresh_notices_search_current on public.canonical_notice_links;
create trigger trg_canonical_notice_links_refresh_notices_search_current
after insert or update or delete on public.canonical_notice_links
for each row
execute function public.trg_refresh_notices_search_current_from_links();

-- -----------------------------
-- RLS
-- -----------------------------
alter table public.notices_search_current enable row level security;
alter table public.notices_search_current force row level security;

drop policy if exists notices_search_current_select on public.notices_search_current;
create policy notices_search_current_select
on public.notices_search_current
for select
to authenticated
using (tenant_id = public.current_tenant_id());

drop policy if exists notices_search_current_insert on public.notices_search_current;
create policy notices_search_current_insert
on public.notices_search_current
for insert
to authenticated
with check (tenant_id = public.current_tenant_id());

drop policy if exists notices_search_current_update on public.notices_search_current;
create policy notices_search_current_update
on public.notices_search_current
for update
to authenticated
using (tenant_id = public.current_tenant_id())
with check (tenant_id = public.current_tenant_id());

drop policy if exists notices_search_current_delete on public.notices_search_current;
create policy notices_search_current_delete
on public.notices_search_current
for delete
to authenticated
using (tenant_id = public.current_tenant_id());

alter table public.notices_search_backfill_state enable row level security;
alter table public.notices_search_backfill_state force row level security;

drop policy if exists notices_search_backfill_state_select on public.notices_search_backfill_state;
create policy notices_search_backfill_state_select
on public.notices_search_backfill_state
for select
to authenticated
using (tenant_id = public.current_tenant_id());

drop policy if exists notices_search_backfill_state_insert on public.notices_search_backfill_state;
create policy notices_search_backfill_state_insert
on public.notices_search_backfill_state
for insert
to authenticated
with check (tenant_id = public.current_tenant_id());

drop policy if exists notices_search_backfill_state_update on public.notices_search_backfill_state;
create policy notices_search_backfill_state_update
on public.notices_search_backfill_state
for update
to authenticated
using (tenant_id = public.current_tenant_id())
with check (tenant_id = public.current_tenant_id());

