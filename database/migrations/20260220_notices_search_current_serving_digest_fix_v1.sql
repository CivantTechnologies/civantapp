-- Fix: Supabase installs pgcrypto functions under the "extensions" schema.
-- Replace digest() calls with extensions.digest() in serving table upsert/backfill helpers.

create extension if not exists pgcrypto;

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
    encode(extensions.digest(convert_to(coalesce(ct.canonical_id, '') || '|' || coalesce(pn.notice_version_hash, ''), 'UTF8'), 'sha256'), 'hex') as payload_hash
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
      encode(extensions.digest(convert_to(coalesce(ct.canonical_id, '') || '|' || coalesce(pn.notice_version_hash, ''), 'UTF8'), 'sha256'), 'hex') as payload_hash
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
      (select canonical_id from candidates order by updated_at asc, canonical_id asc limit 1) as min_canonical_id,
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

