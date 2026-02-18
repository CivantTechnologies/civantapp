-- Payload offload columns + failure tracking (stop DB growth safely)

create extension if not exists pgcrypto;

-- -----------------------------
-- Column additions
-- -----------------------------
alter table if exists public.notices
  add column if not exists raw_object_key text,
  add column if not exists payload_hash_sha256 text,
  add column if not exists payload_bytes integer,
  add column if not exists payload_stored_at timestamptz;

alter table if exists public."TendersCurrent"
  add column if not exists raw_object_key text,
  add column if not exists payload_hash_sha256 text,
  add column if not exists payload_bytes integer,
  add column if not exists payload_stored_at timestamptz;

alter table if exists public.canonical_tenders
  add column if not exists raw_object_key text,
  add column if not exists payload_hash_sha256 text,
  add column if not exists payload_bytes integer,
  add column if not exists payload_stored_at timestamptz;

-- -----------------------------
-- Failure tracking + checkpoints
-- -----------------------------
create table if not exists public.payload_offload_failures (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  table_name text not null,
  primary_key text not null,
  payload_hash_sha256 text,
  raw_object_key text,
  error text not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_payload_offload_failures_tenant_created
  on public.payload_offload_failures (tenant_id, created_at desc);

create index if not exists idx_payload_offload_failures_table
  on public.payload_offload_failures (table_name, created_at desc);

create table if not exists public.payload_offload_state (
  tenant_id text not null,
  table_name text not null,
  last_pk text,
  processed_rows bigint not null default 0,
  updated_at timestamptz not null default now(),
  primary key (tenant_id, table_name)
);

-- -----------------------------
-- Notices sync: propagate payload pointer metadata
-- -----------------------------
create or replace function public.upsert_notice_and_link(
  p_tenant_id text,
  p_source text,
  p_source_notice_id text,
  p_source_url text,
  p_country text,
  p_title text,
  p_buyer_name_raw text,
  p_cpv_codes text[],
  p_publication_date date,
  p_deadline_date date,
  p_status text,
  p_raw_payload jsonb,
  p_parsed_payload jsonb,
  p_notice_version_hash text,
  p_raw_object_key text default null,
  p_payload_hash_sha256 text default null,
  p_payload_bytes integer default null,
  p_payload_stored_at timestamptz default null
)
returns table (
  notice_id uuid,
  canonical_id text,
  link_tier text,
  match_score numeric,
  created_canonical boolean
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_tenant_id text := lower(trim(coalesce(p_tenant_id, '')));
  v_source text := upper(trim(coalesce(p_source, '')));
  v_source_notice_id text := trim(coalesce(p_source_notice_id, ''));
  v_country text := upper(trim(coalesce(p_country, '')));
  v_notice_hash text := coalesce(nullif(trim(coalesce(p_notice_version_hash, '')), ''), public.compute_notice_version_hash(p_raw_payload));

  v_notice_id uuid;
  v_canonical_id text;
  v_link_tier text := 'hard';
  v_match_score numeric := 1.0;
  v_created_canonical boolean := false;
  v_match_features jsonb := '{}'::jsonb;

  v_title_norm text := public.normalize_match_text(p_title);
  v_buyer_norm text := public.normalize_match_text(p_buyer_name_raw);
  v_clean_cpv_codes text[] := '{}'::text[];
  v_external_key text;

  v_best_candidate text;
  v_best_score numeric := 0;
  v_second_score numeric := 0;
  v_candidate_gap numeric := 0;

begin
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  if v_source = '' then
    raise exception 'source is required';
  end if;

  if v_source_notice_id = '' then
    raise exception 'source_notice_id is required';
  end if;

  select coalesce(array_agg(distinct left(cleaned, 8)), '{}'::text[])
  into v_clean_cpv_codes
  from (
    select regexp_replace(lower(trim(v)), '[^0-9]', '', 'g') as cleaned
    from unnest(coalesce(p_cpv_codes, '{}'::text[])) as v
  ) t
  where cleaned <> '';

  v_external_key := v_tenant_id || ':' || v_source || ':' || v_source_notice_id;

  insert into public.notices (
    tenant_id,
    source,
    source_notice_id,
    notice_version_hash,
    source_url,
    country,
    title,
    buyer_name_raw,
    buyer_name_norm,
    cpv_codes,
    publication_date,
    deadline_date,
    status,
    raw_payload,
    parsed_payload,
    raw_object_key,
    payload_hash_sha256,
    payload_bytes,
    payload_stored_at,
    ingested_at
  )
  values (
    v_tenant_id,
    v_source,
    v_source_notice_id,
    v_notice_hash,
    nullif(trim(coalesce(p_source_url, '')), ''),
    nullif(v_country, ''),
    nullif(trim(coalesce(p_title, '')), ''),
    nullif(trim(coalesce(p_buyer_name_raw, '')), ''),
    v_buyer_norm,
    v_clean_cpv_codes,
    p_publication_date,
    p_deadline_date,
    nullif(trim(coalesce(p_status, '')), ''),
    case when p_raw_object_key is not null then null else coalesce(p_raw_payload, '{}'::jsonb) end,
    coalesce(p_parsed_payload, '{}'::jsonb),
    nullif(p_raw_object_key, ''),
    nullif(p_payload_hash_sha256, ''),
    p_payload_bytes,
    p_payload_stored_at,
    now()
  )
  on conflict (tenant_id, source, source_notice_id, notice_version_hash)
  do update set
    source_url = excluded.source_url,
    country = excluded.country,
    title = excluded.title,
    buyer_name_raw = excluded.buyer_name_raw,
    buyer_name_norm = excluded.buyer_name_norm,
    cpv_codes = excluded.cpv_codes,
    publication_date = excluded.publication_date,
    deadline_date = excluded.deadline_date,
    status = excluded.status,
    raw_payload = excluded.raw_payload,
    parsed_payload = excluded.parsed_payload,
    raw_object_key = excluded.raw_object_key,
    payload_hash_sha256 = excluded.payload_hash_sha256,
    payload_bytes = excluded.payload_bytes,
    payload_stored_at = excluded.payload_stored_at,
    ingested_at = now()
  returning notices.notice_id into v_notice_id;

  -- (rest of function body unchanged)
  -- The remainder below is copied from the existing function definition.

  if v_source = 'TED' then
    v_match_features := jsonb_build_object(
      'source', v_source,
      'source_notice_id', v_source_notice_id,
      'link_tier', 'hard',
      'match_score', 1.0,
      'reason', 'TED canonical IDs are deterministic'
    );
  end if;

  if v_source <> 'TED' then
    select
      l.canonical_id,
      l.link_tier,
      l.match_score,
      jsonb_build_object(
        'source', v_source,
        'source_notice_id', v_source_notice_id,
        'link_tier', l.link_tier,
        'match_score', l.match_score,
        'reason', 'existing notice link'
      )
    into
      v_canonical_id,
      v_link_tier,
      v_match_score,
      v_match_features
    from public.canonical_notice_links l
    where l.tenant_id = v_tenant_id
      and l.notice_id = v_notice_id
    order by l.created_at desc
    limit 1;
  end if;

  if v_canonical_id is null then
    -- best candidate match path (unchanged)
    select
      c.canonical_id,
      c.score,
      c.features
    into v_best_candidate, v_best_score, v_match_features
    from public.find_best_canonical_tender_match(
      v_tenant_id,
      v_source,
      v_source_notice_id,
      p_title,
      p_buyer_name_raw,
      v_clean_cpv_codes,
      p_publication_date,
      p_deadline_date
    ) c
    order by c.score desc
    limit 1;

    select
      c.score
    into v_second_score
    from public.find_best_canonical_tender_match(
      v_tenant_id,
      v_source,
      v_source_notice_id,
      p_title,
      p_buyer_name_raw,
      v_clean_cpv_codes,
      p_publication_date,
      p_deadline_date
    ) c
    order by c.score desc
    offset 1
    limit 1;

    v_candidate_gap := coalesce(v_best_score, 0) - coalesce(v_second_score, 0);

    if v_best_candidate is not null and v_best_score >= 0.90 then
      v_canonical_id := v_best_candidate;
      v_link_tier := 'strong';
      v_match_score := v_best_score;
    elsif v_best_candidate is not null and v_best_score >= 0.75 and v_best_score < 0.90 then
      if to_regclass('public.reconciliation_queue') is not null then
        insert into public.reconciliation_queue (
          tenant_id,
          notice_id,
          candidate_canonical_id,
          score,
          reasons,
          status,
          created_at
        )
        values (
          v_tenant_id,
          v_notice_id,
          v_best_candidate,
          v_best_score,
          jsonb_build_object(
            'source', v_source,
            'source_notice_id', v_source_notice_id,
            'score', v_best_score,
            'gap', v_candidate_gap,
            'title', p_title,
            'buyer_name_raw', p_buyer_name_raw,
            'thresholds', jsonb_build_object(
              'auto_link_min', 0.90,
              'queue_min', 0.75
            )
          ),
          'pending',
          now()
        )
        on conflict do nothing;
      end if;

      v_match_score := v_best_score;
      v_link_tier := 'seeded';
    end if;
  end if;

  if v_canonical_id is null then
    v_canonical_id := 'can_' || substr(md5(v_external_key), 1, 24);

    if not exists (
      select 1
      from public.canonical_tenders ct
      where ct.canonical_id = v_canonical_id
        and ct.tenant_id = v_tenant_id
    ) then
      insert into public.canonical_tenders (
        canonical_id,
        tenant_id,
        source,
        external_id,
        title,
        title_norm,
        buyer_name_raw,
        buyer_name_norm,
        country,
        cpv_codes,
        publication_date,
        deadline_date,
        status,
        is_open,
        first_seen_at,
        last_seen_at,
        notice_count,
        verification_sources,
        has_ted,
        ted_notice_ids,
        coverage_status,
        verification_level,
        verification_state,
        dedupe_quality,
        normalized_json,
        updated_at
      )
      values (
        v_canonical_id,
        v_tenant_id,
        v_source,
        v_external_key,
        nullif(trim(coalesce(p_title, '')), ''),
        v_title_norm,
        nullif(trim(coalesce(p_buyer_name_raw, '')), ''),
        v_buyer_norm,
        nullif(v_country, ''),
        v_clean_cpv_codes,
        p_publication_date,
        p_deadline_date,
        nullif(trim(coalesce(p_status, '')), ''),
        case
          when p_deadline_date is null then true
          when p_deadline_date >= current_date and lower(coalesce(p_status, '')) not in ('closed', 'cancelled', 'canceled', 'awarded', 'completed', 'unsuccessful') then true
          else false
        end,
        now(),
        now(),
        1,
        case when v_source = 'TED' then array['TED']::text[] else array[v_source]::text[] end,
        v_source = 'TED',
        case when v_source = 'TED' then array[v_source_notice_id]::text[] else '{}'::text[] end,
        case when v_source = 'TED' then 'ted_only' else 'national_only' end,
        'unverified',
        'unverified',
        coalesce(v_match_score, 0),
        coalesce(p_parsed_payload, '{}'::jsonb),
        now()
      )
      on conflict on constraint canonical_tenders_pkey do nothing;

      v_created_canonical := true;
    end if;

    if v_link_tier = 'hard' then
      v_match_score := 1.0;
    end if;
  end if;

  insert into public.canonical_notice_links (
    tenant_id,
    notice_id,
    canonical_id,
    link_tier,
    match_score,
    match_features,
    created_at
  )
  values (
    v_tenant_id,
    v_notice_id,
    v_canonical_id,
    v_link_tier,
    v_match_score,
    v_match_features,
    now()
  )
  on conflict (tenant_id, notice_id)
  do update set
    canonical_id = excluded.canonical_id,
    link_tier = excluded.link_tier,
    match_score = excluded.match_score,
    match_features = excluded.match_features,
    updated_at = now();

  perform public.refresh_canonical_tender_from_links(v_tenant_id, v_canonical_id, v_match_score);

  return query
  select v_notice_id, v_canonical_id, v_link_tier, v_match_score, v_created_canonical;
end;
$$;

-- Backward-compatible wrapper for older signature.
create or replace function public.upsert_notice_and_link(
  p_tenant_id text,
  p_source text,
  p_source_notice_id text,
  p_source_url text,
  p_country text,
  p_title text,
  p_buyer_name_raw text,
  p_cpv_codes text[],
  p_publication_date date,
  p_deadline_date date,
  p_status text,
  p_raw_payload jsonb,
  p_parsed_payload jsonb,
  p_notice_version_hash text
)
returns table (
  notice_id uuid,
  canonical_id text,
  link_tier text,
  match_score numeric,
  created_canonical boolean
)
language plpgsql
security definer
set search_path = public
as $$
begin
  return query
  select * from public.upsert_notice_and_link(
    p_tenant_id,
    p_source,
    p_source_notice_id,
    p_source_url,
    p_country,
    p_title,
    p_buyer_name_raw,
    p_cpv_codes,
    p_publication_date,
    p_deadline_date,
    p_status,
    p_raw_payload,
    p_parsed_payload,
    p_notice_version_hash,
    null,
    null,
    null,
    null
  );
end;
$$;

create or replace function public.process_tenderscurrent_record(
  p_tenant_id text,
  p_tender_id text,
  p_source text,
  p_published_at timestamptz,
  p_data jsonb,
  p_raw_object_key text default null,
  p_payload_hash_sha256 text default null,
  p_payload_bytes integer default null,
  p_payload_stored_at timestamptz default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_source text := upper(trim(coalesce(p_source, p_data->>'source', 'UNKNOWN')));
  v_tenant_id text := lower(trim(coalesce(p_tenant_id, '')));
  v_source_notice_id text;
  v_source_url text;
  v_country text;
  v_title text;
  v_buyer_name_raw text;
  v_cpv_codes text[] := '{}'::text[];
  v_publication_date date;
  v_deadline_date date;
  v_status text;
  v_parsed_payload jsonb;
begin
  if v_tenant_id = '' then
    return;
  end if;

  v_source_notice_id := trim(coalesce(
    p_data->>'source_notice_id',
    p_data->>'notice_id',
    p_data->>'resourceId',
    p_data->>'resource_id',
    p_data->>'tender_id',
    case when position(':' in coalesce(p_tender_id, '')) > 0 then split_part(p_tender_id, ':', 2) else p_tender_id end,
    p_tender_id
  ));

  if coalesce(v_source_notice_id, '') = '' then
    return;
  end if;

  v_source_url := nullif(trim(coalesce(p_data->>'source_url', p_data->>'url', '')), '');

  v_country := upper(trim(coalesce(
    p_data->>'country',
    p_data->>'country_code',
    p_data->>'country_iso',
    case
      when v_source like '%_IE' then 'IE'
      when v_source like '%_FR' then 'FR'
      when v_source like '%_ES' then 'ES'
      else null
    end,
    ''
  )));

  v_title := nullif(trim(coalesce(p_data->>'title', p_data->>'tender_name', p_data->>'notice_title', '')), '');
  v_buyer_name_raw := nullif(trim(coalesce(p_data->>'buyer_name', p_data->>'contracting_authority', p_data->>'buyer', '')), '');

  if jsonb_typeof(p_data->'cpv_codes') = 'array' then
    select coalesce(array_agg(distinct left(regexp_replace(lower(trim(v)), '[^0-9]', '', 'g'), 8)), '{}'::text[])
    into v_cpv_codes
    from jsonb_array_elements_text(p_data->'cpv_codes') as v
    where regexp_replace(lower(trim(v)), '[^0-9]', '', 'g') <> '';
  elsif jsonb_typeof(p_data->'cpv_codes') = 'string' then
    select coalesce(array_agg(distinct left(regexp_replace(lower(trim(v)), '[^0-9]', '', 'g'), 8)), '{}'::text[])
    into v_cpv_codes
    from unnest(regexp_split_to_array(coalesce(p_data->>'cpv_codes', ''), '[,;| ]+')) as v
    where regexp_replace(lower(trim(v)), '[^0-9]', '', 'g') <> '';
  else
    v_cpv_codes := '{}'::text[];
  end if;

  if pg_input_is_valid(left(coalesce(p_data->>'publication_date', ''), 10), 'date') then
    v_publication_date := left(p_data->>'publication_date', 10)::date;
  elsif p_published_at is not null then
    v_publication_date := p_published_at::date;
  else
    v_publication_date := null;
  end if;

  if pg_input_is_valid(left(coalesce(
      p_data->>'deadline_date',
      p_data->>'event_deadline_date',
      p_data->>'submission_deadline',
      ''
    ), 10), 'date') then
    v_deadline_date := left(coalesce(
      p_data->>'deadline_date',
      p_data->>'event_deadline_date',
      p_data->>'submission_deadline'
    ), 10)::date;
  else
    v_deadline_date := null;
  end if;

  v_status := nullif(trim(coalesce(p_data->>'status', p_data->>'status_code', p_data->>'notice_status', '')), '');

  v_parsed_payload := jsonb_build_object(
    'tenant_id', v_tenant_id,
    'tender_id', p_tender_id,
    'source', v_source,
    'source_notice_id', v_source_notice_id,
    'country', nullif(v_country, ''),
    'title', v_title,
    'buyer_name_raw', v_buyer_name_raw,
    'cpv_codes', coalesce(v_cpv_codes, '{}'::text[]),
    'publication_date', v_publication_date,
    'deadline_date', v_deadline_date,
    'status', v_status,
    'source_url', v_source_url
  );

  perform public.upsert_notice_and_link(
    p_tenant_id => v_tenant_id,
    p_source => v_source,
    p_source_notice_id => v_source_notice_id,
    p_source_url => v_source_url,
    p_country => nullif(v_country, ''),
    p_title => v_title,
    p_buyer_name_raw => v_buyer_name_raw,
    p_cpv_codes => v_cpv_codes,
    p_publication_date => v_publication_date,
    p_deadline_date => v_deadline_date,
    p_status => v_status,
    p_raw_payload => coalesce(p_data, '{}'::jsonb),
    p_parsed_payload => v_parsed_payload,
    p_notice_version_hash => public.compute_notice_version_hash(coalesce(p_data, '{}'::jsonb)),
    p_raw_object_key => p_raw_object_key,
    p_payload_hash_sha256 => p_payload_hash_sha256,
    p_payload_bytes => p_payload_bytes,
    p_payload_stored_at => p_payload_stored_at
  );
end;
$$;

-- Backward-compatible wrapper for older signature.
create or replace function public.process_tenderscurrent_record(
  p_tenant_id text,
  p_tender_id text,
  p_source text,
  p_published_at timestamptz,
  p_data jsonb
)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.process_tenderscurrent_record(
    p_tenant_id,
    p_tender_id,
    p_source,
    p_published_at,
    p_data,
    null,
    null,
    null,
    null
  );
end;
$$;

create or replace function public.sync_tenderscurrent_to_notices()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  perform public.process_tenderscurrent_record(
    new.tenant_id,
    new.tender_id,
    new.source,
    new.published_at,
    new.data,
    new.raw_object_key,
    new.payload_hash_sha256,
    new.payload_bytes,
    new.payload_stored_at
  );

  return new;
end;
$$;

do $$
begin
  if to_regclass('public."TendersCurrent"') is not null then
    execute 'drop trigger if exists trg_tenderscurrent_sync_notice on public."TendersCurrent"';
    execute '
      create trigger trg_tenderscurrent_sync_notice
      after insert or update of tenant_id, tender_id, source, published_at, data, raw_object_key, payload_hash_sha256, payload_bytes, payload_stored_at
      on public."TendersCurrent"
      for each row
      execute function public.sync_tenderscurrent_to_notices()
    ';
  end if;
end $$;

create or replace function public.backfill_notices_from_tenderscurrent(
  p_tenant_id text default null,
  p_limit integer default null,
  p_disable_trigger boolean default true
)
returns table (
  processed_count integer,
  error_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_processed integer := 0;
  v_errors integer := 0;
begin
  if p_disable_trigger then
    perform public.set_tenderscurrent_notice_trigger_enabled(false);
  end if;

  for r in
    select tc.tenant_id, tc.tender_id, tc.source, tc.published_at, tc.data, tc.raw_object_key, tc.payload_hash_sha256, tc.payload_bytes, tc.payload_stored_at
    from public."TendersCurrent" tc
    where p_tenant_id is null or tc.tenant_id = p_tenant_id
    order by tc.updated_at desc nulls last, tc.published_at desc nulls last
    limit coalesce(p_limit, 2147483647)
  loop
    begin
      perform public.process_tenderscurrent_record(
        r.tenant_id,
        r.tender_id,
        r.source,
        r.published_at,
        r.data,
        r.raw_object_key,
        r.payload_hash_sha256,
        r.payload_bytes,
        r.payload_stored_at
      );
      v_processed := v_processed + 1;
    exception when others then
      v_errors := v_errors + 1;
    end;
  end loop;

  if p_disable_trigger then
    perform public.set_tenderscurrent_notice_trigger_enabled(true);
  end if;

  return query select v_processed, v_errors;
exception when others then
  if p_disable_trigger then
    perform public.set_tenderscurrent_notice_trigger_enabled(true);
  end if;
  raise;
end;
$$;
