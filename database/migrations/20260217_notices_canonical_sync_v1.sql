-- Canonical notice linking layer: deterministic sync, match, trigger, and backfill

create extension if not exists pgcrypto;
create extension if not exists pg_trgm;

create or replace function public.normalize_match_text(p_text text)
returns text
language sql
immutable
as $$
  select nullif(trim(regexp_replace(lower(coalesce(p_text, '')), '[^a-z0-9]+', ' ', 'g')), '')
$$;

create or replace function public.compute_notice_version_hash(p_payload jsonb)
returns text
language sql
immutable
as $$
  select encode(extensions.digest(convert_to(coalesce(p_payload, '{}'::jsonb)::text, 'UTF8'), 'sha256'), 'hex')
$$;

create or replace function public.refresh_canonical_tender_from_links(
  p_tenant_id text,
  p_canonical_id text,
  p_latest_score numeric default null
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_notice_count integer := 0;
  v_min_publication date;
  v_latest_deadline date;
  v_first_seen timestamptz;
  v_last_seen timestamptz;
  v_sources text[] := '{}'::text[];
  v_ted_notice_ids text[] := '{}'::text[];
  v_has_ted boolean := false;
  v_has_national boolean := false;
  v_has_hard_ted_link boolean := false;
  v_max_ted_score numeric := 0;

  v_best_source text;
  v_best_source_notice_id text;
  v_best_country text;
  v_best_title text;
  v_best_buyer_raw text;
  v_best_buyer_norm text;
  v_best_cpv text[] := '{}'::text[];
  v_best_deadline date;
  v_best_status text;

  v_coverage_status text := 'national_only';
  v_verification_level text := 'unverified';
  v_is_open boolean := true;
begin
  select
    count(*)::int,
    min(coalesce(n.publication_date, n.ingested_at::date)),
    max(n.deadline_date),
    min(n.ingested_at),
    max(n.ingested_at),
    coalesce(array_agg(distinct n.source) filter (where n.source is not null), '{}'::text[]),
    coalesce(array_agg(distinct n.source_notice_id) filter (where n.source = 'TED'), '{}'::text[]),
    bool_or(n.source = 'TED'),
    bool_or(n.source <> 'TED'),
    bool_or(l.link_tier = 'hard' and n.source = 'TED'),
    max(case when n.source = 'TED' then coalesce(l.match_score, 0) else null end)
  into
    v_notice_count,
    v_min_publication,
    v_latest_deadline,
    v_first_seen,
    v_last_seen,
    v_sources,
    v_ted_notice_ids,
    v_has_ted,
    v_has_national,
    v_has_hard_ted_link,
    v_max_ted_score
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  where l.tenant_id = p_tenant_id
    and l.canonical_id = p_canonical_id;

  if coalesce(v_notice_count, 0) = 0 then
    update public.canonical_tenders ct
    set
      notice_count = greatest(coalesce(ct.notice_count, 1), 1),
      has_ted = false,
      ted_notice_ids = '{}'::text[],
      verification_sources = '{}'::text[],
      coverage_status = 'national_only',
      verification_level = 'unverified',
      verification_state = 'unverified',
      updated_at = now()
    where ct.tenant_id = p_tenant_id
      and ct.canonical_id = p_canonical_id;
    return;
  end if;

  select
    n.source,
    n.source_notice_id,
    n.country,
    n.title,
    n.buyer_name_raw,
    n.buyer_name_norm,
    n.cpv_codes,
    n.deadline_date,
    n.status
  into
    v_best_source,
    v_best_source_notice_id,
    v_best_country,
    v_best_title,
    v_best_buyer_raw,
    v_best_buyer_norm,
    v_best_cpv,
    v_best_deadline,
    v_best_status
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  where l.tenant_id = p_tenant_id
    and l.canonical_id = p_canonical_id
  order by
    (n.source <> 'TED') desc,
    n.publication_date desc nulls last,
    n.ingested_at desc,
    coalesce(l.match_score, 0) desc
  limit 1;

  if v_has_ted and v_has_national then
    v_coverage_status := 'linked';
    v_verification_level := case
      when v_has_hard_ted_link or coalesce(v_max_ted_score, 0) >= 0.90 then 'verified'
      else 'partially_verified'
    end;
  elsif v_has_ted then
    v_coverage_status := 'ted_only';
    v_verification_level := 'unverified';
  else
    v_coverage_status := 'national_only';
    v_verification_level := 'unverified';
  end if;

  v_is_open := (
    coalesce(v_best_deadline, v_latest_deadline) is null
    or (
      coalesce(v_best_deadline, v_latest_deadline) >= current_date
      and lower(coalesce(v_best_status, '')) not in ('closed', 'cancelled', 'canceled', 'awarded', 'completed', 'unsuccessful')
    )
  );

  update public.canonical_tenders ct
  set
    country = coalesce(nullif(ct.country, ''), nullif(v_best_country, '')),
    buyer_name_raw = case
      when coalesce(length(ct.buyer_name_raw), 0) >= coalesce(length(v_best_buyer_raw), 0) then ct.buyer_name_raw
      else v_best_buyer_raw
    end,
    buyer_name_norm = case
      when coalesce(length(ct.buyer_name_norm), 0) >= coalesce(length(v_best_buyer_norm), 0) then ct.buyer_name_norm
      else v_best_buyer_norm
    end,
    title = case
      when coalesce(length(ct.title), 0) >= coalesce(length(v_best_title), 0) then ct.title
      else v_best_title
    end,
    title_norm = case
      when coalesce(length(ct.title_norm), 0) >= coalesce(length(public.normalize_match_text(v_best_title)), 0) then ct.title_norm
      else public.normalize_match_text(v_best_title)
    end,
    cpv_codes = (
      select coalesce(array_agg(distinct c), '{}'::text[])
      from unnest(coalesce(ct.cpv_codes, '{}'::text[]) || coalesce(v_best_cpv, '{}'::text[])) as c
      where c is not null and c <> ''
    ),
    publication_date = case
      when ct.publication_date is null then v_min_publication
      when v_min_publication is null then ct.publication_date
      else least(ct.publication_date, v_min_publication)
    end,
    deadline_date = coalesce(v_best_deadline, ct.deadline_date),
    status = coalesce(nullif(v_best_status, ''), ct.status),
    is_open = v_is_open,
    first_seen_at = case
      when ct.first_seen_at is null then coalesce(v_first_seen, now())
      when v_first_seen is null then ct.first_seen_at
      else least(ct.first_seen_at, v_first_seen)
    end,
    last_seen_at = greatest(coalesce(ct.last_seen_at, '-infinity'::timestamptz), coalesce(v_last_seen, '-infinity'::timestamptz), now()),
    notice_count = greatest(coalesce(v_notice_count, 0), 1),
    verification_sources = coalesce(v_sources, '{}'::text[]),
    has_ted = coalesce(v_has_ted, false),
    ted_notice_ids = coalesce(v_ted_notice_ids, '{}'::text[]),
    coverage_status = v_coverage_status,
    verification_level = v_verification_level,
    verification_state = v_verification_level,
    dedupe_quality = greatest(
      coalesce(ct.dedupe_quality, 0),
      coalesce(v_max_ted_score, 0),
      coalesce(p_latest_score, 0)
    ),
    source = case
      when coalesce(ct.source, '') = '' then coalesce(v_best_source, 'TED')
      when ct.source = 'TED' and coalesce(v_best_source, 'TED') <> 'TED' then v_best_source
      else ct.source
    end,
    external_id = coalesce(
      nullif(ct.external_id, ''),
      p_tenant_id || ':' || coalesce(v_best_source, ct.source, 'UNKNOWN') || ':' || coalesce(v_best_source_notice_id, ct.canonical_id)
    ),
    normalized_json = coalesce(ct.normalized_json, '{}'::jsonb) || jsonb_build_object(
      'coverage_status', v_coverage_status,
      'verification_level', v_verification_level,
      'verification_sources', coalesce(v_sources, '{}'::text[]),
      'notice_count', coalesce(v_notice_count, 0),
      'has_ted', coalesce(v_has_ted, false),
      'ted_notice_ids', coalesce(v_ted_notice_ids, '{}'::text[])
    ),
    updated_at = now()
  where ct.tenant_id = p_tenant_id
    and ct.canonical_id = p_canonical_id;
end;
$$;

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
  p_parsed_payload jsonb default '{}'::jsonb,
  p_notice_version_hash text default null
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
    coalesce(p_raw_payload, '{}'::jsonb),
    coalesce(p_parsed_payload, '{}'::jsonb),
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
    ingested_at = now()
  returning notices.notice_id into v_notice_id;

  -- Hard match tier: reuse any canonical already linked to this source notice lineage.
  select l.canonical_id
  into v_canonical_id
  from public.notices n
  join public.canonical_notice_links l
    on l.tenant_id = n.tenant_id
   and l.notice_id = n.notice_id
  where n.tenant_id = v_tenant_id
    and n.source = v_source
    and n.source_notice_id = v_source_notice_id
  order by n.ingested_at desc
  limit 1;

  if v_canonical_id is null then
    select ct.canonical_id
    into v_canonical_id
    from public.canonical_tenders ct
    where ct.tenant_id = v_tenant_id
      and ct.external_id in (
        v_external_key,
        v_source || ':' || v_source_notice_id,
        v_source_notice_id
      )
    order by ct.updated_at desc
    limit 1;
  end if;

  if v_canonical_id is null then
    with seeded_candidates as (
      select ct.*
      from public.canonical_tenders ct
      where ct.tenant_id = v_tenant_id
        and (v_country is null or ct.country is null or ct.country = v_country)
        and (
          (
            v_buyer_norm is not null and v_buyer_norm <> ''
            and ct.buyer_name_norm = v_buyer_norm
          )
          or (
            (v_buyer_norm is null or v_buyer_norm = '')
            and v_title_norm is not null and v_title_norm <> ''
            and ct.title_norm is not null and ct.title_norm % v_title_norm
          )
          or (
            (v_buyer_norm is null or v_buyer_norm = '')
            and (v_title_norm is null or v_title_norm = '')
            and cardinality(v_clean_cpv_codes) > 0
            and cardinality(coalesce(ct.cpv_codes, '{}'::text[])) > 0
            and ct.cpv_codes && v_clean_cpv_codes
          )
        )
        and (
          p_publication_date is null
          or ct.publication_date is null
          or abs(ct.publication_date - p_publication_date) <= 180
        )
      order by coalesce(ct.last_seen_at, ct.updated_at, ct.created_at) desc nulls last
      limit 600
    ),
    candidates as (
      select
        ct.canonical_id as candidate_canonical_id,
        (
          case
            when v_buyer_norm is not null and v_buyer_norm <> '' and ct.buyer_name_norm = v_buyer_norm
              then 0.45
            else 0
          end
          + case
              when v_title_norm is not null and v_title_norm <> '' and ct.title_norm is not null and ct.title_norm <> ''
                then least(greatest(similarity(ct.title_norm, v_title_norm), 0), 1) * 0.35
              else 0
            end
          + case
              when v_country is not null and v_country <> '' and ct.country = v_country
                then 0.05
              else 0
            end
          + case
              when p_publication_date is not null and ct.publication_date is not null
                then case
                  when abs(ct.publication_date - p_publication_date) <= 14 then 0.10
                  when abs(ct.publication_date - p_publication_date) <= 45 then 0.05
                  else 0
                end
              else 0
            end
          + case
              when p_deadline_date is not null and ct.deadline_date is not null and abs(ct.deadline_date - p_deadline_date) <= 14
                then 0.05
              else 0
            end
          + case
              when cardinality(v_clean_cpv_codes) > 0 and cardinality(coalesce(ct.cpv_codes, '{}'::text[])) > 0 and ct.cpv_codes && v_clean_cpv_codes
                then 0.10
              else 0
            end
        )::numeric as score
      from seeded_candidates ct
    ),
    ranked as (
      select
        c.candidate_canonical_id,
        score,
        row_number() over (order by score desc, c.candidate_canonical_id) as rn
      from candidates c
      where score > 0
    )
    select
      r1.candidate_canonical_id,
      r1.score,
      coalesce(r2.score, 0)
    into
      v_best_candidate,
      v_best_score,
      v_second_score
    from ranked r1
    left join ranked r2 on r2.rn = 2
    where r1.rn = 1;

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

  v_match_features := jsonb_build_object(
    'best_candidate', v_best_candidate,
    'best_score', coalesce(v_best_score, 0),
    'second_score', coalesce(v_second_score, 0),
    'score_gap', coalesce(v_candidate_gap, 0),
    'country', nullif(v_country, ''),
    'source', v_source
  );

  if not exists (
    select 1
    from public.notices n
    where n.notice_id = v_notice_id
      and n.tenant_id = v_tenant_id
  ) then
    raise exception 'notice tenant mismatch for notice_id=% tenant_id=%', v_notice_id, v_tenant_id;
  end if;

  if not exists (
    select 1
    from public.canonical_tenders ct
    where ct.canonical_id = v_canonical_id
      and ct.tenant_id = v_tenant_id
  ) then
    raise exception 'canonical tenant mismatch for canonical_id=% tenant_id=%', v_canonical_id, v_tenant_id;
  end if;

  insert into public.canonical_notice_links (
    tenant_id,
    canonical_id,
    notice_id,
    link_tier,
    match_score,
    match_features,
    is_primary,
    linked_at
  )
  values (
    v_tenant_id,
    v_canonical_id,
    v_notice_id,
    v_link_tier,
    coalesce(v_match_score, 0),
    v_match_features,
    v_source <> 'TED',
    now()
  )
  on conflict on constraint canonical_notice_links_tenant_id_notice_id_key
  do update set
    canonical_id = excluded.canonical_id,
    link_tier = excluded.link_tier,
    match_score = excluded.match_score,
    match_features = excluded.match_features,
    is_primary = excluded.is_primary,
    linked_at = now();

  perform public.refresh_canonical_tender_from_links(v_tenant_id, v_canonical_id, v_match_score);

  return query
  select v_notice_id, v_canonical_id, v_link_tier, v_match_score, v_created_canonical;
end;
$$;

create or replace function public.set_tenderscurrent_notice_trigger_enabled(p_enabled boolean)
returns void
language plpgsql
security definer
set search_path = public
as $$
begin
  if to_regclass('public."TendersCurrent"') is null then
    return;
  end if;

  if exists (
    select 1
    from pg_trigger
    where tgrelid = 'public."TendersCurrent"'::regclass
      and tgname = 'trg_tenderscurrent_sync_notice'
  ) then
    if p_enabled then
      execute 'alter table public."TendersCurrent" enable trigger trg_tenderscurrent_sync_notice';
    else
      execute 'alter table public."TendersCurrent" disable trigger trg_tenderscurrent_sync_notice';
    end if;
  end if;
end;
$$;

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
    p_notice_version_hash => public.compute_notice_version_hash(coalesce(p_data, '{}'::jsonb))
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
    new.data
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
      after insert or update of tenant_id, tender_id, source, published_at, data
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
    select tc.tenant_id, tc.tender_id, tc.source, tc.published_at, tc.data
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
        r.data
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
