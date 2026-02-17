-- Canonical source-of-truth enhancements:
-- 1) Derive primary source metadata from linked notices (not snapshot URL).
-- 2) Add deterministic TED -> eTenders reconciliation to improve linked/verified coverage
--    without introducing duplicate canonical tenders.

create extension if not exists pg_trgm;

alter table public.canonical_tenders
  add column if not exists primary_source_url text,
  add column if not exists primary_source_kind text not null default 'none',
  add column if not exists primary_source_notice_id uuid,
  add column if not exists primary_source_notice_source text;

create index if not exists idx_canonical_tenders_tenant_primary_source_notice
  on public.canonical_tenders (tenant_id, primary_source_notice_id);

create index if not exists idx_canonical_tenders_tenant_primary_source_kind
  on public.canonical_tenders (tenant_id, primary_source_kind);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_tenders_primary_source_kind'
      and conrelid = 'public.canonical_tenders'::regclass
  ) then
    alter table public.canonical_tenders
      add constraint ck_canonical_tenders_primary_source_kind
      check (primary_source_kind in ('none', 'linked_notice', 'snapshot_fallback'));
  end if;
end
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

  v_best_notice_id uuid;
  v_best_source text;
  v_best_source_notice_id text;
  v_best_source_url text;
  v_best_country text;
  v_best_title text;
  v_best_buyer_raw text;
  v_best_buyer_norm text;
  v_best_cpv text[] := '{}'::text[];
  v_best_deadline date;
  v_best_status text;

  v_existing_source text;
  v_existing_source_url text;

  v_coverage_status text := 'national_only';
  v_verification_level text := 'unverified';
  v_is_open boolean := true;
begin
  select ct.source, ct.source_url
  into v_existing_source, v_existing_source_url
  from public.canonical_tenders ct
  where ct.tenant_id = p_tenant_id
    and ct.canonical_id = p_canonical_id;

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
      primary_source_url = coalesce(ct.primary_source_url, ct.source_url),
      primary_source_kind = case
        when coalesce(ct.primary_source_url, ct.source_url) is not null then 'snapshot_fallback'
        else 'none'
      end,
      primary_source_notice_id = null,
      primary_source_notice_source = null,
      updated_at = now()
    where ct.tenant_id = p_tenant_id
      and ct.canonical_id = p_canonical_id;
    return;
  end if;

  select
    n.notice_id,
    n.source,
    n.source_notice_id,
    n.source_url,
    n.country,
    n.title,
    n.buyer_name_raw,
    n.buyer_name_norm,
    n.cpv_codes,
    n.deadline_date,
    n.status
  into
    v_best_notice_id,
    v_best_source,
    v_best_source_notice_id,
    v_best_source_url,
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
    (n.source = coalesce(v_existing_source, n.source)) desc,
    coalesce(l.is_primary, false) desc,
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
    source_url = coalesce(v_best_source_url, ct.source_url),
    primary_source_url = coalesce(v_best_source_url, ct.source_url),
    primary_source_kind = case
      when v_best_source_url is not null then 'linked_notice'
      when coalesce(ct.source_url, v_existing_source_url) is not null then 'snapshot_fallback'
      else 'none'
    end,
    primary_source_notice_id = case when v_best_source_url is not null then v_best_notice_id else null end,
    primary_source_notice_source = case when v_best_source_url is not null then v_best_source else null end,
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
      'ted_notice_ids', coalesce(v_ted_notice_ids, '{}'::text[]),
      'primary_source_url', coalesce(v_best_source_url, ct.source_url),
      'primary_source_kind', case
        when v_best_source_url is not null then 'linked_notice'
        when coalesce(ct.source_url, v_existing_source_url) is not null then 'snapshot_fallback'
        else 'none'
      end,
      'primary_source_notice_source', case when v_best_source_url is not null then v_best_source else null end
    ),
    updated_at = now()
  where ct.tenant_id = p_tenant_id
    and ct.canonical_id = p_canonical_id;
end;
$$;

create or replace function public.backfill_canonical_primary_sources(
  p_tenant_id text default null,
  p_limit integer default null
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
  for r in
    select ct.tenant_id, ct.canonical_id
    from public.canonical_tenders ct
    where p_tenant_id is null or ct.tenant_id = p_tenant_id
    order by ct.updated_at desc nulls last
    limit coalesce(p_limit, 2147483647)
  loop
    begin
      perform public.refresh_canonical_tender_from_links(r.tenant_id, r.canonical_id, null);
      v_processed := v_processed + 1;
    exception when others then
      v_errors := v_errors + 1;
    end;
  end loop;

  return query select v_processed, v_errors;
end;
$$;

create or replace function public.reconcile_ted_etenders_links(
  p_tenant_id text,
  p_country text default 'IE',
  p_limit integer default 500,
  p_apply boolean default true,
  p_auto_link_min numeric default 0.90,
  p_queue_min numeric default 0.75
)
returns table (
  scanned_count integer,
  relinked_count integer,
  queued_count integer,
  unchanged_count integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_tenant_id text := lower(trim(coalesce(p_tenant_id, '')));
  v_country text := upper(nullif(trim(coalesce(p_country, '')), ''));

  v_title_norm text;
  v_buyer_norm text;

  v_best_candidate text;
  v_best_score numeric := 0;
  v_second_score numeric := 0;
  v_score_gap numeric := 0;

  v_scanned integer := 0;
  v_relinked integer := 0;
  v_queued integer := 0;
  v_unchanged integer := 0;
begin
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  for r in
    select
      l.notice_id,
      l.canonical_id as current_canonical_id,
      n.source_notice_id,
      n.title,
      n.buyer_name_raw,
      n.buyer_name_norm,
      n.country,
      n.publication_date,
      n.deadline_date,
      coalesce(n.cpv_codes, '{}'::text[]) as cpv_codes
    from public.canonical_notice_links l
    join public.notices n
      on n.notice_id = l.notice_id
     and n.tenant_id = l.tenant_id
    where l.tenant_id = v_tenant_id
      and n.source = 'TED'
      and (v_country is null or n.country = v_country)
    order by l.linked_at desc nulls last
    limit greatest(coalesce(p_limit, 0), 0)
  loop
    v_scanned := v_scanned + 1;

    v_title_norm := public.normalize_match_text(r.title);
    v_buyer_norm := coalesce(nullif(r.buyer_name_norm, ''), public.normalize_match_text(r.buyer_name_raw));

    with seeded_candidates as (
      select ct.*
      from public.canonical_tenders ct
      where ct.tenant_id = v_tenant_id
        and ct.canonical_id <> r.current_canonical_id
        and upper(coalesce(ct.source, '')) = 'ETENDERS_IE'
        and (v_country is null or ct.country is null or ct.country = v_country)
        and (
          (
            v_buyer_norm is not null and v_buyer_norm <> ''
            and ct.buyer_name_norm = v_buyer_norm
          )
          or (
            v_title_norm is not null and v_title_norm <> ''
            and ct.title_norm is not null and ct.title_norm <> ''
            and ct.title_norm % v_title_norm
          )
          or (
            cardinality(r.cpv_codes) > 0
            and cardinality(coalesce(ct.cpv_codes, '{}'::text[])) > 0
            and ct.cpv_codes && r.cpv_codes
          )
        )
        and (
          r.publication_date is null
          or ct.publication_date is null
          or abs(ct.publication_date - r.publication_date) <= 365
        )
      order by coalesce(ct.last_seen_at, ct.updated_at, ct.created_at) desc nulls last
      limit 500
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
              when r.publication_date is not null and ct.publication_date is not null
                then case
                  when abs(ct.publication_date - r.publication_date) <= 14 then 0.10
                  when abs(ct.publication_date - r.publication_date) <= 45 then 0.05
                  else 0
                end
              else 0
            end
          + case
              when r.deadline_date is not null and ct.deadline_date is not null and abs(ct.deadline_date - r.deadline_date) <= 14
                then 0.05
              else 0
            end
          + case
              when cardinality(r.cpv_codes) > 0 and cardinality(coalesce(ct.cpv_codes, '{}'::text[])) > 0 and ct.cpv_codes && r.cpv_codes
                then 0.10
              else 0
            end
        )::numeric as score
      from seeded_candidates ct
    ),
    ranked as (
      select
        c.candidate_canonical_id,
        c.score,
        row_number() over (order by c.score desc, c.candidate_canonical_id) as rn
      from candidates c
      where c.score > 0
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

    v_score_gap := coalesce(v_best_score, 0) - coalesce(v_second_score, 0);

    if v_best_candidate is null then
      v_unchanged := v_unchanged + 1;
      continue;
    end if;

    if v_best_score >= p_auto_link_min and v_best_candidate <> r.current_canonical_id then
      if p_apply then
        update public.canonical_notice_links l
        set
          canonical_id = v_best_candidate,
          link_tier = 'strong',
          match_score = v_best_score,
          match_features = coalesce(l.match_features, '{}'::jsonb)
            || jsonb_build_object(
              'reconciler', 'ted_etenders',
              'reconciled_at', now(),
              'previous_canonical_id', r.current_canonical_id,
              'new_canonical_id', v_best_candidate,
              'score', v_best_score,
              'score_gap', v_score_gap,
              'auto_link_min', p_auto_link_min
            ),
          linked_at = now()
        where l.tenant_id = v_tenant_id
          and l.notice_id = r.notice_id;

        perform public.refresh_canonical_tender_from_links(v_tenant_id, v_best_candidate, v_best_score);
        perform public.refresh_canonical_tender_from_links(v_tenant_id, r.current_canonical_id, null);

        if to_regprocedure('public.assert_canonical_tender_invariants(text,text)') is not null then
          perform public.assert_canonical_tender_invariants(v_tenant_id, v_best_candidate);
          perform public.assert_canonical_tender_invariants(v_tenant_id, r.current_canonical_id);
        end if;
      end if;

      v_relinked := v_relinked + 1;
      continue;
    end if;

    if v_best_score >= p_queue_min and v_best_score < p_auto_link_min then
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
        select
          v_tenant_id,
          r.notice_id,
          v_best_candidate,
          v_best_score,
          jsonb_build_object(
            'reconciler', 'ted_etenders',
            'country', v_country,
            'best_score', v_best_score,
            'second_score', v_second_score,
            'score_gap', v_score_gap,
            'queue_min', p_queue_min,
            'auto_link_min', p_auto_link_min,
            'source_notice_id', r.source_notice_id
          ),
          'pending',
          now()
        where not exists (
          select 1
          from public.reconciliation_queue rq
          where rq.tenant_id = v_tenant_id
            and rq.notice_id = r.notice_id
            and coalesce(rq.candidate_canonical_id, '') = coalesce(v_best_candidate, '')
            and rq.status = 'pending'
        );
      end if;

      v_queued := v_queued + 1;
      continue;
    end if;

    v_unchanged := v_unchanged + 1;
  end loop;

  return query
  select v_scanned, v_relinked, v_queued, v_unchanged;
end;
$$;
