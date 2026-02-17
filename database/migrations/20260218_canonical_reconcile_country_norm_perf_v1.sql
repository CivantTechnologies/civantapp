-- Country normalization + reconcile performance guardrails for TED<->national linking.

create index if not exists idx_notices_tenant_source_country_notice_id
  on public.notices (tenant_id, source, country, notice_id);

create index if not exists idx_canonical_tenders_tenant_source_country
  on public.canonical_tenders (tenant_id, source, country);

create index if not exists idx_canonical_tenders_tenant_source_buyer_norm
  on public.canonical_tenders (tenant_id, source, buyer_name_norm);

create index if not exists idx_canonical_tenders_tenant_source_publication
  on public.canonical_tenders (tenant_id, source, publication_date);

create index if not exists idx_canonical_tenders_cpv_codes_gin
  on public.canonical_tenders using gin (cpv_codes);

update public.notices n
set country = public.normalize_country_code(
  coalesce(nullif(n.country, ''), public.infer_country_from_source(n.source))
)
where coalesce(n.country, '') is distinct from coalesce(
  public.normalize_country_code(
    coalesce(nullif(n.country, ''), public.infer_country_from_source(n.source))
  ),
  ''
);

update public.canonical_tenders ct
set country = public.normalize_country_code(
  coalesce(nullif(ct.country, ''), public.infer_country_from_source(ct.source))
)
where coalesce(ct.country, '') is distinct from coalesce(
  public.normalize_country_code(
    coalesce(nullif(ct.country, ''), public.infer_country_from_source(ct.source))
  ),
  ''
);

create or replace function public.reconcile_ted_national_links(
  p_tenant_id text,
  p_country text default null,
  p_national_sources text[] default null,
  p_limit integer default 500,
  p_apply boolean default true,
  p_auto_link_min numeric default 0.90,
  p_queue_min numeric default 0.75,
  p_after_notice_id uuid default null
)
returns table (
  scanned_count integer,
  relinked_count integer,
  queued_count integer,
  unchanged_count integer,
  last_notice_id uuid
)
language plpgsql
security definer
set search_path = public
as $$
declare
  r record;
  v_tenant_id text := lower(trim(coalesce(p_tenant_id, '')));
  v_country text := public.normalize_country_code(nullif(trim(coalesce(p_country, '')), ''));
  v_notice_country text;

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

  v_national_sources text[] := '{}'::text[];
  v_after_notice_id uuid := p_after_notice_id;
  v_limit integer := greatest(coalesce(p_limit, 500), 0);
  v_last_notice_id uuid := null;
begin
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  if p_national_sources is null or cardinality(p_national_sources) = 0 then
    if v_country = 'IE' then
      v_national_sources := array['ETENDERS_IE']::text[];
    elsif v_country = 'FR' then
      v_national_sources := array['BOAMP_FR']::text[];
    elsif v_country = 'ES' then
      v_national_sources := array['PLACSP_ES']::text[];
    else
      select coalesce(array_agg(distinct upper(trim(n.source))), '{}'::text[])
      into v_national_sources
      from public.notices n
      where n.tenant_id = v_tenant_id
        and (v_country is null or public.normalize_country_code(n.country) = v_country)
        and upper(trim(coalesce(n.source, ''))) <> 'TED';
    end if;
  else
    select coalesce(array_agg(distinct upper(trim(s))), '{}'::text[])
    into v_national_sources
    from unnest(p_national_sources) as s
    where trim(coalesce(s, '')) <> ''
      and upper(trim(s)) <> 'TED';
  end if;

  if v_limit = 0 then
    return query select 0, 0, 0, 0, null::uuid;
    return;
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
      and (v_country is null or public.normalize_country_code(n.country) = v_country)
      and (v_after_notice_id is null or l.notice_id > v_after_notice_id)
    order by l.notice_id
    limit v_limit
  loop
    v_scanned := v_scanned + 1;
    v_last_notice_id := r.notice_id;

    v_title_norm := public.normalize_match_text(r.title);
    v_buyer_norm := coalesce(nullif(r.buyer_name_norm, ''), public.normalize_match_text(r.buyer_name_raw));
    v_notice_country := coalesce(public.normalize_country_code(r.country), v_country);

    if coalesce(v_buyer_norm, '') = ''
      and (coalesce(v_title_norm, '') = '' or length(v_title_norm) < 12)
      and cardinality(coalesce(r.cpv_codes, '{}'::text[])) = 0 then
      v_unchanged := v_unchanged + 1;
      continue;
    end if;

    with seeded_candidates as (
      select ct.*
      from public.canonical_tenders ct
      where ct.tenant_id = v_tenant_id
        and ct.canonical_id <> r.current_canonical_id
        and upper(coalesce(ct.source, '')) = any(v_national_sources)
        and (
          v_notice_country is null
          or coalesce(
            public.normalize_country_code(ct.country),
            public.normalize_country_code(public.infer_country_from_source(ct.source))
          ) = v_notice_country
        )
        and (
          (
            v_buyer_norm is not null and v_buyer_norm <> ''
            and ct.buyer_name_norm = v_buyer_norm
          )
          or (
            v_title_norm is not null and v_title_norm <> '' and length(v_title_norm) >= 12
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
          or abs(ct.publication_date - r.publication_date) <= 180
        )
      order by coalesce(ct.last_seen_at, ct.updated_at, ct.created_at) desc nulls last
      limit 250
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
              when v_title_norm is not null and v_title_norm <> '' and length(v_title_norm) >= 12 and ct.title_norm is not null and ct.title_norm <> ''
                then least(greatest(similarity(ct.title_norm, v_title_norm), 0), 1) * 0.35
              else 0
            end
          + case
              when v_notice_country is not null and v_notice_country <> ''
                and coalesce(public.normalize_country_code(ct.country), public.normalize_country_code(public.infer_country_from_source(ct.source))) = v_notice_country
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
              'reconciler', 'ted_national',
              'reconciled_at', now(),
              'country', v_notice_country,
              'national_sources', v_national_sources,
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
            'reconciler', 'ted_national',
            'country', v_notice_country,
            'national_sources', v_national_sources,
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
  select v_scanned, v_relinked, v_queued, v_unchanged, v_last_notice_id;
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

  v_country := public.normalize_country_code(coalesce(
    p_data->>'country',
    p_data->>'country_code',
    p_data->>'country_iso',
    case
      when v_source like '%_IE' then 'IE'
      when v_source like '%_FR' then 'FR'
      when v_source like '%_ES' then 'ES'
      else null
    end
  ));

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
