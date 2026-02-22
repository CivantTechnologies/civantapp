-- Search latency hotfix: force index-first recency retrieval for notices_search_current.
-- Targets P0 route/search latency by eliminating seq-scan + top-N sort over tenant-wide rows.

create index if not exists idx_notices_search_current_tenant_source_last_seen_desc
  on public.notices_search_current (tenant_id, source, last_seen_at desc nulls last, canonical_id);

create index if not exists idx_notices_search_current_tenant_region_last_seen_desc
  on public.notices_search_current (tenant_id, region, last_seen_at desc nulls last, canonical_id);

create or replace function public.search_tenders_ranked(
  p_tenant_id text,
  p_limit integer default 200,
  p_keyword text default null,
  p_country text default null,
  p_source text default null,
  p_buyer_search text default null,
  p_cpv_search_codes text[] default null,
  p_deadline_within integer default null,
  p_industry text default null,
  p_institution_type text default null,
  p_last_tendered integer default null
)
returns table (
  tenant_id text,
  tender_id text,
  source text,
  published_at timestamptz,
  data jsonb,
  updated_at timestamptz,
  relevance_score numeric
)
language plpgsql
stable
as $$
declare
  v_tenant_id text := lower(trim(coalesce(p_tenant_id, '')));
  v_limit integer := greatest(1, least(coalesce(p_limit, 200), 2000));
  v_keyword text := nullif(lower(trim(coalesce(p_keyword, ''))), '');
  v_country text := nullif(upper(trim(coalesce(p_country, ''))), '');
  v_source text := nullif(upper(trim(coalesce(p_source, ''))), '');
  v_buyer_search text := nullif(lower(trim(coalesce(p_buyer_search, ''))), '');
  v_deadline_within integer := case when coalesce(p_deadline_within, 0) > 0 then p_deadline_within else null end;
  v_industry text := nullif(lower(trim(coalesce(p_industry, ''))), '');
  v_institution_type text := nullif(lower(trim(coalesce(p_institution_type, ''))), '');
  v_last_tendered integer := case when coalesce(p_last_tendered, 0) > 0 then p_last_tendered else null end;
  v_published_cutoff timestamptz := null;
  v_cpv_search_codes text[] := array[]::text[];
  v_candidate_cap integer := 14000;
  v_serving_exists boolean := false;
begin
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  select exists (
    select 1
    from public.notices_search_current nsc
    where nsc.tenant_id = v_tenant_id
    limit 1
  )
  into v_serving_exists;

  if not v_serving_exists then
    raise notice '[search-serving-fallback] tenant=% reason=serving_empty', v_tenant_id;
    return query
      select * from public.search_tenders_ranked_legacy(
        p_tenant_id,
        p_limit,
        p_keyword,
        p_country,
        p_source,
        p_buyer_search,
        p_cpv_search_codes,
        p_deadline_within,
        p_industry,
        p_institution_type,
        p_last_tendered
      );
    return;
  end if;

  select coalesce(array_agg(distinct cleaned), array[]::text[])
  into v_cpv_search_codes
  from (
    select left(regexp_replace(lower(trim(v)), '[^0-9]', '', 'g'), 8) as cleaned
    from unnest(coalesce(p_cpv_search_codes, array[]::text[])) as v
  ) t
  where cleaned <> '';

  if v_last_tendered is not null and v_last_tendered > 1 then
    v_published_cutoff := now() - (interval '1 day' * v_last_tendered);
  end if;

  if v_keyword is not null then
    v_candidate_cap := least(greatest(v_limit * 16, 5000), 24000);
  elsif v_deadline_within is not null then
    v_candidate_cap := least(greatest(v_limit * 8, 4000), 18000);
  else
    v_candidate_cap := least(greatest(v_limit * 10, 4000), 20000);
  end if;

  return query
  with base as (
    select
      nsc.*,
      coalesce(nsc.last_seen_at, nsc.updated_at, nsc.source_updated_at, nsc.last_updated_at) as sort_ts
    from public.notices_search_current nsc
    where nsc.tenant_id = v_tenant_id
      and (v_country is null or nsc.region = v_country)
      and (
        v_source is null
        or nsc.source = v_source
      )
      and (
        v_deadline_within is null
        or (
          coalesce(nsc.is_open, true)
          and nsc.deadline_date is not null
          and nsc.deadline_date between current_date and (current_date + v_deadline_within)
        )
      )
      and (
        v_published_cutoff is null
        or (
          nsc.last_seen_at >= v_published_cutoff
          or (nsc.last_seen_at is null and coalesce(nsc.updated_at, nsc.last_updated_at) >= v_published_cutoff)
        )
      )
    order by nsc.last_seen_at desc nulls last
    limit v_candidate_cap
  ),
  filtered as (
    select b.*
    from base b
    where (
      v_keyword is null
      or lower(coalesce(b.title, '')) like '%' || v_keyword || '%'
      or lower(coalesce(b.search_text, '')) like '%' || v_keyword || '%'
      or lower(coalesce(b.buyer_name_raw, '')) like '%' || v_keyword || '%'
    )
      and (
        v_buyer_search is null
        or lower(coalesce(b.buyer_name_raw, '')) like '%' || v_buyer_search || '%'
      )
      and (
        cardinality(v_cpv_search_codes) = 0
        or exists (
          select 1
          from unnest(coalesce(b.cpv_codes, '{}'::text[])) as c
          where exists (
            select 1
            from unnest(v_cpv_search_codes) as wanted
            where c like wanted || '%'
          )
        )
      )
      and (
        v_industry is null
        or (
          (v_industry = 'construction' and exists (select 1 from unnest(coalesce(b.cpv_codes, '{}'::text[])) c where c like '45%'))
          or (v_industry = 'it' and exists (select 1 from unnest(coalesce(b.cpv_codes, '{}'::text[])) c where c like '72%' or c like '48%'))
          or (v_industry = 'health' and exists (select 1 from unnest(coalesce(b.cpv_codes, '{}'::text[])) c where c like '33%' or c like '85%'))
          or (v_industry = 'transport' and exists (select 1 from unnest(coalesce(b.cpv_codes, '{}'::text[])) c where c like '60%' or c like '34%'))
          or (v_industry = 'consulting' and exists (select 1 from unnest(coalesce(b.cpv_codes, '{}'::text[])) c where c like '79%' or c like '71%'))
          or (v_industry = 'food' and exists (select 1 from unnest(coalesce(b.cpv_codes, '{}'::text[])) c where c like '15%' or c like '55%'))
        )
      )
      and (
        v_institution_type is null
        or (
          (v_institution_type = 'ministry' and lower(coalesce(b.buyer_name_raw, '')) ~ '(ministry|ministère|minister)')
          or (v_institution_type = 'local' and lower(coalesce(b.buyer_name_raw, '')) ~ '(council|city|county|commune|ville)')
          or (v_institution_type = 'health' and lower(coalesce(b.buyer_name_raw, '')) ~ '(health|hospital|santé|hôpital)')
          or (v_institution_type = 'education' and lower(coalesce(b.buyer_name_raw, '')) ~ '(university|college|school|université|école)')
          or (v_institution_type = 'transport' and lower(coalesce(b.buyer_name_raw, '')) ~ '(transport|railway|road)')
        )
      )
  ),
  scored as (
    select
      f.*,
      (
        case
          when v_keyword is null then 0
          when lower(coalesce(f.title, '')) = v_keyword then 55
          when lower(coalesce(f.title, '')) like v_keyword || '%' then 44
          when lower(coalesce(f.title, '')) like '%' || v_keyword || '%' then 32
          else 0
        end
        + case
            when v_keyword is not null and lower(coalesce(f.buyer_name_raw, '')) like '%' || v_keyword || '%' then 18
            else 0
          end
        + case
            when v_buyer_search is not null and lower(coalesce(f.buyer_name_raw, '')) like '%' || v_buyer_search || '%' then 14
            else 0
          end
        + case
            when f.deadline_date is not null and f.deadline_date >= current_date then greatest(0, 20 - least((f.deadline_date - current_date), 20))
            when f.deadline_date is not null and f.deadline_date < current_date then -8
            else 0
          end
        + greatest(0, 15 - floor(extract(epoch from (now() - coalesce(f.sort_ts, now()))) / 604800.0))
        + case when v_source is not null then 5 else 0 end
        + case when v_country is not null then 5 else 0 end
        + case when f.coverage_status = 'linked' then 3 else 0 end
      )::numeric as rel
    from filtered f
  ),
  deduped as (
    select
      s.*,
      row_number() over (
        partition by s.canonical_id
        order by s.rel desc, s.sort_ts desc nulls last
      ) as canonical_rank
    from scored s
  )
  select
    s.tenant_id,
    s.canonical_id as tender_id,
    s.source,
    s.sort_ts as published_at,
    jsonb_build_object(
      'id', s.canonical_id,
      'canonical_id', s.canonical_id,
      'title', s.title,
      'description', nullif(s.short_summary, ''),
      'buyer_name', s.buyer_name_raw,
      'country', s.region,
      'cpv_codes', array_to_string(coalesce(s.cpv_codes, '{}'::text[]), ','),
      'publication_date', case when s.publication_date is null then null else s.publication_date::text end,
      'deadline_date', case when s.deadline_date is null then null else s.deadline_date::text end,
      'status', s.status,
      'status_code', s.status,
      'is_open', s.is_open,
      'coverage_status', s.coverage_status,
      'verification_level', s.verification_level,
      'verification_sources', coalesce(s.verification_sources, '{}'::text[]),
      'has_ted', coalesce(s.has_ted, false),
      'ted_notice_ids', coalesce(s.ted_notice_ids, '{}'::text[]),
      'notice_count', coalesce(s.notice_count, 0),
      'version_count', coalesce(s.notice_count, 0),
      'source', s.source,
      'source_summary', case
        when cardinality(coalesce(s.verification_sources, '{}'::text[])) > 0
          then array_to_string(s.verification_sources, ', ')
        else s.source
      end,
      'source_url', s.source_url,
      'url', s.source_url,
      'first_seen_at', s.first_seen_at,
      'last_seen_at', s.last_seen_at,
      'engine', 'serving'
    ) as data,
    coalesce(s.last_seen_at, s.updated_at, s.sort_ts) as updated_at,
    s.rel as relevance_score
  from deduped s
  where s.canonical_rank = 1
  order by s.rel desc, s.sort_ts desc nulls last
  limit v_limit;
end;
$$;
