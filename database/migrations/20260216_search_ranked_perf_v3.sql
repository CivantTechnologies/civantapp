-- Search ranked performance hardening (v3)
-- Rewrites search_tenders_ranked to PL/pgSQL so constant predicates are pushed down
-- and indexes can be used (instead of full-table scans from CTE join filters).

create extension if not exists pg_trgm;

-- Critical ordering indexes for fast top-N retrieval on recent tenders.
-- Keep NULLS LAST aligned with query ORDER BY clauses.
create index concurrently if not exists idx_tenderscurrent_tenant_published_at_desc_nl
  on public."TendersCurrent"(tenant_id, published_at desc nulls last);

create index concurrently if not exists idx_tenderscurrent_published_at_desc_nl
  on public."TendersCurrent"(published_at desc nulls last);

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
  v_source text := nullif(trim(coalesce(p_source, '')), '');
  v_buyer_search text := nullif(lower(trim(coalesce(p_buyer_search, ''))), '');
  v_deadline_within integer := case when coalesce(p_deadline_within, 0) > 0 then p_deadline_within else null end;
  v_industry text := nullif(lower(trim(coalesce(p_industry, ''))), '');
  v_institution_type text := nullif(lower(trim(coalesce(p_institution_type, ''))), '');
  v_last_tendered integer := case when coalesce(p_last_tendered, 0) > 0 then p_last_tendered else null end;
  v_published_cutoff timestamptz := null;
  v_candidate_cap integer;
  v_cpv_search_codes text[] := array[]::text[];
  v_use_fast_path boolean := false;
begin
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  select coalesce(array_agg(distinct cleaned), array[]::text[])
  into v_cpv_search_codes
  from (
    select left(regexp_replace(lower(trim(v)), '[^0-9]', '', 'g'), 8) as cleaned
    from unnest(coalesce(p_cpv_search_codes, array[]::text[])) as v
  ) cleaned_codes
  where cleaned <> '';

  if v_last_tendered is not null and v_last_tendered > 1 then
    v_published_cutoff := now() - (interval '1 day' * v_last_tendered);
  end if;

  v_use_fast_path := (
    v_keyword is null
    and v_buyer_search is null
    and cardinality(v_cpv_search_codes) = 0
    and v_industry is null
    and v_institution_type is null
  );

  if v_keyword is not null then
    v_candidate_cap := least(greatest(v_limit * 10, 1500), 10000);
  elsif v_deadline_within is not null then
    v_candidate_cap := least(greatest(v_limit * 4, 1500), 7000);
  elsif cardinality(v_cpv_search_codes) > 0 or v_buyer_search is not null then
    v_candidate_cap := least(greatest(v_limit * 12, 2000), 12000);
  else
    v_candidate_cap := least(greatest(v_limit * 10, 3000), 12000);
  end if;

  if v_use_fast_path then
    if v_deadline_within is null and v_source is null and v_country is null and (v_last_tendered is null or v_last_tendered > 1) then
      if v_published_cutoff is null then
        return query execute format(
          $q$
          select
            tc.tenant_id,
            tc.tender_id,
            tc.source,
            tc.published_at,
            tc.data,
            tc.updated_at,
            greatest(0, 15 - floor(extract(epoch from (now() - coalesce(tc.published_at, tc.updated_at))) / 604800.0))::numeric as relevance_score
          from public."TendersCurrent" tc
          where tc.tenant_id = %L
          order by tc.published_at desc nulls last
          limit %s
          $q$,
          v_tenant_id,
          v_limit
        );
        return;
      end if;

      return query execute format(
        $q$
        select
          tc.tenant_id,
          tc.tender_id,
          tc.source,
          tc.published_at,
          tc.data,
          tc.updated_at,
          greatest(0, 15 - floor(extract(epoch from (now() - coalesce(tc.published_at, tc.updated_at))) / 604800.0))::numeric as relevance_score
        from public."TendersCurrent" tc
        where tc.tenant_id = %L
          and tc.published_at >= %L::timestamptz
        order by tc.published_at desc nulls last
        limit %s
        $q$,
        v_tenant_id,
        v_published_cutoff,
        v_limit
      );
      return;
    end if;

    if v_deadline_within is null then
      return query
      with base_seed as (
        select
          tc.tenant_id,
          tc.tender_id,
          tc.source,
          tc.published_at,
          tc.data,
          tc.updated_at
        from public."TendersCurrent" tc
        where tc.tenant_id = v_tenant_id
          and (v_source is null or tc.source = v_source)
          and (v_country is null or upper(coalesce(tc.data->>'country', tc.data->>'country_code', tc.data->>'country_iso', '')) = v_country)
          and (v_published_cutoff is null or tc.published_at >= v_published_cutoff)
        order by tc.published_at desc nulls last
        limit v_candidate_cap
      )
      select
        bs.tenant_id,
        bs.tender_id,
        bs.source,
        bs.published_at,
        bs.data,
        bs.updated_at,
        (
          greatest(0, 15 - floor(extract(epoch from (now() - coalesce(bs.published_at, bs.updated_at))) / 604800.0))
          + case when bs.source = 'PLACSP_ES' and upper(coalesce(bs.data->>'country', bs.data->>'country_code', bs.data->>'country_iso', '')) = 'ES' then 3 else 0 end
        )::numeric as relevance_score
      from base_seed bs
      where (
        v_last_tendered is null
        or v_last_tendered > 1
        or (
          case
            when pg_input_is_valid(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 'timestamp with time zone')
              then coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', '')::timestamptz
            when pg_input_is_valid(left(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 10), 'date')
              then left(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 10)::date::timestamptz
            when bs.published_at is not null then bs.published_at
            else null
          end
        ) >= (now() - interval '1 day')
      )
      order by bs.published_at desc nulls last
      limit v_limit;
      return;
    end if;

    return query
    with base_seed as (
      select
        tc.tenant_id,
        tc.tender_id,
        tc.source,
        tc.published_at,
        tc.data,
        tc.updated_at
      from public."TendersCurrent" tc
      where tc.tenant_id = v_tenant_id
        and (v_source is null or tc.source = v_source)
        and (v_country is null or upper(coalesce(tc.data->>'country', tc.data->>'country_code', tc.data->>'country_iso', '')) = v_country)
        and (v_published_cutoff is null or tc.published_at >= v_published_cutoff)
        and (
          v_deadline_within is null
          or (
            lower(coalesce(tc.data->>'notice_type', '')) <> 'award'
            and upper(coalesce(tc.data->>'status_code', '')) not in ('RES', 'ADJ', 'CAN', 'DES')
            and left(coalesce(tc.data->>'deadline_date', tc.data->>'event_deadline_date', tc.data->>'submission_deadline', ''), 10) ~ '^\d{4}-\d{2}-\d{2}$'
            and left(coalesce(tc.data->>'deadline_date', tc.data->>'event_deadline_date', tc.data->>'submission_deadline', ''), 10)
              between to_char(current_date, 'YYYY-MM-DD')
                  and to_char(current_date + v_deadline_within, 'YYYY-MM-DD')
          )
        )
      order by tc.published_at desc nulls last
      limit v_candidate_cap
    )
    select
      bs.tenant_id,
      bs.tender_id,
      bs.source,
      bs.published_at,
      bs.data,
      bs.updated_at,
      (
        case
          when v_deadline_within is not null
            and left(coalesce(bs.data->>'deadline_date', bs.data->>'event_deadline_date', bs.data->>'submission_deadline', ''), 10) ~ '^\d{4}-\d{2}-\d{2}$'
            and left(coalesce(bs.data->>'deadline_date', bs.data->>'event_deadline_date', bs.data->>'submission_deadline', ''), 10)::date >= current_date
            then greatest(0, 20 - (left(coalesce(bs.data->>'deadline_date', bs.data->>'event_deadline_date', bs.data->>'submission_deadline', ''), 10)::date - current_date))
          else 0
        end
        + greatest(0, 15 - floor(extract(epoch from (now() - coalesce(bs.published_at, bs.updated_at))) / 604800.0))
        + case when bs.source = 'PLACSP_ES' and upper(coalesce(bs.data->>'country', bs.data->>'country_code', bs.data->>'country_iso', '')) = 'ES' then 3 else 0 end
      )::numeric as relevance_score
    from base_seed bs
    where (
      v_last_tendered is null
      or v_last_tendered > 1
      or (
        case
          when pg_input_is_valid(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 'timestamp with time zone')
            then coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', '')::timestamptz
          when pg_input_is_valid(left(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 10), 'date')
            then left(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 10)::date::timestamptz
          when bs.published_at is not null then bs.published_at
          else null
        end
      ) >= (now() - interval '1 day')
    )
    order by relevance_score desc, bs.published_at desc nulls last
    limit v_limit;
    return;
  end if;

  return query
  with base_seed as (
    select
      tc.tenant_id,
      tc.tender_id,
      tc.source,
      tc.published_at,
      tc.data,
      tc.updated_at
    from public."TendersCurrent" tc
    where tc.tenant_id = v_tenant_id
      and (v_source is null or tc.source = v_source)
      and (v_country is null or upper(coalesce(tc.data->>'country', tc.data->>'country_code', tc.data->>'country_iso', '')) = v_country)
      and (v_published_cutoff is null or tc.published_at >= v_published_cutoff)
      and (
        v_keyword is null
        or lower(coalesce(tc.data->>'title', tc.data->>'tender_name', '')) like ('%' || v_keyword || '%')
        or lower(coalesce(tc.data->>'buyer_name', tc.data->>'contracting_authority', '')) like ('%' || v_keyword || '%')
      )
      and (
        v_buyer_search is null
        or lower(coalesce(tc.data->>'buyer_name', tc.data->>'contracting_authority', '')) like ('%' || v_buyer_search || '%')
      )
      and (
        v_deadline_within is null
        or (
          lower(coalesce(tc.data->>'notice_type', '')) <> 'award'
          and upper(coalesce(tc.data->>'status_code', '')) not in ('RES', 'ADJ', 'CAN', 'DES')
          and left(coalesce(tc.data->>'deadline_date', tc.data->>'event_deadline_date', tc.data->>'submission_deadline', ''), 10) ~ '^\d{4}-\d{2}-\d{2}$'
          and left(coalesce(tc.data->>'deadline_date', tc.data->>'event_deadline_date', tc.data->>'submission_deadline', ''), 10)
            between to_char(current_date, 'YYYY-MM-DD')
                and to_char(current_date + v_deadline_within, 'YYYY-MM-DD')
        )
      )
    order by tc.published_at desc nulls last
    limit v_candidate_cap
  ),
  enriched as (
    select
      bs.tenant_id,
      bs.tender_id,
      bs.source,
      bs.published_at,
      bs.data,
      bs.updated_at,
      lower(coalesce(bs.data->>'title', bs.data->>'tender_name', '')) as title_lc,
      lower(coalesce(bs.data->>'buyer_name', bs.data->>'contracting_authority', '')) as buyer_name_lc,
      upper(coalesce(bs.data->>'country', bs.data->>'country_code', bs.data->>'country_iso', '')) as country_code,
      upper(coalesce(bs.data->>'status_code', '')) as status_code_up,
      lower(coalesce(bs.data->>'notice_type', '')) as notice_type_lc,
      lower(coalesce(bs.data->>'cpv_codes', '')) as cpv_codes_lc,
      regexp_replace(lower(coalesce(bs.data->>'cpv_codes', '')), '[^0-9]', '', 'g') as cpv_digits,
      case
        when pg_input_is_valid(left(coalesce(bs.data->>'deadline_date', bs.data->>'event_deadline_date', bs.data->>'submission_deadline', ''), 10), 'date')
          then left(coalesce(bs.data->>'deadline_date', bs.data->>'event_deadline_date', bs.data->>'submission_deadline', ''), 10)::date
        else null
      end as deadline_day,
      case
        when pg_input_is_valid(coalesce(bs.data->>'publication_date', bs.data->>'first_seen_at', ''), 'timestamp with time zone')
          then coalesce(bs.data->>'publication_date', bs.data->>'first_seen_at', '')::timestamptz
        when pg_input_is_valid(left(coalesce(bs.data->>'publication_date', bs.data->>'first_seen_at', ''), 10), 'date')
          then left(coalesce(bs.data->>'publication_date', bs.data->>'first_seen_at', ''), 10)::date::timestamptz
        when bs.published_at is not null then bs.published_at
        else null
      end as publication_ts,
      case
        when pg_input_is_valid(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 'timestamp with time zone')
          then coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', '')::timestamptz
        when pg_input_is_valid(left(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 10), 'date')
          then left(coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', ''), 10)::date::timestamptz
        when bs.published_at is not null then bs.published_at
        else null
      end as first_seen_ts
    from base_seed bs
  ),
  filtered as (
    select
      e.*,
      (
        cardinality(v_cpv_search_codes) > 0
        and exists (
          select 1
          from unnest(v_cpv_search_codes) as wanted
          where wanted <> ''
            and e.cpv_digits like ('%' || wanted || '%')
        )
      ) as cpv_match
    from enriched e
    where (
        cardinality(v_cpv_search_codes) = 0
        or exists (
          select 1
          from unnest(v_cpv_search_codes) as wanted
          where wanted <> ''
            and e.cpv_digits like ('%' || wanted || '%')
        )
      )
      and (
        v_deadline_within is null
        or (
          e.notice_type_lc <> 'award'
          and e.status_code_up not in ('RES', 'ADJ', 'CAN', 'DES')
          and e.deadline_day is not null
          and e.deadline_day between current_date and (current_date + v_deadline_within)
        )
      )
      and (
        v_industry is null
        or (v_industry = 'construction' and e.cpv_codes_lc like '%45%')
        or (v_industry = 'it' and (e.cpv_codes_lc like '%72%' or e.cpv_codes_lc like '%48%'))
        or (v_industry = 'health' and (e.cpv_codes_lc like '%33%' or e.cpv_codes_lc like '%85%'))
        or (v_industry = 'transport' and (e.cpv_codes_lc like '%60%' or e.cpv_codes_lc like '%34%'))
        or (v_industry = 'consulting' and (e.cpv_codes_lc like '%79%' or e.cpv_codes_lc like '%71%'))
        or (v_industry = 'food' and (e.cpv_codes_lc like '%15%' or e.cpv_codes_lc like '%55%'))
      )
      and (
        v_institution_type is null
        or (
          v_institution_type = 'ministry'
          and (e.buyer_name_lc like '%ministry%' or e.buyer_name_lc like '%ministère%' or e.buyer_name_lc like '%minister%')
        )
        or (
          v_institution_type = 'local'
          and (e.buyer_name_lc like '%council%' or e.buyer_name_lc like '%city%' or e.buyer_name_lc like '%county%' or e.buyer_name_lc like '%commune%' or e.buyer_name_lc like '%ville%')
        )
        or (
          v_institution_type = 'health'
          and (e.buyer_name_lc like '%health%' or e.buyer_name_lc like '%hospital%' or e.buyer_name_lc like '%santé%' or e.buyer_name_lc like '%hôpital%')
        )
        or (
          v_institution_type = 'education'
          and (e.buyer_name_lc like '%university%' or e.buyer_name_lc like '%college%' or e.buyer_name_lc like '%school%' or e.buyer_name_lc like '%université%' or e.buyer_name_lc like '%école%')
        )
        or (
          v_institution_type = 'transport'
          and (e.buyer_name_lc like '%transport%' or e.buyer_name_lc like '%railway%' or e.buyer_name_lc like '%road%')
        )
      )
      and (
        v_last_tendered is null
        or (
          case
            when v_last_tendered = 1 then coalesce(e.first_seen_ts, e.publication_ts)
            else e.publication_ts
          end
        ) >= (now() - (interval '1 day' * v_last_tendered))
      )
  ),
  scored as (
    select
      f.tenant_id,
      f.tender_id,
      f.source,
      f.published_at,
      f.data,
      f.updated_at,
      (
        case
          when v_keyword is null then 0
          when f.title_lc = v_keyword then 60
          when f.title_lc like (v_keyword || '%') then 46
          when f.title_lc like ('%' || v_keyword || '%') then 34
          else 0
        end
        + case when v_keyword is not null and f.buyer_name_lc like ('%' || v_keyword || '%') then 20 else 0 end
        + case when v_buyer_search is not null and f.buyer_name_lc like ('%' || v_buyer_search || '%') then 15 else 0 end
        + case when f.cpv_match then 18 else 0 end
        + case
            when f.deadline_day is not null and f.deadline_day >= current_date
              then greatest(0, 20 - (f.deadline_day - current_date))
            else 0
          end
        + case
            when coalesce(f.publication_ts, f.updated_at) is not null
              then greatest(0, 15 - floor(extract(epoch from (now() - coalesce(f.publication_ts, f.updated_at))) / 604800.0))
            else 0
          end
        + case when f.source = 'PLACSP_ES' and f.country_code = 'ES' then 3 else 0 end
      )::numeric as relevance_score
    from filtered f
  )
  select
    s.tenant_id,
    s.tender_id,
    s.source,
    s.published_at,
    s.data,
    s.updated_at,
    s.relevance_score
  from scored s
  order by s.relevance_score desc, s.published_at desc nulls last
  limit v_limit;
end;
$$;
