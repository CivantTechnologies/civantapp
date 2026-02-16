-- Search ranked performance hardening (v2)
-- Goals:
-- 1) Keep search_tenders_ranked on the fast path under typical UI filters.
-- 2) Improve keyword/buyer contains predicates with trigram indexes.
-- 3) Reduce candidate set before scoring to avoid statement timeouts.

create extension if not exists pg_trgm;

-- Expression indexes used by keyword/buyer contains filters.
-- NOTE: run with statement_timeout=0 when applying on large datasets.
create index concurrently if not exists idx_tenderscurrent_title_lc_trgm
  on public."TendersCurrent"
  using gin (lower(coalesce(data->>'title', data->>'tender_name', '')) gin_trgm_ops);

create index concurrently if not exists idx_tenderscurrent_buyer_lc_trgm
  on public."TendersCurrent"
  using gin (lower(coalesce(data->>'buyer_name', data->>'contracting_authority', '')) gin_trgm_ops);

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
language sql
stable
as $$
with normalized as (
  select
    lower(trim(coalesce(p_tenant_id, ''))) as tenant_id,
    greatest(1, least(coalesce(p_limit, 200), 2000))::integer as row_limit,
    nullif(lower(trim(coalesce(p_keyword, ''))), '') as keyword,
    nullif(upper(trim(coalesce(p_country, ''))), '') as country,
    nullif(trim(coalesce(p_source, '')), '') as source,
    nullif(lower(trim(coalesce(p_buyer_search, ''))), '') as buyer_search,
    coalesce((
      select array_agg(distinct cleaned)
      from (
        select left(regexp_replace(lower(trim(v)), '[^0-9]', '', 'g'), 8) as cleaned
        from unnest(coalesce(p_cpv_search_codes, array[]::text[])) as v
      ) cleaned_codes
      where cleaned <> ''
    ), array[]::text[]) as cpv_search_codes,
    case when coalesce(p_deadline_within, 0) > 0 then p_deadline_within else null end as deadline_within,
    nullif(lower(trim(coalesce(p_industry, ''))), '') as industry,
    nullif(lower(trim(coalesce(p_institution_type, ''))), '') as institution_type,
    case when coalesce(p_last_tendered, 0) > 0 then p_last_tendered else null end as last_tendered,
    case
      -- Keep exact "last 24h" semantics based on first_seen/publication in final filter.
      when coalesce(p_last_tendered, 0) > 1 then now() - (interval '1 day' * p_last_tendered)
      else null
    end as published_cutoff,
    least(greatest(greatest(1, least(coalesce(p_limit, 200), 2000)) * 40, 5000), 30000)::integer as candidate_cap
),
base_seed as (
  select
    tc.tenant_id,
    tc.tender_id,
    tc.source,
    tc.published_at,
    tc.data,
    tc.updated_at
  from public."TendersCurrent" tc
  join normalized n on n.tenant_id = tc.tenant_id
  where n.tenant_id <> ''
    and (n.source is null or tc.source = n.source)
    and (n.country is null or upper(coalesce(tc.data->>'country', tc.data->>'country_code', tc.data->>'country_iso', '')) = n.country)
    and (n.published_cutoff is null or tc.published_at >= n.published_cutoff)
    and (
      n.keyword is null
      or lower(coalesce(tc.data->>'title', tc.data->>'tender_name', '')) like ('%' || n.keyword || '%')
      or lower(coalesce(tc.data->>'buyer_name', tc.data->>'contracting_authority', '')) like ('%' || n.keyword || '%')
    )
    and (
      n.buyer_search is null
      or lower(coalesce(tc.data->>'buyer_name', tc.data->>'contracting_authority', '')) like ('%' || n.buyer_search || '%')
    )
  order by tc.published_at desc nulls last
  limit (select candidate_cap from normalized)
),
base as (
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
    coalesce(bs.data->>'deadline_date', bs.data->>'event_deadline_date', bs.data->>'submission_deadline', '') as deadline_raw,
    coalesce(bs.data->>'publication_date', bs.data->>'first_seen_at', '') as publication_raw,
    coalesce(bs.data->>'first_seen_at', bs.data->>'publication_date', '') as first_seen_raw
  from base_seed bs
),
enriched as (
  select
    b.*,
    n.row_limit,
    n.keyword,
    n.country,
    n.buyer_search,
    n.cpv_search_codes,
    n.deadline_within,
    n.industry,
    n.institution_type,
    n.last_tendered,
    case
      when pg_input_is_valid(left(b.deadline_raw, 10), 'date') then left(b.deadline_raw, 10)::date
      else null
    end as deadline_day,
    case
      when pg_input_is_valid(b.publication_raw, 'timestamp with time zone') then b.publication_raw::timestamptz
      when pg_input_is_valid(left(b.publication_raw, 10), 'date') then left(b.publication_raw, 10)::date::timestamptz
      when b.published_at is not null then b.published_at
      else null
    end as publication_ts,
    case
      when pg_input_is_valid(b.first_seen_raw, 'timestamp with time zone') then b.first_seen_raw::timestamptz
      when pg_input_is_valid(left(b.first_seen_raw, 10), 'date') then left(b.first_seen_raw, 10)::date::timestamptz
      when b.published_at is not null then b.published_at
      else null
    end as first_seen_ts
  from base b
  cross join normalized n
),
filtered as (
  select
    e.*,
    (
      cardinality(e.cpv_search_codes) > 0
      and exists (
        select 1
        from unnest(e.cpv_search_codes) as wanted
        where wanted <> ''
          and e.cpv_digits like ('%' || wanted || '%')
      )
    ) as cpv_match
  from enriched e
  where (
      cardinality(e.cpv_search_codes) = 0
      or exists (
        select 1
        from unnest(e.cpv_search_codes) as wanted
        where wanted <> ''
          and e.cpv_digits like ('%' || wanted || '%')
      )
    )
    and (
      e.deadline_within is null
      or (
        e.notice_type_lc <> 'award'
        and e.status_code_up not in ('RES', 'ADJ', 'CAN', 'DES')
        and e.deadline_day is not null
        and e.deadline_day between current_date and (current_date + e.deadline_within)
      )
    )
    and (
      e.industry is null
      or (e.industry = 'construction' and e.cpv_codes_lc like '%45%')
      or (e.industry = 'it' and (e.cpv_codes_lc like '%72%' or e.cpv_codes_lc like '%48%'))
      or (e.industry = 'health' and (e.cpv_codes_lc like '%33%' or e.cpv_codes_lc like '%85%'))
      or (e.industry = 'transport' and (e.cpv_codes_lc like '%60%' or e.cpv_codes_lc like '%34%'))
      or (e.industry = 'consulting' and (e.cpv_codes_lc like '%79%' or e.cpv_codes_lc like '%71%'))
      or (e.industry = 'food' and (e.cpv_codes_lc like '%15%' or e.cpv_codes_lc like '%55%'))
    )
    and (
      e.institution_type is null
      or (
        e.institution_type = 'ministry'
        and (e.buyer_name_lc like '%ministry%' or e.buyer_name_lc like '%ministère%' or e.buyer_name_lc like '%minister%')
      )
      or (
        e.institution_type = 'local'
        and (e.buyer_name_lc like '%council%' or e.buyer_name_lc like '%city%' or e.buyer_name_lc like '%county%' or e.buyer_name_lc like '%commune%' or e.buyer_name_lc like '%ville%')
      )
      or (
        e.institution_type = 'health'
        and (e.buyer_name_lc like '%health%' or e.buyer_name_lc like '%hospital%' or e.buyer_name_lc like '%santé%' or e.buyer_name_lc like '%hôpital%')
      )
      or (
        e.institution_type = 'education'
        and (e.buyer_name_lc like '%university%' or e.buyer_name_lc like '%college%' or e.buyer_name_lc like '%school%' or e.buyer_name_lc like '%université%' or e.buyer_name_lc like '%école%')
      )
      or (
        e.institution_type = 'transport'
        and (e.buyer_name_lc like '%transport%' or e.buyer_name_lc like '%railway%' or e.buyer_name_lc like '%road%')
      )
    )
    and (
      e.last_tendered is null
      or (
        case
          when e.last_tendered = 1 then coalesce(e.first_seen_ts, e.publication_ts)
          else e.publication_ts
        end
      ) >= (now() - (interval '1 day' * e.last_tendered))
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
        when f.keyword is null then 0
        when f.title_lc = f.keyword then 60
        when f.title_lc like (f.keyword || '%') then 46
        when f.title_lc like ('%' || f.keyword || '%') then 34
        else 0
      end
      + case when f.keyword is not null and f.buyer_name_lc like ('%' || f.keyword || '%') then 20 else 0 end
      + case when f.buyer_search is not null and f.buyer_name_lc like ('%' || f.buyer_search || '%') then 15 else 0 end
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
    )::numeric as relevance_score,
    f.row_limit
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
limit (select row_limit from normalized);
$$;
