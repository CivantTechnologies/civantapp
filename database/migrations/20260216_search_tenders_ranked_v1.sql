-- Ranked search RPC for tender discovery (tenant-scoped).
-- Designed for API fallback safety: if this function is missing, API code falls back to in-memory ranking.

do $$
begin
  if to_regclass('public."TendersCurrent"') is not null then
    create index if not exists idx_tenderscurrent_tenant_source_published_desc
      on public."TendersCurrent"(tenant_id, source, published_at desc);

    create index if not exists idx_tenderscurrent_tenant_country_source_published_desc
      on public."TendersCurrent"(
        tenant_id,
        upper(coalesce(data->>'country', data->>'country_code', data->>'country_iso', '')),
        source,
        published_at desc
      );

    create index if not exists idx_tenderscurrent_tenant_status_notice
      on public."TendersCurrent"(
        tenant_id,
        upper(coalesce(data->>'status_code', '')),
        lower(coalesce(data->>'notice_type', ''))
      );
  end if;
end $$;

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
    case when coalesce(p_last_tendered, 0) > 0 then p_last_tendered else null end as last_tendered
),
base as (
  select
    tc.tenant_id,
    tc.tender_id,
    tc.source,
    tc.published_at,
    tc.data,
    tc.updated_at,
    lower(coalesce(tc.data->>'title', tc.data->>'tender_name', '')) as title_lc,
    lower(coalesce(tc.data->>'buyer_name', tc.data->>'contracting_authority', '')) as buyer_name_lc,
    upper(coalesce(tc.data->>'country', tc.data->>'country_code', tc.data->>'country_iso', '')) as country_code,
    upper(coalesce(tc.data->>'status_code', '')) as status_code_up,
    lower(coalesce(tc.data->>'notice_type', '')) as notice_type_lc,
    lower(coalesce(tc.data->>'cpv_codes', '')) as cpv_codes_lc,
    regexp_replace(lower(coalesce(tc.data->>'cpv_codes', '')), '[^0-9]', '', 'g') as cpv_digits,
    coalesce(tc.data->>'deadline_date', tc.data->>'event_deadline_date', tc.data->>'submission_deadline', '') as deadline_raw,
    coalesce(tc.data->>'publication_date', tc.data->>'first_seen_at', '') as publication_raw,
    coalesce(tc.data->>'first_seen_at', tc.data->>'publication_date', '') as first_seen_raw
  from public."TendersCurrent" tc
  join normalized n on n.tenant_id = tc.tenant_id
  where n.tenant_id <> ''
    and (n.source is null or tc.source = n.source)
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
      array_length(e.cpv_search_codes, 1) > 0
      and exists (
        select 1
        from unnest(e.cpv_search_codes) as wanted
        where wanted <> ''
          and e.cpv_digits like ('%' || wanted || '%')
      )
    ) as cpv_match
  from enriched e
  where (e.country is null or e.country_code = e.country)
    and (e.keyword is null or e.title_lc like ('%' || e.keyword || '%') or e.buyer_name_lc like ('%' || e.keyword || '%'))
    and (e.buyer_search is null or e.buyer_name_lc like ('%' || e.buyer_search || '%'))
    and (
      array_length(e.cpv_search_codes, 1) = 0
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
