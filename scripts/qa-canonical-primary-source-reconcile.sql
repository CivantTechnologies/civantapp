-- QA pack: canonical primary source + TED/eTenders reconciliation integrity
-- Usage:
--   \set tenant_id 'civant_default'
--   \i scripts/qa-canonical-primary-source-reconcile.sql

with params as (
  select coalesce(nullif(:'tenant_id', ''), 'civant_default') as tenant_id
),
link_stats as (
  select
    l.tenant_id,
    l.canonical_id,
    count(*)::int as linked_total,
    count(*) filter (where n.source = 'TED')::int as ted_linked,
    count(*) filter (where n.source <> 'TED')::int as national_linked,
    max(case when n.source = 'TED' then n.source_url else null end) as sample_ted_url,
    max(case when n.source <> 'TED' then n.source_url else null end) as sample_national_url
  from public.canonical_notice_links l
  join public.notices n
    on n.tenant_id = l.tenant_id
   and n.notice_id = l.notice_id
  join params p
    on p.tenant_id = l.tenant_id
  group by 1,2
)
select
  'coverage_mismatch' as check_name,
  count(*)::int as violations
from public.canonical_tenders ct
join link_stats s
  on s.tenant_id = ct.tenant_id
 and s.canonical_id = ct.canonical_id
join params p
  on p.tenant_id = ct.tenant_id
where (
  (ct.coverage_status = 'national_only' and s.ted_linked > 0)
  or (ct.coverage_status = 'ted_only' and s.national_linked > 0)
  or (ct.coverage_status = 'linked' and not (s.ted_linked > 0 and s.national_linked > 0))
)

union all

select
  'verification_mismatch' as check_name,
  count(*)::int as violations
from public.canonical_tenders ct
join link_stats s
  on s.tenant_id = ct.tenant_id
 and s.canonical_id = ct.canonical_id
join params p
  on p.tenant_id = ct.tenant_id
where (
  ct.verification_level = 'verified'
  and not (
    ct.coverage_status = 'linked'
    and exists (
      select 1
      from public.canonical_notice_links l
      join public.notices n
        on n.tenant_id = l.tenant_id
       and n.notice_id = l.notice_id
      where l.tenant_id = ct.tenant_id
        and l.canonical_id = ct.canonical_id
        and n.source = 'TED'
        and (l.link_tier = 'hard' or coalesce(l.match_score, 0) >= 0.90)
    )
  )
)

union all

select
  'notice_count_mismatch' as check_name,
  count(*)::int as violations
from public.canonical_tenders ct
join link_stats s
  on s.tenant_id = ct.tenant_id
 and s.canonical_id = ct.canonical_id
join params p
  on p.tenant_id = ct.tenant_id
where coalesce(ct.notice_count, 0) <> coalesce(s.linked_total, 0)

union all

select
  'primary_source_linked_kind_without_notice_id' as check_name,
  count(*)::int as violations
from public.canonical_tenders ct
join params p
  on p.tenant_id = ct.tenant_id
where ct.primary_source_kind = 'linked_notice'
  and ct.primary_source_notice_id is null

union all

select
  'primary_source_ted_while_marked_national_only' as check_name,
  count(*)::int as violations
from public.canonical_tenders ct
join params p
  on p.tenant_id = ct.tenant_id
where ct.coverage_status = 'national_only'
  and upper(coalesce(ct.primary_source_notice_source, '')) = 'TED'
;

-- Review candidates where TED URL is still shown but national-only label remains.
with params as (
  select coalesce(nullif(:'tenant_id', ''), 'civant_default') as tenant_id
),
link_stats as (
  select
    l.tenant_id,
    l.canonical_id,
    count(*) filter (where n.source = 'TED')::int as ted_linked,
    count(*) filter (where n.source <> 'TED')::int as national_linked,
    max(case when n.source = 'TED' then n.source_url else null end) as sample_ted_url
  from public.canonical_notice_links l
  join public.notices n
    on n.tenant_id = l.tenant_id
   and n.notice_id = l.notice_id
  join params p
    on p.tenant_id = l.tenant_id
  group by 1,2
)
select
  ct.canonical_id,
  ct.source,
  ct.coverage_status,
  ct.verification_level,
  ct.primary_source_kind,
  ct.primary_source_notice_source,
  ct.primary_source_url,
  s.ted_linked,
  s.national_linked,
  s.sample_ted_url
from public.canonical_tenders ct
left join link_stats s
  on s.tenant_id = ct.tenant_id
 and s.canonical_id = ct.canonical_id
join params p
  on p.tenant_id = ct.tenant_id
where (
  ct.primary_source_url ilike 'https://ted.europa.eu%'
  or s.sample_ted_url is not null
)
order by ct.updated_at desc nulls last
limit 25;
