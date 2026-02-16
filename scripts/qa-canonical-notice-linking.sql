\set ON_ERROR_STOP on

create extension if not exists pg_trgm;

-- Usage:
--   psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-canonical-notice-linking.sql

\echo '=== Canonical Notice Linking QA ==='
\echo 'tenant_id=' :tenant_id

-- 1) Search should never return duplicate canonical ids.
with sr as (
  select *
  from public.search_tenders_ranked(
    :'tenant_id'::text,
    1000,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null
  )
)
select
  count(*) as search_rows,
  count(distinct tender_id) as distinct_canonical_rows,
  (count(*) = count(distinct tender_id)) as no_duplicates
from sr;

-- 2) Raw payload must be preserved.
select
  count(*) as notices_checked,
  count(*) filter (where raw_payload is not null) as raw_payload_present,
  count(*) filter (where raw_payload is null) as raw_payload_missing
from public.notices
where tenant_id = :'tenant_id'::text;

-- 3) Tenant-scoped duplicate check by source notice version key.
select
  tenant_id,
  source,
  source_notice_id,
  notice_version_hash,
  count(*) as dup_count
from public.notices
where tenant_id = :'tenant_id'::text
group by 1,2,3,4
having count(*) > 1
order by dup_count desc
limit 20;

-- 4) Search-level dedupe and coverage sanity by status.
select
  coverage_status,
  verification_level,
  count(*) as canonical_count
from public.canonical_tenders
where tenant_id = :'tenant_id'::text
group by 1,2
order by 1,2;

-- 4a) Invariant assertions (fail-fast).
select set_config('civant.qa_tenant_id', :'tenant_id'::text, false);

do $$
declare
  v_tenant_id text := current_setting('civant.qa_tenant_id', true);
  v_violations integer := 0;
begin
  -- Invariant 1: search_tenders_ranked returns one row per canonical_id for representative filter sets.
  select count(*)
  into v_violations
  from (
    with filter_cases as (
      select *
      from (values
        ('all', null::text, null::text, null::text, null::int),
        ('ie_open', 'IE'::text, null::text, null::text, 30::int),
        ('fr_ted', 'FR'::text, 'TED'::text, null::text, null::int),
        ('es_source', 'ES'::text, 'PLACSP_ES'::text, null::text, null::int),
        ('keyword', null::text, null::text, 'services'::text, null::int)
      ) as t(case_id, country, source, keyword, deadline_within)
    ),
    results as (
      select
        c.case_id,
        r.tender_id
      from filter_cases c
      cross join lateral public.search_tenders_ranked(
        v_tenant_id,
        300,
        c.keyword,
        c.country,
        c.source,
        null,
        null,
        c.deadline_within,
        null,
        null,
        null
      ) r
    )
    select case_id, tender_id
    from results
    group by 1, 2
    having count(*) > 1
  ) dups;

  if v_violations > 0 then
    raise exception 'Invariant #1 violated: duplicate canonical rows returned by search_tenders_ranked (% rows)', v_violations;
  end if;

  -- Invariant 2: each notice_id maps to exactly one canonical_id.
  select count(*)
  into v_violations
  from (
    select l.notice_id
    from public.canonical_notice_links l
    where l.tenant_id = v_tenant_id
    group by l.notice_id
    having count(distinct l.canonical_id) <> 1
  ) bad;

  if v_violations > 0 then
    raise exception 'Invariant #2 violated: notice_id mapped to multiple canonical_id values (% rows)', v_violations;
  end if;

  -- Invariant 3 + 4 computed from links.
  select count(*)
  into v_violations
  from (
    with stats as (
      select
        l.tenant_id,
        l.canonical_id,
        count(*) filter (where n.source = 'TED')::int as ted_notice_count,
        count(*) filter (where n.source <> 'TED')::int as national_notice_count,
        bool_or(n.source = 'TED' and l.link_tier = 'hard') as has_hard_ted,
        max(case when n.source = 'TED' then coalesce(l.match_score, 0) else null end) as max_ted_score
      from public.canonical_notice_links l
      join public.notices n
        on n.notice_id = l.notice_id
       and n.tenant_id = l.tenant_id
      where l.tenant_id = v_tenant_id
      group by 1,2
    )
    select ct.canonical_id
    from public.canonical_tenders ct
    left join stats s
      on s.tenant_id = ct.tenant_id
     and s.canonical_id = ct.canonical_id
    where ct.tenant_id = v_tenant_id
      and (
        (ct.coverage_status = 'linked' and not (coalesce(s.ted_notice_count, 0) > 0 and coalesce(s.national_notice_count, 0) > 0))
        or (
          ct.verification_level = 'verified'
          and not (
            ct.coverage_status = 'linked'
            and (coalesce(s.has_hard_ted, false) or coalesce(s.max_ted_score, 0) >= 0.90)
          )
        )
      )
  ) bad;

  if v_violations > 0 then
    raise exception 'Invariant #3/#4 violated: linked/verified flags inconsistent with linked TED+national evidence (% rows)', v_violations;
  end if;

  -- Invariant 5: tenant isolation across link joins.
  select count(*)
  into v_violations
  from public.canonical_notice_links l
  join public.notices n on n.notice_id = l.notice_id
  join public.canonical_tenders ct on ct.canonical_id = l.canonical_id
  where l.tenant_id = v_tenant_id
    and (n.tenant_id <> l.tenant_id or ct.tenant_id <> l.tenant_id);

  if v_violations > 0 then
    raise exception 'Invariant #5 violated: cross-tenant canonical_notice_links detected (% rows)', v_violations;
  end if;

  -- Invariant 6: no canonical row can have notice_count=0.
  select count(*)
  into v_violations
  from public.canonical_tenders ct
  where ct.tenant_id = v_tenant_id
    and coalesce(ct.notice_count, 0) <= 0;

  if v_violations > 0 then
    raise exception 'Invariant #6 violated: canonical_tenders.notice_count <= 0 (% rows)', v_violations;
  end if;
end
$$;

-- 4b) Suspicious merges: same canonical with buyer names that are materially different.
with candidate_canon as (
  select canonical_id
  from public.canonical_tenders
  where tenant_id = :'tenant_id'::text
    and coalesce(notice_count, 0) between 2 and 12
  order by coalesce(last_seen_at, updated_at, created_at) desc nulls last
  limit 500
),
notice_rows as (
  select
    l.canonical_id,
    n.notice_id,
    nullif(n.buyer_name_norm, '') as buyer_name_norm
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  join candidate_canon c
    on c.canonical_id = l.canonical_id
  where l.tenant_id = :'tenant_id'::text
),
pairs as (
  select
    a.canonical_id,
    a.buyer_name_norm as buyer_a,
    b.buyer_name_norm as buyer_b,
    similarity(a.buyer_name_norm, b.buyer_name_norm) as buyer_similarity
  from notice_rows a
  join notice_rows b
    on a.canonical_id = b.canonical_id
   and a.notice_id::text < b.notice_id::text
  where a.buyer_name_norm is not null
    and b.buyer_name_norm is not null
    and a.buyer_name_norm <> b.buyer_name_norm
)
select
  canonical_id,
  min(buyer_similarity) as min_buyer_similarity,
  count(*) as pair_count
from pairs
group by 1
having min(buyer_similarity) < 0.80
order by min_buyer_similarity asc
limit 50;

-- 4c) Suspicious merges: low CPV overlap for notices linked to same canonical.
with candidate_canon as (
  select canonical_id
  from public.canonical_tenders
  where tenant_id = :'tenant_id'::text
    and coalesce(notice_count, 0) between 2 and 12
  order by coalesce(last_seen_at, updated_at, created_at) desc nulls last
  limit 500
),
notice_rows as (
  select
    l.canonical_id,
    n.notice_id,
    coalesce(n.cpv_codes, '{}'::text[]) as cpv_codes
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  join candidate_canon c
    on c.canonical_id = l.canonical_id
  where l.tenant_id = :'tenant_id'::text
),
pairs as (
  select
    a.canonical_id,
    a.notice_id as notice_a,
    b.notice_id as notice_b,
    (
      select count(distinct x)
      from unnest(a.cpv_codes) as x
      where x = any(b.cpv_codes)
    )::numeric as cpv_intersection,
    (
      select count(distinct x)
      from (
        select unnest(a.cpv_codes) as x
        union
        select unnest(b.cpv_codes) as x
      ) u
    )::numeric as cpv_union
  from notice_rows a
  join notice_rows b
    on a.canonical_id = b.canonical_id
   and a.notice_id::text < b.notice_id::text
),
scored as (
  select
    canonical_id,
    notice_a,
    notice_b,
    case
      when cpv_union <= 0 then null
      else cpv_intersection / cpv_union
    end as cpv_overlap
  from pairs
)
select
  canonical_id,
  notice_a,
  notice_b,
  cpv_overlap
from scored
where cpv_overlap is not null
  and cpv_overlap < 0.10
order by cpv_overlap asc
limit 50;

-- 5) Synthetic deterministic checks (transaction-scoped + rollback).
begin;

-- Cross-source dedupe target (same real-world tender from national + TED).
select *
from public.upsert_notice_and_link(
  p_tenant_id => :'tenant_id'::text,
  p_source => 'ETENDERS_IE',
  p_source_notice_id => 'QA_CANONICAL_LINK_001_NAT',
  p_source_url => 'https://example.test/national/QA_CANONICAL_LINK_001_NAT',
  p_country => 'IE',
  p_title => 'QA Tender Canonical Link Pilot',
  p_buyer_name_raw => 'Department of Public Expenditure',
  p_cpv_codes => array['72000000'],
  p_publication_date => current_date - 2,
  p_deadline_date => current_date + 20,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'kind', 'national', 'id', 'QA_CANONICAL_LINK_001_NAT'),
  p_parsed_payload => jsonb_build_object('qa', true)
);

select *
from public.upsert_notice_and_link(
  p_tenant_id => :'tenant_id'::text,
  p_source => 'TED',
  p_source_notice_id => 'QA_CANONICAL_LINK_001_TED',
  p_source_url => 'https://example.test/ted/QA_CANONICAL_LINK_001_TED',
  p_country => 'IE',
  p_title => 'QA Tender Canonical Link Pilot',
  p_buyer_name_raw => 'Department of Public Expenditure',
  p_cpv_codes => array['72000000'],
  p_publication_date => current_date - 2,
  p_deadline_date => current_date + 20,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'kind', 'ted', 'id', 'QA_CANONICAL_LINK_001_TED'),
  p_parsed_payload => jsonb_build_object('qa', true)
);

with q_notices as (
  select count(*) as c
  from public.notices
  where tenant_id = :'tenant_id'::text
    and source_notice_id in ('QA_CANONICAL_LINK_001_NAT', 'QA_CANONICAL_LINK_001_TED')
),
q_links as (
  select count(*) as c
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  where n.tenant_id = :'tenant_id'::text
    and n.source_notice_id in ('QA_CANONICAL_LINK_001_NAT', 'QA_CANONICAL_LINK_001_TED')
),
q_canon as (
  select count(distinct l.canonical_id) as c
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  where n.tenant_id = :'tenant_id'::text
    and n.source_notice_id in ('QA_CANONICAL_LINK_001_NAT', 'QA_CANONICAL_LINK_001_TED')
)
select
  (select c from q_notices) as notices,
  (select c from q_links) as links,
  (select c from q_canon) as canonical_rows,
  ((select c from q_notices) >= 2) as notices_ok,
  ((select c from q_links) = 2) as links_ok,
  ((select c from q_canon) = 1) as canonical_dedup_ok;

-- TED-only should map to ted_only coverage.
select *
from public.upsert_notice_and_link(
  p_tenant_id => :'tenant_id'::text,
  p_source => 'TED',
  p_source_notice_id => 'QA_CANONICAL_TED_ONLY_001',
  p_source_url => 'https://example.test/ted/QA_CANONICAL_TED_ONLY_001',
  p_country => 'FR',
  p_title => 'QA TED only notice',
  p_buyer_name_raw => 'Ville de Test',
  p_cpv_codes => array['45000000'],
  p_publication_date => current_date - 1,
  p_deadline_date => current_date + 10,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'kind', 'ted-only'),
  p_parsed_payload => jsonb_build_object('qa', true)
);

select
  ct.canonical_id,
  ct.coverage_status,
  ct.verification_level
from public.canonical_tenders ct
join public.canonical_notice_links l
  on l.canonical_id = ct.canonical_id
 and l.tenant_id = ct.tenant_id
join public.notices n
  on n.notice_id = l.notice_id
 and n.tenant_id = l.tenant_id
where ct.tenant_id = :'tenant_id'::text
  and n.source_notice_id = 'QA_CANONICAL_TED_ONLY_001';

-- National-only should map to national_only coverage.
select *
from public.upsert_notice_and_link(
  p_tenant_id => :'tenant_id'::text,
  p_source => 'BOAMP_FR',
  p_source_notice_id => 'QA_CANONICAL_NATIONAL_ONLY_001',
  p_source_url => 'https://example.test/boamp/QA_CANONICAL_NATIONAL_ONLY_001',
  p_country => 'FR',
  p_title => 'QA national only notice',
  p_buyer_name_raw => 'Région test',
  p_cpv_codes => array['45000000'],
  p_publication_date => current_date - 1,
  p_deadline_date => current_date + 25,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'kind', 'national-only'),
  p_parsed_payload => jsonb_build_object('qa', true)
);

select
  ct.canonical_id,
  ct.coverage_status,
  ct.verification_level
from public.canonical_tenders ct
join public.canonical_notice_links l
  on l.canonical_id = ct.canonical_id
 and l.tenant_id = ct.tenant_id
join public.notices n
  on n.notice_id = l.notice_id
 and n.tenant_id = l.tenant_id
where ct.tenant_id = :'tenant_id'::text
  and n.source_notice_id = 'QA_CANONICAL_NATIONAL_ONLY_001';

-- Versioning: second payload variant should create another notice row version, same canonical.
select *
from public.upsert_notice_and_link(
  p_tenant_id => :'tenant_id'::text,
  p_source => 'ETENDERS_IE',
  p_source_notice_id => 'QA_VERSIONING_001',
  p_source_url => 'https://example.test/ie/QA_VERSIONING_001',
  p_country => 'IE',
  p_title => 'QA versioned notice',
  p_buyer_name_raw => 'Office of Public Works',
  p_cpv_codes => array['71000000'],
  p_publication_date => current_date - 3,
  p_deadline_date => current_date + 21,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'rev', 1),
  p_parsed_payload => jsonb_build_object('qa', true, 'rev', 1)
);

select *
from public.upsert_notice_and_link(
  p_tenant_id => :'tenant_id'::text,
  p_source => 'ETENDERS_IE',
  p_source_notice_id => 'QA_VERSIONING_001',
  p_source_url => 'https://example.test/ie/QA_VERSIONING_001',
  p_country => 'IE',
  p_title => 'QA versioned notice (updated)',
  p_buyer_name_raw => 'Office of Public Works',
  p_cpv_codes => array['71000000'],
  p_publication_date => current_date - 3,
  p_deadline_date => current_date + 18,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'rev', 2),
  p_parsed_payload => jsonb_build_object('qa', true, 'rev', 2)
);

with v as (
  select
    count(*) as version_rows,
    count(distinct l.canonical_id) as canonical_rows
  from public.notices n
  join public.canonical_notice_links l
    on l.notice_id = n.notice_id
   and l.tenant_id = n.tenant_id
  where n.tenant_id = :'tenant_id'::text
    and n.source = 'ETENDERS_IE'
    and n.source_notice_id = 'QA_VERSIONING_001'
)
select
  version_rows,
  canonical_rows,
  (version_rows = 2) as versioning_ok,
  (canonical_rows = 1) as same_canonical_ok
from v;

-- Tenant isolation test with same source/source_notice_id.
select *
from public.upsert_notice_and_link(
  p_tenant_id => 'qa_tenant_alpha',
  p_source => 'TED',
  p_source_notice_id => 'QA_ISOLATION_001',
  p_source_url => 'https://example.test/ted/QA_ISOLATION_001',
  p_country => 'ES',
  p_title => 'Tenant alpha notice',
  p_buyer_name_raw => 'Buyer alpha',
  p_cpv_codes => array['30000000'],
  p_publication_date => current_date,
  p_deadline_date => current_date + 15,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'tenant', 'alpha'),
  p_parsed_payload => '{}'::jsonb
);

select *
from public.upsert_notice_and_link(
  p_tenant_id => 'qa_tenant_beta',
  p_source => 'TED',
  p_source_notice_id => 'QA_ISOLATION_001',
  p_source_url => 'https://example.test/ted/QA_ISOLATION_001',
  p_country => 'ES',
  p_title => 'Tenant beta notice',
  p_buyer_name_raw => 'Buyer beta',
  p_cpv_codes => array['30000000'],
  p_publication_date => current_date,
  p_deadline_date => current_date + 15,
  p_status => 'open',
  p_raw_payload => jsonb_build_object('qa', true, 'tenant', 'beta'),
  p_parsed_payload => '{}'::jsonb
);

select
  (select count(distinct l.canonical_id)
   from public.canonical_notice_links l
   join public.notices n on n.notice_id = l.notice_id and n.tenant_id = l.tenant_id
   where n.tenant_id = 'qa_tenant_alpha' and n.source_notice_id = 'QA_ISOLATION_001') as alpha_canonical,
  (select count(distinct l.canonical_id)
   from public.canonical_notice_links l
   join public.notices n on n.notice_id = l.notice_id and n.tenant_id = l.tenant_id
   where n.tenant_id = 'qa_tenant_beta' and n.source_notice_id = 'QA_ISOLATION_001') as beta_canonical;

rollback;

-- 6) Trigger safety + backfill duration sanity (transaction-scoped / rolled back).
select tgname, tgenabled
from pg_trigger
where tgrelid = 'public."TendersCurrent"'::regclass
  and tgname = 'trg_tenderscurrent_sync_notice';

begin;
select clock_timestamp() as backfill_started_at \gset
select * from public.backfill_notices_from_tenderscurrent(:'tenant_id'::text, 50::integer, true);
select
  round(extract(epoch from (clock_timestamp() - :'backfill_started_at'::timestamptz))::numeric * 1000, 2) as backfill_elapsed_ms;
select tgname, tgenabled
from pg_trigger
where tgrelid = 'public."TendersCurrent"'::regclass
  and tgname = 'trg_tenderscurrent_sync_notice';
rollback;

\echo '=== QA complete ==='
