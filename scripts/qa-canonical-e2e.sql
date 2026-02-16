\set ON_ERROR_STOP on

-- Canonical E2E SQL harness
-- Requires psql var: tenant_id (recommended: qa_canonical_test)

\echo '=== Canonical E2E Test ==='
\echo 'tenant_id=' :tenant_id

select set_config('civant.qa_tenant_id', :'tenant_id'::text, false);
select set_config('civant.e2e_tag', to_char(clock_timestamp(), 'YYYYMMDDHH24MISSMS'), false);

-- Safety: verify required objects/functions exist before seeding data.
do $$
begin
  if to_regclass('public.canonical_tenders') is null then
    raise exception 'missing required table: public.canonical_tenders';
  end if;
  if to_regclass('public.notices') is null then
    raise exception 'missing required table: public.notices';
  end if;
  if to_regclass('public.canonical_notice_links') is null then
    raise exception 'missing required table: public.canonical_notice_links';
  end if;

  if to_regprocedure('public.upsert_notice_and_link(text,text,text,text,text,text,text,text[],date,date,text,jsonb,jsonb,text)') is null then
    raise exception 'missing required function: public.upsert_notice_and_link(...)';
  end if;

  if to_regprocedure('public.search_tenders_ranked(text,integer,text,text,text,text,text[],integer,text,text,integer)') is null then
    raise exception 'missing required function: public.search_tenders_ranked(...)';
  end if;
end
$$;

-- Phase 1: linked / ted-only / national-only + first version insert.
do $$
declare
  v_tenant text := current_setting('civant.qa_tenant_id', true);
  v_tag text := current_setting('civant.e2e_tag', true);

  v_linked_token text;
  v_ted_only_token text;
  v_national_only_token text;
  v_version_token text;

  v_linked_canonical_1 text;
  v_linked_canonical_2 text;
  v_version_canonical_1 text;

  v_last_seen_before timestamptz;
  v_row_count integer;
  v_distinct_count integer;
  v_notice_count integer;
  v_coverage text;
begin
  if coalesce(v_tenant, '') = '' then
    raise exception 'tenant_id is required';
  end if;
  if coalesce(v_tag, '') = '' then
    raise exception 'missing generated e2e tag';
  end if;

  v_linked_token := lower('e2e-linked-' || v_tag);
  v_ted_only_token := lower('e2e-tedonly-' || v_tag);
  v_national_only_token := lower('e2e-nationalonly-' || v_tag);
  v_version_token := lower('e2e-version-' || v_tag);

  -- 1) TED + national -> 1 canonical row in search, coverage_status=linked.
  select u.canonical_id
  into v_linked_canonical_1
  from public.upsert_notice_and_link(
    p_tenant_id => v_tenant,
    p_source => 'ETENDERS_IE',
    p_source_notice_id => 'E2E-NAT-LINKED-' || v_tag,
    p_source_url => 'https://example.test/ie/' || v_tag,
    p_country => 'IE',
    p_title => 'E2E Linked Platform Procurement ' || v_linked_token,
    p_buyer_name_raw => 'E2E Linked Buyer',
    p_cpv_codes => array['72000000'],
    p_publication_date => current_date - 5,
    p_deadline_date => current_date + 21,
    p_status => 'open',
    p_raw_payload => jsonb_build_object('scenario','linked','version',1,'token',v_linked_token),
    p_parsed_payload => jsonb_build_object('scenario','linked','token',v_linked_token),
    p_notice_version_hash => null
  ) as u;

  select u.canonical_id
  into v_linked_canonical_2
  from public.upsert_notice_and_link(
    p_tenant_id => v_tenant,
    p_source => 'TED',
    p_source_notice_id => 'E2E-TED-LINKED-' || v_tag,
    p_source_url => 'https://ted.europa.eu/notice/' || v_tag,
    p_country => 'IE',
    p_title => 'E2E Linked Platform Procurement ' || v_linked_token,
    p_buyer_name_raw => 'E2E Linked Buyer',
    p_cpv_codes => array['72000000'],
    p_publication_date => current_date - 5,
    p_deadline_date => current_date + 21,
    p_status => 'open',
    p_raw_payload => jsonb_build_object('scenario','linked','version',2,'token',v_linked_token),
    p_parsed_payload => jsonb_build_object('scenario','linked','token',v_linked_token),
    p_notice_version_hash => null
  ) as u;

  if v_linked_canonical_1 is distinct from v_linked_canonical_2 then
    raise exception 'TED+national should resolve to same canonical_id (got % vs %)', v_linked_canonical_1, v_linked_canonical_2;
  end if;

  select count(*), count(distinct sr.tender_id), min(sr.data->>'coverage_status')
  into v_row_count, v_distinct_count, v_coverage
  from public.search_tenders_ranked(v_tenant, 50, v_linked_token, null, null, null, null, null, null, null, null) sr;

  if v_row_count <> 1 or v_distinct_count <> 1 then
    raise exception 'TED+national search expected exactly 1 row/distinct canonical (got rows=%, distinct=%)', v_row_count, v_distinct_count;
  end if;
  if v_coverage <> 'linked' then
    raise exception 'TED+national expected coverage_status=linked (got %)', coalesce(v_coverage, '<null>');
  end if;

  -- 2) TED-only -> 1 canonical row, coverage_status=ted_only.
  perform *
  from public.upsert_notice_and_link(
    p_tenant_id => v_tenant,
    p_source => 'TED',
    p_source_notice_id => 'E2E-TED-ONLY-' || v_tag,
    p_source_url => 'https://ted.europa.eu/notice/ted-only-' || v_tag,
    p_country => 'FR',
    p_title => 'E2E TED Only Opportunity ' || v_ted_only_token,
    p_buyer_name_raw => 'E2E TED Only Buyer',
    p_cpv_codes => array['79000000'],
    p_publication_date => current_date - 3,
    p_deadline_date => current_date + 30,
    p_status => 'open',
    p_raw_payload => jsonb_build_object('scenario','ted_only','token',v_ted_only_token),
    p_parsed_payload => jsonb_build_object('scenario','ted_only','token',v_ted_only_token),
    p_notice_version_hash => null
  );

  select count(*), count(distinct sr.tender_id), min(sr.data->>'coverage_status')
  into v_row_count, v_distinct_count, v_coverage
  from public.search_tenders_ranked(v_tenant, 50, v_ted_only_token, null, null, null, null, null, null, null, null) sr;

  if v_row_count <> 1 or v_distinct_count <> 1 then
    raise exception 'TED-only search expected exactly 1 row/distinct canonical (got rows=%, distinct=%)', v_row_count, v_distinct_count;
  end if;
  if v_coverage <> 'ted_only' then
    raise exception 'TED-only expected coverage_status=ted_only (got %)', coalesce(v_coverage, '<null>');
  end if;

  -- 3) national-only -> 1 canonical row, coverage_status=national_only.
  perform *
  from public.upsert_notice_and_link(
    p_tenant_id => v_tenant,
    p_source => 'BOAMP_FR',
    p_source_notice_id => 'E2E-NAT-ONLY-' || v_tag,
    p_source_url => 'https://boamp.fr/notice/' || v_tag,
    p_country => 'FR',
    p_title => 'E2E National Only Opportunity ' || v_national_only_token,
    p_buyer_name_raw => 'E2E National Only Buyer',
    p_cpv_codes => array['45000000'],
    p_publication_date => current_date - 2,
    p_deadline_date => current_date + 18,
    p_status => 'open',
    p_raw_payload => jsonb_build_object('scenario','national_only','token',v_national_only_token),
    p_parsed_payload => jsonb_build_object('scenario','national_only','token',v_national_only_token),
    p_notice_version_hash => null
  );

  select count(*), count(distinct sr.tender_id), min(sr.data->>'coverage_status')
  into v_row_count, v_distinct_count, v_coverage
  from public.search_tenders_ranked(v_tenant, 50, v_national_only_token, null, null, null, null, null, null, null, null) sr;

  if v_row_count <> 1 or v_distinct_count <> 1 then
    raise exception 'National-only search expected exactly 1 row/distinct canonical (got rows=%, distinct=%)', v_row_count, v_distinct_count;
  end if;
  if v_coverage <> 'national_only' then
    raise exception 'National-only expected coverage_status=national_only (got %)', coalesce(v_coverage, '<null>');
  end if;

  -- 4a) version update phase 1 -> first notice version.
  select u.canonical_id
  into v_version_canonical_1
  from public.upsert_notice_and_link(
    p_tenant_id => v_tenant,
    p_source => 'ETENDERS_IE',
    p_source_notice_id => 'E2E-VERSION-' || v_tag,
    p_source_url => 'https://example.test/version/' || v_tag,
    p_country => 'IE',
    p_title => 'E2E Versioned Opportunity ' || v_version_token,
    p_buyer_name_raw => 'E2E Version Buyer',
    p_cpv_codes => array['48000000'],
    p_publication_date => current_date - 1,
    p_deadline_date => current_date + 25,
    p_status => 'open',
    p_raw_payload => jsonb_build_object('scenario','version','version',1,'token',v_version_token,'budget',100),
    p_parsed_payload => jsonb_build_object('scenario','version','version',1,'token',v_version_token),
    p_notice_version_hash => null
  ) u;

  select ct.last_seen_at, ct.notice_count
  into v_last_seen_before, v_notice_count
  from public.canonical_tenders ct
  where ct.tenant_id = v_tenant
    and ct.canonical_id = v_version_canonical_1;

  if v_last_seen_before is null then
    raise exception 'Version phase 1 expected non-null last_seen_at';
  end if;

  if coalesce(v_notice_count, 0) < 1 then
    raise exception 'Version phase 1 expected notice_count >= 1 (got %)', coalesce(v_notice_count, 0);
  end if;

  -- 5) Search never duplicates canonical_id.
  select count(*), count(distinct sr.tender_id)
  into v_row_count, v_distinct_count
  from public.search_tenders_ranked(v_tenant, 2000, null, null, null, null, null, null, null, null, null) sr;

  if v_row_count <> v_distinct_count then
    raise exception 'Search duplicate invariant failed (rows %, distinct canonical %)', v_row_count, v_distinct_count;
  end if;

  perform set_config('civant.e2e_version_token', v_version_token, false);
  perform set_config('civant.e2e_version_canonical_id', v_version_canonical_1, false);
  perform set_config('civant.e2e_version_last_seen_before', v_last_seen_before::text, false);

  raise notice 'PASS scenario: linked canonical_id=%', v_linked_canonical_1;
  raise notice 'PASS scenario: ted_only coverage';
  raise notice 'PASS scenario: national_only coverage';
  raise notice 'PASS invariant: no duplicate canonical_ids in search (rows=%)', v_row_count;
  raise notice 'PASS scenario: version phase-1 canonical_id=%', v_version_canonical_1;
end
$$;

-- Ensure a new transaction timestamp for version update comparison.
select pg_sleep(1.1);

-- Phase 2: second version insert + strict timestamp progression assertion.
do $$
declare
  v_tenant text := current_setting('civant.qa_tenant_id', true);
  v_tag text := current_setting('civant.e2e_tag', true);
  v_version_token text := current_setting('civant.e2e_version_token', true);
  v_version_canonical_1 text := current_setting('civant.e2e_version_canonical_id', true);
  v_version_canonical_2 text;
  v_last_seen_before timestamptz := current_setting('civant.e2e_version_last_seen_before', true)::timestamptz;
  v_last_seen_after timestamptz;
  v_notice_count integer;
begin
  if coalesce(v_tenant, '') = '' or coalesce(v_tag, '') = '' then
    raise exception 'missing required e2e session state';
  end if;
  if coalesce(v_version_token, '') = '' or coalesce(v_version_canonical_1, '') = '' then
    raise exception 'missing version scenario state from phase 1';
  end if;

  select u.canonical_id
  into v_version_canonical_2
  from public.upsert_notice_and_link(
    p_tenant_id => v_tenant,
    p_source => 'ETENDERS_IE',
    p_source_notice_id => 'E2E-VERSION-' || v_tag,
    p_source_url => 'https://example.test/version/' || v_tag,
    p_country => 'IE',
    p_title => 'E2E Versioned Opportunity ' || v_version_token,
    p_buyer_name_raw => 'E2E Version Buyer',
    p_cpv_codes => array['48000000'],
    p_publication_date => current_date - 1,
    p_deadline_date => current_date + 35,
    p_status => 'open',
    p_raw_payload => jsonb_build_object('scenario','version','version',2,'token',v_version_token,'budget',250),
    p_parsed_payload => jsonb_build_object('scenario','version','version',2,'token',v_version_token),
    p_notice_version_hash => null
  ) u;

  if v_version_canonical_1 is distinct from v_version_canonical_2 then
    raise exception 'Version update should keep same canonical_id (got % vs %)', v_version_canonical_1, v_version_canonical_2;
  end if;

  select count(*)
  into v_notice_count
  from public.notices n
  where n.tenant_id = v_tenant
    and n.source = 'ETENDERS_IE'
    and n.source_notice_id = 'E2E-VERSION-' || v_tag;

  if v_notice_count <> 2 then
    raise exception 'Version update expected 2 notice rows for same source_notice_id (got %)', v_notice_count;
  end if;

  select ct.last_seen_at
  into v_last_seen_after
  from public.canonical_tenders ct
  where ct.tenant_id = v_tenant
    and ct.canonical_id = v_version_canonical_1;

  if v_last_seen_before is null or v_last_seen_after is null then
    raise exception 'Version update expected non-null last_seen values (before %, after %)', v_last_seen_before, v_last_seen_after;
  end if;

  if v_last_seen_after <= v_last_seen_before then
    raise exception 'Version update expected last_seen_at to increase (before %, after %)', v_last_seen_before, v_last_seen_after;
  end if;

  raise notice 'PASS scenario: version update canonical_id=%', v_version_canonical_1;
end
$$;

\echo 'PASS: canonical E2E completed'
