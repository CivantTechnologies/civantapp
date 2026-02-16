-- Canonical linking invariants hardening
-- Enforces tenant-safe link integrity and canonical status invariants.

create extension if not exists pg_trgm;

create unique index if not exists idx_notices_tenant_notice_id_unique
  on public.notices (tenant_id, notice_id);

create unique index if not exists idx_canonical_tenders_tenant_canonical_id_unique
  on public.canonical_tenders (tenant_id, canonical_id);

create index if not exists idx_canonical_notice_links_tenant_notice
  on public.canonical_notice_links (tenant_id, notice_id);

create index if not exists idx_canonical_notice_links_tenant_canonical_notice
  on public.canonical_notice_links (tenant_id, canonical_id, notice_id);

do $$
begin
  if exists (
    select 1
    from public.canonical_notice_links l
    join public.notices n on n.notice_id = l.notice_id
    where n.tenant_id <> l.tenant_id
  ) then
    raise exception 'canonical_notice_links has cross-tenant notice references';
  end if;

  if exists (
    select 1
    from public.canonical_notice_links l
    join public.canonical_tenders ct on ct.canonical_id = l.canonical_id
    where ct.tenant_id <> l.tenant_id
  ) then
    raise exception 'canonical_notice_links has cross-tenant canonical references';
  end if;
end
$$;

alter table public.canonical_notice_links
  drop constraint if exists fk_canonical_notice_links_tenant_notice;

alter table public.canonical_notice_links
  add constraint fk_canonical_notice_links_tenant_notice
  foreign key (tenant_id, notice_id)
  references public.notices (tenant_id, notice_id)
  on delete cascade;

alter table public.canonical_notice_links
  drop constraint if exists fk_canonical_notice_links_tenant_canonical;

alter table public.canonical_notice_links
  add constraint fk_canonical_notice_links_tenant_canonical
  foreign key (tenant_id, canonical_id)
  references public.canonical_tenders (tenant_id, canonical_id)
  on delete cascade;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_notice_links_match_score'
      and conrelid = 'public.canonical_notice_links'::regclass
  ) then
    alter table public.canonical_notice_links
      add constraint ck_canonical_notice_links_match_score
      check (match_score >= 0 and match_score <= 1.5);
  end if;
end
$$;

alter table public.canonical_tenders
  alter column notice_count set default 1;

update public.canonical_tenders
set notice_count = 1
where coalesce(notice_count, 0) <= 0;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_tenders_notice_count_positive'
      and conrelid = 'public.canonical_tenders'::regclass
  ) then
    alter table public.canonical_tenders
      add constraint ck_canonical_tenders_notice_count_positive
      check (notice_count > 0);
  end if;
end
$$;

create or replace function public.canonical_has_non_ted_source(p_sources text[])
returns boolean
language sql
immutable
as $$
  select exists (
    select 1
    from unnest(coalesce(p_sources, '{}'::text[])) as s
    where upper(trim(coalesce(s, ''))) <> 'TED'
      and trim(coalesce(s, '')) <> ''
  )
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_tenders_linked_source_requirements'
      and conrelid = 'public.canonical_tenders'::regclass
  ) then
    alter table public.canonical_tenders
      add constraint ck_canonical_tenders_linked_source_requirements
      check (
        coverage_status <> 'linked'
        or (
          coalesce(has_ted, false)
          and array_position(coalesce(verification_sources, '{}'::text[]), 'TED') is not null
          and public.canonical_has_non_ted_source(verification_sources)
        )
      );
  end if;
end
$$;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'ck_canonical_tenders_verified_requires_linked'
      and conrelid = 'public.canonical_tenders'::regclass
  ) then
    alter table public.canonical_tenders
      add constraint ck_canonical_tenders_verified_requires_linked
      check (verification_level <> 'verified' or coverage_status = 'linked');
  end if;
end
$$;

-- Re-sync canonical summary columns from existing links with invariant logic.
with link_stats as (
  select
    l.tenant_id,
    l.canonical_id,
    count(*)::int as link_count,
    count(*) filter (where n.source = 'TED')::int as ted_notice_count,
    count(*) filter (where n.source <> 'TED')::int as national_notice_count,
    bool_or(n.source = 'TED' and l.link_tier = 'hard') as has_hard_ted_link,
    max(case when n.source = 'TED' then coalesce(l.match_score, 0) else null end) as max_ted_match_score,
    coalesce(array_agg(distinct n.source) filter (where n.source is not null), '{}'::text[]) as verification_sources,
    coalesce(array_agg(distinct n.source_notice_id) filter (where n.source = 'TED'), '{}'::text[]) as ted_notice_ids
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  group by 1, 2
)
update public.canonical_tenders ct
set
  notice_count = greatest(1, ls.link_count),
  has_ted = ls.ted_notice_count > 0,
  verification_sources = ls.verification_sources,
  ted_notice_ids = ls.ted_notice_ids,
  coverage_status = case
    when ls.ted_notice_count > 0 and ls.national_notice_count > 0 then 'linked'
    when ls.ted_notice_count > 0 then 'ted_only'
    else 'national_only'
  end,
  verification_level = case
    when ls.ted_notice_count > 0 and ls.national_notice_count > 0 and (ls.has_hard_ted_link or coalesce(ls.max_ted_match_score, 0) >= 0.90)
      then 'verified'
    when ls.ted_notice_count > 0 and ls.national_notice_count > 0
      then 'partially_verified'
    else 'unverified'
  end,
  verification_state = case
    when ls.ted_notice_count > 0 and ls.national_notice_count > 0 and (ls.has_hard_ted_link or coalesce(ls.max_ted_match_score, 0) >= 0.90)
      then 'verified'
    when ls.ted_notice_count > 0 and ls.national_notice_count > 0
      then 'partially_verified'
    else 'unverified'
  end,
  updated_at = now()
from link_stats ls
where ct.tenant_id = ls.tenant_id
  and ct.canonical_id = ls.canonical_id;

create or replace function public.assert_canonical_tender_invariants(
  p_tenant_id text,
  p_canonical_id text
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_ct record;
  v_link_count integer := 0;
  v_ted_count integer := 0;
  v_national_count integer := 0;
  v_has_hard_ted boolean := false;
  v_max_ted_score numeric := 0;
begin
  select *
  into v_ct
  from public.canonical_tenders ct
  where ct.tenant_id = p_tenant_id
    and ct.canonical_id = p_canonical_id;

  if not found then
    return;
  end if;

  if coalesce(v_ct.notice_count, 0) <= 0 then
    raise exception 'Invariant #6 violated: canonical % has notice_count=%', p_canonical_id, coalesce(v_ct.notice_count, 0);
  end if;

  if exists (
    select 1
    from public.canonical_notice_links l
    join public.notices n on n.notice_id = l.notice_id
    where l.tenant_id = p_tenant_id
      and l.canonical_id = p_canonical_id
      and n.tenant_id <> l.tenant_id
  ) then
    raise exception 'Invariant #5 violated: cross-tenant notice link for canonical %', p_canonical_id;
  end if;

  select
    count(*)::int,
    count(*) filter (where n.source = 'TED')::int,
    count(*) filter (where n.source <> 'TED')::int,
    bool_or(n.source = 'TED' and l.link_tier = 'hard'),
    max(case when n.source = 'TED' then coalesce(l.match_score, 0) else null end)
  into
    v_link_count,
    v_ted_count,
    v_national_count,
    v_has_hard_ted,
    v_max_ted_score
  from public.canonical_notice_links l
  join public.notices n
    on n.notice_id = l.notice_id
   and n.tenant_id = l.tenant_id
  where l.tenant_id = p_tenant_id
    and l.canonical_id = p_canonical_id;

  if v_ct.coverage_status = 'linked' and not (coalesce(v_ted_count, 0) > 0 and coalesce(v_national_count, 0) > 0) then
    raise exception 'Invariant #3 violated: canonical % is linked without TED+national notices', p_canonical_id;
  end if;

  if v_ct.verification_level = 'verified'
     and not (
       v_ct.coverage_status = 'linked'
       and (coalesce(v_has_hard_ted, false) or coalesce(v_max_ted_score, 0) >= 0.90)
     ) then
    raise exception 'Invariant #4 violated: canonical % marked verified without strong TED evidence', p_canonical_id;
  end if;

  if v_link_count > 0 and v_ct.notice_count <> v_link_count then
    update public.canonical_tenders ct
    set notice_count = v_link_count,
        updated_at = now()
    where ct.tenant_id = p_tenant_id
      and ct.canonical_id = p_canonical_id;
  end if;
end;
$$;

create or replace function public.sync_canonical_after_link_change()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
begin
  if tg_op in ('INSERT', 'UPDATE') then
    perform public.refresh_canonical_tender_from_links(new.tenant_id, new.canonical_id, new.match_score);
    perform public.assert_canonical_tender_invariants(new.tenant_id, new.canonical_id);
  end if;

  if tg_op = 'UPDATE' and (old.tenant_id <> new.tenant_id or old.canonical_id <> new.canonical_id) then
    perform public.refresh_canonical_tender_from_links(old.tenant_id, old.canonical_id, null);
    perform public.assert_canonical_tender_invariants(old.tenant_id, old.canonical_id);
  end if;

  if tg_op = 'DELETE' then
    perform public.refresh_canonical_tender_from_links(old.tenant_id, old.canonical_id, null);
    perform public.assert_canonical_tender_invariants(old.tenant_id, old.canonical_id);
  end if;

  return coalesce(new, old);
end;
$$;

drop trigger if exists trg_sync_canonical_after_link_change on public.canonical_notice_links;
create trigger trg_sync_canonical_after_link_change
after insert or update or delete
on public.canonical_notice_links
for each row
execute function public.sync_canonical_after_link_change();

create or replace function public.enforce_canonical_tender_invariants_tg()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
  v_ted_count integer := 0;
  v_national_count integer := 0;
  v_has_hard_ted boolean := false;
  v_max_ted_score numeric := 0;
begin
  if coalesce(new.notice_count, 0) <= 0 then
    raise exception 'Invariant #6 violated: notice_count must be > 0 (canonical=%)', new.canonical_id;
  end if;

  if new.coverage_status = 'linked' or new.verification_level = 'verified' then
    select
      count(*) filter (where n.source = 'TED')::int,
      count(*) filter (where n.source <> 'TED')::int,
      bool_or(n.source = 'TED' and l.link_tier = 'hard'),
      max(case when n.source = 'TED' then coalesce(l.match_score, 0) else null end)
    into
      v_ted_count,
      v_national_count,
      v_has_hard_ted,
      v_max_ted_score
    from public.canonical_notice_links l
    join public.notices n
      on n.notice_id = l.notice_id
     and n.tenant_id = l.tenant_id
    where l.tenant_id = new.tenant_id
      and l.canonical_id = new.canonical_id;
  end if;

  if new.coverage_status = 'linked' and not (coalesce(v_ted_count, 0) > 0 and coalesce(v_national_count, 0) > 0) then
    raise exception 'Invariant #3 violated: linked canonical requires >=1 TED and >=1 non-TED notice (canonical=%)', new.canonical_id;
  end if;

  if new.verification_level = 'verified'
     and not (
       new.coverage_status = 'linked'
       and (coalesce(v_has_hard_ted, false) or coalesce(v_max_ted_score, 0) >= 0.90)
     ) then
    raise exception 'Invariant #4 violated: verified canonical requires linked + hard TED or TED score >= 0.90 (canonical=%)', new.canonical_id;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_enforce_canonical_tender_invariants on public.canonical_tenders;
create trigger trg_enforce_canonical_tender_invariants
before insert or update of tenant_id, canonical_id, coverage_status, verification_level, notice_count
on public.canonical_tenders
for each row
execute function public.enforce_canonical_tender_invariants_tg();
