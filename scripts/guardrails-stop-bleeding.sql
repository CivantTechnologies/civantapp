-- Guardrails: Stop-the-bleeding search architecture
--
-- Run with:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f scripts/guardrails-stop-bleeding.sql
--
-- Optional:
--   -v guardrail_mode=canary   (runs only cheap checks: A, B, serving_rows>0)

\set QUIET 1
\set ON_ERROR_STOP on
\unset QUIET

-- -----------------------------
-- Config (safe to tweak)
-- -----------------------------
\if :{?guardrail_mode}
\else
\set guardrail_mode 'full'
\endif

\if :{?guardrail_notices_index_size_mb_limit}
\else
\set guardrail_notices_index_size_mb_limit 900
\endif

\if :{?guardrail_serving_ratio_tolerance}
\else
\set guardrail_serving_ratio_tolerance 0.05
\endif

-- Expose config to plpgsql via custom GUCs.
set civant.guardrail_mode = :'guardrail_mode';
set civant.guardrail_notices_index_size_mb_limit = :'guardrail_notices_index_size_mb_limit';
set civant.guardrail_serving_ratio_tolerance = :'guardrail_serving_ratio_tolerance';

-- Fail fast if anything runs away.
set statement_timeout = '2min';
set lock_timeout = '10s';
set idle_in_transaction_session_timeout = '30s';

-- -----------------------------
-- Tenant list (deterministic)
-- -----------------------------
create temporary table if not exists guardrail_tenants(
  tenant_id text primary key
);

truncate table guardrail_tenants;

do $$
begin
  if to_regclass('public.tenants') is not null then
    insert into guardrail_tenants(tenant_id)
    select lower(trim(id)) as tenant_id
    from public.tenants
    where coalesce(trim(id), '') <> '';
  else
    insert into guardrail_tenants(tenant_id) values ('civant_default');
  end if;
end $$;

do $$
begin
  if not exists (select 1 from guardrail_tenants) then
    raise exception 'guardrail: no tenants found (public.tenants missing/empty and no fallback applied)';
  end if;
end $$;

-- -----------------------------
-- Guardrail A: Heavy JSON GIN index must be absent
-- -----------------------------
\echo ::group::Guardrail A - Assert raw_payload GIN index is absent

do $$
declare
  v_bad_indexes text;
begin
  if to_regclass('public.idx_notices_raw_payload_gin') is not null then
    raise exception 'guardrail A failed: forbidden index exists: public.idx_notices_raw_payload_gin';
  end if;

  select string_agg(indexname || ' => ' || indexdef, E'\n' order by indexname)
  into v_bad_indexes
  from pg_indexes
  where schemaname = 'public'
    and tablename = 'notices'
    and indexdef ilike '%using gin%'
    and indexdef ilike '%raw_payload%';

  if v_bad_indexes is not null then
    raise exception 'guardrail A failed: forbidden GIN/raw_payload index detected on public.notices:%', E'\n' || v_bad_indexes;
  end if;
end $$;

\echo ::endgroup::

-- -----------------------------
-- Guardrail B/C (+E): Serving path + serving coverage (tenant-safe)
-- -----------------------------
create temporary table if not exists guardrail_health_summary(
  checked_at timestamptz not null,
  tenant_id text not null,
  notices_index_size_mb numeric,
  serving_rows bigint not null,
  canonical_rows bigint not null,
  serving_ratio numeric,
  engine_mode text,
  primary key (checked_at, tenant_id)
);

truncate table guardrail_health_summary;

\echo ::group::Guardrail B/C - Assert search uses serving path + serving table coverage

do $$
declare
  r record;
  v_serving_rows bigint;
  v_canonical_rows bigint;
  v_ratio numeric;
  v_engine text;
  v_has_engine_marker boolean;
  v_sample_rows integer;
  v_all_have_engine boolean;
  v_engine_min text;
  v_engine_max text;
  v_tolerance numeric;
  v_guardrail_mode text;
  v_checked_at timestamptz := now();
  v_is_canary boolean;
  v_serving_exists boolean;
begin
  v_tolerance := coalesce(nullif(current_setting('civant.guardrail_serving_ratio_tolerance', true), ''), '0.05')::numeric;
  v_guardrail_mode := coalesce(nullif(current_setting('civant.guardrail_mode', true), ''), 'full');
  if v_guardrail_mode is null or v_guardrail_mode = '' then
    v_guardrail_mode := 'full';
  end if;
  v_is_canary := (v_guardrail_mode = 'canary');
  if to_regclass('public.notices_search_current') is null then
    raise exception 'guardrail failed: missing table public.notices_search_current';
  end if;
  if to_regclass('public.canonical_tenders') is null then
    raise exception 'guardrail failed: missing table public.canonical_tenders';
  end if;
  if to_regprocedure('public.search_tenders_ranked(text, integer, text, text, text, text, text[], integer, text, text, integer)') is null then
    raise exception 'guardrail failed: missing function public.search_tenders_ranked(...)';
  end if;

  for r in (select tenant_id from guardrail_tenants order by tenant_id) loop
    if v_is_canary then
      select exists (
        select 1
        from public.notices_search_current nsc
        where nsc.tenant_id = r.tenant_id
        limit 1
      ) into v_serving_exists;

      if not v_serving_exists then
        raise exception 'guardrail C failed: serving rows are 0 for tenant=%', r.tenant_id;
      end if;

      -- Canary mode: keep it cheap; don't do full counts.
      v_serving_rows := -1;
      v_canonical_rows := -1;
      v_ratio := null;
    else
      select count(*) into v_serving_rows
      from public.notices_search_current nsc
      where nsc.tenant_id = r.tenant_id;

      select count(*) into v_canonical_rows
      from public.canonical_tenders ct
      where ct.tenant_id = r.tenant_id;
    end if;

    -- Engine marker must be present + must be "serving" (no fallback to legacy).
    -- Spec: call search_tenders_ranked(<tenant>, 20, ...) and validate engine markers.
    select
      count(*)::int as sample_rows,
      bool_and(t.data ? 'engine') as all_have_engine,
      min(nullif(t.data->>'engine', '')) as engine_min,
      max(nullif(t.data->>'engine', '')) as engine_max
    into v_sample_rows, v_all_have_engine, v_engine_min, v_engine_max
    from public.search_tenders_ranked(
      r.tenant_id,
      20,
      null, null, null, null,
      null, null, null, null, null
    ) as t;

    if v_sample_rows = 0 then
      raise exception 'guardrail B failed: search_tenders_ranked returned 0 rows for tenant=%', r.tenant_id;
    end if;
    if v_all_have_engine is distinct from true then
      raise exception 'guardrail B failed: search_tenders_ranked returned rows missing data.engine marker for tenant=%', r.tenant_id;
    end if;
    if v_is_canary or v_serving_rows > 0 then
      if v_engine_min is distinct from 'serving' or v_engine_max is distinct from 'serving' then
        raise exception 'guardrail B failed: search_tenders_ranked engine is not serving for tenant=% (engine_min=% engine_max=%)',
          r.tenant_id, coalesce(v_engine_min, '<null>'), coalesce(v_engine_max, '<null>');
      end if;
    else
      if v_engine_min is null or v_engine_max is null then
        raise exception 'guardrail B failed: search_tenders_ranked engine marker missing for tenant=% (engine_min=% engine_max=%)',
          r.tenant_id, coalesce(v_engine_min, '<null>'), coalesce(v_engine_max, '<null>');
      end if;
      if v_engine_min is distinct from v_engine_max then
        raise exception 'guardrail B failed: mixed engine markers returned for tenant=% (engine_min=% engine_max=%)',
          r.tenant_id, coalesce(v_engine_min, '<null>'), coalesce(v_engine_max, '<null>');
      end if;
      if v_engine_min not in ('legacy', 'serving') then
        raise exception 'guardrail B failed: unexpected engine marker for tenant=% (engine=%)',
          r.tenant_id, coalesce(v_engine_min, '<null>');
      end if;
    end if;

    v_has_engine_marker := true;
    v_engine := v_engine_min;

    -- Coverage ratio (skip in canary mode).
    if not v_is_canary and v_serving_rows > 0 then
      v_ratio := (v_serving_rows::numeric / nullif(v_canonical_rows::numeric, 0));
      if (abs(v_serving_rows - v_canonical_rows)::numeric / greatest(v_canonical_rows, 1)) > v_tolerance then
        raise exception 'guardrail C failed: serving coverage out of tolerance for tenant=% (serving=% canonical=% tolerance=% ratio=%)',
          r.tenant_id, v_serving_rows, v_canonical_rows, v_tolerance, coalesce(v_ratio, -1);
      end if;
    end if;

    insert into guardrail_health_summary(
      checked_at,
      tenant_id,
      notices_index_size_mb,
      serving_rows,
      canonical_rows,
      serving_ratio,
      engine_mode
    ) values (
      v_checked_at,
      r.tenant_id,
      null,
      v_serving_rows,
      v_canonical_rows,
      v_ratio,
      v_engine
    );
  end loop;
end $$;

\echo ::endgroup::

-- -----------------------------
-- Guardrail D: notices index bloat ceiling
-- -----------------------------
\echo ::group::Guardrail D - Assert public.notices index size stays under threshold

do $$
declare
  v_limit_mb numeric := coalesce(nullif(current_setting('civant.guardrail_notices_index_size_mb_limit', true), ''), '900')::numeric;
  v_bytes bigint;
  v_mb numeric;
  v_mode text := coalesce(nullif(current_setting('civant.guardrail_mode', true), ''), 'full');
begin
  if v_mode = 'canary' then
    raise notice 'guardrail D skipped in canary mode';
    return;
  end if;

  if to_regclass('public.notices') is null then
    raise exception 'guardrail D failed: missing table public.notices';
  end if;

  select pg_indexes_size('public.notices') into v_bytes;
  v_mb := round((v_bytes::numeric / 1024.0 / 1024.0)::numeric, 2);

  -- Visibility for logs.
  raise notice 'notices_index_size_bytes=% notices_index_size_mb=% (limit_mb=%)', v_bytes, v_mb, v_limit_mb;

  if v_mb > v_limit_mb then
    raise exception 'guardrail D failed: public.notices index size too large (mb=% limit_mb=% bytes=%)', v_mb, v_limit_mb, v_bytes;
  end if;

  update guardrail_health_summary
  set notices_index_size_mb = v_mb
  where checked_at = (select max(checked_at) from guardrail_health_summary);
end $$;

\echo ::endgroup::

-- -----------------------------
-- Guardrail E: Health summary row set (for logs)
-- -----------------------------
\echo ::group::Guardrail E - Health summary

select
  checked_at as "timestamp",
  tenant_id,
  notices_index_size_mb,
  serving_rows,
  canonical_rows,
  round(serving_ratio::numeric, 6) as serving_ratio,
  engine_mode
from guardrail_health_summary
order by tenant_id;

\echo ::endgroup::
