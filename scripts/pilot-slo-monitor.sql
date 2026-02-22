-- Pilot SLO monitor
--
-- Usage:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f scripts/pilot-slo-monitor.sql

\set QUIET 1
\set ON_ERROR_STOP on
\unset QUIET

\if :{?national_connector_max_stale_minutes}
\else
\set national_connector_max_stale_minutes 180
\endif

\if :{?ted_connector_max_stale_minutes}
\else
\set ted_connector_max_stale_minutes 1560
\endif

\if :{?connector_failed_runs_24h_limit}
\else
\set connector_failed_runs_24h_limit 10
\endif

\if :{?predictive_signals_max_stale_minutes}
\else
\set predictive_signals_max_stale_minutes 45
\endif

\if :{?predictive_incremental_max_stale_minutes}
\else
\set predictive_incremental_max_stale_minutes 180
\endif

\if :{?predictive_full_max_stale_minutes}
\else
\set predictive_full_max_stale_minutes 2160
\endif

\if :{?search_rpc_max_ms}
\else
\set search_rpc_max_ms 7000
\endif

set statement_timeout = '90s';
set lock_timeout = '5s';
set civant.national_connector_max_stale_minutes = :'national_connector_max_stale_minutes';
set civant.ted_connector_max_stale_minutes = :'ted_connector_max_stale_minutes';
set civant.connector_failed_runs_24h_limit = :'connector_failed_runs_24h_limit';
set civant.predictive_signals_max_stale_minutes = :'predictive_signals_max_stale_minutes';
set civant.predictive_incremental_max_stale_minutes = :'predictive_incremental_max_stale_minutes';
set civant.predictive_full_max_stale_minutes = :'predictive_full_max_stale_minutes';
set civant.search_rpc_max_ms = :'search_rpc_max_ms';

create temporary table if not exists pilot_tenants(
  tenant_id text primary key
);

truncate table pilot_tenants;

do $$
begin
  if to_regclass('public.tenants') is not null then
    insert into pilot_tenants(tenant_id)
    select lower(trim(id))
    from public.tenants
    where coalesce(trim(id), '') <> '';
  end if;

  if not exists (select 1 from pilot_tenants) then
    insert into pilot_tenants(tenant_id) values ('civant_default');
  end if;
end $$;

create temporary table if not exists pilot_slo_summary(
  checked_at timestamptz not null,
  tenant_id text not null,
  check_name text not null,
  metric_value numeric,
  threshold numeric,
  details text,
  primary key (checked_at, tenant_id, check_name)
);

truncate table pilot_slo_summary;

\echo ::group::Connector freshness and failures

do $$
declare
  t record;
  c record;
  v_checked_at timestamptz := now();
  v_connector_key text;
  v_last_success timestamptz;
  v_failed_24h integer;
  v_age_minutes numeric;
  v_failed_limit integer := current_setting('civant.connector_failed_runs_24h_limit', true)::integer;
  v_national_max numeric := current_setting('civant.national_connector_max_stale_minutes', true)::numeric;
  v_ted_max numeric := current_setting('civant.ted_connector_max_stale_minutes', true)::numeric;
begin
  for t in (select tenant_id from pilot_tenants order by tenant_id) loop
    for c in (
      select *
      from (
        values
          ('boamp_fr_incremental', v_national_max),
          ('etenders_ie_incremental', v_national_max),
          ('placsp_es_incremental', v_national_max),
          ('ted_incremental', v_ted_max)
      ) as source(prefix, stale_minutes_limit)
    ) loop
      v_connector_key := c.prefix || ':' || t.tenant_id;

      select
        max(finished_at) filter (where status = 'success'),
        count(*) filter (where status = 'failed' and started_at >= now() - interval '24 hours')
      into v_last_success, v_failed_24h
      from public."ConnectorRuns"
      where tenant_id = t.tenant_id
        and connector_key = v_connector_key;

      if v_last_success is null then
        raise exception 'pilot-slo failed: connector % has no successful run for tenant %', v_connector_key, t.tenant_id;
      end if;

      v_age_minutes := extract(epoch from (now() - v_last_success)) / 60.0;

      if v_age_minutes > c.stale_minutes_limit then
        raise exception 'pilot-slo failed: connector % stale for tenant % (age_minutes=%, limit=%)',
          v_connector_key, t.tenant_id, round(v_age_minutes, 2), c.stale_minutes_limit;
      end if;

      if coalesce(v_failed_24h, 0) > v_failed_limit then
        raise exception 'pilot-slo failed: connector % has too many failures in 24h for tenant % (failures=%, limit=%)',
          v_connector_key, t.tenant_id, v_failed_24h, v_failed_limit;
      end if;

      insert into pilot_slo_summary(checked_at, tenant_id, check_name, metric_value, threshold, details)
      values (
        v_checked_at,
        t.tenant_id,
        'connector:' || c.prefix,
        round(v_age_minutes, 2),
        c.stale_minutes_limit,
        'failed_24h=' || coalesce(v_failed_24h, 0)::text
      );
    end loop;
  end loop;
end $$;

\echo ::endgroup::

\echo ::group::Predictive engine freshness

do $$
declare
  t record;
  r record;
  v_checked_at timestamptz := now();
  v_last_success timestamptz;
  v_age_minutes numeric;
  v_signals_limit numeric := current_setting('civant.predictive_signals_max_stale_minutes', true)::numeric;
  v_incremental_limit numeric := current_setting('civant.predictive_incremental_max_stale_minutes', true)::numeric;
  v_full_limit numeric := current_setting('civant.predictive_full_max_stale_minutes', true)::numeric;
begin
  for t in (select tenant_id from pilot_tenants order by tenant_id) loop
    for r in (
      select *
      from (
        values
          ('signals_only', v_signals_limit),
          ('incremental', v_incremental_limit),
          ('full', v_full_limit)
      ) as source(run_type, stale_minutes_limit)
    ) loop
      select max(finished_at)
      into v_last_success
      from public.prediction_runs
      where tenant_id = t.tenant_id
        and run_type = r.run_type
        and status = 'success';

      if v_last_success is null then
        raise exception 'pilot-slo failed: prediction run_type % has no successful run for tenant %', r.run_type, t.tenant_id;
      end if;

      v_age_minutes := extract(epoch from (now() - v_last_success)) / 60.0;
      if v_age_minutes > r.stale_minutes_limit then
        raise exception 'pilot-slo failed: prediction run_type % stale for tenant % (age_minutes=%, limit=%)',
          r.run_type, t.tenant_id, round(v_age_minutes, 2), r.stale_minutes_limit;
      end if;

      insert into pilot_slo_summary(checked_at, tenant_id, check_name, metric_value, threshold, details)
      values (
        v_checked_at,
        t.tenant_id,
        'prediction:' || r.run_type,
        round(v_age_minutes, 2),
        r.stale_minutes_limit,
        'last_success=' || v_last_success::text
      );
    end loop;
  end loop;
end $$;

\echo ::endgroup::

\echo ::group::Search serving path latency

do $$
declare
  t record;
  v_checked_at timestamptz := now();
  v_started timestamptz;
  v_elapsed_ms numeric;
  v_rows integer;
  v_search_max_ms numeric := current_setting('civant.search_rpc_max_ms', true)::numeric;
begin
  if to_regprocedure('public.search_tenders_ranked(text, integer, text, text, text, text, text[], integer, text, text, integer)') is null then
    raise exception 'pilot-slo failed: missing function public.search_tenders_ranked(...)';
  end if;

  for t in (select tenant_id from pilot_tenants order by tenant_id) loop
    v_started := clock_timestamp();

    select count(*)::int
    into v_rows
    from public.search_tenders_ranked(
      t.tenant_id,
      20,
      null, null, null, null,
      null, null, null, null, null
    );

    v_elapsed_ms := extract(epoch from (clock_timestamp() - v_started)) * 1000.0;

    if v_rows <= 0 then
      raise exception 'pilot-slo failed: search_tenders_ranked returned 0 rows for tenant %', t.tenant_id;
    end if;

    if v_elapsed_ms > v_search_max_ms then
      raise exception 'pilot-slo failed: search_tenders_ranked latency too high for tenant % (elapsed_ms=%, limit_ms=%)',
        t.tenant_id, round(v_elapsed_ms, 2), v_search_max_ms;
    end if;

    insert into pilot_slo_summary(checked_at, tenant_id, check_name, metric_value, threshold, details)
    values (
      v_checked_at,
      t.tenant_id,
      'search:search_tenders_ranked_ms',
      round(v_elapsed_ms, 2),
      v_search_max_ms,
      'rows=' || v_rows::text
    );
  end loop;
end $$;

\echo ::endgroup::

\echo ::group::Pilot SLO summary

select
  checked_at as "timestamp",
  tenant_id,
  check_name,
  metric_value,
  threshold,
  details
from pilot_slo_summary
order by tenant_id, check_name;

\echo ::endgroup::
