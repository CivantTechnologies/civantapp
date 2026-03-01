-- Migration: 20260301_multi_tenant_rls_complete_v1.sql
-- Purpose: Complete RLS audit — close every gap in multi-tenant row isolation.
--
-- Table classification
-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 1  User-mutable      Full CRUD for authenticated users within tenant.
--          Competitors, Alerts, user_profiles, users (own-row), ConnectorConfig
--
-- GROUP 2  Platform read-only  SELECT for tenant's authenticated users;
--          all writes are service_role / pipeline only.
--          TendersCurrent, TenderVersions, canonical_tenders, notices,
--          notices_search_current, canonical_notice_links, predictions,
--          predictions_current, prediction_drivers, prediction_scorecard,
--          entities, entity_aliases, buyer_aliases, buyer_category_stats,
--          external_signals_ie/fr/es, external_signal_rollup_ie/fr/es,
--          signals, market_signals, ConnectorRuns
--
-- GROUP 3  Pipeline-internal   No authenticated access.
--          service_role only (bypasses RLS). All existing policies dropped.
--          raw_documents, staging_records, prediction_runs, prediction_extensions,
--          prediction_inputs_snapshot, external_signal_raw, signal_mappings,
--          external_signal_to_cpv, payload_offload_failures, payload_offload_state
--
-- GROUP 4  Reference data      No tenant_id; all authenticated users may SELECT.
--          cpv_cluster, cpv_cluster_map
--
-- Already covered by prior migrations — NOT touched here:
--   company_profiles        (full CRUD, 20260225_company_profiles_predictions_rpc_hardening_v1)
--   mobile_push_subscriptions (full CRUD, 20260228183000_mobile_push_subscriptions_v1)
--   award_fact_ie / award_signals_ie / support_access_grants / support_access_audit
--                          (service_role lockdown, 20260225_db_security_lockdown_awards_support_access_v1)
--   tenants / user_roles / reconciliation_queue / tender_features_weekly /
--   ingestion_runs / AlertEvents
--                          (SELECT policies, 20260209_public_rls_hardening)
--
-- Policy design
-- ─────────────────────────────────────────────────────────────────────────────
-- SELECT  : current_tenant_id()  — reads tenant from JWT claims (fast, STABLE)
-- WRITE   : current_user_tenant_id() subquery — resolves tenant via users table
--           (DB-verified ownership; consistent with company_profiles pattern)
-- For Alerts INSERT/UPDATE the user_email column is additionally pinned to the
-- authenticated user's email via a users-table subquery (row-level user isolation
-- within a tenant).
--
-- Idempotency: all DROP POLICY IF EXISTS precede CREATE POLICY.
-- Safe to re-run.
-- ─────────────────────────────────────────────────────────────────────────────

begin;

-- ─────────────────────────────────────────────────────────────────────────────
-- Helper function: current_user_tenant_id()
-- Resolves the calling user's tenant_id directly from the users table rather
-- than trusting the JWT claim alone. Used in WRITE policy WITH CHECK / USING.
-- SECURITY DEFINER so the lookup bypasses users-table RLS (avoids chicken-and-
-- egg: the policy check itself must not be blocked by another policy).
-- ─────────────────────────────────────────────────────────────────────────────
create or replace function public.current_user_tenant_id()
  returns text
  language sql
  stable
  security definer
  set search_path = public
as $$
  select lower(u.tenant_id)
  from   public.users u
  where  u.id = auth.uid()::text
  limit  1;
$$;

revoke all    on function public.current_user_tenant_id() from public;
grant  execute on function public.current_user_tenant_id() to authenticated;
grant  execute on function public.current_user_tenant_id() to service_role;

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 1A: Competitors — full CRUD, tenant-scoped
-- ─────────────────────────────────────────────────────────────────────────────
alter table public."Competitors" enable row level security;
alter table public."Competitors" force  row level security;

revoke all    on table public."Competitors" from anon;
grant  select, insert, update, delete on table public."Competitors" to authenticated;
grant  all    on table public."Competitors" to service_role;

drop policy if exists competitors_tenant_select on public."Competitors";
create policy competitors_tenant_select
  on public."Competitors" for select to authenticated
  using (tenant_id = public.current_tenant_id());

drop policy if exists competitors_tenant_insert on public."Competitors";
create policy competitors_tenant_insert
  on public."Competitors" for insert to authenticated
  with check (tenant_id = public.current_user_tenant_id());

drop policy if exists competitors_tenant_update on public."Competitors";
create policy competitors_tenant_update
  on public."Competitors" for update to authenticated
  using  (tenant_id = public.current_user_tenant_id())
  with check (tenant_id = public.current_user_tenant_id());

drop policy if exists competitors_tenant_delete on public."Competitors";
create policy competitors_tenant_delete
  on public."Competitors" for delete to authenticated
  using (tenant_id = public.current_user_tenant_id());

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 1B: Alerts — full CRUD, tenant + user-email scoped
-- user_email is pinned to the caller's email on write (row-level user isolation)
-- ─────────────────────────────────────────────────────────────────────────────
alter table public."Alerts" enable row level security;
alter table public."Alerts" force  row level security;

revoke all    on table public."Alerts" from anon;
grant  select, insert, update, delete on table public."Alerts" to authenticated;
grant  all    on table public."Alerts" to service_role;

drop policy if exists alerts_tenant_select on public."Alerts";
create policy alerts_tenant_select
  on public."Alerts" for select to authenticated
  using (tenant_id = public.current_tenant_id());

drop policy if exists alerts_tenant_insert on public."Alerts";
create policy alerts_tenant_insert
  on public."Alerts" for insert to authenticated
  with check (
    tenant_id  = public.current_user_tenant_id()
    and user_email = (
      select u.email from public.users u
      where  u.id = auth.uid()::text limit 1
    )
  );

drop policy if exists alerts_tenant_update on public."Alerts";
create policy alerts_tenant_update
  on public."Alerts" for update to authenticated
  using (
    tenant_id  = public.current_user_tenant_id()
    and user_email = (
      select u.email from public.users u
      where  u.id = auth.uid()::text limit 1
    )
  )
  with check (
    tenant_id  = public.current_user_tenant_id()
    and user_email = (
      select u.email from public.users u
      where  u.id = auth.uid()::text limit 1
    )
  );

drop policy if exists alerts_tenant_delete on public."Alerts";
create policy alerts_tenant_delete
  on public."Alerts" for delete to authenticated
  using (
    tenant_id  = public.current_user_tenant_id()
    and user_email = (
      select u.email from public.users u
      where  u.id = auth.uid()::text limit 1
    )
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 1C: AlertEvents — SELECT only for tenant users
-- INSERT / UPDATE / DELETE is pipeline (service_role) only.
-- ─────────────────────────────────────────────────────────────────────────────
alter table public."AlertEvents" enable row level security;
alter table public."AlertEvents" force  row level security;

revoke all    on table public."AlertEvents" from anon;
grant  select on table public."AlertEvents" to authenticated;
grant  all    on table public."AlertEvents" to service_role;

drop policy if exists alertevents_tenant_select on public."AlertEvents";
create policy alertevents_tenant_select
  on public."AlertEvents" for select to authenticated
  using (tenant_id = public.current_tenant_id());

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 1D: user_profiles — full CRUD, user_id pinned to auth.uid()
-- ─────────────────────────────────────────────────────────────────────────────
alter table public.user_profiles enable row level security;
alter table public.user_profiles force  row level security;

revoke all    on table public.user_profiles from anon;
grant  select, insert, update, delete on table public.user_profiles to authenticated;
grant  all    on table public.user_profiles to service_role;

drop policy if exists user_profiles_tenant_select on public.user_profiles;
create policy user_profiles_tenant_select
  on public.user_profiles for select to authenticated
  using (tenant_id = public.current_tenant_id());

drop policy if exists user_profiles_own_insert on public.user_profiles;
create policy user_profiles_own_insert
  on public.user_profiles for insert to authenticated
  with check (
    user_id   = auth.uid()::text
    and tenant_id = public.current_user_tenant_id()
  );

drop policy if exists user_profiles_own_update on public.user_profiles;
create policy user_profiles_own_update
  on public.user_profiles for update to authenticated
  using  (user_id = auth.uid()::text)
  with check (
    user_id   = auth.uid()::text
    and tenant_id = public.current_user_tenant_id()
  );

drop policy if exists user_profiles_own_delete on public.user_profiles;
create policy user_profiles_own_delete
  on public.user_profiles for delete to authenticated
  using (user_id = auth.uid()::text);

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 1E: users — own-record UPDATE only
-- INSERT / DELETE are service_role controlled (auth bootstrap).
-- SELECT already covered by 20260209; recreated here for completeness.
-- ─────────────────────────────────────────────────────────────────────────────
alter table public.users enable row level security;
alter table public.users force  row level security;

revoke all     on table public.users from anon;
grant  select, update on table public.users to authenticated;
grant  all    on table public.users to service_role;

drop policy if exists users_tenant_select on public.users;
create policy users_tenant_select
  on public.users for select to authenticated
  using (tenant_id = public.current_tenant_id());

drop policy if exists users_own_update on public.users;
create policy users_own_update
  on public.users for update to authenticated
  using  (id = auth.uid()::text)
  with check (
    id        = auth.uid()::text
    and tenant_id = public.current_user_tenant_id()
  );

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 1F: ConnectorConfig — full CRUD, tenant-scoped (conditional: may not exist)
-- ─────────────────────────────────────────────────────────────────────────────
do $$ begin
  if to_regclass('public."ConnectorConfig"') is not null then
    alter table public."ConnectorConfig" enable row level security;
    alter table public."ConnectorConfig" force  row level security;

    execute 'revoke all on table public."ConnectorConfig" from anon';
    execute 'grant select, insert, update, delete on table public."ConnectorConfig" to authenticated';
    execute 'grant all on table public."ConnectorConfig" to service_role';

    execute 'drop policy if exists connectorconfig_tenant_select on public."ConnectorConfig"';
    execute $p$
      create policy connectorconfig_tenant_select
        on public."ConnectorConfig" for select to authenticated
        using (tenant_id = public.current_tenant_id())
    $p$;

    execute 'drop policy if exists connectorconfig_tenant_insert on public."ConnectorConfig"';
    execute $p$
      create policy connectorconfig_tenant_insert
        on public."ConnectorConfig" for insert to authenticated
        with check (tenant_id = public.current_user_tenant_id())
    $p$;

    execute 'drop policy if exists connectorconfig_tenant_update on public."ConnectorConfig"';
    execute $p$
      create policy connectorconfig_tenant_update
        on public."ConnectorConfig" for update to authenticated
        using  (tenant_id = public.current_user_tenant_id())
        with check (tenant_id = public.current_user_tenant_id())
    $p$;

    execute 'drop policy if exists connectorconfig_tenant_delete on public."ConnectorConfig"';
    execute $p$
      create policy connectorconfig_tenant_delete
        on public."ConnectorConfig" for delete to authenticated
        using (tenant_id = public.current_user_tenant_id())
    $p$;
  end if;
end $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 2: Platform read-only tables
-- Enable RLS, drop any permissive policies, add tenant-scoped SELECT only.
-- The loop skips tables that do not exist (idempotent).
-- ─────────────────────────────────────────────────────────────────────────────
do $$
declare
  tbl       text;
  pol       record;
  tbl_list  text[] := array[
    'TendersCurrent',
    'TenderVersions',
    'canonical_tenders',
    'notices',
    'notices_search_current',
    'canonical_notice_links',
    'predictions',
    'predictions_current',
    'prediction_drivers',
    'prediction_scorecard',
    'entities',
    'entity_aliases',
    'external_signals_ie',
    'external_signals_fr',
    'external_signals_es',
    'external_signal_rollup_ie',
    'external_signal_rollup_fr',
    'external_signal_rollup_es',
    'signals',
    'market_signals',
    'ConnectorRuns'
  ];
begin
  foreach tbl in array tbl_list loop
    if to_regclass(format('public.%I', tbl)) is not null then
      execute format('alter table public.%I enable row level security', tbl);
      execute format('alter table public.%I force  row level security', tbl);
      execute format('revoke all   on table public.%I from anon', tbl);
      execute format('grant  select on table public.%I to authenticated', tbl);
      execute format('grant  all    on table public.%I to service_role', tbl);

      -- Drop every existing policy before applying the canonical one
      for pol in
        select policyname
        from   pg_policies
        where  schemaname = 'public' and tablename = tbl
      loop
        execute format('drop policy if exists %I on public.%I', pol.policyname, tbl);
      end loop;

      execute format($f$
        create policy %I
          on public.%I for select to authenticated
          using (tenant_id = public.current_tenant_id())
      $f$, tbl || '_tenant_select', tbl);
    end if;
  end loop;
end $$;

-- buyer_aliases and buyer_category_stats handled separately because they may or
-- may not have a tenant_id column depending on migration history.
do $$
declare
  tbl text;
  pol record;
begin
  foreach tbl in array array['buyer_aliases', 'buyer_category_stats'] loop
    if to_regclass(format('public.%I', tbl)) is null then
      continue;
    end if;

    -- Only apply tenant-scoped policy if column exists
    if not exists (
      select 1 from information_schema.columns
      where  table_schema = 'public'
        and  table_name   = tbl
        and  column_name  = 'tenant_id'
    ) then
      continue;
    end if;

    execute format('alter table public.%I enable row level security', tbl);
    execute format('alter table public.%I force  row level security', tbl);
    execute format('revoke all    on table public.%I from anon', tbl);
    execute format('grant  select on table public.%I to authenticated', tbl);
    execute format('grant  all    on table public.%I to service_role', tbl);

    for pol in
      select policyname
      from   pg_policies
      where  schemaname = 'public' and tablename = tbl
    loop
      execute format('drop policy if exists %I on public.%I', pol.policyname, tbl);
    end loop;

    execute format($f$
      create policy %I
        on public.%I for select to authenticated
        using (tenant_id = public.current_tenant_id())
    $f$, tbl || '_tenant_select', tbl);
  end loop;
end $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 3: Pipeline-internal tables — service_role ONLY
-- Revoke all direct access; no policies needed (service_role bypasses RLS).
-- ─────────────────────────────────────────────────────────────────────────────
do $$
declare
  tbl      text;
  pol      record;
  tbl_list text[] := array[
    'raw_documents',
    'staging_records',
    'prediction_extensions',
    'prediction_inputs_snapshot',
    'external_signal_raw',
    'signal_mappings',
    'external_signal_to_cpv',
    'payload_offload_failures',
    'payload_offload_state'
  ];
begin
  foreach tbl in array tbl_list loop
    if to_regclass(format('public.%I', tbl)) is not null then
      execute format('alter table public.%I enable row level security', tbl);
      execute format('alter table public.%I force  row level security', tbl);
      execute format('revoke all on table public.%I from anon',          tbl);
      execute format('revoke all on table public.%I from authenticated', tbl);
      execute format('grant  all on table public.%I to service_role',    tbl);

      for pol in
        select policyname
        from   pg_policies
        where  schemaname = 'public' and tablename = tbl
      loop
        execute format('drop policy if exists %I on public.%I', pol.policyname, tbl);
      end loop;
      -- No policies: authenticated role has no grant, so RLS is moot,
      -- but enabling + forcing RLS provides defence in depth.
    end if;
  end loop;
end $$;

-- prediction_runs and ingestion_runs: monitoring-visible SELECT only
do $$
declare
  tbl  text;
  pol  record;
begin
  foreach tbl in array array['prediction_runs'] loop
    -- ingestion_runs already covered by 20260209; only prediction_runs is new
    if to_regclass(format('public.%I', tbl)) is not null then
      execute format('alter table public.%I enable row level security', tbl);
      execute format('alter table public.%I force  row level security', tbl);
      execute format('revoke all    on table public.%I from anon', tbl);
      execute format('grant  select on table public.%I to authenticated', tbl);
      execute format('grant  all    on table public.%I to service_role', tbl);

      for pol in
        select policyname
        from   pg_policies
        where  schemaname = 'public' and tablename = tbl
      loop
        execute format('drop policy if exists %I on public.%I', pol.policyname, tbl);
      end loop;

      execute format($f$
        create policy %I
          on public.%I for select to authenticated
          using (tenant_id = public.current_tenant_id())
      $f$, tbl || '_tenant_select', tbl);
    end if;
  end loop;
end $$;

-- ─────────────────────────────────────────────────────────────────────────────
-- GROUP 4: Shared reference tables — no tenant_id, all authenticated may SELECT
-- Do NOT force RLS (so service_role writes remain clean without policies needed).
-- ─────────────────────────────────────────────────────────────────────────────
do $$
declare
  tbl  text;
  pol  record;
begin
  foreach tbl in array array['cpv_cluster', 'cpv_cluster_map'] loop
    if to_regclass(format('public.%I', tbl)) is null then
      continue;
    end if;

    execute format('alter table public.%I enable row level security', tbl);
    -- NOTE: intentionally no FORCE here — service_role must write without a policy.
    execute format('revoke all    on table public.%I from anon', tbl);
    execute format('grant  select on table public.%I to authenticated', tbl);
    execute format('grant  all    on table public.%I to service_role', tbl);

    for pol in
      select policyname
      from   pg_policies
      where  schemaname = 'public' and tablename = tbl
    loop
      execute format('drop policy if exists %I on public.%I', pol.policyname, tbl);
    end loop;

    execute format($f$
      create policy %I
        on public.%I for select to authenticated
        using (true)
    $f$, tbl || '_authenticated_select', tbl);
  end loop;
end $$;

commit;

-- ─────────────────────────────────────────────────────────────────────────────
-- Verification queries (run after applying):
-- ─────────────────────────────────────────────────────────────────────────────
-- Check all tenant-scoped tables have RLS enabled:
--   select tablename, rowsecurity, forcedrowsecurity
--   from   pg_tables
--   where  schemaname = 'public'
--     and  tablename in (
--       'Competitors','Alerts','AlertEvents','user_profiles','users',
--       'TendersCurrent','predictions','entities','notices'
--     );
--
-- List all policies in effect:
--   select tablename, policyname, cmd, roles, qual, with_check
--   from   pg_policies
--   where  schemaname = 'public'
--   order  by tablename, cmd;
--
-- Check no anon grants remain on sensitive tables:
--   select grantee, table_name, privilege_type
--   from   information_schema.role_table_grants
--   where  table_schema = 'public'
--     and  grantee = 'anon'
--     and  table_name in ('Competitors','Alerts','users','predictions','TendersCurrent');
--
-- ─────────────────────────────────────────────────────────────────────────────
-- Rollback (manual, per table):
--   alter table public.<table> disable row level security;
--   drop policy if exists <policy_name> on public.<table>;
--   drop function if exists public.current_user_tenant_id();
-- ─────────────────────────────────────────────────────────────────────────────
