-- Migration: 20260225_db_security_lockdown_awards_support_access_v1.sql
-- Purpose:
--   1) Remove broad anon/authenticated access from sensitive award/support tables.
--   2) Enforce RLS on tenant-scoped tables.
--   3) Allow only tenant-scoped authenticated read on award_fact_ie.
-- Notes:
--   - Additive and replay-safe.
--   - Canonical/support writes remain service-role controlled.

begin;

-- ---------------------------------------------------------------------------
-- Table privileges
-- ---------------------------------------------------------------------------
revoke all privileges on table public.award_fact_ie from anon;
revoke all privileges on table public.award_fact_ie from authenticated;
revoke all privileges on table public.award_signals_ie from anon;
revoke all privileges on table public.award_signals_ie from authenticated;
revoke all privileges on table public.support_access_grants from anon;
revoke all privileges on table public.support_access_grants from authenticated;
revoke all privileges on table public.support_access_audit from anon;
revoke all privileges on table public.support_access_audit from authenticated;

grant all privileges on table public.award_fact_ie to service_role;
grant all privileges on table public.award_signals_ie to service_role;
grant all privileges on table public.support_access_grants to service_role;
grant all privileges on table public.support_access_audit to service_role;

grant select on table public.award_fact_ie to authenticated;

-- ---------------------------------------------------------------------------
-- Row-level security
-- ---------------------------------------------------------------------------
alter table public.award_fact_ie enable row level security;
alter table public.award_fact_ie force row level security;
alter table public.award_signals_ie enable row level security;
alter table public.award_signals_ie force row level security;
alter table public.support_access_grants enable row level security;
alter table public.support_access_grants force row level security;
alter table public.support_access_audit enable row level security;
alter table public.support_access_audit force row level security;

-- Authenticated users may read only their tenant rows from award_fact_ie.
drop policy if exists award_fact_ie_tenant_select on public.award_fact_ie;
create policy award_fact_ie_tenant_select
on public.award_fact_ie
for select
to authenticated
using (
  tenant_id = (
    select u.tenant_id
    from public.users u
    where u.id = auth.uid()::text
    limit 1
  )
);

-- Explicitly remove any accidental direct access policies on support tables.
drop policy if exists support_access_grants_tenant_select on public.support_access_grants;
drop policy if exists support_access_grants_tenant_insert on public.support_access_grants;
drop policy if exists support_access_grants_tenant_update on public.support_access_grants;
drop policy if exists support_access_grants_tenant_delete on public.support_access_grants;
drop policy if exists support_access_audit_tenant_select on public.support_access_audit;
drop policy if exists support_access_audit_tenant_insert on public.support_access_audit;
drop policy if exists support_access_audit_tenant_update on public.support_access_audit;
drop policy if exists support_access_audit_tenant_delete on public.support_access_audit;
drop policy if exists award_signals_ie_select on public.award_signals_ie;
drop policy if exists award_signals_ie_insert on public.award_signals_ie;
drop policy if exists award_signals_ie_update on public.award_signals_ie;
drop policy if exists award_signals_ie_delete on public.award_signals_ie;

commit;
