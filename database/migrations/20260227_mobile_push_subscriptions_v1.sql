-- Mobile push subscriptions (tenant-safe, additive).
-- Purpose:
--   Persist per-device Expo push tokens for authenticated Civant users.
--   Used by server-side dispatch jobs with strict tenant isolation.
--
-- Rollback plan:
--   1) drop table public.mobile_push_subscriptions;
--   2) drop associated policies automatically with table drop.

create extension if not exists pgcrypto;

create table if not exists public.mobile_push_subscriptions (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  user_id text not null,
  user_email text,
  device_id text not null,
  platform text not null check (platform in ('ios', 'android')),
  expo_push_token text,
  app_version text,
  enabled boolean not null default true,
  last_seen_at timestamptz not null default now(),
  last_notified_on date,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (tenant_id, user_id, device_id)
);

create index if not exists idx_mobile_push_subscriptions_tenant_enabled
  on public.mobile_push_subscriptions (tenant_id, enabled, updated_at desc);

create index if not exists idx_mobile_push_subscriptions_token
  on public.mobile_push_subscriptions (expo_push_token);

create index if not exists idx_mobile_push_subscriptions_last_seen
  on public.mobile_push_subscriptions (tenant_id, last_seen_at desc);

alter table public.mobile_push_subscriptions enable row level security;
alter table public.mobile_push_subscriptions force row level security;

drop policy if exists mobile_push_subscriptions_tenant_select on public.mobile_push_subscriptions;
create policy mobile_push_subscriptions_tenant_select
  on public.mobile_push_subscriptions
  for select
  to authenticated
  using (tenant_id = public.current_tenant_id());

drop policy if exists mobile_push_subscriptions_tenant_insert on public.mobile_push_subscriptions;
create policy mobile_push_subscriptions_tenant_insert
  on public.mobile_push_subscriptions
  for insert
  to authenticated
  with check (tenant_id = public.current_tenant_id());

drop policy if exists mobile_push_subscriptions_tenant_update on public.mobile_push_subscriptions;
create policy mobile_push_subscriptions_tenant_update
  on public.mobile_push_subscriptions
  for update
  to authenticated
  using (tenant_id = public.current_tenant_id())
  with check (tenant_id = public.current_tenant_id());

drop policy if exists mobile_push_subscriptions_tenant_delete on public.mobile_push_subscriptions;
create policy mobile_push_subscriptions_tenant_delete
  on public.mobile_push_subscriptions
  for delete
  to authenticated
  using (tenant_id = public.current_tenant_id());
