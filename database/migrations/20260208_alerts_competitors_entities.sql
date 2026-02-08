-- Tenant-scoped entities for competitors and alerts workflows

create table if not exists "Competitors" (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null default 'civant_default',
  company_name text not null,
  country text,
  industry_sectors text,
  notes text,
  active boolean not null default true,
  tracked_by_user text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists "Alerts" (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null default 'civant_default',
  alert_name text not null,
  user_email text not null,
  country text,
  keywords text,
  buyer_contains text,
  cpv_contains text,
  deadline_within_days integer,
  notification_frequency text not null default 'immediate',
  expiry_date date,
  active boolean not null default true,
  last_checked_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists "AlertEvents" (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null default 'civant_default',
  alert_id uuid not null references "Alerts"(id) on delete cascade,
  tender_uid text,
  matched_at timestamptz not null default now(),
  sent boolean not null default false,
  sent_at timestamptz,
  created_at timestamptz not null default now(),
  unique (tenant_id, alert_id, tender_uid)
);

create index if not exists idx_competitors_tenant_id on "Competitors"(tenant_id);
create index if not exists idx_competitors_tracked_by_user on "Competitors"(tracked_by_user);
create index if not exists idx_alerts_tenant_id on "Alerts"(tenant_id);
create index if not exists idx_alerts_user_email on "Alerts"(user_email);
create index if not exists idx_alert_events_tenant_id on "AlertEvents"(tenant_id);
create index if not exists idx_alert_events_alert_id on "AlertEvents"(alert_id);
create index if not exists idx_alert_events_matched_at on "AlertEvents"(matched_at desc);
