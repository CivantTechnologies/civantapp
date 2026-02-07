-- Audited, consent-based support access

create table if not exists support_access_grants (
  id text primary key,
  tenant_id text not null references tenants(id) on delete cascade,
  enabled boolean not null default false,
  expires_at timestamptz,
  enabled_by_user_id text,
  reason text,
  created_at timestamptz not null default now(),
  revoked_at timestamptz,
  revoked_by_user_id text,
  revoke_reason text
);

create index if not exists idx_support_access_grants_tenant_id on support_access_grants(tenant_id);
create index if not exists idx_support_access_grants_created_at on support_access_grants(created_at);

create table if not exists support_access_audit (
  id text primary key,
  tenant_id text not null references tenants(id) on delete cascade,
  actor_user_id text not null,
  actor_email text,
  action text not null,
  reason text,
  metadata_json jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_support_access_audit_tenant_id on support_access_audit(tenant_id);
create index if not exists idx_support_access_audit_created_at on support_access_audit(created_at);
