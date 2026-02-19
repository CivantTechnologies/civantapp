alter table if exists support_access_grants
  add column if not exists support_user_id text;

create index if not exists idx_support_access_grants_tenant_user_enabled
  on support_access_grants(tenant_id, support_user_id, enabled);
