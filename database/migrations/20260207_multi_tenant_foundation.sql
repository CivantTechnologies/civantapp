-- Multi-tenant foundation

create table if not exists tenants (
  id text primary key,
  name text not null,
  regions text[] not null default '{}',
  plan text not null default 'starter',
  created_at timestamptz not null default now()
);

create table if not exists users (
  id text primary key,
  email text not null unique,
  tenant_id text not null references tenants(id) on delete cascade,
  created_at timestamptz not null default now()
);

create table if not exists user_roles (
  user_id text not null references users(id) on delete cascade,
  role text not null,
  primary key (user_id, role)
);

insert into tenants (id, name, regions, plan)
values ('civant_default', 'Civant Default Tenant', array['IE', 'FR'], 'starter')
on conflict (id) do nothing;

-- Tenant scoping for pipeline domain tables
alter table if exists raw_documents add column if not exists tenant_id text not null default 'civant_default';
alter table if exists ingestion_runs add column if not exists tenant_id text not null default 'civant_default';
alter table if exists staging_records add column if not exists tenant_id text not null default 'civant_default';
alter table if exists canonical_tenders add column if not exists tenant_id text not null default 'civant_default';
alter table if exists entities add column if not exists tenant_id text not null default 'civant_default';
alter table if exists entity_aliases add column if not exists tenant_id text not null default 'civant_default';
alter table if exists reconciliation_queue add column if not exists tenant_id text not null default 'civant_default';
alter table if exists tender_features_weekly add column if not exists tenant_id text not null default 'civant_default';
alter table if exists market_signals add column if not exists tenant_id text not null default 'civant_default';
alter table if exists predictions add column if not exists tenant_id text not null default 'civant_default';

create index if not exists idx_raw_documents_tenant_id on raw_documents(tenant_id);
create index if not exists idx_ingestion_runs_tenant_id on ingestion_runs(tenant_id);
create index if not exists idx_staging_records_tenant_id on staging_records(tenant_id);
create index if not exists idx_canonical_tenders_tenant_id on canonical_tenders(tenant_id);
create index if not exists idx_entities_tenant_id on entities(tenant_id);
create index if not exists idx_entity_aliases_tenant_id on entity_aliases(tenant_id);
create index if not exists idx_reconciliation_queue_tenant_id on reconciliation_queue(tenant_id);
create index if not exists idx_tender_features_weekly_tenant_id on tender_features_weekly(tenant_id);
create index if not exists idx_market_signals_tenant_id on market_signals(tenant_id);
create index if not exists idx_predictions_tenant_id on predictions(tenant_id);
