-- Base operational schema for Civant MVP
-- Repeatable migration: creates core operational tables if they do not exist

create extension if not exists pgcrypto;

create table if not exists "ConnectorConfig" (
  id uuid primary key default gen_random_uuid(),
  connector_key text not null unique,
  enabled boolean not null default true,
  config jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists "ConnectorRuns" (
  id uuid primary key default gen_random_uuid(),
  connector_key text not null,
  status text not null,
  started_at timestamptz,
  finished_at timestamptz,
  metadata jsonb not null default '{}'::jsonb
);

create table if not exists "TendersCurrent" (
  tender_id text primary key,
  source text,
  published_at timestamptz,
  data jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists "TenderVersions" (
  id uuid primary key default gen_random_uuid(),
  tender_id text not null,
  version_hash text not null,
  data jsonb,
  created_at timestamptz not null default now()
);

-- Minimal useful indexes
create index if not exists idx_connector_runs_connector_key on "ConnectorRuns" (connector_key);
create index if not exists idx_connector_runs_status on "ConnectorRuns" (status);
create index if not exists idx_connector_runs_started_at on "ConnectorRuns" (started_at desc);

create index if not exists idx_tenders_current_source on "TendersCurrent" (source);
create index if not exists idx_tenders_current_published_at on "TendersCurrent" (published_at desc);

create index if not exists idx_tender_versions_tender_id on "TenderVersions" (tender_id);
create unique index if not exists uq_tender_versions_tender_hash on "TenderVersions" (tender_id, version_hash);
