-- Agentic pipeline core tables

create table if not exists raw_documents (
  id text primary key,
  run_id text not null,
  source text not null,
  source_url text,
  document_type text,
  external_id text,
  raw_text text,
  raw_json jsonb,
  fetched_at timestamptz not null,
  checksum text not null,
  created_at timestamptz not null default now()
);
create index if not exists idx_raw_documents_run_id on raw_documents(run_id);
create unique index if not exists idx_raw_documents_checksum on raw_documents(checksum);

create table if not exists ingestion_runs (
  run_id text primary key,
  source text not null,
  cursor text,
  status text not null,
  metrics jsonb not null default '{}'::jsonb,
  errors jsonb not null default '[]'::jsonb,
  started_at timestamptz not null default now(),
  finished_at timestamptz
);

create table if not exists staging_records (
  id text primary key,
  run_id text not null,
  external_id text not null,
  parsed_json jsonb,
  validation_status text not null,
  errors jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_staging_records_run_id on staging_records(run_id);

create table if not exists canonical_tenders (
  canonical_id text primary key,
  source text not null,
  external_id text not null,
  buyer_entity_id text,
  supplier_entity_id text,
  title text,
  category text,
  subcategory text,
  cpv_codes text[],
  publication_date date,
  deadline_date date,
  estimated_value numeric,
  currency text,
  source_url text,
  dedupe_quality numeric,
  normalized_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (source, external_id)
);

create table if not exists entities (
  entity_id text primary key,
  entity_type text not null,
  canonical_name text not null,
  country text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists entity_aliases (
  id text primary key,
  entity_id text not null,
  alias text not null,
  source text,
  confidence numeric,
  evidence jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  unique (entity_id, alias)
);
create index if not exists idx_entity_aliases_alias on entity_aliases(alias);

create table if not exists reconciliation_queue (
  id text primary key,
  run_id text,
  candidate_json jsonb not null,
  agent_output jsonb,
  status text not null,
  reviewed_by text,
  reviewed_at timestamptz,
  review_notes text,
  created_at timestamptz not null default now()
);
create index if not exists idx_reconciliation_queue_status on reconciliation_queue(status);

create table if not exists tender_features_weekly (
  id text primary key,
  week_start date not null,
  buyer_entity_id text,
  category text,
  cpv_family text,
  tender_count integer not null,
  avg_value numeric,
  median_days_between numeric,
  active_suppliers integer,
  created_at timestamptz not null default now(),
  unique (week_start, buyer_entity_id, category, cpv_family)
);

create table if not exists market_signals (
  id text primary key,
  signal_type text not null,
  entity_id text,
  source_url text,
  source_quality numeric not null,
  signal_strength numeric not null,
  start_date date,
  end_date date,
  evidence_snippet text,
  extracted_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_market_signals_entity on market_signals(entity_id);

create table if not exists predictions (
  id text primary key,
  buyer_id text,
  category text,
  cpv_family text,
  time_window text not null,
  probability numeric not null,
  confidence numeric not null,
  confidence_breakdown jsonb not null,
  top_drivers jsonb not null default '[]'::jsonb,
  evidence jsonb not null default '[]'::jsonb,
  model_version text,
  generated_at timestamptz not null default now(),
  unique (buyer_id, category, cpv_family, time_window, model_version)
);
