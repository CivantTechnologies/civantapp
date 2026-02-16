-- Search zero-result telemetry (tenant-scoped).
-- Stores only hashed free-text fields to avoid persisting raw keyword/buyer input.

create extension if not exists pgcrypto;

create table if not exists public.search_zero_results_log (
  id uuid primary key default gen_random_uuid(),
  tenant_id text not null,
  user_id text null,
  search_engine text not null default 'unknown',
  latency_ms integer not null default 0,
  scanned_rows integer not null default 0,
  candidate_rows integer not null default 0,
  returned_rows integer not null default 0,
  query_hash text not null,
  filters_json jsonb not null default '{}'::jsonb,
  metadata_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists idx_search_zero_results_log_tenant_created_at_desc
  on public.search_zero_results_log(tenant_id, created_at desc);

create index if not exists idx_search_zero_results_log_tenant_query_hash_created_at_desc
  on public.search_zero_results_log(tenant_id, query_hash, created_at desc);

do $$
begin
  if to_regclass('public.search_zero_results_log') is not null then
    alter table public.search_zero_results_log enable row level security;
    alter table public.search_zero_results_log force row level security;

    if to_regprocedure('public.current_tenant_id()') is not null then
      drop policy if exists search_zero_results_log_tenant_select on public.search_zero_results_log;
      create policy search_zero_results_log_tenant_select
        on public.search_zero_results_log
        for select
        to authenticated
        using (tenant_id = public.current_tenant_id());

      drop policy if exists search_zero_results_log_tenant_insert on public.search_zero_results_log;
      create policy search_zero_results_log_tenant_insert
        on public.search_zero_results_log
        for insert
        to authenticated
        with check (tenant_id = public.current_tenant_id());
    end if;
  end if;
end $$;
