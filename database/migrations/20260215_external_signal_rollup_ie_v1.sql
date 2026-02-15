-- Civant: Ireland-only external signals rollup v1
-- Additive: creates a derived buyer-level rollup table + refresh function + RLS.
-- Does NOT modify canonical tables and does NOT change public.external_signals_ie schema.

begin;

-- =====================================================================================
-- Rollup Table: buyer-level features derived from public.external_signals_ie
-- =====================================================================================
create table if not exists public.external_signal_rollup_ie (
  tenant_id text not null,
  buyer_key text not null,
  buyer_id text null,
  buyer_name_normalized text null,

  hiring_last_date date null,
  hiring_count_30d int not null default 0,
  hiring_count_90d int not null default 0,
  hiring_max_strength_90d numeric(4,3) null,

  funding_last_date date null,
  funding_count_30d int not null default 0,
  funding_count_90d int not null default 0,
  funding_max_strength_90d numeric(4,3) null,

  combined_external_strength_90d numeric(5,3) not null default 0.000,

  -- Explainability payload:
  -- - counts used in rollup
  -- - top 3 evidence per signal_type (last 90d) with evidence_url + source_event_id
  explainability jsonb not null default '{}'::jsonb,

  refreshed_at timestamptz not null default now(),

  primary key (tenant_id, buyer_key)
);

-- Query indexes
create index if not exists idx_external_signal_rollup_ie_tenant_combined_strength_desc
  on public.external_signal_rollup_ie (tenant_id, combined_external_strength_90d desc);

create index if not exists idx_external_signal_rollup_ie_tenant_hiring_last_date_desc
  on public.external_signal_rollup_ie (tenant_id, hiring_last_date desc);

create index if not exists idx_external_signal_rollup_ie_tenant_funding_last_date_desc
  on public.external_signal_rollup_ie (tenant_id, funding_last_date desc);

-- =====================================================================================
-- Refresh Function: recompute rollup and upsert (idempotent + replay-safe)
-- =====================================================================================
create or replace function public.refresh_external_signal_rollup_ie(p_tenant_id text default null)
returns table (
  tenants_scoped int,
  inserted int,
  updated int,
  deleted int
)
language sql
as $$
  with
  tenant_scope as (
    select count(*)::int as tenants_scoped
    from (
      select distinct tenant_id
      from public.external_signals_ie
      where (p_tenant_id is null or tenant_id = p_tenant_id)
      union
      select distinct tenant_id
      from public.external_signal_rollup_ie
      where (p_tenant_id is null or tenant_id = p_tenant_id)
    ) t
  ),
  evidence_ranked as (
    select
      es.tenant_id,
      coalesce(es.buyer_id, es.buyer_name_normalized, 'UNKNOWN') as buyer_key,
      es.signal_type,
      es.signal_source,
      es.source_event_id,
      es.evidence_url,
      es.signal_date,
      es.signal_strength,
      row_number() over (
        partition by es.tenant_id, coalesce(es.buyer_id, es.buyer_name_normalized, 'UNKNOWN'), es.signal_type
        order by es.signal_strength desc, es.signal_date desc, es.captured_at desc
      ) as rn
    from public.external_signals_ie es
    where (p_tenant_id is null or es.tenant_id = p_tenant_id)
      and es.signal_date >= (current_date - interval '90 days')::date
  ),
  evidence_by_type as (
    select
      tenant_id,
      buyer_key,
      signal_type,
      jsonb_agg(
        jsonb_build_object(
          'source_event_id', source_event_id,
          'signal_source', signal_source,
          'evidence_url', evidence_url,
          'signal_date', signal_date,
          'signal_strength', signal_strength
        )
        order by signal_strength desc, signal_date desc
      ) as evidence
    from evidence_ranked
    where rn <= 3
    group by 1,2,3
  ),
  evidence_agg as (
    select
      tenant_id,
      buyer_key,
      jsonb_build_object(
        'hiring', coalesce(max(evidence) filter (where signal_type = 'hiring'), '[]'::jsonb),
        'funding', coalesce(max(evidence) filter (where signal_type = 'funding'), '[]'::jsonb)
      ) as evidence
    from evidence_by_type
    group by 1,2
  ),
  computed as (
    select
      es.tenant_id,
      coalesce(es.buyer_id, es.buyer_name_normalized, 'UNKNOWN') as buyer_key,
      max(es.buyer_id) filter (where es.buyer_id is not null) as buyer_id,
      max(es.buyer_name_normalized) filter (where es.buyer_name_normalized is not null) as buyer_name_normalized,

      max(es.signal_date) filter (where es.signal_type = 'hiring') as hiring_last_date,
      count(*) filter (
        where es.signal_type = 'hiring'
          and es.signal_date >= (current_date - interval '30 days')::date
      )::int as hiring_count_30d,
      count(*) filter (
        where es.signal_type = 'hiring'
          and es.signal_date >= (current_date - interval '90 days')::date
      )::int as hiring_count_90d,
      max(es.signal_strength) filter (
        where es.signal_type = 'hiring'
          and es.signal_date >= (current_date - interval '90 days')::date
      )::numeric(4,3) as hiring_max_strength_90d,

      max(es.signal_date) filter (where es.signal_type = 'funding') as funding_last_date,
      count(*) filter (
        where es.signal_type = 'funding'
          and es.signal_date >= (current_date - interval '30 days')::date
      )::int as funding_count_30d,
      count(*) filter (
        where es.signal_type = 'funding'
          and es.signal_date >= (current_date - interval '90 days')::date
      )::int as funding_count_90d,
      max(es.signal_strength) filter (
        where es.signal_type = 'funding'
          and es.signal_date >= (current_date - interval '90 days')::date
      )::numeric(4,3) as funding_max_strength_90d
    from public.external_signals_ie es
    where (p_tenant_id is null or es.tenant_id = p_tenant_id)
    group by 1,2
  ),
  finalized as (
    select
      c.tenant_id,
      c.buyer_key,
      c.buyer_id,
      c.buyer_name_normalized,

      c.hiring_last_date,
      c.hiring_count_30d,
      c.hiring_count_90d,
      c.hiring_max_strength_90d,

      c.funding_last_date,
      c.funding_count_30d,
      c.funding_count_90d,
      c.funding_max_strength_90d,

      round(
        (
          0.5 * coalesce(c.hiring_max_strength_90d, 0)::numeric
          + 0.5 * coalesce(c.funding_max_strength_90d, 0)::numeric
        ),
        3
      )::numeric(5,3) as combined_external_strength_90d,

      jsonb_build_object(
        'as_of_date', current_date,
        'counts', jsonb_build_object(
          'hiring_count_30d', c.hiring_count_30d,
          'hiring_count_90d', c.hiring_count_90d,
          'funding_count_30d', c.funding_count_30d,
          'funding_count_90d', c.funding_count_90d
        ),
        'evidence', coalesce(ea.evidence, jsonb_build_object('hiring','[]'::jsonb,'funding','[]'::jsonb))
      ) as explainability
    from computed c
    left join evidence_agg ea
      on ea.tenant_id = c.tenant_id
     and ea.buyer_key = c.buyer_key
  ),
  upserted as (
    insert into public.external_signal_rollup_ie (
      tenant_id,
      buyer_key,
      buyer_id,
      buyer_name_normalized,
      hiring_last_date,
      hiring_count_30d,
      hiring_count_90d,
      hiring_max_strength_90d,
      funding_last_date,
      funding_count_30d,
      funding_count_90d,
      funding_max_strength_90d,
      combined_external_strength_90d,
      explainability,
      refreshed_at
    )
    select
      tenant_id,
      buyer_key,
      buyer_id,
      buyer_name_normalized,
      hiring_last_date,
      hiring_count_30d,
      hiring_count_90d,
      hiring_max_strength_90d,
      funding_last_date,
      funding_count_30d,
      funding_count_90d,
      funding_max_strength_90d,
      combined_external_strength_90d,
      explainability,
      now()
    from finalized
    on conflict (tenant_id, buyer_key) do update set
      buyer_id = excluded.buyer_id,
      buyer_name_normalized = excluded.buyer_name_normalized,
      hiring_last_date = excluded.hiring_last_date,
      hiring_count_30d = excluded.hiring_count_30d,
      hiring_count_90d = excluded.hiring_count_90d,
      hiring_max_strength_90d = excluded.hiring_max_strength_90d,
      funding_last_date = excluded.funding_last_date,
      funding_count_30d = excluded.funding_count_30d,
      funding_count_90d = excluded.funding_count_90d,
      funding_max_strength_90d = excluded.funding_max_strength_90d,
      combined_external_strength_90d = excluded.combined_external_strength_90d,
      explainability = excluded.explainability,
      refreshed_at = excluded.refreshed_at
    where
      public.external_signal_rollup_ie.buyer_id is distinct from excluded.buyer_id
      or public.external_signal_rollup_ie.buyer_name_normalized is distinct from excluded.buyer_name_normalized
      or public.external_signal_rollup_ie.hiring_last_date is distinct from excluded.hiring_last_date
      or public.external_signal_rollup_ie.hiring_count_30d is distinct from excluded.hiring_count_30d
      or public.external_signal_rollup_ie.hiring_count_90d is distinct from excluded.hiring_count_90d
      or public.external_signal_rollup_ie.hiring_max_strength_90d is distinct from excluded.hiring_max_strength_90d
      or public.external_signal_rollup_ie.funding_last_date is distinct from excluded.funding_last_date
      or public.external_signal_rollup_ie.funding_count_30d is distinct from excluded.funding_count_30d
      or public.external_signal_rollup_ie.funding_count_90d is distinct from excluded.funding_count_90d
      or public.external_signal_rollup_ie.funding_max_strength_90d is distinct from excluded.funding_max_strength_90d
      or public.external_signal_rollup_ie.combined_external_strength_90d is distinct from excluded.combined_external_strength_90d
      or public.external_signal_rollup_ie.explainability is distinct from excluded.explainability
    returning (xmax = 0) as inserted_flag
  ),
  upsert_counts as (
    select
      count(*) filter (where inserted_flag)::int as inserted,
      count(*) filter (where not inserted_flag)::int as updated
    from upserted
  ),
  deleted_rows as (
    delete from public.external_signal_rollup_ie r
    where (p_tenant_id is null or r.tenant_id = p_tenant_id)
      and not exists (
        select 1
        from finalized f
        where f.tenant_id = r.tenant_id
          and f.buyer_key = r.buyer_key
      )
    returning 1
  ),
  delete_counts as (
    select count(*)::int as deleted from deleted_rows
  )
  select
    tenant_scope.tenants_scoped,
    upsert_counts.inserted,
    upsert_counts.updated,
    delete_counts.deleted
  from tenant_scope, upsert_counts, delete_counts;
$$;

-- =====================================================================================
-- RLS (tenant isolation)
-- =====================================================================================
alter table public.external_signal_rollup_ie enable row level security;
alter table public.external_signal_rollup_ie force row level security;

drop policy if exists external_signal_rollup_ie_tenant_select on public.external_signal_rollup_ie;
drop policy if exists external_signal_rollup_ie_tenant_insert on public.external_signal_rollup_ie;
drop policy if exists external_signal_rollup_ie_tenant_update on public.external_signal_rollup_ie;
drop policy if exists external_signal_rollup_ie_tenant_delete on public.external_signal_rollup_ie;

create policy external_signal_rollup_ie_tenant_select
on public.external_signal_rollup_ie
for select
to authenticated
using (tenant_id = public.civant_current_tenant_id());

create policy external_signal_rollup_ie_tenant_insert
on public.external_signal_rollup_ie
for insert
to authenticated
with check (tenant_id = public.civant_current_tenant_id());

create policy external_signal_rollup_ie_tenant_update
on public.external_signal_rollup_ie
for update
to authenticated
using (tenant_id = public.civant_current_tenant_id())
with check (tenant_id = public.civant_current_tenant_id());

create policy external_signal_rollup_ie_tenant_delete
on public.external_signal_rollup_ie
for delete
to authenticated
using (tenant_id = public.civant_current_tenant_id());

commit;
