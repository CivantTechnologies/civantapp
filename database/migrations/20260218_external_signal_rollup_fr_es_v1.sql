-- Civant: External signals rollup v1 (France + Spain)
-- Additive: buyer-level rollups mirroring IE schema.

begin;

-- -----------------------------------------------------------------------------
-- FR rollup table + refresh function
-- -----------------------------------------------------------------------------
create table if not exists public.external_signal_rollup_fr (
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

  explainability jsonb not null default '{}'::jsonb,
  refreshed_at timestamptz not null default now(),

  primary key (tenant_id, buyer_key)
);

create index if not exists idx_external_signal_rollup_fr_strength
  on public.external_signal_rollup_fr (tenant_id, combined_external_strength_90d desc);

create or replace function public.refresh_external_signal_rollup_fr(p_tenant_id text default null)
returns table (
  tenants_scoped int,
  inserted int,
  updated int,
  deleted int
)
language sql
as $$
with scoped as (
  select *
  from public.external_signals_fr
  where p_tenant_id is null or tenant_id = p_tenant_id
),
evidence_ranked as (
  select *,
         row_number() over (
           partition by tenant_id,
                        coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN'),
                        signal_type
           order by signal_strength desc, signal_date desc
         ) rn
  from scoped
  where signal_date >= current_date - interval '90 days'
),
evidence_top as (
  select
    tenant_id,
    coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN') buyer_key,
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
    ) evidence
  from evidence_ranked
  where rn <= 3
  group by tenant_id, buyer_key, signal_type
),
evidence_agg as (
  select
    tenant_id,
    buyer_key,
    jsonb_build_object(
      'hiring', coalesce((jsonb_agg(evidence) filter (where signal_type='hiring'))->0, '[]'::jsonb),
      'funding', coalesce((jsonb_agg(evidence) filter (where signal_type='funding'))->0, '[]'::jsonb)
    ) evidence
  from evidence_top
  group by tenant_id, buyer_key
),
computed as (
  select
    tenant_id,
    coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN') buyer_key,
    max(buyer_id) as buyer_id,
    max(buyer_name_normalized) as buyer_name_normalized,

    max(signal_date) filter (where signal_type='hiring') hiring_last_date,
    count(*) filter (where signal_type='hiring' and signal_date >= current_date - interval '30 days') hiring_count_30d,
    count(*) filter (where signal_type='hiring' and signal_date >= current_date - interval '90 days') hiring_count_90d,
    max(signal_strength) filter (where signal_type='hiring' and signal_date >= current_date - interval '90 days') hiring_max_strength_90d,

    max(signal_date) filter (where signal_type='funding') funding_last_date,
    count(*) filter (where signal_type='funding' and signal_date >= current_date - interval '30 days') funding_count_30d,
    count(*) filter (where signal_type='funding' and signal_date >= current_date - interval '90 days') funding_count_90d,
    max(signal_strength) filter (where signal_type='funding' and signal_date >= current_date - interval '90 days') funding_max_strength_90d
  from scoped
  group by tenant_id, buyer_key
),
finalized as (
  select
    c.*, 
    round((coalesce(hiring_max_strength_90d,0) + coalesce(funding_max_strength_90d,0)) / 2, 3) combined_external_strength_90d,
    jsonb_build_object('as_of_date', current_date, 'evidence', coalesce(e.evidence, '{}'::jsonb)) explainability
  from computed c
  left join evidence_agg e
    on e.tenant_id = c.tenant_id
   and e.buyer_key = c.buyer_key
),
upserted as (
  insert into public.external_signal_rollup_fr
  select *, now()
  from finalized
  on conflict (tenant_id, buyer_key) do update
  set
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
    refreshed_at = now()
  returning xmax = 0 as inserted_flag
),
counts as (
  select
    count(*) filter (where inserted_flag) inserted,
    count(*) filter (where not inserted_flag) updated
  from upserted
)
select
  (select count(distinct tenant_id) from scoped),
  inserted,
  updated,
  0
from counts;
$$;

alter table public.external_signal_rollup_fr enable row level security;
alter table public.external_signal_rollup_fr force row level security;

drop policy if exists external_signal_rollup_fr_select on public.external_signal_rollup_fr;
create policy external_signal_rollup_fr_select
on public.external_signal_rollup_fr
for select
to authenticated
using (tenant_id = public.civant_current_tenant_id());

-- -----------------------------------------------------------------------------
-- ES rollup table + refresh function
-- -----------------------------------------------------------------------------
create table if not exists public.external_signal_rollup_es (
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

  explainability jsonb not null default '{}'::jsonb,
  refreshed_at timestamptz not null default now(),

  primary key (tenant_id, buyer_key)
);

create index if not exists idx_external_signal_rollup_es_strength
  on public.external_signal_rollup_es (tenant_id, combined_external_strength_90d desc);

create or replace function public.refresh_external_signal_rollup_es(p_tenant_id text default null)
returns table (
  tenants_scoped int,
  inserted int,
  updated int,
  deleted int
)
language sql
as $$
with scoped as (
  select *
  from public.external_signals_es
  where p_tenant_id is null or tenant_id = p_tenant_id
),
evidence_ranked as (
  select *,
         row_number() over (
           partition by tenant_id,
                        coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN'),
                        signal_type
           order by signal_strength desc, signal_date desc
         ) rn
  from scoped
  where signal_date >= current_date - interval '90 days'
),
evidence_top as (
  select
    tenant_id,
    coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN') buyer_key,
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
    ) evidence
  from evidence_ranked
  where rn <= 3
  group by tenant_id, buyer_key, signal_type
),
evidence_agg as (
  select
    tenant_id,
    buyer_key,
    jsonb_build_object(
      'hiring', coalesce((jsonb_agg(evidence) filter (where signal_type='hiring'))->0, '[]'::jsonb),
      'funding', coalesce((jsonb_agg(evidence) filter (where signal_type='funding'))->0, '[]'::jsonb)
    ) evidence
  from evidence_top
  group by tenant_id, buyer_key
),
computed as (
  select
    tenant_id,
    coalesce(buyer_id, buyer_name_normalized, 'UNKNOWN') buyer_key,
    max(buyer_id) as buyer_id,
    max(buyer_name_normalized) as buyer_name_normalized,

    max(signal_date) filter (where signal_type='hiring') hiring_last_date,
    count(*) filter (where signal_type='hiring' and signal_date >= current_date - interval '30 days') hiring_count_30d,
    count(*) filter (where signal_type='hiring' and signal_date >= current_date - interval '90 days') hiring_count_90d,
    max(signal_strength) filter (where signal_type='hiring' and signal_date >= current_date - interval '90 days') hiring_max_strength_90d,

    max(signal_date) filter (where signal_type='funding') funding_last_date,
    count(*) filter (where signal_type='funding' and signal_date >= current_date - interval '30 days') funding_count_30d,
    count(*) filter (where signal_type='funding' and signal_date >= current_date - interval '90 days') funding_count_90d,
    max(signal_strength) filter (where signal_type='funding' and signal_date >= current_date - interval '90 days') funding_max_strength_90d
  from scoped
  group by tenant_id, buyer_key
),
finalized as (
  select
    c.*, 
    round((coalesce(hiring_max_strength_90d,0) + coalesce(funding_max_strength_90d,0)) / 2, 3) combined_external_strength_90d,
    jsonb_build_object('as_of_date', current_date, 'evidence', coalesce(e.evidence, '{}'::jsonb)) explainability
  from computed c
  left join evidence_agg e
    on e.tenant_id = c.tenant_id
   and e.buyer_key = c.buyer_key
),
upserted as (
  insert into public.external_signal_rollup_es
  select *, now()
  from finalized
  on conflict (tenant_id, buyer_key) do update
  set
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
    refreshed_at = now()
  returning xmax = 0 as inserted_flag
),
counts as (
  select
    count(*) filter (where inserted_flag) inserted,
    count(*) filter (where not inserted_flag) updated
  from upserted
)
select
  (select count(distinct tenant_id) from scoped),
  inserted,
  updated,
  0
from counts;
$$;

alter table public.external_signal_rollup_es enable row level security;
alter table public.external_signal_rollup_es force row level security;

drop policy if exists external_signal_rollup_es_select on public.external_signal_rollup_es;
create policy external_signal_rollup_es_select
on public.external_signal_rollup_es
for select
to authenticated
using (tenant_id = public.civant_current_tenant_id());

commit;
