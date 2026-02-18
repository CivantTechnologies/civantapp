-- Civant Predictive Engine V1 hotfix:
-- Ensure signals upsert conflict target is backed by a full unique index.

update public.signals
set source_event_id = md5(
  coalesce(tenant_id, '') || '|' ||
  coalesce(source, '') || '|' ||
  coalesce(signal_type, '') || '|' ||
  coalesce(buyer_entity_id, '') || '|' ||
  coalesce(cpv_cluster_id, '') || '|' ||
  coalesce(region, '') || '|' ||
  coalesce(occurred_at::text, '') || '|' ||
  coalesce(canonical_id, '') || '|' ||
  coalesce(source_url, '')
)
where source_event_id is null;

with ranked as (
  select
    signal_id,
    row_number() over (
      partition by tenant_id, source, source_event_id, signal_type
      order by ingested_at desc, signal_id desc
    ) as rn
  from public.signals
)
delete from public.signals s
using ranked r
where s.signal_id = r.signal_id
  and r.rn > 1;

drop index if exists uq_signals_source_event;
create unique index if not exists uq_signals_source_event
  on public.signals (tenant_id, source, source_event_id, signal_type);

alter table public.signals
  alter column source_event_id set not null;
