-- Performance hardening for dashboard stats.
-- Keeps tenant-scoped homepage stats fast on large TendersCurrent tables.

do $$
begin
  if to_regclass('public."TendersCurrent"') is not null then
    -- Speeds up deadline window counts by tenant using lexicographic YYYY-MM-DD comparison.
    create index if not exists idx_tenderscurrent_tenant_deadline_yyyymmdd
      on public."TendersCurrent"(tenant_id, left((data->>'deadline_date'), 10))
      where (data ? 'deadline_date')
        and left((data->>'deadline_date'), 10) ~ '^\d{4}-\d{2}-\d{2}$';
  end if;
end $$;

create or replace function public.get_dashboard_stats(p_tenant_id text)
returns table (
  tenant_id text,
  total_tenders bigint,
  new_tenders_24h bigint,
  deadlines_in_7_days bigint,
  alerts_triggered_24h bigint
)
language plpgsql
stable
as $$
declare
  v_tenant_id text;
begin
  v_tenant_id := lower(trim(coalesce(p_tenant_id, '')));
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  return query
  with
  t as (
    select
      (select count(*)::bigint
       from public."TendersCurrent" tc
       where tc.tenant_id = v_tenant_id) as total_tenders,

      (select count(*)::bigint
       from public."TendersCurrent" tc
       where tc.tenant_id = v_tenant_id
         and tc.published_at >= (now() - interval '1 day')) as new_tenders_24h,

      (select count(*)::bigint
       from public."TendersCurrent" tc
       where tc.tenant_id = v_tenant_id
         and (tc.data ? 'deadline_date')
         and left((tc.data->>'deadline_date'), 10) ~ '^\d{4}-\d{2}-\d{2}$'
         and left((tc.data->>'deadline_date'), 10)
             between to_char(current_date, 'YYYY-MM-DD')
                 and to_char(current_date + 7, 'YYYY-MM-DD')) as deadlines_in_7_days
  ),
  a as (
    select
      (select count(*)::bigint
       from public."AlertEvents" ae
       where ae.tenant_id = v_tenant_id
         and ae.matched_at >= (now() - interval '1 day')) as alerts_triggered_24h
  )
  select
    v_tenant_id as tenant_id,
    t.total_tenders,
    t.new_tenders_24h,
    t.deadlines_in_7_days,
    a.alerts_triggered_24h
  from t, a;
end $$;

