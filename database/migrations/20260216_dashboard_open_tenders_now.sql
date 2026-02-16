-- Dashboard card metric update:
-- Add an exact, tenant-scoped open-tenders counter where open means
-- submission deadline date >= current_date.
--
-- Kept as a separate RPC to avoid changing get_dashboard_stats(text) return type.

create or replace function public.count_open_tenders_now(p_tenant_id text)
returns bigint
language sql
stable
as $$
  select count(*)::bigint
  from public."TendersCurrent" tc
  where tc.tenant_id = lower(trim(coalesce(p_tenant_id, '')))
    and (tc.data ? 'deadline_date')
    and left((tc.data->>'deadline_date'), 10) ~ '^\d{4}-\d{2}-\d{2}$'
    and left((tc.data->>'deadline_date'), 10) >= to_char(current_date, 'YYYY-MM-DD');
$$;
