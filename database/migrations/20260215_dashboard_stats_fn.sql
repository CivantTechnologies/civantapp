-- Dashboard stats helper (tenant-scoped).
-- Purpose: avoid fetching large tender lists client-side just to compute homepage cards.
--
-- Returns:
--   total_tenders: total rows in public."TendersCurrent" for tenant
--   new_tenders_24h: first_seen_at/published_at within last 24h (best-effort)
--   deadlines_in_7_days: tenders with a deadline_date within next 7 days (best-effort)
--   alerts_triggered_24h: alert events matched within last 24h

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
  v_tc regclass;
  v_ae regclass;
begin
  v_tenant_id := lower(trim(coalesce(p_tenant_id, '')));
  if v_tenant_id = '' then
    raise exception 'tenant_id is required';
  end if;

  v_tc := to_regclass('public."TendersCurrent"');
  v_ae := to_regclass('public."AlertEvents"');

  tenant_id := v_tenant_id;

  if v_tc is null then
    total_tenders := 0;
    new_tenders_24h := 0;
    deadlines_in_7_days := 0;
  else
    -- Total tenders (exact).
    execute format('select count(*) from %s where tenant_id = $1', v_tc)
      into total_tenders
      using v_tenant_id;

    -- New tenders (24h) using best-effort "first_seen_at" from jsonb, falling back to columns.
    execute format($sql$
      select count(*)
      from %s
      where tenant_id = $1
        and coalesce(
          nullif((data->>'first_seen_at'), '')::timestamptz,
          published_at,
          updated_at
        ) >= (now() - interval '1 day')
    $sql$, v_tc)
      into new_tenders_24h
      using v_tenant_id;

    -- Deadlines (7 days). We limit scan to the last year of published rows to keep this query cheap.
    execute format($sql$
      select count(*)
      from %s
      where tenant_id = $1
        and published_at >= (now() - interval '365 days')
        and (
          case
            when (data ? 'deadline_date')
             and (data->>'deadline_date') ~ '^\d{4}-\d{2}-\d{2}'
              then left((data->>'deadline_date'), 10)::date
            else null
          end
        ) between current_date and (current_date + 7)
    $sql$, v_tc)
      into deadlines_in_7_days
      using v_tenant_id;
  end if;

  if v_ae is null then
    alerts_triggered_24h := 0;
  else
    execute format($sql$
      select count(*)
      from %s
      where tenant_id = $1
        and matched_at >= (now() - interval '1 day')
    $sql$, v_ae)
      into alerts_triggered_24h
      using v_tenant_id;
  end if;

  return next;
end $$;

