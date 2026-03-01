-- =============================================================================
-- Civant: Per-user rate limiting infrastructure
-- Migration: 20260301100000_rate_limit_fn_v1.sql
-- =============================================================================
--
-- PURPOSE:
--   Provides a reusable check_rate_limit() function for expensive RPCs.
--   Uses a fixed-window counter stored in api_rate_limits. One index-lookup
--   per call; no external service required.
--
-- DESIGN:
--   - Fixed window aligned to p_window_seconds boundaries (e.g. :00–:59 for 60s)
--   - Atomic INSERT … ON CONFLICT DO UPDATE to increment counter
--   - service_role bypasses all limits (pipeline operations unaffected)
--   - Raises P0001 (raise_exception) with a clear message on limit exceeded
--   - Old rows accumulate slowly; clean up periodically via:
--       DELETE FROM public.api_rate_limits WHERE window_start < now() - interval '1 hour';
--
-- ROLLBACK:
--   DROP FUNCTION IF EXISTS public.check_rate_limit(text, int, int);
--   DROP TABLE IF EXISTS public.api_rate_limits;
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Table
-- ---------------------------------------------------------------------------
create table if not exists public.api_rate_limits (
  user_id      text        not null,
  rpc_name     text        not null,
  window_start timestamptz not null,
  call_count   int         not null default 1,
  primary key (user_id, rpc_name, window_start)
);

comment on table public.api_rate_limits is
  'Fixed-window per-user rate limit counters for expensive RPCs.';

-- Index for cleanup queries (DELETE WHERE window_start < …)
create index if not exists api_rate_limits_window_idx
  on public.api_rate_limits (window_start);

-- Block direct access; only check_rate_limit (SECURITY DEFINER) writes here
revoke all on public.api_rate_limits from anon, authenticated;

-- ---------------------------------------------------------------------------
-- Function
-- ---------------------------------------------------------------------------
create or replace function public.check_rate_limit(
  p_rpc_name      text,
  p_max_calls     int,
  p_window_seconds int default 60
)
returns void
language plpgsql
volatile
security definer
set search_path = public
as $$
declare
  v_user_id    text;
  v_window     timestamptz;
  v_count      int;
begin
  -- service_role (pipeline jobs, admin) bypasses all limits
  if coalesce(auth.role(), '') = 'service_role' then
    return;
  end if;

  v_user_id := auth.uid()::text;
  if v_user_id is null then
    raise exception 'authentication required' using errcode = 'P0001';
  end if;

  -- Align to fixed window boundary
  v_window := to_timestamp(
    floor(extract(epoch from now()) / p_window_seconds) * p_window_seconds
  );

  insert into public.api_rate_limits (user_id, rpc_name, window_start, call_count)
  values (v_user_id, p_rpc_name, v_window, 1)
  on conflict (user_id, rpc_name, window_start)
  do update set call_count = api_rate_limits.call_count + 1
  returning call_count into v_count;

  if v_count > p_max_calls then
    raise exception 'Rate limit exceeded for %. Max % calls per % seconds. Try again shortly.',
      p_rpc_name, p_max_calls, p_window_seconds
      using errcode = 'P0001';
  end if;
end;
$$;

revoke all on function public.check_rate_limit(text, int, int) from public;
grant execute on function public.check_rate_limit(text, int, int) to authenticated;
grant execute on function public.check_rate_limit(text, int, int) to service_role;

-- Verification:
-- SELECT public.check_rate_limit('test_rpc', 3, 60); -- run 4x, 4th should fail
-- SELECT * FROM public.api_rate_limits;
