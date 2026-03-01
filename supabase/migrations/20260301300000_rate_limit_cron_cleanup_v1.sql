-- =============================================================================
-- Civant: pg_cron cleanup job for api_rate_limits
-- Migration: 20260301300000_rate_limit_cron_cleanup_v1.sql
-- =============================================================================
--
-- PURPOSE:
--   Deletes api_rate_limits rows older than 1 hour, run every hour at :05.
--   Without this the table grows unboundedly (slowly — only rate-limited RPCs
--   write to it, but still needs housekeeping).
--
-- PREREQUISITE:
--   pg_cron must be enabled. Enable via:
--     Supabase Dashboard → Database → Extensions → pg_cron → Enable
--   or:
--     create extension if not exists pg_cron with schema extensions;
--
-- ROLLBACK:
--   SELECT cron.unschedule('cleanup-rate-limits');
-- =============================================================================

-- Enable pg_cron if not already on (no-op if already enabled)
create extension if not exists pg_cron with schema extensions;

-- Idempotent: remove existing job of same name before (re)scheduling
select cron.unschedule('cleanup-rate-limits')
where exists (
  select 1 from cron.job where jobname = 'cleanup-rate-limits'
);

-- Run at :05 past every hour
select cron.schedule(
  'cleanup-rate-limits',
  '5 * * * *',
  $$
    delete from public.api_rate_limits
    where window_start < now() - interval '1 hour';
  $$
);

-- Verify:
-- SELECT jobid, jobname, schedule, command, active FROM cron.job WHERE jobname = 'cleanup-rate-limits';
