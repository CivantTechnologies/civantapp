-- =============================================================================
-- Civant: Fix mutable search_path on critical SECURITY DEFINER functions
-- Migration: 20260301410000_fix_search_path_definer_fns.sql
-- =============================================================================
--
-- PURPOSE:
--   Supabase security linter warns that SECURITY DEFINER functions without an
--   explicit SET search_path are vulnerable to search_path injection.
--   Uses pg_get_function_identity_arguments() (no DEFAULT values) which is
--   the correct form for ALTER FUNCTION signatures.
--
-- ROLLBACK:
--   ALTER FUNCTION <name>(<args>) RESET search_path;
-- =============================================================================

DO $$
DECLARE
  r record;
  fn_sig text;
BEGIN
  FOR r IN
    SELECT
      p.proname,
      pg_get_function_identity_arguments(p.oid) AS fn_args,
      p.oid
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public'
      AND p.prosecdef = true
      AND p.proname IN (
        'check_rate_limit',
        'get_competitor_intelligence',
        'get_buyer_brief',
        'validate_predictions',
        'get_renewal_signals',
        'current_tenant_id',
        'search_tenders_ranked',
        'get_dashboard_stats',
        'get_renewal_signals_v2'
      )
  LOOP
    fn_sig := 'public.' || quote_ident(r.proname) || '(' || r.fn_args || ')';
    EXECUTE format('ALTER FUNCTION %s SET search_path = public', fn_sig);
    RAISE NOTICE 'Fixed search_path on %', fn_sig;
  END LOOP;
END;
$$;

-- Verification:
-- SELECT proname, pg_get_function_identity_arguments(oid) as args, proconfig
-- FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
-- WHERE n.nspname = 'public' AND p.prosecdef = true ORDER BY proname;
