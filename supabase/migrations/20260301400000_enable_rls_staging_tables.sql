-- =============================================================================
-- Civant: Enable RLS on internal/staging tables exposed to PostgREST
-- Migration: 20260301400000_enable_rls_staging_tables.sql
-- =============================================================================
--
-- PURPOSE:
--   Supabase security linter reported 20 tables in public schema with RLS
--   disabled, meaning any authenticated user can query them via PostgREST.
--   These are internal ETL/staging tables that must not be accessible to
--   end users. Enabling RLS with no policies = deny all (no rows returned).
--
-- EFFECT:
--   RLS enabled with zero policies → all authenticated queries return 0 rows.
--   service_role (used by all server-side API handlers) bypasses RLS by default.
--   No existing pipeline or server functionality is affected.
--
-- ROLLBACK:
--   ALTER TABLE <table> DISABLE ROW LEVEL SECURITY; (per table)
-- =============================================================================

-- ETL merge maps and logs
ALTER TABLE IF EXISTS public._fr_buyer_merge_map         ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.buyer_entity_merge_log       ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.fr_buyer_entity_merge        ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.fr_entity_norms              ENABLE ROW LEVEL SECURITY;

-- Staging tables
ALTER TABLE IF EXISTS public._stg_boamp_cpv               ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public._stg_placsp_canonical        ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public._stg_placsp_current          ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public._stg_boamp_deadline_backfill ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public._stg_spain_winners           ENABLE ROW LEVEL SECURITY;

-- Dimension and lookup tables
ALTER TABLE IF EXISTS public.ie_supplier_dim              ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.cpv_cluster_map_ie           ENABLE ROW LEVEL SECURITY;

-- Enrichment tables
ALTER TABLE IF EXISTS public.fr_award_enrichment_2023     ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.fr_award_enrichment_2024     ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.fr_award_enrichment_2025     ENABLE ROW LEVEL SECURITY;

-- Research and buyer tables
ALTER TABLE IF EXISTS public.buyer_research_briefs        ENABLE ROW LEVEL SECURITY;

-- Validation tables
ALTER TABLE IF EXISTS public._val_pred_norm               ENABLE ROW LEVEL SECURITY;

-- Buyer cluster tables
ALTER TABLE IF EXISTS public.buyer_cluster_last_award     ENABLE ROW LEVEL SECURITY;

-- Verification:
-- SELECT tablename, rowsecurity FROM pg_tables
-- WHERE schemaname = 'public' AND rowsecurity = false
-- ORDER BY tablename;
