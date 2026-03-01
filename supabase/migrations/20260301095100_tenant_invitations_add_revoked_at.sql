-- =============================================================================
-- Civant: Add revoked_at to tenant_invitations
-- Migration: 20260301095100_tenant_invitations_add_revoked_at.sql
-- =============================================================================
--
-- PURPOSE:
--   Adds revoked_at column so invitations can be soft-revoked (preserving
--   audit trail) rather than deleted.
--
-- ROLLBACK:
--   ALTER TABLE tenant_invitations DROP COLUMN IF EXISTS revoked_at;
-- =============================================================================

ALTER TABLE tenant_invitations ADD COLUMN IF NOT EXISTS revoked_at timestamptz;
