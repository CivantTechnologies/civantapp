-- Civant Predictive Engine V1 hotfix:
-- Speed up incremental procurement normalization by indexing the coalesced update timestamp path.

create index if not exists idx_canonical_tenders_tenant_updated_coalesce
  on public.canonical_tenders (tenant_id, (coalesce(updated_at, created_at)));
