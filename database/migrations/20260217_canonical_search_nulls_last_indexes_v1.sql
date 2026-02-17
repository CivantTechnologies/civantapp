-- Align recency indexes with search ordering expression (DESC NULLS LAST)
-- This avoids planner fallback to large seq-scan + top-N sort for search_tenders_ranked.

create index if not exists idx_canonical_tenders_tenant_recency_desc_nl
  on public.canonical_tenders (tenant_id, (coalesce(last_seen_at, updated_at, created_at)) desc nulls last);

create index if not exists idx_canonical_tenders_tenant_source_recency_desc_nl
  on public.canonical_tenders (tenant_id, source, (coalesce(last_seen_at, updated_at, created_at)) desc nulls last);

create index if not exists idx_canonical_tenders_tenant_country_recency_desc_nl
  on public.canonical_tenders (tenant_id, country, (coalesce(last_seen_at, updated_at, created_at)) desc nulls last);
