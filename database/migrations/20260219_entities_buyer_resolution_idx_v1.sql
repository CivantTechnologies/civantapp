-- Speed up resolve_buyer_entity_id lookups used by predictive runs.
-- Matches the filter/order pattern:
--   tenant_id + buyer entity type + normalized canonical name + recency order.
create index concurrently if not exists idx_entities_tenant_buyer_match_key_recent
  on public.entities (
    tenant_id,
    public.normalize_match_key(coalesce(canonical_name, '')),
    updated_at desc,
    created_at desc
  )
  where lower(coalesce(entity_type, '')) = 'buyer';
