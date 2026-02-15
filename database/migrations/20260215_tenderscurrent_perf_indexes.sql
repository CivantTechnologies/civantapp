-- Perf: speed up tenant-scoped dashboard queries (and protect against accidental unscoped sorts).
do $$
begin
  if to_regclass('public."TendersCurrent"') is not null then
    -- Common dashboard/search sort: ORDER BY published_at DESC LIMIT N (tenant-scoped).
    create index if not exists idx_tenderscurrent_tenant_published_at_desc
      on public."TendersCurrent"(tenant_id, published_at desc);

    -- Defensive index for any accidental unscoped query (should not happen, but avoids full-table sort).
    create index if not exists idx_tenderscurrent_published_at_desc
      on public."TendersCurrent"(published_at desc);
  end if;
end $$;

