-- Backfill buyer entities from canonical_tenders for a single tenant/source.
-- Creates buyer entities when missing, adds aliases when table exists,
-- and links canonical_tenders.buyer_entity_id in bounded batches.

create or replace function public.backfill_buyer_entities_from_canonical(
  p_tenant_id text,
  p_source text,
  p_batch_size int default 500
)
returns table (
  entities_created integer,
  rows_linked integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_batch_size integer := greatest(coalesce(p_batch_size, 500), 1);
  v_entities_created integer := 0;
  v_rows_linked integer := 0;
  v_has_entity_aliases boolean := false;
  v_entity_aliases_has_tenant boolean := false;
  v_country_code text := case upper(trim(coalesce(p_source, '')))
    when 'ETENDERS_IE' then 'IE'
    when 'PLACSP_ES' then 'ES'
    when 'BOAMP_FR' then 'FR'
    else null
  end;
begin
  if nullif(trim(coalesce(p_tenant_id, '')), '') is null then
    raise exception 'p_tenant_id is required';
  end if;

  if nullif(trim(coalesce(p_source, '')), '') is null then
    raise exception 'p_source is required';
  end if;

  create temp table if not exists tmp_buyer_backfill_batch (
    buyer_name_raw text not null,
    buyer_name_norm text,
    buyer_key text primary key
  ) on commit drop;
  truncate tmp_buyer_backfill_batch;

  -- Requirement: select distinct buyer_name_raw/buyer_name_norm from canonical_tenders.
  insert into tmp_buyer_backfill_batch (buyer_name_raw, buyer_name_norm, buyer_key)
  with raw_candidates as (
    select distinct
      trim(ct.buyer_name_raw) as buyer_name_raw,
      nullif(trim(ct.buyer_name_norm), '') as buyer_name_norm
    from public.canonical_tenders ct
    where ct.tenant_id = p_tenant_id
      and ct.source = p_source
      and ct.buyer_entity_id is null
      and ct.buyer_name_raw is not null
      and trim(ct.buyer_name_raw) <> ''
  ),
  normalized as (
    select
      rc.buyer_name_raw,
      rc.buyer_name_norm,
      lower(trim(coalesce(rc.buyer_name_norm, rc.buyer_name_raw))) as buyer_key
    from raw_candidates rc
  ),
  deduped as (
    select distinct on (n.buyer_key)
      n.buyer_name_raw,
      n.buyer_name_norm,
      n.buyer_key
    from normalized n
    where n.buyer_key is not null
      and n.buyer_key <> ''
    order by n.buyer_key, length(coalesce(n.buyer_name_raw, '')) desc, n.buyer_name_raw
  )
  select
    d.buyer_name_raw,
    d.buyer_name_norm,
    d.buyer_key
  from deduped d
  order by d.buyer_key
  limit v_batch_size;

  if not exists (select 1 from tmp_buyer_backfill_batch) then
    return query select 0, 0;
    return;
  end if;

  create temp table if not exists tmp_buyer_backfill_map (
    buyer_key text primary key,
    entity_id text not null,
    canonical_name text not null
  ) on commit drop;
  truncate tmp_buyer_backfill_map;

  create temp table if not exists tmp_buyer_backfill_new_entities (
    entity_id text primary key,
    canonical_name text not null
  ) on commit drop;
  truncate tmp_buyer_backfill_new_entities;

  -- First map to existing buyer entities using case-insensitive canonical_name match.
  insert into tmp_buyer_backfill_map (buyer_key, entity_id, canonical_name)
  select
    b.buyer_key,
    e.entity_id,
    e.canonical_name
  from tmp_buyer_backfill_batch b
  join lateral (
    select e.entity_id, e.canonical_name
    from public.entities e
    where e.tenant_id = p_tenant_id
      and lower(coalesce(e.entity_type, '')) = 'buyer'
      and lower(trim(e.canonical_name)) = b.buyer_key
    order by e.updated_at desc nulls last, e.created_at desc nulls last, e.entity_id
    limit 1
  ) e on true
  on conflict (buyer_key) do nothing;

  -- Create missing buyer entities.
  with inserted as (
    insert into public.entities (
      entity_id,
      tenant_id,
      entity_type,
      canonical_name,
      country,
      metadata,
      created_at,
      updated_at
    )
    select
      'ent_' || substr(md5(p_tenant_id || '|buyer|' || b.buyer_key), 1, 24) as entity_id,
      p_tenant_id,
      'buyer',
      coalesce(
        nullif(trim(b.buyer_name_norm), ''),
        nullif(trim(b.buyer_name_raw), ''),
        b.buyer_key
      ) as canonical_name,
      v_country_code,
      jsonb_build_object(
        'source', p_source,
        'origin', 'backfill_buyer_entities_from_canonical',
        'buyer_key', b.buyer_key
      ),
      now(),
      now()
    from tmp_buyer_backfill_batch b
    where not exists (
      select 1
      from tmp_buyer_backfill_map m
      where m.buyer_key = b.buyer_key
    )
      and not exists (
        select 1
        from public.entities e
        where e.tenant_id = p_tenant_id
          and lower(coalesce(e.entity_type, '')) = 'buyer'
          and lower(trim(e.canonical_name)) = b.buyer_key
      )
    on conflict (entity_id) do nothing
    returning entity_id, canonical_name
  )
  insert into tmp_buyer_backfill_new_entities (entity_id, canonical_name)
  select i.entity_id, i.canonical_name
  from inserted i;

  get diagnostics v_entities_created = row_count;

  -- Map newly created entities.
  insert into tmp_buyer_backfill_map (buyer_key, entity_id, canonical_name)
  select
    b.buyer_key,
    ne.entity_id,
    ne.canonical_name
  from tmp_buyer_backfill_batch b
  join tmp_buyer_backfill_new_entities ne
    on lower(trim(ne.canonical_name)) = b.buyer_key
  on conflict (buyer_key) do nothing;

  -- Final safety pass: fill any unresolved map entries from entities table.
  insert into tmp_buyer_backfill_map (buyer_key, entity_id, canonical_name)
  select
    b.buyer_key,
    e.entity_id,
    e.canonical_name
  from tmp_buyer_backfill_batch b
  join lateral (
    select e.entity_id, e.canonical_name
    from public.entities e
    where e.tenant_id = p_tenant_id
      and lower(coalesce(e.entity_type, '')) = 'buyer'
      and lower(trim(e.canonical_name)) = b.buyer_key
    order by e.updated_at desc nulls last, e.created_at desc nulls last, e.entity_id
    limit 1
  ) e on true
  where not exists (
    select 1
    from tmp_buyer_backfill_map m
    where m.buyer_key = b.buyer_key
  )
  on conflict (buyer_key) do nothing;

  -- Recovery path: map previously-created deterministic entity IDs even if canonical_name drifted.
  insert into tmp_buyer_backfill_map (buyer_key, entity_id, canonical_name)
  select
    b.buyer_key,
    e.entity_id,
    e.canonical_name
  from tmp_buyer_backfill_batch b
  join public.entities e
    on e.entity_id = 'ent_' || substr(md5(p_tenant_id || '|buyer|' || b.buyer_key), 1, 24)
   and e.tenant_id = p_tenant_id
   and lower(coalesce(e.entity_type, '')) = 'buyer'
  where not exists (
    select 1
    from tmp_buyer_backfill_map m
    where m.buyer_key = b.buyer_key
  )
  on conflict (buyer_key) do nothing;

  v_has_entity_aliases := to_regclass('public.entity_aliases') is not null;
  v_entity_aliases_has_tenant := exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'entity_aliases'
      and column_name = 'tenant_id'
  );

  if v_has_entity_aliases then
    if v_entity_aliases_has_tenant then
      insert into public.entity_aliases (
        id,
        tenant_id,
        entity_id,
        alias,
        source,
        confidence,
        evidence,
        created_at
      )
      select
        'ea_' || substr(md5(m.entity_id || '|' || b.buyer_name_raw), 1, 24) as id,
        p_tenant_id,
        m.entity_id,
        b.buyer_name_raw,
        p_source,
        1.0,
        jsonb_build_array(
          jsonb_build_object(
            'from', 'backfill_buyer_entities_from_canonical',
            'source', p_source,
            'buyer_key', b.buyer_key
          )
        ),
        now()
      from tmp_buyer_backfill_batch b
      join tmp_buyer_backfill_map m
        on m.buyer_key = b.buyer_key
      where b.buyer_name_raw is not null
        and trim(b.buyer_name_raw) <> ''
      on conflict (entity_id, alias) do nothing;
    else
      insert into public.entity_aliases (
        id,
        entity_id,
        alias,
        source,
        confidence,
        evidence,
        created_at
      )
      select
        'ea_' || substr(md5(m.entity_id || '|' || b.buyer_name_raw), 1, 24) as id,
        m.entity_id,
        b.buyer_name_raw,
        p_source,
        1.0,
        jsonb_build_array(
          jsonb_build_object(
            'from', 'backfill_buyer_entities_from_canonical',
            'source', p_source,
            'buyer_key', b.buyer_key
          )
        ),
        now()
      from tmp_buyer_backfill_batch b
      join tmp_buyer_backfill_map m
        on m.buyer_key = b.buyer_key
      where b.buyer_name_raw is not null
        and trim(b.buyer_name_raw) <> ''
      on conflict (entity_id, alias) do nothing;
    end if;
  end if;

  -- Link canonical tenders for the selected batch.
  update public.canonical_tenders ct
  set
    buyer_entity_id = m.entity_id,
    updated_at = now()
  from tmp_buyer_backfill_batch b
  join tmp_buyer_backfill_map m
    on m.buyer_key = b.buyer_key
  where ct.tenant_id = p_tenant_id
    and ct.source = p_source
    and ct.buyer_entity_id is null
    and lower(trim(coalesce(nullif(ct.buyer_name_norm, ''), ct.buyer_name_raw))) = b.buyer_key;

  get diagnostics v_rows_linked = row_count;

  return query select v_entities_created, v_rows_linked;
end;
$$;
