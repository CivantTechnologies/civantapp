-- Civant: Award-to-Prediction Signals v1 (Ireland only)
-- Source system: ETENDERS_IE
--
-- This migration is additive. It does not modify canonical tables.
--
-- =====================================================================================
-- Field Discovery (run manually to confirm best canonical source for awards)
-- =====================================================================================
-- 1) Find candidate tables/views and relevant columns
--    (Looks across tables + views in the current database.)
--
-- with
-- patterns as (
--   select * from unnest(array[
--     'canonical_id','tender_id','source','source_system','notice_type',
--     'buyer','authority','contracting','supplier','winner','awardee',
--     'value','amount','price','cpv','procedure','title','description',
--     'publication_date','published_at','award_date','date_awarded','contract_award_date',
--     'start_date','end_date','duration'
--   ]) as p(pattern)
-- ),
-- cols as (
--   select
--     c.table_schema,
--     c.table_name,
--     c.column_name,
--     p.pattern
--   from information_schema.columns c
--   join patterns p
--     on c.column_name ilike '%' || p.pattern || '%'
--   where c.table_schema not in ('pg_catalog','information_schema')
-- )
-- select
--   table_schema,
--   table_name,
--   count(*) as matched_columns,
--   array_agg(distinct column_name order by column_name) as columns
-- from cols
-- group by 1,2
-- order by matched_columns desc, table_schema, table_name;
--
-- 2) If multiple candidates exist, prefer a canonical relation that has:
--    - stable id: canonical_id (or tender_id)
--    - notice_type with reliable award values ('award')
--    - buyer identity (buyer_id or buyer_name)
--    - explicit award date (award_date/date_awarded/contract_award_date)
--    - supplier/winner fields + awarded value fields
--
-- NOTE: The refresh function below auto-selects a source relation when not provided,
--       but you should still validate the picked relation using the discovery output.

begin;

-- =====================================================================================
-- Minimal CPV -> cluster mapping (seed empty; populate as you define clusters)
-- =====================================================================================
create table if not exists cpv_cluster_map_ie (
  cpv_primary text primary key,
  cpv_cluster text not null,
  created_at timestamptz not null default now()
);

-- =====================================================================================
-- Award facts (Ireland)
-- =====================================================================================
create table if not exists award_fact_ie (
  tenant_id text not null,
  source_system text not null default 'ETENDERS_IE',
  award_canonical_id text primary key,
  buyer_id text not null,
  supplier_id text null,
  award_date date not null,
  award_value_eur numeric null,
  cpv_primary text null,
  cpv_cluster text null,
  procedure_type text null,
  framework_flag boolean not null default false,
  duration_months integer null,
  start_date date null,
  end_date date null,
  framework_evidence jsonb null,
  award_date_evidence jsonb null,
  created_at timestamptz not null default now()
);

-- Indexes for refresh + query speed
create index if not exists idx_award_fact_ie_buyer_cluster_date
  on award_fact_ie (buyer_id, cpv_cluster, award_date);
create index if not exists idx_award_fact_ie_buyer_date
  on award_fact_ie (buyer_id, award_date);
create index if not exists idx_award_fact_ie_supplier_id_not_null
  on award_fact_ie (supplier_id) where supplier_id is not null;

-- =====================================================================================
-- Award signals (Ireland)
-- =====================================================================================
create table if not exists award_signals_ie (
  -- NOTE: Spec requested PK (buyer_id, cpv_cluster). If you need strict tenant isolation,
  -- add tenant_id and include it in the PK; see commentary in refresh function header.
  buyer_id text not null,
  cpv_cluster text not null,
  last_award_date date not null,
  awards_count int not null,
  avg_cycle_days numeric null,
  cycle_std_days numeric null,
  cycle_consistency_score numeric not null,
  supplier_concentration numeric not null,
  switch_rate numeric not null,
  expected_next_tender_date date not null,
  expected_window_start date not null,
  expected_window_end date not null,
  renewal_probability numeric not null,
  switching_probability numeric not null,
  confidence_score int not null,
  explanations jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now(),
  primary key (buyer_id, cpv_cluster)
);

-- =====================================================================================
-- Helpers (schema-flexible extraction without hard-coding every canonical column)
-- =====================================================================================
create or replace function civant__col_exists(p_rel regclass, p_col text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from pg_attribute a
    where a.attrelid = p_rel
      and a.attname = p_col
      and a.attnum > 0
      and not a.attisdropped
  );
$$;

create or replace function civant__first_existing_col(p_rel regclass, p_candidates text[])
returns text
language plpgsql
stable
as $$
declare
  c text;
begin
  foreach c in array p_candidates loop
    if civant__col_exists(p_rel, c) then
      return c;
    end if;
  end loop;
  return null;
end;
$$;

create or replace function civant__parse_date(p text)
returns date
language sql
immutable
as $$
  select case
    when p is null then null
    -- ISO-ish: take first 10 chars (YYYY-MM-DD)
    when p ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' then substring(p from 1 for 10)::date
    else null
  end;
$$;

create or replace function civant__parse_numeric(p text)
returns numeric
language sql
immutable
as $$
  select case
    when p is null then null
    -- Normalize common thousands separators (e.g. "1,234.56" -> "1234.56").
    else nullif(replace(regexp_replace(p, '[^0-9\\.,-]', '', 'g'), ',', ''), '')::numeric
  end;
$$;

create or replace function civant_ie__stable_id(
  p_prefix text,
  p_tenant_id text,
  p_source_system text,
  p_source_id text,
  p_name text
)
returns text
language sql
immutable
as $$
  select case
    when nullif(trim(coalesce(p_source_id,'')), '') is not null and nullif(trim(coalesce(p_name,'')), '') is not null
      then p_prefix || md5(lower(trim(p_source_id)) || '|' || lower(trim(p_name)) || '|' || p_source_system || '|' || p_tenant_id)
    when nullif(trim(coalesce(p_source_id,'')), '') is not null
      then p_prefix || md5(lower(trim(p_source_id)) || '|' || p_source_system || '|' || p_tenant_id)
    when nullif(trim(coalesce(p_name,'')), '') is not null
      then p_prefix || md5(lower(trim(p_name)) || '|' || p_source_system || '|' || p_tenant_id)
    else null
  end;
$$;

-- =====================================================================================
-- Source selection for ETENDERS_IE awards
-- =====================================================================================
create or replace function civant_ie__choose_award_source_relation()
returns regclass
language plpgsql
stable
as $$
declare
  r record;
  v_rel regclass;
  v_source_col text;
  v_notice_type_col text;
  v_score int;
  v_best regclass := null;
  v_best_score int := -1;
  v_has_data int;
begin
  -- Prefer known canonical tables when present.
  if to_regclass('public.canonical_tenders') is not null then
    v_rel := to_regclass('public.canonical_tenders');
    if v_rel is not null
      and civant__col_exists(v_rel, 'tenant_id')
      and (civant__col_exists(v_rel, 'canonical_id') or civant__col_exists(v_rel, 'tender_id'))
      and (civant__col_exists(v_rel, 'source') or civant__col_exists(v_rel, 'source_system'))
      and civant__col_exists(v_rel, 'notice_type')
    then
      v_best := v_rel;
      v_best_score := 0;
    end if;
  end if;
  if to_regclass('public.\"TendersCurrent\"') is not null then
    -- TendersCurrent is often a denormalized "latest snapshot" and may have value/cpv/title.
    v_rel := to_regclass('public.\"TendersCurrent\"');
    if v_rel is null
      or not civant__col_exists(v_rel, 'tenant_id')
      or not (civant__col_exists(v_rel, 'canonical_id') or civant__col_exists(v_rel, 'tender_id'))
      or not (civant__col_exists(v_rel, 'source') or civant__col_exists(v_rel, 'source_system'))
      or not civant__col_exists(v_rel, 'notice_type')
    then
      v_rel := null;
    end if;
    if v_rel is not null then
    v_score := 0;
    if civant__col_exists(v_rel, 'supplier_id') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'supplier_name') then v_score := v_score + 2; end if;
    if civant__col_exists(v_rel, 'estimated_value') then v_score := v_score + 2; end if;
    if civant__col_exists(v_rel, 'award_value') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'award_date') then v_score := v_score + 4; end if;
    if v_score > v_best_score then
      v_best := v_rel;
      v_best_score := v_score;
    end if;
    end if;
  end if;

  -- Heuristic scan across public tables/views for a better match (even if we found a baseline).

  for r in
    select
      c.table_schema,
      c.table_name
    from information_schema.columns c
    where c.table_schema = 'public'
    group by 1,2
    having
      bool_or(c.column_name = 'tenant_id')
      and (bool_or(c.column_name = 'canonical_id') or bool_or(c.column_name = 'tender_id'))
      and (bool_or(c.column_name = 'source') or bool_or(c.column_name = 'source_system'))
      and bool_or(c.column_name = 'notice_type')
  loop
    v_rel := to_regclass(format('%I.%I', r.table_schema, r.table_name));
    if v_rel is null then
      continue;
    end if;

    v_source_col := civant__first_existing_col(v_rel, array['source','source_system']);
    v_notice_type_col := 'notice_type';

    -- Skip if it can't be filtered to ETENDERS_IE at all.
    begin
      execute format('select 1 from %s where %I = $1 limit 1', v_rel::text, v_source_col)
        into v_has_data using 'ETENDERS_IE';
      if v_has_data is null then
        continue;
      end if;
    exception when undefined_column then
      continue;
    end;

    v_score := 0;
    -- award date fields
    if civant__col_exists(v_rel, 'award_date') then v_score := v_score + 4; end if;
    if civant__col_exists(v_rel, 'date_awarded') then v_score := v_score + 4; end if;
    if civant__col_exists(v_rel, 'contract_award_date') then v_score := v_score + 4; end if;
    if civant__col_exists(v_rel, 'publication_date') or civant__col_exists(v_rel, 'published_at') then v_score := v_score + 2; end if;
    -- buyer / supplier
    if civant__col_exists(v_rel, 'buyer_id') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'buyer_name') then v_score := v_score + 2; end if;
    if civant__col_exists(v_rel, 'supplier_id') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'winner_id') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'supplier_name') then v_score := v_score + 2; end if;
    if civant__col_exists(v_rel, 'winner_name') then v_score := v_score + 2; end if;
    -- value / cpv / title
    if civant__col_exists(v_rel, 'award_value') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'awarded_value') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'contract_value') then v_score := v_score + 3; end if;
    if civant__col_exists(v_rel, 'estimated_value') then v_score := v_score + 2; end if;
    if civant__col_exists(v_rel, 'cpv_primary') then v_score := v_score + 2; end if;
    if civant__col_exists(v_rel, 'cpv_codes') then v_score := v_score + 2; end if;
    if civant__col_exists(v_rel, 'procedure_type') then v_score := v_score + 1; end if;
    if civant__col_exists(v_rel, 'title') then v_score := v_score + 1; end if;
    if civant__col_exists(v_rel, 'description') then v_score := v_score + 1; end if;

    if v_score > v_best_score then
      v_best := v_rel;
      v_best_score := v_score;
    end if;
  end loop;

  return v_best;
end;
$$;

-- =====================================================================================
-- Refresh: award_fact_ie (idempotent upsert by award_canonical_id)
-- =====================================================================================
-- Notes:
-- - Filters to ETENDERS_IE awards only.
-- - Award date mapping:
--   - Prefer explicit award date fields (award_date/date_awarded/contract_award_date)
--   - Fallback to publication date only if no explicit award date exists
-- - Skips rows missing BOTH explicit award date and publication date.
create or replace function refresh_award_fact_ie(p_tenant_id text default null, p_source_relation regclass default null)
returns table (
  source_relation text,
  total_award_rows int,
  eligible_rows int,
  inserted int,
  updated int,
  skipped_missing_award_date int,
  skipped_missing_buyer_id int
)
language plpgsql
as $$
declare
  v_rel regclass;
  v_rel_text text;
  v_id_col text;
  v_tenant_col text;
  v_source_col text;
  v_notice_type_col text;
  v_buyer_id_col text;
  v_buyer_name_col text;
  v_buyer_source_id_col text;
  v_supplier_id_col text;
  v_supplier_name_col text;
  v_supplier_source_id_col text;
  v_award_date_col text;
  v_date_awarded_col text;
  v_contract_award_date_col text;
  v_publication_date_col text;
  v_published_at_col text;
  v_value_col text;
  v_currency_col text;
  v_cpv_primary_col text;
  v_cpv_codes_col text;
  v_procedure_type_col text;
  v_title_col text;
  v_description_col text;
  v_duration_months_col text;
  v_start_date_col text;
  v_end_date_col text;
  v_sql text;
begin
  v_rel := coalesce(p_source_relation, civant_ie__choose_award_source_relation());
  if v_rel is null then
    raise exception 'No canonical source relation found for ETENDERS_IE awards. Run the Field Discovery queries in the migration header.';
  end if;

  v_rel_text := v_rel::text;

  v_id_col := civant__first_existing_col(v_rel, array['canonical_id','tender_id','award_canonical_id','id']);
  v_tenant_col := civant__first_existing_col(v_rel, array['tenant_id']);
  v_source_col := civant__first_existing_col(v_rel, array['source','source_system']);
  v_notice_type_col := civant__first_existing_col(v_rel, array['notice_type']);

  if v_id_col is null or v_tenant_col is null or v_source_col is null or v_notice_type_col is null then
    raise exception 'Chosen relation % is missing required columns (need tenant_id + source/source_system + notice_type + canonical_id/tender_id).', v_rel_text;
  end if;

  v_buyer_id_col := civant__first_existing_col(v_rel, array['buyer_id','buyer_org_id','authority_id','contracting_authority_id']);
  v_buyer_name_col := civant__first_existing_col(v_rel, array['buyer_name','buyer','authority_name','contracting_authority_name']);
  v_buyer_source_id_col := civant__first_existing_col(v_rel, array['buyer_source_id','buyer_external_id','authority_source_id','authority_external_id']);

  v_supplier_id_col := civant__first_existing_col(v_rel, array['supplier_id','winner_id','awardee_id','supplier_org_id']);
  v_supplier_name_col := civant__first_existing_col(v_rel, array['supplier_name','winner_name','awardee_name','contractor_name']);
  v_supplier_source_id_col := civant__first_existing_col(v_rel, array['supplier_source_id','winner_source_id','awardee_source_id','supplier_external_id','winner_external_id']);

  v_award_date_col := civant__first_existing_col(v_rel, array['award_date']);
  v_date_awarded_col := civant__first_existing_col(v_rel, array['date_awarded']);
  v_contract_award_date_col := civant__first_existing_col(v_rel, array['contract_award_date']);
  v_publication_date_col := civant__first_existing_col(v_rel, array['publication_date']);
  v_published_at_col := civant__first_existing_col(v_rel, array['published_at']);

  v_value_col := civant__first_existing_col(v_rel, array['award_value_eur','award_value','awarded_value','contract_value','estimated_value','value','amount']);
  v_currency_col := civant__first_existing_col(v_rel, array['currency','value_currency']);

  v_cpv_primary_col := civant__first_existing_col(v_rel, array['cpv_primary']);
  v_cpv_codes_col := civant__first_existing_col(v_rel, array['cpv_codes','cpv']);
  v_procedure_type_col := civant__first_existing_col(v_rel, array['procedure_type']);
  v_title_col := civant__first_existing_col(v_rel, array['title']);
  v_description_col := civant__first_existing_col(v_rel, array['description']);

  v_duration_months_col := civant__first_existing_col(v_rel, array['duration_months','contract_duration_months']);
  v_start_date_col := civant__first_existing_col(v_rel, array['start_date','contract_start_date']);
  v_end_date_col := civant__first_existing_col(v_rel, array['end_date','contract_end_date']);

  v_sql := format($f$
    with
    src as (
      select
        t.%1$I::text as tenant_id,
        t.%2$I::text as src_system,
        t.%3$I::text as award_canonical_id,
        t.%4$I::text as notice_type,
        %5$s as buyer_id_raw,
        %6$s as buyer_name_raw,
        %7$s as buyer_source_id_raw,
        %8$s as supplier_id_raw,
        %9$s as supplier_name_raw,
        %10$s as supplier_source_id_raw,
        %11$s as award_date_raw,
        %12$s as date_awarded_raw,
        %13$s as contract_award_date_raw,
        %14$s as publication_date_raw,
        %15$s as published_at_raw,
        %16$s as value_raw,
        %17$s as currency_raw,
        %18$s as cpv_primary_raw,
        %19$s as cpv_codes_raw,
        %20$s as procedure_type_raw,
        %21$s as title_raw,
        %22$s as description_raw,
        %23$s as duration_months_raw,
        %24$s as start_date_raw,
        %25$s as end_date_raw
      from %26$s t
      where t.%2$I = 'ETENDERS_IE'
        and lower(t.%4$I::text) = 'award'
        and ($1 is null or t.%1$I = $1)
    ),
    normalized as (
      select
        tenant_id,
        'ETENDERS_IE'::text as source_system,
        award_canonical_id,
        coalesce(
          nullif(buyer_id_raw,''),
          civant_ie__stable_id('IE_BUYER:', tenant_id, 'ETENDERS_IE', nullif(buyer_source_id_raw,''), nullif(buyer_name_raw,''))
        ) as buyer_id,
        nullif(
          coalesce(
            nullif(supplier_id_raw,''),
            civant_ie__stable_id('IE_SUPPLIER:', tenant_id, 'ETENDERS_IE', nullif(supplier_source_id_raw,''), nullif(supplier_name_raw,''))
          ),
          ''
        ) as supplier_id,
        -- award_date mapping: explicit award date -> fallback publication
        coalesce(
          civant__parse_date(nullif(award_date_raw,'')),
          civant__parse_date(nullif(date_awarded_raw,'')),
          civant__parse_date(nullif(contract_award_date_raw,'')),
          civant__parse_date(nullif(publication_date_raw,'')),
          civant__parse_date(nullif(published_at_raw,''))
        ) as award_date,
        case
          when civant__parse_date(nullif(award_date_raw,'')) is not null then jsonb_build_object('source_field','award_date','raw',award_date_raw)
          when civant__parse_date(nullif(date_awarded_raw,'')) is not null then jsonb_build_object('source_field','date_awarded','raw',date_awarded_raw)
          when civant__parse_date(nullif(contract_award_date_raw,'')) is not null then jsonb_build_object('source_field','contract_award_date','raw',contract_award_date_raw)
          when civant__parse_date(nullif(publication_date_raw,'')) is not null then jsonb_build_object('source_field','publication_date','raw',publication_date_raw)
          when civant__parse_date(nullif(published_at_raw,'')) is not null then jsonb_build_object('source_field','published_at','raw',published_at_raw)
          else null
        end as award_date_evidence,
        case
          when value_raw is null then null
          else civant__parse_numeric(value_raw)
        end as award_value_eur,
        nullif(
          regexp_replace(
            coalesce(
              nullif(cpv_primary_raw,''),
              nullif(split_part(replace(replace(cpv_codes_raw,'{',''),'}',''), ',', 1),'')
            ),
            '[^0-9-]',
            '',
            'g'
          ),
          ''
        ) as cpv_primary,
        null::text as cpv_cluster, -- mapped below
        nullif(procedure_type_raw,'') as procedure_type,
        false as framework_flag, -- set below
        case
          when duration_months_raw is not null and duration_months_raw <> '' then civant__parse_numeric(duration_months_raw)::int
          else null
        end as duration_months,
        civant__parse_date(nullif(start_date_raw,'')) as start_date,
        civant__parse_date(nullif(end_date_raw,'')) as end_date,
        nullif(title_raw,'') as title,
        nullif(description_raw,'') as description,
        nullif(currency_raw,'') as currency
      from src
    ),
    enriched as (
      select
        n.*,
        m.cpv_cluster as mapped_cpv_cluster,
        -- framework detection: procedure_type/title/description keyword
        (
          position('framework' in lower(coalesce(n.procedure_type,''))) > 0
          or position('framework' in lower(coalesce(n.title,''))) > 0
          or position('framework' in lower(coalesce(n.description,''))) > 0
          or position('panel' in lower(coalesce(n.title,''))) > 0
          or position('dps' in lower(coalesce(n.title,''))) > 0
        ) as framework_detected
      from normalized n
      left join cpv_cluster_map_ie m
        on m.cpv_primary = n.cpv_primary
    ),
    final_rows as (
      select
        tenant_id,
        source_system,
        award_canonical_id,
        buyer_id,
        supplier_id,
        award_date,
        award_value_eur,
        cpv_primary,
        mapped_cpv_cluster as cpv_cluster,
        procedure_type,
        framework_detected as framework_flag,
        -- duration_months: explicit, else from start/end if both present
        coalesce(
          duration_months,
          case
            when start_date is not null and end_date is not null then
              (date_part('year', age(end_date, start_date))::int * 12) + date_part('month', age(end_date, start_date))::int
            else null
          end
        ) as duration_months,
        start_date,
        end_date,
        jsonb_strip_nulls(
          jsonb_build_object(
            'framework_detected', framework_detected,
            'framework_keywords', case
              when framework_detected then
                to_jsonb(array_remove(array[
                  case when position('framework' in lower(coalesce(procedure_type,''))) > 0 then 'procedure_type:framework' end,
                  case when position('framework' in lower(coalesce(title,''))) > 0 then 'title:framework' end,
                  case when position('framework' in lower(coalesce(description,''))) > 0 then 'description:framework' end,
                  case when position('panel' in lower(coalesce(title,''))) > 0 then 'title:panel' end,
                  case when position('dps' in lower(coalesce(title,''))) > 0 then 'title:dps' end
                ]::text[], null))
              else '[]'::jsonb
            end,
            'value_currency', currency,
            'value_currency_is_eur', case when currency is null then null else (currency = 'EUR') end
          )
        ) as framework_evidence,
        award_date_evidence
      from enriched
    ),
    qa as (
      select
        count(*) as total_award_rows,
        count(*) filter (where award_date is not null and buyer_id is not null) as eligible_rows,
        count(*) filter (where award_date is null) as missing_award_date,
        count(*) filter (where buyer_id is null) as missing_buyer_id
      from final_rows
    ),
    upserted as (
      insert into award_fact_ie (
        tenant_id, source_system, award_canonical_id, buyer_id, supplier_id, award_date,
        award_value_eur, cpv_primary, cpv_cluster, procedure_type, framework_flag,
        duration_months, start_date, end_date, framework_evidence, award_date_evidence
      )
      select
        tenant_id, source_system, award_canonical_id, buyer_id, supplier_id, award_date,
        award_value_eur, cpv_primary, cpv_cluster, procedure_type, framework_flag,
        duration_months, start_date, end_date, framework_evidence, award_date_evidence
      from final_rows
      where award_date is not null
        and buyer_id is not null
      on conflict (award_canonical_id) do update set
        tenant_id = excluded.tenant_id,
        buyer_id = excluded.buyer_id,
        supplier_id = excluded.supplier_id,
        award_date = excluded.award_date,
        award_value_eur = excluded.award_value_eur,
        cpv_primary = excluded.cpv_primary,
        cpv_cluster = excluded.cpv_cluster,
        procedure_type = excluded.procedure_type,
        framework_flag = excluded.framework_flag,
        duration_months = excluded.duration_months,
        start_date = excluded.start_date,
        end_date = excluded.end_date,
        framework_evidence = excluded.framework_evidence,
        award_date_evidence = excluded.award_date_evidence
      returning (xmax = 0) as inserted_flag
    ),
    counts as (
      select
        count(*) filter (where inserted_flag) as inserted,
        count(*) filter (where not inserted_flag) as updated
      from upserted
    )
    select
      %27$L::text as source_relation,
      qa.total_award_rows::int,
      qa.eligible_rows::int,
      counts.inserted::int,
      counts.updated::int,
      qa.missing_award_date::int as skipped_missing_award_date,
      qa.missing_buyer_id::int as skipped_missing_buyer_id
    from counts, qa;
  $f$,
    v_tenant_col,          -- 1
    v_source_col,          -- 2
    v_id_col,              -- 3
    v_notice_type_col,     -- 4
    case when v_buyer_id_col is null then 'null::text' else format('t.%I::text', v_buyer_id_col) end,              -- 5
    case when v_buyer_name_col is null then 'null::text' else format('t.%I::text', v_buyer_name_col) end,          -- 6
    case when v_buyer_source_id_col is null then 'null::text' else format('t.%I::text', v_buyer_source_id_col) end,-- 7
    case when v_supplier_id_col is null then 'null::text' else format('t.%I::text', v_supplier_id_col) end,        -- 8
    case when v_supplier_name_col is null then 'null::text' else format('t.%I::text', v_supplier_name_col) end,    -- 9
    case when v_supplier_source_id_col is null then 'null::text' else format('t.%I::text', v_supplier_source_id_col) end, --10
    case when v_award_date_col is null then 'null::text' else format('t.%I::text', v_award_date_col) end,          --11
    case when v_date_awarded_col is null then 'null::text' else format('t.%I::text', v_date_awarded_col) end,      --12
    case when v_contract_award_date_col is null then 'null::text' else format('t.%I::text', v_contract_award_date_col) end, --13
    case when v_publication_date_col is null then 'null::text' else format('t.%I::text', v_publication_date_col) end, --14
    case when v_published_at_col is null then 'null::text' else format('t.%I::text', v_published_at_col) end,      --15
    case when v_value_col is null then 'null::text' else format('t.%I::text', v_value_col) end,                    --16
    case when v_currency_col is null then 'null::text' else format('t.%I::text', v_currency_col) end,              --17
    case when v_cpv_primary_col is null then 'null::text' else format('t.%I::text', v_cpv_primary_col) end,        --18
    case when v_cpv_codes_col is null then 'null::text' else format('t.%I::text', v_cpv_codes_col) end,            --19
    case when v_procedure_type_col is null then 'null::text' else format('t.%I::text', v_procedure_type_col) end,  --20
    case when v_title_col is null then 'null::text' else format('t.%I::text', v_title_col) end,                    --21
    case when v_description_col is null then 'null::text' else format('t.%I::text', v_description_col) end,        --22
    case when v_duration_months_col is null then 'null::text' else format('t.%I::text', v_duration_months_col) end,--23
    case when v_start_date_col is null then 'null::text' else format('t.%I::text', v_start_date_col) end,          --24
    case when v_end_date_col is null then 'null::text' else format('t.%I::text', v_end_date_col) end,              --25
    v_rel_text,                                                                                                     --26
    v_rel_text                                                                                                      --27
  );

  return query execute v_sql using p_tenant_id;
end;
$$;

-- =====================================================================================
-- Refresh: award_signals_ie (full recompute, idempotent upsert by buyer_id + derived cluster)
-- =====================================================================================
create or replace function refresh_award_signals_ie()
returns table (
  inserted int,
  updated int
)
language sql
as $$
  with
  base as (
    select
      buyer_id,
      coalesce(cpv_cluster, cpv_primary, 'UNKNOWN') as cluster_key,
      award_date,
      supplier_id,
      award_value_eur,
      cpv_cluster,
      cpv_primary,
      duration_months,
      start_date,
      end_date
    from award_fact_ie
    where buyer_id is not null
      and award_date is not null
  ),
  ordered as (
    select
      b.*,
      lag(award_date) over (partition by buyer_id, cluster_key order by award_date) as prev_award_date,
      lag(supplier_id) over (partition by buyer_id, cluster_key order by award_date) as prev_supplier_id
    from base b
  ),
  cycles as (
    select
      buyer_id,
      cluster_key,
      (award_date - prev_award_date)::numeric as cycle_days
    from ordered
    where prev_award_date is not null
  ),
  cycle_stats as (
    select
      buyer_id,
      cluster_key,
      count(*) as cycles_count,
      avg(cycle_days) as avg_cycle_days,
      stddev_samp(cycle_days) as cycle_std_days
    from cycles
    group by 1,2
  ),
  supplier_changes as (
    select
      buyer_id,
      cluster_key,
      count(*) filter (
        where prev_supplier_id is not null
          and supplier_id is not null
          and supplier_id <> prev_supplier_id
      ) as supplier_change_events
    from ordered
    group by 1,2
  ),
  supplier_top as (
    select
      buyer_id,
      cluster_key,
      max(cnt) as top_supplier_count
    from (
      select
        buyer_id,
        cluster_key,
        supplier_id,
        count(*) as cnt
      from base
      where supplier_id is not null
      group by 1,2,3
    ) x
    group by 1,2
  ),
  agg as (
    select
      buyer_id,
      cluster_key,
      max(award_date) as last_award_date,
      count(*)::int as awards_count
    from base
    group by 1,2
  ),
  last_row as (
    select distinct on (buyer_id, cluster_key)
      buyer_id,
      cluster_key,
      award_date,
      award_value_eur,
      duration_months,
      cpv_cluster,
      cpv_primary
    from base
    order by buyer_id, cluster_key, award_date desc
  ),
  metrics as (
    select
      a.buyer_id,
      a.cluster_key as cpv_cluster,
      a.last_award_date,
      a.awards_count,
      case when cs.cycles_count >= 2 then cs.avg_cycle_days else null end as avg_cycle_days,
      case when cs.cycles_count >= 2 then cs.cycle_std_days else null end as cycle_std_days,
      case
        when cs.cycles_count >= 2 and cs.avg_cycle_days is not null and cs.avg_cycle_days > 0 then
          least(greatest(1 - (coalesce(cs.cycle_std_days, 0) / cs.avg_cycle_days), 0), 1)::numeric
        else 0::numeric
      end as cycle_consistency_score,
      case
        when coalesce(st.top_supplier_count, 0) = 0 then 0::numeric
        else (st.top_supplier_count::numeric / a.awards_count::numeric)
      end as supplier_concentration,
      case
        when a.awards_count <= 1 then 0::numeric
        else (coalesce(sc.supplier_change_events, 0)::numeric / (a.awards_count - 1)::numeric)
      end as switch_rate,
      lr.duration_months as last_duration_months,
      lr.award_value_eur as last_award_value_eur,
      lr.cpv_cluster as last_cpv_cluster_raw,
      lr.cpv_primary as last_cpv_primary
    from agg a
    left join cycle_stats cs
      on cs.buyer_id = a.buyer_id and cs.cluster_key = a.cluster_key
    left join supplier_changes sc
      on sc.buyer_id = a.buyer_id and sc.cluster_key = a.cluster_key
    left join supplier_top st
      on st.buyer_id = a.buyer_id and st.cluster_key = a.cluster_key
    left join last_row lr
      on lr.buyer_id = a.buyer_id and lr.cluster_key = a.cluster_key
  ),
  predicted as (
    select
      m.*,
      case
        when m.awards_count >= 3 and m.avg_cycle_days is not null
          then (m.last_award_date + round(m.avg_cycle_days)::int)
        when m.last_duration_months is not null
          then (m.last_award_date + (m.last_duration_months::text || ' months')::interval)::date
        else (m.last_award_date + 365)
      end as expected_next_tender_date,
      case
        when m.awards_count >= 3 and m.avg_cycle_days is not null
          then greatest(30::numeric, coalesce(m.cycle_std_days, 30::numeric))
        when m.last_duration_months is not null
          then 60::numeric
        else 90::numeric
      end as window_days,
      -- Probabilities (v1)
      least(greatest(0.2 + 0.6*m.supplier_concentration + 0.2*(1 - m.switch_rate), 0), 1) as renewal_probability,
      least(greatest(0.2 + 0.6*m.switch_rate + 0.2*(1 - m.supplier_concentration), 0), 1) as switching_probability
    from metrics m
  ),
  finalized as (
    select
      buyer_id,
      cpv_cluster,
      last_award_date,
      awards_count,
      avg_cycle_days,
      cycle_std_days,
      cycle_consistency_score,
      supplier_concentration,
      switch_rate,
      expected_next_tender_date,
      (expected_next_tender_date - ceil(window_days)::int) as expected_window_start,
      (expected_next_tender_date + ceil(window_days)::int) as expected_window_end,
      renewal_probability,
      switching_probability,
      least(greatest(
        (
          20
          + case when awards_count >= 3 then 20 else 0 end
          + (20 * cycle_consistency_score)
          + case when last_award_value_eur is not null then 10 else 0 end
          + case when cpv_cluster <> 'UNKNOWN' then 10 else 0 end
          - case when last_award_date < (current_date - interval '5 years') then 20 else 0 end
        )::int,
        0
      ), 100) as confidence_score,
      to_jsonb(array_remove(array[
        case
          when awards_count >= 3 and avg_cycle_days is not null
            then format('Using buyer cycle avg %s days from %s awards', round(avg_cycle_days)::int, awards_count)
        end,
        format('Consistency score %s', round(cycle_consistency_score::numeric, 2)),
        format('Switch rate %s', round(switch_rate::numeric, 2)),
        case
          when cpv_cluster = 'UNKNOWN' then 'CPV cluster missing: using UNKNOWN fallback'
          when last_cpv_cluster_raw is null and last_cpv_primary is not null then format('CPV cluster missing: using cpv_primary fallback (%s)', last_cpv_primary)
        end,
        case
          when awards_count < 3 or avg_cycle_days is null then
            case when last_duration_months is not null then 'Low cycle data: using last award duration_months' else 'Low data: defaulting to 12 months' end
        end
      ]::text[], null)) as explanations
    from predicted
  ),
  upserted as (
    insert into award_signals_ie (
      buyer_id, cpv_cluster, last_award_date, awards_count,
      avg_cycle_days, cycle_std_days, cycle_consistency_score,
      supplier_concentration, switch_rate,
      expected_next_tender_date, expected_window_start, expected_window_end,
      renewal_probability, switching_probability,
      confidence_score, explanations, updated_at
    )
    select
      buyer_id, cpv_cluster, last_award_date, awards_count,
      avg_cycle_days, cycle_std_days, cycle_consistency_score,
      supplier_concentration, switch_rate,
      expected_next_tender_date, expected_window_start, expected_window_end,
      renewal_probability, switching_probability,
      confidence_score, explanations, now()
    from finalized
    on conflict (buyer_id, cpv_cluster) do update set
      last_award_date = excluded.last_award_date,
      awards_count = excluded.awards_count,
      avg_cycle_days = excluded.avg_cycle_days,
      cycle_std_days = excluded.cycle_std_days,
      cycle_consistency_score = excluded.cycle_consistency_score,
      supplier_concentration = excluded.supplier_concentration,
      switch_rate = excluded.switch_rate,
      expected_next_tender_date = excluded.expected_next_tender_date,
      expected_window_start = excluded.expected_window_start,
      expected_window_end = excluded.expected_window_end,
      renewal_probability = excluded.renewal_probability,
      switching_probability = excluded.switching_probability,
      confidence_score = excluded.confidence_score,
      explanations = excluded.explanations,
      updated_at = excluded.updated_at
    returning (xmax = 0) as inserted_flag
  )
  select
    count(*) filter (where inserted_flag)::int as inserted,
    count(*) filter (where not inserted_flag)::int as updated
  from upserted;
$$;

commit;
