-- Civant: Award-to-Prediction Signals v1 (Ireland only) - Option A (embedded award fields)
-- Staging finding: ETENDERS_IE awards are represented as tender rows in public."TendersCurrent".data
-- with embedded award fields (e.g. award_published, awarded_value, awarded_suppliers).
--
-- Guardrails:
-- - Do not change canonical tables.
-- - Treat a row as an award-event iff:
--   - source = 'ETENDERS_IE'
--   - award_published is not null
--   - award_published parses as DD/MM/YYYY
-- - award_canonical_id remains the tender canonical id (tender_id).
-- - Idempotent upsert: one row per award_canonical_id; updates if award_published changes.

begin;

alter table if exists award_fact_ie
  add column if not exists award_event_source text not null default 'embedded_award_fields';

alter table if exists award_fact_ie
  add column if not exists supplier_name_raw text null;

-- Strict DD/MM/YYYY parse. Returns NULL if invalid (including impossible dates).
create or replace function civant__parse_date_ddmmyyyy(p text)
returns date
language sql
immutable
as $$
  select case
    when p is null then null
    when trim(p) ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}' then
      case
        when to_char(to_date(substring(trim(p) from 1 for 10), 'DD/MM/YYYY'), 'DD/MM/YYYY') = substring(trim(p) from 1 for 10)
          then to_date(substring(trim(p) from 1 for 10), 'DD/MM/YYYY')
        else null
      end
    else null
  end;
$$;

-- Best-effort supplier name extraction from awarded_suppliers (jsonb):
-- - array: first element string or first object.name
-- - string: trimmed, leading pipes removed
create or replace function civant_ie__supplier_name_from_awarded_suppliers(p_awarded_suppliers jsonb)
returns text
language sql
immutable
as $$
  select case
    when p_awarded_suppliers is null then null
    when jsonb_typeof(p_awarded_suppliers) = 'array' then
      case
        when jsonb_typeof(p_awarded_suppliers->0) = 'object'
          then nullif(trim(p_awarded_suppliers->0->>'name'), '')
        when jsonb_typeof(p_awarded_suppliers->0) = 'string'
          then nullif(trim(both '|' from trim(p_awarded_suppliers->>0)), '')
        else null
      end
    when jsonb_typeof(p_awarded_suppliers) = 'string' then
      nullif(trim(both '|' from trim(p_awarded_suppliers::text, '\"')), '')
    else null
  end;
$$;

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
language sql
as $$
  with
  src as (
    select
      tc.tenant_id::text as tenant_id,
      tc.tender_id::text as award_canonical_id,
      tc.data as d
    from public."TendersCurrent" tc
    where tc.source = 'ETENDERS_IE'
      and (p_tenant_id is null or tc.tenant_id = p_tenant_id)
      and nullif(tc.data->>'award_published','') is not null
  ),
  detected as (
    select
      tenant_id,
      award_canonical_id,
      d,
      civant__parse_date_ddmmyyyy(nullif(d->>'award_published','')) as award_date
    from src
  ),
  normalized as (
    select
      tenant_id,
      'ETENDERS_IE'::text as source_system,
      'embedded_award_fields'::text as award_event_source,
      award_canonical_id,
      coalesce(
        nullif(d->>'buyer_id',''),
        nullif(d->>'buyer_entity_id',''),
        civant_ie__stable_id(
          'IE_BUYER:',
          tenant_id,
          'ETENDERS_IE',
          nullif(d->>'source_notice_id',''),
          nullif(coalesce(d->>'contracting_authority', d->>'buyer_name'), '')
        )
      ) as buyer_id,
      nullif(
        coalesce(
          nullif(d->>'supplier_id',''),
          nullif(d->>'supplier_entity_id',''),
          nullif(d->>'winner_id','')
        ),
        ''
      ) as supplier_id,
      nullif(
        coalesce(
          civant_ie__supplier_name_from_awarded_suppliers(d->'awarded_suppliers'),
          nullif(d->>'winner_name',''),
          nullif(d->>'supplier_name','')
        ),
        ''
      ) as supplier_name_raw,
      award_date,
      jsonb_build_object(
        'field', 'award_published',
        'format', 'DD/MM/YYYY',
        'raw', d->>'award_published'
      ) as award_date_evidence,
      -- Award value: prefer awarded_value, else awarded_value_eur if present.
      coalesce(
        civant__parse_numeric(nullif(d->>'awarded_value','')),
        civant__parse_numeric(nullif(d->>'awarded_value_eur',''))
      ) as award_value_eur,
      nullif(
        regexp_replace(
          coalesce(
            nullif(d->>'main_cpv_code',''),
            nullif(d->>'cpv_primary',''),
            nullif(split_part(coalesce(d->>'cpv_codes',''), ',', 1), '')
          ),
          '[^0-9-]',
          '',
          'g'
        ),
        ''
      ) as cpv_primary,
      null::text as cpv_cluster,
      nullif(coalesce(d->>'procedure_type', d->>'procedure'), '') as procedure_type,
      nullif(coalesce(d->>'contract_duration_months', d->>'duration_months'), '') as duration_months_raw,
      civant__parse_date(nullif(d->>'start_date','')) as start_date,
      civant__parse_date(nullif(d->>'end_date','')) as end_date,
      nullif(coalesce(d->>'title', d->>'tender_name'), '') as title,
      nullif(d->>'description','') as description,
      nullif(d->>'currency','') as currency,
      d->'awarded_suppliers' as awarded_suppliers_raw,
      d
    from detected
  ),
  enriched as (
    select
      n.*,
      m.cpv_cluster as mapped_cpv_cluster,
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
      award_event_source,
      award_canonical_id,
      buyer_id,
      supplier_id,
      supplier_name_raw,
      award_date,
      award_value_eur,
      cpv_primary,
      mapped_cpv_cluster as cpv_cluster,
      procedure_type,
      framework_detected as framework_flag,
      coalesce(
        case when duration_months_raw <> '' then civant__parse_numeric(duration_months_raw)::int end,
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
          'award_event_source', award_event_source,
          'framework_detected', framework_detected,
          'awarded_suppliers_raw', awarded_suppliers_raw,
          'award_value_field', case when nullif(d->>'awarded_value','') is not null then 'awarded_value' when nullif(d->>'awarded_value_eur','') is not null then 'awarded_value_eur' end,
          'value_currency', currency,
          'value_currency_is_eur', case when currency is null then null else (currency = 'EUR') end
        )
      ) as framework_evidence,
      award_date_evidence
    from enriched
    where award_date is not null -- parseable award_published required
  ),
  qa as (
    select
      count(*) as total_award_rows,
      count(*) filter (where award_date is not null and buyer_id is not null) as eligible_rows,
      0::bigint as missing_award_date, -- enforced by where clause above
      count(*) filter (where buyer_id is null) as missing_buyer_id
    from final_rows
  ),
  upserted as (
    insert into award_fact_ie (
      tenant_id, source_system, award_canonical_id, award_event_source,
      buyer_id, supplier_id, supplier_name_raw, award_date,
      award_value_eur, cpv_primary, cpv_cluster, procedure_type, framework_flag,
      duration_months, start_date, end_date, framework_evidence, award_date_evidence
    )
    select
      tenant_id, source_system, award_canonical_id, award_event_source,
      buyer_id, supplier_id, supplier_name_raw, award_date,
      award_value_eur, cpv_primary, cpv_cluster, procedure_type, framework_flag,
      duration_months, start_date, end_date, framework_evidence, award_date_evidence
    from final_rows
    where buyer_id is not null
    on conflict (award_canonical_id) do update set
      tenant_id = excluded.tenant_id,
      source_system = excluded.source_system,
      award_event_source = excluded.award_event_source,
      buyer_id = excluded.buyer_id,
      supplier_id = excluded.supplier_id,
      supplier_name_raw = excluded.supplier_name_raw,
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
    'public."TendersCurrent"'::text as source_relation,
    qa.total_award_rows::int,
    qa.eligible_rows::int,
    counts.inserted::int,
    counts.updated::int,
    0::int as skipped_missing_award_date,
    qa.missing_buyer_id::int as skipped_missing_buyer_id
  from counts, qa;
$$;

commit;

