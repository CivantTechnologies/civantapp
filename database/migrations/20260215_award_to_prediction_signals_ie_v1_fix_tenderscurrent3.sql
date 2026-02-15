-- Civant: Award-to-Prediction Signals v1 (Ireland only) - Fix 3
-- Force extraction from public."TendersCurrent" (jsonb-based canonical) because this environment
-- does not have a structured canonical awards table with notice_type, and prior regclass guards
-- behaved unexpectedly.

begin;

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
      tc.data as d,
      tc.published_at
    from public."TendersCurrent" tc
    where tc.source = 'ETENDERS_IE'
      and (p_tenant_id is null or tc.tenant_id = p_tenant_id)
      and lower(coalesce(tc.data->>'notice_type','')) = 'award'
  ),
  normalized as (
    select
      tenant_id,
      'ETENDERS_IE'::text as source_system,
      award_canonical_id,
      coalesce(
        nullif(d->>'buyer_id',''),
        nullif(d->>'buyer_entity_id',''),
        civant_ie__stable_id('IE_BUYER:', tenant_id, 'ETENDERS_IE', null, nullif(coalesce(d->>'contracting_authority', d->>'buyer_name'), ''))
      ) as buyer_id,
      nullif(
        coalesce(
          nullif(d->>'supplier_id',''),
          nullif(d->>'supplier_entity_id',''),
          nullif(d->>'winner_id',''),
          civant_ie__stable_id('IE_SUPPLIER:', tenant_id, 'ETENDERS_IE', null, nullif(coalesce(d->>'awarded_suppliers', d->>'winner_name', d->>'supplier_name'), ''))
        ),
        ''
      ) as supplier_id,
      coalesce(
        civant__parse_date(nullif(d->>'award_date','')),
        civant__parse_date(nullif(d->>'date_awarded','')),
        civant__parse_date(nullif(d->>'contract_award_date','')),
        civant__parse_date(nullif(d->>'award_published','')),
        civant__parse_date(nullif(d->>'publication_date','')),
        (published_at::date)
      ) as award_date,
      case
        when civant__parse_date(nullif(d->>'award_date','')) is not null then jsonb_build_object('source_field','data.award_date','raw',d->>'award_date')
        when civant__parse_date(nullif(d->>'date_awarded','')) is not null then jsonb_build_object('source_field','data.date_awarded','raw',d->>'date_awarded')
        when civant__parse_date(nullif(d->>'contract_award_date','')) is not null then jsonb_build_object('source_field','data.contract_award_date','raw',d->>'contract_award_date')
        when civant__parse_date(nullif(d->>'award_published','')) is not null then jsonb_build_object('source_field','data.award_published','raw',d->>'award_published')
        when civant__parse_date(nullif(d->>'publication_date','')) is not null then jsonb_build_object('source_field','data.publication_date','raw',d->>'publication_date')
        when published_at is not null then jsonb_build_object('source_field','published_at','raw',published_at::text)
        else null
      end as award_date_evidence,
      coalesce(
        civant__parse_numeric(nullif(d->>'award_value_eur','')),
        civant__parse_numeric(nullif(d->>'awarded_value','')),
        civant__parse_numeric(nullif(d->>'contract_value','')),
        civant__parse_numeric(nullif(d->>'estimated_value',''))
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
      nullif(coalesce(d->>'procedure_type', d->>'procedure'), '') as procedure_type,
      nullif(coalesce(d->>'contract_duration_months', d->>'duration_months'), '') as duration_months_raw,
      civant__parse_date(nullif(d->>'start_date','')) as start_date,
      civant__parse_date(nullif(d->>'end_date','')) as end_date,
      nullif(coalesce(d->>'title', d->>'tender_name'), '') as title,
      nullif(d->>'description','') as description,
      nullif(d->>'currency','') as currency
    from src
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
      award_canonical_id,
      buyer_id,
      supplier_id,
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
          'framework_detected', framework_detected,
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
    'public."TendersCurrent"'::text as source_relation,
    qa.total_award_rows::int,
    qa.eligible_rows::int,
    counts.inserted::int,
    counts.updated::int,
    qa.missing_award_date::int as skipped_missing_award_date,
    qa.missing_buyer_id::int as skipped_missing_buyer_id
  from counts, qa;
$$;

commit;

