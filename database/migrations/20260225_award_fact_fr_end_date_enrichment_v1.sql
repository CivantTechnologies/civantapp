-- Migration: 20260225_award_fact_fr_end_date_enrichment_v1.sql
-- Goal: backfill FR award duration/start/end fields from embedded BOAMP raw payload.
-- Notes:
--   - Additive, idempotent, replay-safe.
--   - Evidence-backed only: no synthetic default duration.
-- Rollback plan:
--   - drop function if exists public.civant_fr__boamp_raw_blob(jsonb);
--   - drop function if exists public.civant_fr__extract_duration_months_from_boamp_raw(text);
--   - drop function if exists public.civant_fr__extract_start_date_from_boamp_raw(text);
--   - Data updates can be reverted by restoring from backup snapshot.

begin;

create or replace function public.civant_fr__boamp_raw_blob(p_data jsonb)
returns text
language sql
stable
as $$
  select coalesce(p_data->'raw'->>'donnees', p_data->'raw'->>'DONNEES', '');
$$;

create or replace function public.civant_fr__extract_duration_months_from_boamp_raw(p_raw text)
returns integer
language plpgsql
stable
as $$
declare
  v_raw text := lower(coalesce(p_raw, ''));
  v_match text[];
  v_token text;
  v_num integer;
begin
  if v_raw = '' then
    return null;
  end if;

  -- Direct month fields.
  v_match := regexp_match(v_raw, '"(?:duree_mois|dureemois)"[[:space:]]*:[[:space:]]*"?([0-9]{1,3})"?');
  if v_match is not null then
    v_num := v_match[1]::integer;
    if v_num between 1 and 240 then
      return v_num;
    end if;
  end if;

  -- Explicit year/day variants.
  v_match := regexp_match(v_raw, '"duree_an"[[:space:]]*:[[:space:]]*"?([0-9]{1,3})"?');
  if v_match is not null then
    v_num := greatest(1, least(240, v_match[1]::integer * 12));
    return v_num;
  end if;

  v_match := regexp_match(v_raw, '"(?:duree_jour|duree_jours)"[[:space:]]*:[[:space:]]*"?([0-9]{1,4})"?');
  if v_match is not null then
    v_num := greatest(1, least(240, ceil(v_match[1]::numeric / 30.0)::integer));
    return v_num;
  end if;

  -- Generic duration text field.
  v_match := regexp_match(v_raw, '"(?:duree_delai|duree|dureelot)"[[:space:]]*:[[:space:]]*"?([^"\\},\\]]{1,80})"?');
  if v_match is null then
    return null;
  end if;

  v_token := v_match[1];
  v_match := regexp_match(v_token, '([0-9]{1,4})');
  if v_match is null then
    return null;
  end if;

  v_num := v_match[1]::integer;
  if v_token ~ '(an|annee|year)' then
    v_num := v_num * 12;
  elsif v_token ~ '(jour|day)' then
    v_num := ceil(v_num::numeric / 30.0)::integer;
  end if;

  if v_num between 1 and 240 then
    return v_num;
  end if;
  return null;
end;
$$;

create or replace function public.civant_fr__extract_start_date_from_boamp_raw(p_raw text)
returns date
language plpgsql
stable
as $$
declare
  v_raw text := lower(coalesce(p_raw, ''));
  v_match text[];
begin
  if v_raw = '' then
    return null;
  end if;

  v_match := regexp_match(
    v_raw,
    '"(?:date_debut|datedebutprestation|datedebuttravaux|cbc:startdate)"[[:space:]]*:[[:space:]]*"?([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{2}/[0-9]{2}/[0-9]{4})"?'
  );
  if v_match is null then
    return null;
  end if;

  return public.civant__parse_date(v_match[1]);
end;
$$;

with candidate as (
  select
    af.tenant_id,
    af.award_canonical_id,
    coalesce(af.duration_months, public.civant_fr__extract_duration_months_from_boamp_raw(rb.raw_blob)) as duration_final,
    coalesce(af.start_date, public.civant_fr__extract_start_date_from_boamp_raw(rb.raw_blob)) as start_final,
    af.award_date,
    af.duration_months as duration_existing,
    af.start_date as start_existing,
    af.end_date as end_existing,
    af.framework_evidence as framework_existing
  from public.award_fact_fr af
  join public."TendersCurrent" tc
    on tc.tenant_id = af.tenant_id
   and tc.tender_id = af.award_canonical_id
  cross join lateral (
    select public.civant_fr__boamp_raw_blob(tc.data) as raw_blob
  ) rb
  where af.tenant_id = 'civant_default'
    and tc.source = 'BOAMP_FR'
    and rb.raw_blob <> ''
    and (
      position('duree' in lower(rb.raw_blob)) > 0
      or position('startdate' in lower(rb.raw_blob)) > 0
      or position('date_debut' in lower(rb.raw_blob)) > 0
      or position('datedebutprestation' in lower(rb.raw_blob)) > 0
      or position('datedebuttravaux' in lower(rb.raw_blob)) > 0
    )
), resolved as (
  select
    tenant_id,
    award_canonical_id,
    duration_existing,
    start_existing,
    end_existing,
    framework_existing,
    duration_final,
    start_final,
    coalesce(start_final, award_date) as anchor_date,
    case
      when duration_final is not null
        and duration_final > 0
        and coalesce(start_final, award_date) is not null
      then (coalesce(start_final, award_date) + make_interval(months => duration_final))::date
      else null
    end as end_final
  from candidate
), updated as (
  update public.award_fact_fr af
  set
    duration_months = r.duration_final,
    start_date = r.start_final,
    end_date = coalesce(af.end_date, r.end_final),
    framework_evidence = case
      when (af.duration_months is distinct from r.duration_final)
        or (af.start_date is distinct from r.start_final)
        or (af.end_date is null and r.end_final is not null)
      then jsonb_strip_nulls(
        coalesce(r.framework_existing, '{}'::jsonb) ||
        jsonb_build_object(
          'fr_duration_months_source', case when af.duration_months is null and r.duration_final is not null then 'boamp_raw_embedded' end,
          'fr_start_date_source', case when af.start_date is null and r.start_final is not null then 'boamp_raw_embedded' end,
          'fr_end_date_source', case
            when af.end_date is null and r.end_final is not null and r.start_final is not null then 'start_date_plus_duration_months'
            when af.end_date is null and r.end_final is not null and r.start_final is null then 'award_date_plus_duration_months'
          end
        )
      )
      else af.framework_evidence
    end
  from resolved r
  where af.tenant_id = r.tenant_id
    and af.award_canonical_id = r.award_canonical_id
    and (
      af.duration_months is distinct from r.duration_final
      or af.start_date is distinct from r.start_final
      or (af.end_date is null and r.end_final is not null)
    )
  returning 1
)
select count(*) as updated_rows from updated;

commit;
