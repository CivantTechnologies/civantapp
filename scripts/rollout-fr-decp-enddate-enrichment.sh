#!/usr/bin/env bash
set -euo pipefail

# FR DECP end-date enrichment (2020+ by default)
#
# Ingests FR DECP awards from data.economie.gouv.fr and enriches award_fact_fr rows
# that still miss end_date using a strict deterministic join strategy:
#   1) Normalized notice id match (separator-agnostic)
#   2) Normalized base-id match (drop lot suffix "-<n>")
#   3) Structured high-confidence match with uniqueness guard:
#      buyer_id9 + award_date + cpv8 + amount tolerance
#      buyer_id9 + award_date + title-prefix + (cpv8 or amount)
#
# Updates are evidence-tagged in framework_evidence and are replay-safe.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEFAULT_PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "${PSQL_BIN}" ]]; then
  if [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
    PSQL_BIN="${DEFAULT_PSQL_BIN}"
  else
    PSQL_BIN="psql"
  fi
fi

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${1:-${TENANT_ID:-civant_default}}"
START_DATE="${2:-${START_DATE:-2020-01-01}}"
DRY_RUN_RAW="${3:-${DRY_RUN:-false}}"
MAX_ROWS="${MAX_ROWS:-}"
DATASET="${DATASET:-decp-2022-marches-valides}"
SOURCE_NAME="${SOURCE_NAME:-decp_fr}"
BATCH_ID="${BATCH_ID:-fr_decp_$(date -u +%Y%m%dT%H%M%SZ)_$RANDOM}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: DATABASE_URL (or SUPABASE_DB_URL) is required."
  exit 1
fi

if [[ ! "${START_DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: START_DATE must be YYYY-MM-DD. Got: ${START_DATE}"
  exit 1
fi

if [[ -n "${MAX_ROWS}" && ! "${MAX_ROWS}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_ROWS must be an integer."
  exit 1
fi

DRY_RUN="false"
case "${DRY_RUN_RAW}" in
  1|true|TRUE|yes|YES|y|Y|on|ON) DRY_RUN="true" ;;
  0|false|FALSE|no|NO|n|N|off|OFF|"") DRY_RUN="false" ;;
  *)
    echo "ERROR: DRY_RUN must be true/false. Got: ${DRY_RUN_RAW}"
    exit 1
    ;;
esac

APPLY_SQL_BOOL="true"
if [[ "${DRY_RUN}" == "true" ]]; then
  APPLY_SQL_BOOL="false"
fi

if [[ "${PSQL_BIN}" == */* ]]; then
  if [[ ! -x "${PSQL_BIN}" ]]; then
    echo "ERROR: psql not found at ${PSQL_BIN}"
    exit 1
  fi
else
  if ! command -v "${PSQL_BIN}" >/dev/null 2>&1; then
    echo "ERROR: psql not found on PATH: ${PSQL_BIN}"
    exit 1
  fi
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

if ! command -v node >/dev/null 2>&1; then
  echo "ERROR: node is required but was not found on PATH."
  exit 1
fi

TMP_DIR="${TMPDIR:-/tmp}"
CSV_FILE="$(mktemp "${TMP_DIR%/}/civant_fr_decp_XXXXXX.csv" 2>/dev/null || mktemp -t civant_fr_decp)"
SQL_FILE="$(mktemp "${TMP_DIR%/}/civant_fr_decp_XXXXXX.sql" 2>/dev/null || mktemp -t civant_fr_decp_sql)"
RUN_SQL_FILE="$(mktemp "${TMP_DIR%/}/civant_fr_decp_XXXXXX.run.sql" 2>/dev/null || mktemp -t civant_fr_decp_run_sql)"
PSQL_RETRY_ATTEMPTS="${PSQL_RETRY_ATTEMPTS:-5}"
PSQL_RETRY_DELAY_SEC="${PSQL_RETRY_DELAY_SEC:-5}"

cleanup() {
  rm -f "${CSV_FILE}" "${SQL_FILE}" "${RUN_SQL_FILE}" 2>/dev/null || true
}
trap cleanup EXIT

run_psql_file_with_retry() {
  local sql_path="$1"
  local attempt=1
  while (( attempt <= PSQL_RETRY_ATTEMPTS )); do
    if "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -f "${sql_path}"; then
      return 0
    fi
    if (( attempt == PSQL_RETRY_ATTEMPTS )); then
      return 1
    fi
    echo "psql_retry attempt=${attempt}/${PSQL_RETRY_ATTEMPTS} sleeping=${PSQL_RETRY_DELAY_SEC}s"
    sleep "${PSQL_RETRY_DELAY_SEC}"
    attempt=$((attempt + 1))
  done
}

WHERE_RAW="datenotification >= date'${START_DATE}'"
SELECT_RAW="id,datenotification,dureemois,montant,codecpv,objet,acheteur_id,titulaire_id_1,source"

WHERE_ENC="$(node -e "console.log(encodeURIComponent(process.argv[1]))" "${WHERE_RAW}")"
SELECT_ENC="$(node -e "console.log(encodeURIComponent(process.argv[1]))" "${SELECT_RAW}")"

URL="https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/${DATASET}/exports/csv?where=${WHERE_ENC}&select=${SELECT_ENC}"
if [[ -n "${MAX_ROWS}" ]]; then
  URL="${URL}&limit=${MAX_ROWS}"
fi

echo "== FR DECP End-Date Enrichment =="
echo "tenant_id=${TENANT_ID} start_date=${START_DATE} dry_run=${DRY_RUN} dataset=${DATASET} source_name=${SOURCE_NAME} batch_id=${BATCH_ID} max_rows=${MAX_ROWS:-<none>}"
echo "== Downloading DECP CSV =="
curl -fsSL "${URL}" -o "${CSV_FILE}"

LINE_COUNT="$(wc -l < "${CSV_FILE}" | tr -d ' ')"
if [[ "${LINE_COUNT}" -le 1 ]]; then
  echo "ERROR: DECP export returned no data rows."
  exit 1
fi
echo "downloaded_rows=$((LINE_COUNT - 1))"

cat > "${SQL_FILE}" <<SQL
\\set ON_ERROR_STOP on
\\pset pager off
\\timing on

set statement_timeout = 0;
set lock_timeout = 0;
set idle_in_transaction_session_timeout = 0;

begin;

do \$\$
begin
  if to_regclass('public.fr_award_enrichment_batches') is null
     or to_regclass('public.fr_award_enrichment_raw') is null
     or to_regclass('public.fr_award_enrichment_stage') is null
     or to_regclass('public.fr_award_enrichment_rejects') is null
     or to_regclass('public.fr_award_enrichment_run_metrics') is null then
    raise exception 'Missing FR enrichment gate tables. Apply migration 20260226_fr_award_enrichment_gate_v1.sql first.';
  end if;
end
\$\$;

create temp table tmp_fr_decp_raw (
  id text,
  datenotification text,
  dureemois text,
  montant text,
  codecpv text,
  objet text,
  acheteur_id text,
  titulaire_id_1 text,
  source text
);
\\copy tmp_fr_decp_raw (id,datenotification,dureemois,montant,codecpv,objet,acheteur_id,titulaire_id_1,source) from '${CSV_FILE}' with (format csv, delimiter ';', header true);

create temp table tmp_fr_decp_norm as
select
  trim(id) as decp_id,
  case
    when trim(id) ~ '^[0-9]{2}-[0-9]+-[0-9]+$' then regexp_replace(trim(id), '-[0-9]+$', '')
    else trim(id)
  end as decp_id_base,
  nullif(regexp_replace(lower(trim(id)), '[^a-z0-9]', '', 'g'), '') as decp_id_key,
  nullif(regexp_replace(
    lower(
      case
        when trim(id) ~ '^[0-9]{2}-[0-9]+-[0-9]+$' then regexp_replace(trim(id), '-[0-9]+$', '')
        else trim(id)
      end
    ),
    '[^a-z0-9]',
    '',
    'g'
  ), '') as decp_id_base_key,
  case when datenotification ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then datenotification::date else null end as award_date,
  case
    when nullif(trim(dureemois), '') is null then null
    when lower(trim(dureemois)) in ('cdl', 'null', 'nan') then null
    when trim(dureemois) ~ '^[0-9]+$' then least(240, greatest(1, trim(dureemois)::int))
    else null
  end as duration_months,
  case
    when nullif(trim(montant), '') is null then null
    when lower(trim(montant)) in ('cdl', 'null', 'nan') then null
    else nullif(regexp_replace(trim(montant), '[^0-9.,-]', '', 'g'), '')::numeric
  end as amount_eur,
  nullif(regexp_replace(coalesce(codecpv,''), '[^0-9]', '', 'g'), '') as cpv_digits,
  nullif(btrim(regexp_replace(lower(coalesce(objet,'')), '[^a-z0-9]+', ' ', 'g')), '') as object_norm,
  nullif(trim(acheteur_id), '') as acheteur_id,
  nullif(regexp_replace(coalesce(acheteur_id,''), '[^0-9]', '', 'g'), '') as acheteur_id_digits,
  case
    when length(nullif(regexp_replace(coalesce(acheteur_id,''), '[^0-9]', '', 'g'), '')) >= 9
      then left(nullif(regexp_replace(coalesce(acheteur_id,''), '[^0-9]', '', 'g'), ''), 9)
    else null
  end as acheteur_id9,
  nullif(trim(titulaire_id_1), '') as titulaire_id_1,
  nullif(trim(source), '') as source_system
from tmp_fr_decp_raw
where nullif(trim(id), '') is not null
  and (case when datenotification ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then datenotification::date else null end) >= '${START_DATE}'::date;

create index on tmp_fr_decp_norm (decp_id);
create index on tmp_fr_decp_norm (decp_id_base);
create index on tmp_fr_decp_norm (decp_id_key);
create index on tmp_fr_decp_norm (decp_id_base_key);
create index on tmp_fr_decp_norm (award_date);
create index on tmp_fr_decp_norm (acheteur_id);
create index on tmp_fr_decp_norm (award_date, acheteur_id9);

\\echo 'DECP normalized rows'
select count(*) as decp_rows from tmp_fr_decp_norm;

create temp table tmp_fr_award_candidates as
with af as (
  select
    af.tenant_id,
    af.award_canonical_id,
    split_part(af.award_canonical_id, ':', 2) as source_notice_id,
    af.award_date,
    af.award_value_eur,
    af.end_date,
    nullif(left(regexp_replace(coalesce(af.cpv_primary,''), '[^0-9]', '', 'g'), 8), '') as cpv8,
    tc.data->'raw'->>'objet' as raw_objet,
    coalesce(tc.data->'raw'->>'DONNEES', tc.data->'raw'->>'donnees') as boamp_donnees_text
  from public.award_fact_fr af
  join public."TendersCurrent" tc
    on tc.tenant_id = af.tenant_id
   and tc.tender_id = af.award_canonical_id
  where af.tenant_id = '${TENANT_ID}'
    and af.award_date >= '${START_DATE}'::date
    and af.end_date is null
    and tc.source = 'BOAMP_FR'
), enriched as (
  select
    af.*,
    case
      when nullif(af.boamp_donnees_text, '') is not null then af.boamp_donnees_text::jsonb
      else null::jsonb
    end as boamp_donnees
  from af
), normalized as (
  select
    e.tenant_id,
    e.award_canonical_id,
    e.source_notice_id,
    nullif(regexp_replace(lower(coalesce(e.source_notice_id, '')), '[^a-z0-9]', '', 'g'), '') as source_notice_key,
    e.award_date,
    e.award_value_eur,
    e.end_date,
    e.cpv8,
    nullif(regexp_replace(
      coalesce(
        e.boamp_donnees->'IDENTITE'->>'CODE_IDENT_NATIONAL',
        e.boamp_donnees->'IDENTITE'->>'CODEIDENTIFICATIONNATIONAL',
        e.boamp_donnees->'identite'->>'codeidentificationnational'
      ),
      '[^0-9]',
      '',
      'g'
    ), '') as buyer_id_digits,
    case
      when length(nullif(regexp_replace(
        coalesce(
          e.boamp_donnees->'IDENTITE'->>'CODE_IDENT_NATIONAL',
          e.boamp_donnees->'IDENTITE'->>'CODEIDENTIFICATIONNATIONAL',
          e.boamp_donnees->'identite'->>'codeidentificationnational'
        ),
        '[^0-9]',
        '',
        'g'
      ), '')) >= 9
      then left(nullif(regexp_replace(
        coalesce(
          e.boamp_donnees->'IDENTITE'->>'CODE_IDENT_NATIONAL',
          e.boamp_donnees->'IDENTITE'->>'CODEIDENTIFICATIONNATIONAL',
          e.boamp_donnees->'identite'->>'codeidentificationnational'
        ),
        '[^0-9]',
        '',
        'g'
      ), ''), 9)
      else null
    end as buyer_id9,
    nullif(
      btrim(
        regexp_replace(
          lower(
            coalesce(
              e.boamp_donnees->'OBJET'->>'TITRE_MARCHE',
              e.boamp_donnees->'objet'->>'titre_marche',
              e.raw_objet
            )
          ),
          '[^a-z0-9]+',
          ' ',
          'g'
        )
      ),
      ''
    ) as object_norm
  from enriched e
)
select * from normalized;

create index on tmp_fr_award_candidates (source_notice_id);
create index on tmp_fr_award_candidates (source_notice_key);
create index on tmp_fr_award_candidates (award_date);
create index on tmp_fr_award_candidates (award_date, buyer_id9);

\\echo 'FR award rows missing end_date in scope'
select count(*) as fr_missing_rows from tmp_fr_award_candidates;

create temp table tmp_fr_decp_matches as
with joined as (
  select
    a.tenant_id,
    a.award_canonical_id,
    a.source_notice_id,
    a.source_notice_key,
    a.award_date,
    a.award_value_eur,
    a.cpv8,
    a.buyer_id9,
    a.object_norm,
    d.decp_id,
    d.decp_id_base,
    d.decp_id_key,
    d.decp_id_base_key,
    d.award_date as decp_award_date,
    d.duration_months as decp_duration_months,
    d.amount_eur as decp_amount_eur,
    left(d.cpv_digits, 8) as decp_cpv8,
    d.acheteur_id,
    d.acheteur_id9,
    d.object_norm as decp_object_norm,
    d.source_system,
    case
      when a.source_notice_key is not null
        and d.decp_id_key is not null
        and a.source_notice_key = d.decp_id_key
      then 100
      when a.source_notice_key is not null
        and d.decp_id_base_key is not null
        and a.source_notice_key = d.decp_id_base_key
      then 92
      when a.buyer_id9 is not null
        and d.acheteur_id9 is not null
        and a.buyer_id9 = d.acheteur_id9
        and a.award_date = d.award_date
        and a.cpv8 is not null
        and left(d.cpv_digits,8) = a.cpv8
        and a.award_value_eur is not null
        and d.amount_eur is not null
        and abs(a.award_value_eur - d.amount_eur) <= greatest(100::numeric, a.award_value_eur * 0.02)
      then 86
      when a.buyer_id9 is not null
        and d.acheteur_id9 is not null
        and a.buyer_id9 = d.acheteur_id9
        and a.award_date = d.award_date
        and a.object_norm is not null
        and d.object_norm is not null
        and left(a.object_norm, 80) = left(d.object_norm, 80)
        and (
          (a.cpv8 is not null and left(d.cpv_digits,8) = a.cpv8)
          or (
            a.award_value_eur is not null
            and d.amount_eur is not null
            and abs(a.award_value_eur - d.amount_eur) <= greatest(100::numeric, a.award_value_eur * 0.02)
          )
        )
      then 83
      else 0
    end as match_score,
    case
      when a.award_value_eur is null or d.amount_eur is null then null
      else abs(a.award_value_eur - d.amount_eur)
    end as amount_abs_diff
  from tmp_fr_award_candidates a
  join tmp_fr_decp_norm d
    on (
      (
        a.source_notice_key is not null
        and d.decp_id_key is not null
        and a.source_notice_key = d.decp_id_key
      )
      or (
        a.source_notice_key is not null
        and d.decp_id_base_key is not null
        and a.source_notice_key = d.decp_id_base_key
      )
      or (
        a.buyer_id9 is not null
        and d.acheteur_id9 is not null
        and a.buyer_id9 = d.acheteur_id9
        and a.award_date = d.award_date
        and (
          (
            a.cpv8 is not null
            and left(d.cpv_digits,8) = a.cpv8
            and a.award_value_eur is not null
            and d.amount_eur is not null
            and abs(a.award_value_eur - d.amount_eur) <= greatest(100::numeric, a.award_value_eur * 0.02)
          )
          or (
            a.object_norm is not null
            and d.object_norm is not null
            and left(a.object_norm, 80) = left(d.object_norm, 80)
            and (
              (a.cpv8 is not null and left(d.cpv_digits,8) = a.cpv8)
              or (
                a.award_value_eur is not null
                and d.amount_eur is not null
                and abs(a.award_value_eur - d.amount_eur) <= greatest(100::numeric, a.award_value_eur * 0.02)
              )
            )
          )
        )
      )
    )
  where d.duration_months is not null
    and d.duration_months between 1 and 240
), ranked as (
  select
    j.*,
    count(*) over (
      partition by j.tenant_id, j.award_canonical_id, j.match_score
    ) as score_bucket_candidates,
    row_number() over (
      partition by j.tenant_id, j.award_canonical_id
      order by j.match_score desc, j.amount_abs_diff asc nulls last, j.decp_id
    ) as rn
  from joined j
  where j.match_score >= 83
)
select *
from ranked
where rn = 1
  and (match_score >= 92 or score_bucket_candidates = 1);

create index on tmp_fr_decp_matches (tenant_id, award_canonical_id);

\\echo 'Join matches by strategy'
select
  count(*) as total_matches,
  count(*) filter (where match_score = 100) as id_key_exact_matches,
  count(*) filter (where match_score = 92) as id_key_base_matches,
  count(*) filter (where match_score = 86) as structured_buyer_cpv_amount_matches,
  count(*) filter (where match_score = 83) as structured_buyer_title_matches
from tmp_fr_decp_matches;

\\echo 'Structured match sample (quality check)'
select
  award_canonical_id,
  source_notice_id,
  decp_id,
  match_score,
  buyer_id9,
  acheteur_id9,
  award_date,
  decp_award_date,
  cpv8,
  decp_cpv8,
  award_value_eur,
  decp_amount_eur,
  decp_duration_months
from tmp_fr_decp_matches
where match_score < 92
order by match_score desc, amount_abs_diff asc nulls last, award_canonical_id
limit 15;

insert into public.fr_award_enrichment_batches (
  tenant_id,
  batch_id,
  source_name,
  run_mode,
  status,
  notes
)
values (
  '${TENANT_ID}',
  '${BATCH_ID}',
  '${SOURCE_NAME}',
  case when ${APPLY_SQL_BOOL} then 'apply' else 'dry_run' end,
  'running',
  jsonb_build_object(
    'dataset', '${DATASET}',
    'start_date', '${START_DATE}',
    'script', 'rollout-fr-decp-enddate-enrichment.sh'
  )
)
on conflict (tenant_id, batch_id) do update
  set source_name = excluded.source_name,
      run_mode = excluded.run_mode,
      status = 'running',
      notes = coalesce(public.fr_award_enrichment_batches.notes, '{}'::jsonb) || excluded.notes,
      finished_at = null;

insert into public.fr_award_enrichment_raw (
  tenant_id,
  batch_id,
  source_name,
  source_row_hash,
  payload
)
select
  m.tenant_id,
  '${BATCH_ID}',
  '${SOURCE_NAME}',
  md5(concat_ws(
    '|',
    coalesce(m.decp_id, ''),
    coalesce(m.decp_id_base, ''),
    coalesce(m.decp_award_date::text, ''),
    coalesce(m.decp_duration_months::text, ''),
    coalesce(m.decp_amount_eur::text, ''),
    coalesce(m.match_score::text, '')
  )),
  jsonb_strip_nulls(
    jsonb_build_object(
      'decp_id', m.decp_id,
      'decp_id_base', m.decp_id_base,
      'decp_award_date', m.decp_award_date,
      'decp_duration_months', m.decp_duration_months,
      'decp_amount_eur', m.decp_amount_eur,
      'decp_cpv8', m.decp_cpv8,
      'acheteur_id', m.acheteur_id,
      'acheteur_id9', m.acheteur_id9,
      'source_system', m.source_system,
      'match_score', m.match_score
    )
  )
from tmp_fr_decp_matches m
on conflict (tenant_id, batch_id, source_name, source_row_hash) do nothing;

insert into public.fr_award_enrichment_stage (
  tenant_id,
  batch_id,
  source_name,
  award_canonical_id,
  source_notice_id,
  award_date,
  duration_months,
  end_date_candidate,
  source_method,
  source_confidence,
  source_rank,
  evidence_url,
  source_file,
  raw_payload
)
select
  m.tenant_id,
  '${BATCH_ID}',
  '${SOURCE_NAME}',
  m.award_canonical_id,
  m.source_notice_id,
  m.award_date,
  m.decp_duration_months,
  case
    when m.award_date is not null and m.decp_duration_months between 1 and 240
      then (m.award_date + make_interval(months => m.decp_duration_months))::date
    else null
  end as end_date_candidate,
  case
    when m.match_score = 100 then 'decp_id_key_exact'
    when m.match_score = 92 then 'decp_id_key_base'
    when m.match_score = 86 then 'decp_structured_buyer_cpv_amount'
    else 'decp_structured_buyer_title'
  end as source_method,
  case
    when m.match_score >= 92 then 'high'
    else 'medium'
  end as source_confidence,
  m.match_score as source_rank,
  'https://data.economie.gouv.fr/explore/dataset/${DATASET}/' as evidence_url,
  '${DATASET}' as source_file,
  jsonb_strip_nulls(
    jsonb_build_object(
      'decp_id', m.decp_id,
      'decp_id_base', m.decp_id_base,
      'decp_source_system', m.source_system,
      'decp_match_score', m.match_score,
      'decp_acheteur_id', m.acheteur_id,
      'decp_acheteur_id9', m.acheteur_id9,
      'decp_award_date', m.decp_award_date,
      'decp_amount_eur', m.decp_amount_eur,
      'decp_duration_months', m.decp_duration_months,
      'decp_cpv8', m.decp_cpv8
    )
  ) as raw_payload
from tmp_fr_decp_matches m;

update public.fr_award_enrichment_batches b
set input_rows = (
      select count(*)
      from public.fr_award_enrichment_stage s
      where s.tenant_id = '${TENANT_ID}'
        and s.batch_id = '${BATCH_ID}'
        and s.source_name = '${SOURCE_NAME}'
    ),
    notes = coalesce(b.notes, '{}'::jsonb) || jsonb_build_object(
      'decp_rows', (select count(*) from tmp_fr_decp_norm),
      'join_matches', (select count(*) from tmp_fr_decp_matches)
    )
where b.tenant_id = '${TENANT_ID}'
  and b.batch_id = '${BATCH_ID}';

\\echo 'Gate + merge summary'
select *
from public.civant_fr_enrichment_gate_and_merge(
  '${TENANT_ID}',
  '${BATCH_ID}',
  '${SOURCE_NAME}',
  ${APPLY_SQL_BOOL}
);

\\echo 'Batch stage status'
select quality_status, count(*) as row_count
from public.fr_award_enrichment_stage
where tenant_id = '${TENANT_ID}'
  and batch_id = '${BATCH_ID}'
  and source_name = '${SOURCE_NAME}'
group by quality_status
order by quality_status;

\\echo 'Batch reject reasons'
select reject_reason, count(*) as row_count
from public.fr_award_enrichment_rejects
where tenant_id = '${TENANT_ID}'
  and batch_id = '${BATCH_ID}'
  and source_name = '${SOURCE_NAME}'
group by reject_reason
order by row_count desc, reject_reason
limit 20;

\\echo 'Batch metrics'
select metric_name, metric_value
from public.fr_award_enrichment_run_metrics
where tenant_id = '${TENANT_ID}'
  and batch_id = '${BATCH_ID}'
  and source_name = '${SOURCE_NAME}'
order by metric_name;

\\echo 'Coverage in scope after update'
select
  count(*) as total_since_start,
  count(*) filter (where end_date is not null) as with_end_date,
  round(100.0 * count(*) filter (where end_date is not null)/nullif(count(*),0),2) as pct_with_end_date
from public.award_fact_fr
where tenant_id='${TENANT_ID}'
  and award_date >= '${START_DATE}'::date;

SQL

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "== DRY RUN SQL =="
  {
    cat "${SQL_FILE}"
    echo "rollback;"
  } > "${RUN_SQL_FILE}"
  run_psql_file_with_retry "${RUN_SQL_FILE}"
  echo "dry_run_completed=true"
  exit 0
fi

echo "== Applying DECP enrichment =="
{
  cat "${SQL_FILE}"
  echo "commit;"
} > "${RUN_SQL_FILE}"
run_psql_file_with_retry "${RUN_SQL_FILE}"

echo "done=true"
