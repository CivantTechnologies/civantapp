#!/usr/bin/env bash
set -euo pipefail

# Merge-only loader for Spain (PLACSP_ES):
# - Reuses already-loaded staging tables
# - Skips expensive \copy stage
# - Upserts into canonical_tenders and "TendersCurrent"

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
TENANT_ID="${TENANT_ID:-civant_default}"
CLEANUP_STAGING="${CLEANUP_STAGING:-false}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: DATABASE_URL (or SUPABASE_DB_URL) is required."
  exit 1
fi

if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-60}"

echo "[0/3] Checking staging row counts..."
STAGING_COUNTS="$("${PSQL_BIN}" "${DATABASE_URL}" -At -F $'\t' -c \
  "select (select count(*) from _stg_placsp_canonical), (select count(*) from _stg_placsp_current);")"

CANON_ROWS="$(echo "${STAGING_COUNTS}" | cut -f1)"
CURR_ROWS="$(echo "${STAGING_COUNTS}" | cut -f2)"

echo "  _stg_placsp_canonical: ${CANON_ROWS:-0}"
echo "  _stg_placsp_current  : ${CURR_ROWS:-0}"

if [[ -z "${CANON_ROWS}" || -z "${CURR_ROWS}" || "${CANON_ROWS}" == "0" || "${CURR_ROWS}" == "0" ]]; then
  echo "ERROR: staging tables are empty. Run full copy script first:"
  echo "  ./scripts/upload-placsp-core-copy.sh"
  exit 2
fi

# macOS/BSD mktemp requires trailing XXXXXX; avoid deterministic filenames.
SQL_FILE="$(mktemp /tmp/placsp-merge-only.XXXXXX)"
trap 'rm -f "${SQL_FILE}"' EXIT

PIN_RULE_VERSION="${PIN_RULE_VERSION:-placsp_es_pinned_v1_2026_02_13}"

cat > "${SQL_FILE}" <<SQL
\set ON_ERROR_STOP on
\timing on

set statement_timeout = 0;
set lock_timeout = 0;
set idle_in_transaction_session_timeout = 0;

\echo '[1/5] Building pinned tender facts (publication=min, deadline=max>=pub)...'
drop table if exists tmp_placsp_es_pinned;
create temporary table tmp_placsp_es_pinned as
with base as (
  select
    doc->>'canonical_id' as canonical_id,
    nullif(doc->'normalized_json'->>'notice_type','') as notice_type,
    nullif(doc->'normalized_json'->>'publication_date_source','') as pub_src,
    case
      when nullif(doc->>'publication_date','') is null then null
      else left(doc->>'publication_date',10)::date
    end as pub_date,
    case
      when nullif(doc->>'deadline_date','') is null then null
      else left(doc->>'deadline_date',10)::date
    end as deadline_date
  from _stg_placsp_canonical
  where nullif(doc->>'canonical_id','') is not null
    and nullif(doc->>'source','') is not null
    and nullif(doc->>'external_id','') is not null
),
pub as (
  select
    canonical_id,
    coalesce(
      min(pub_date) filter (where notice_type = 'tender' and pub_src = 'notice_issue_date_doc_cn_cd'),
      min(pub_date) filter (where notice_type = 'tender'),
      min(pub_date) filter (where pub_src = 'notice_issue_date_doc_cn_cd'),
      min(pub_date)
    ) as pinned_pub_date
  from base
  group by canonical_id
),
dead as (
  select
    b.canonical_id,
    max(b.deadline_date) filter (
      where p.pinned_pub_date is not null
        and b.notice_type = 'tender'
        and b.pub_src = 'notice_issue_date_doc_cn_cd'
        and b.deadline_date >= p.pinned_pub_date
    ) as dl_cn_tender,
    max(b.deadline_date) filter (
      where p.pinned_pub_date is not null
        and b.notice_type = 'tender'
        and b.deadline_date >= p.pinned_pub_date
    ) as dl_any_tender,
    max(b.deadline_date) filter (
      where p.pinned_pub_date is not null
        and b.deadline_date >= p.pinned_pub_date
    ) as dl_any
  from base b
  join pub p on p.canonical_id = b.canonical_id
  group by b.canonical_id
)
select
  p.canonical_id,
  p.pinned_pub_date,
  coalesce(d.dl_cn_tender, d.dl_any_tender, d.dl_any) as pinned_deadline_date
from pub p
join dead d on d.canonical_id = p.canonical_id;

create index on tmp_placsp_es_pinned (canonical_id);
analyze tmp_placsp_es_pinned;

\echo 'Pinned facts QA gate (must be spotless)'
select
  count(*) as tenders,
  count(*) filter (where pinned_pub_date is null) as pub_null,
  count(*) filter (where pinned_deadline_date is null) as deadline_null,
  count(*) filter (where pinned_pub_date is not null and pinned_deadline_date is not null and pinned_deadline_date < pinned_pub_date) as inversions
from tmp_placsp_es_pinned;

do \$\$
declare
  pub_null bigint;
  inversions bigint;
begin
  select count(*) into pub_null from tmp_placsp_es_pinned where pinned_pub_date is null;
  select count(*) into inversions from tmp_placsp_es_pinned
    where pinned_pub_date is not null and pinned_deadline_date is not null and pinned_deadline_date < pinned_pub_date;
  if pub_null > 0 then
    raise exception 'QA gate failed: pinned_pub_date null for % tenders', pub_null;
  end if;
  if inversions > 0 then
    raise exception 'QA gate failed: deadline < publication for % tenders', inversions;
  end if;
end
\$\$;

\echo '[2/5] Merging into canonical_tenders (canonical = tender facts)...'
begin;
with base as (
  select
    doc,
    doc->>'canonical_id' as canonical_id,
    nullif(doc->'normalized_json'->>'notice_type','') as notice_type,
    nullif(doc->'normalized_json'->>'publication_date_source','') as pub_src,
    case
      when nullif(doc->>'publication_date','') is null then null
      else left(doc->>'publication_date',10)::date
    end as event_pub_date,
    case
      when nullif(doc->>'deadline_date','') is null then null
      else left(doc->>'deadline_date',10)::date
    end as event_deadline_date,
    case
      when nullif(doc->'normalized_json'->>'entry_updated_at','') is null then timestamptz 'epoch'
      else (doc->'normalized_json'->>'entry_updated_at')::timestamptz
    end as entry_updated_at_ts,
    case
      when jsonb_typeof(doc->'cpv_codes') = 'array' then jsonb_array_length(doc->'cpv_codes')
      else 0
    end as cpv_count,
    (nullif(doc->>'title','') is not null) as has_title
  from _stg_placsp_canonical
  where nullif(doc->>'canonical_id','') is not null
    and nullif(doc->>'source','') is not null
    and nullif(doc->>'external_id','') is not null
),
canonical_winners as (
  select distinct on (b.canonical_id)
    b.doc,
    b.canonical_id,
    p.pinned_pub_date,
    p.pinned_deadline_date
  from base b
  join tmp_placsp_es_pinned p on p.canonical_id = b.canonical_id
  order by
    b.canonical_id,
    (b.notice_type = 'tender') desc,
    (b.event_pub_date = p.pinned_pub_date) desc,
    (b.pub_src = 'notice_issue_date_doc_cn_cd') desc,
    (b.event_deadline_date is not null) desc,
    b.cpv_count desc,
    b.has_title desc,
    b.entry_updated_at_ts desc
)
insert into canonical_tenders (
  canonical_id,
  source,
  external_id,
  title,
  category,
  subcategory,
  cpv_codes,
  publication_date,
  deadline_date,
  estimated_value,
  currency,
  source_url,
  dedupe_quality,
  normalized_json,
  tenant_id
)
select
  doc->>'canonical_id' as canonical_id,
  doc->>'source' as source,
  doc->>'external_id' as external_id,
  nullif(doc->>'title','') as title,
  nullif(doc->>'category','') as category,
  nullif(doc->>'subcategory','') as subcategory,
  case
    when jsonb_typeof(doc->'cpv_codes') = 'array' then array(
      select jsonb_array_elements_text(doc->'cpv_codes')
    )
    else null
  end as cpv_codes,
  pinned_pub_date as publication_date,
  pinned_deadline_date as deadline_date,
  case
    when nullif(doc->>'estimated_value','') is null then null
    else (doc->>'estimated_value')::numeric
  end as estimated_value,
  nullif(doc->>'currency','') as currency,
  nullif(doc->>'source_url','') as source_url,
  case
    when nullif(doc->>'dedupe_quality','') is null then null
    else (doc->>'dedupe_quality')::numeric
  end as dedupe_quality,
  (
    with norm0 as (select coalesce(doc->'normalized_json', '{}'::jsonb) as j)
    select
      jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  jsonb_set(
                    jsonb_set(
                      jsonb_set(
                        norm0.j,
                        '{event_publication_date}', coalesce(norm0.j->'publication_date','null'::jsonb), true
                      ),
                      '{event_publication_date_source}', coalesce(norm0.j->'publication_date_source','null'::jsonb), true
                    ),
                    '{event_deadline_date}', coalesce(norm0.j->'deadline_date','null'::jsonb), true
                  ),
                  '{event_deadline_date_source}', coalesce(norm0.j->'deadline_date_source','null'::jsonb), true
                ),
                '{publication_date}', to_jsonb(to_char(pinned_pub_date, 'YYYY-MM-DD')), true
              ),
              '{deadline_date}', case when pinned_deadline_date is null then 'null'::jsonb else to_jsonb(to_char(pinned_deadline_date, 'YYYY-MM-DD')) end, true
            ),
            '{publication_date_source}', to_jsonb('pinned'::text), true
          ),
          '{deadline_date_source}', case when pinned_deadline_date is null then 'null'::jsonb else to_jsonb('pinned'::text) end, true
        ),
        '{pinned_rule_version}', to_jsonb('${PIN_RULE_VERSION}'::text), true
      )
    from norm0
  ) as normalized_json,
  coalesce(nullif(doc->>'tenant_id',''), '${TENANT_ID}') as tenant_id
from canonical_winners
on conflict (canonical_id) do update
set
  source = excluded.source,
  external_id = excluded.external_id,
  title = excluded.title,
  category = excluded.category,
  subcategory = excluded.subcategory,
  cpv_codes = excluded.cpv_codes,
  publication_date = excluded.publication_date,
  deadline_date = excluded.deadline_date,
  estimated_value = excluded.estimated_value,
  currency = excluded.currency,
  source_url = excluded.source_url,
  dedupe_quality = excluded.dedupe_quality,
  normalized_json = excluded.normalized_json,
  tenant_id = excluded.tenant_id,
  updated_at = now();
commit;

\echo '[3/5] Merging into "TendersCurrent" (latest version + pinned tender facts)...'
begin;
with base as (
  select
    doc,
    doc->>'canonical_id' as canonical_id,
    case
      when nullif(doc->'normalized_json'->>'entry_updated_at','') is null then now()
      else (doc->'normalized_json'->>'entry_updated_at')::timestamptz
    end as entry_updated_at_ts,
    case
      when nullif(doc->>'publication_date','') is null then null
      else left(doc->>'publication_date',10)::date
    end as event_pub_date
  from _stg_placsp_canonical
  where nullif(doc->>'canonical_id','') is not null
    and nullif(doc->>'source','') is not null
    and nullif(doc->>'external_id','') is not null
),
current_winners as (
  select distinct on (b.canonical_id)
    b.doc,
    b.canonical_id,
    b.entry_updated_at_ts as published_at,
    p.pinned_pub_date,
    p.pinned_deadline_date
  from base b
  join tmp_placsp_es_pinned p on p.canonical_id = b.canonical_id
  order by
    b.canonical_id,
    b.entry_updated_at_ts desc,
    b.event_pub_date desc nulls last
)
insert into "TendersCurrent" (
  tender_id,
  source,
  published_at,
  data,
  tenant_id
)
select
  canonical_id as tender_id,
  doc->>'source' as source,
  published_at as published_at,
  (
    with norm0 as (select coalesce(doc->'normalized_json', '{}'::jsonb) as j)
    select
      jsonb_set(
        jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(
                jsonb_set(
                  jsonb_set(
                    jsonb_set(
                      jsonb_set(
                        norm0.j,
                        '{event_publication_date}', coalesce(norm0.j->'publication_date','null'::jsonb), true
                      ),
                      '{event_publication_date_source}', coalesce(norm0.j->'publication_date_source','null'::jsonb), true
                    ),
                    '{event_deadline_date}', coalesce(norm0.j->'deadline_date','null'::jsonb), true
                  ),
                  '{event_deadline_date_source}', coalesce(norm0.j->'deadline_date_source','null'::jsonb), true
                ),
                '{publication_date}', to_jsonb(to_char(pinned_pub_date, 'YYYY-MM-DD')), true
              ),
              '{deadline_date}', case when pinned_deadline_date is null then 'null'::jsonb else to_jsonb(to_char(pinned_deadline_date, 'YYYY-MM-DD')) end, true
            ),
            '{publication_date_source}', to_jsonb('pinned'::text), true
          ),
          '{deadline_date_source}', case when pinned_deadline_date is null then 'null'::jsonb else to_jsonb('pinned'::text) end, true
        ),
        '{pinned_rule_version}', to_jsonb('${PIN_RULE_VERSION}'::text), true
      )
    from norm0
  ) as data,
  coalesce(nullif(doc->>'tenant_id',''), '${TENANT_ID}') as tenant_id
from current_winners
on conflict (tender_id) do update
set
  source = excluded.source,
  published_at = excluded.published_at,
  data = excluded.data,
  tenant_id = excluded.tenant_id;
commit;

\echo '[4/5] Validation summary'
select
  (select count(*) from _stg_placsp_canonical) as staging_canonical_rows,
  (select count(*) from _stg_placsp_current) as staging_current_rows,
  (select count(*) from canonical_tenders where source = 'PLACSP_ES' and tenant_id = '${TENANT_ID}') as live_canonical_rows,
  (select count(*) from "TendersCurrent" where source = 'PLACSP_ES' and tenant_id = '${TENANT_ID}') as live_current_rows;

\echo '[5/5] Post-merge QA (live)'
select
  count(*) as live_canonical,
  count(*) filter (where deadline_date is null) as live_deadline_null,
  count(*) filter (where deadline_date is not null and publication_date is not null and deadline_date < publication_date) as live_inversions
from canonical_tenders
where source = 'PLACSP_ES' and tenant_id = '${TENANT_ID}';
SQL

"${PSQL_BIN}" "${DATABASE_URL}" -f "${SQL_FILE}" 2>&1 | tee /tmp/placsp-merge-only.log

if [[ "${CLEANUP_STAGING}" == "true" || "${CLEANUP_STAGING}" == "1" || "${CLEANUP_STAGING}" == "yes" ]]; then
  echo "Cleanup: truncating staging tables..."
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -c "truncate table _stg_placsp_canonical; truncate table _stg_placsp_current;"
  echo "Cleanup: staging tables truncated."
fi

echo "Done: PLACSP_ES merge-only completed."
