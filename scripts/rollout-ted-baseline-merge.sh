#!/usr/bin/env bash
set -euo pipefail

# Merge staged TED baseline NDJSON into Supabase tables.
# Additive path only: TenderVersions + TendersCurrent + ConnectorConfig/ConnectorRuns
#
# Usage:
#   ./scripts/rollout-ted-baseline-merge.sh TENANT_ID [START_MONTH] [END_MONTH] [DRY_RUN]
#
# Example:
#   ./scripts/rollout-ted-baseline-merge.sh civant_default 2025-09 2026-02 true
#   ./scripts/rollout-ted-baseline-merge.sh civant_default 2025-09 2026-02 false

TENANT_ID="${1:-${TENANT_ID:-}}"
START_MONTH="${2:-${START_MONTH:-2025-09}}"
END_MONTH="${3:-${END_MONTH:-2026-02}}"
DRY_RUN_RAW="${4:-${DRY_RUN:-true}}"

RAW_ROOT="${RAW_ROOT:-/Volumes/Civant/_system/raw/ted_baseline}"
CONNECTOR_KEY="${CONNECTOR_KEY:-ted_baseline_loader:${TENANT_ID}}"

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
DEFAULT_PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "${PSQL_BIN}" ]]; then
  if [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
    PSQL_BIN="${DEFAULT_PSQL_BIN}"
  else
    PSQL_BIN="psql"
  fi
fi

if [[ -z "${TENANT_ID}" ]]; then
  echo "ERROR: TENANT_ID required"
  exit 1
fi
if [[ ! "${START_MONTH}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
  echo "ERROR: START_MONTH must be YYYY-MM"
  exit 1
fi
if [[ ! "${END_MONTH}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
  echo "ERROR: END_MONTH must be YYYY-MM"
  exit 1
fi
if [[ ! -d "${RAW_ROOT}" ]]; then
  echo "ERROR: RAW_ROOT not found: ${RAW_ROOT}"
  exit 1
fi

DRY_RUN="true"
case "${DRY_RUN_RAW}" in
  1|true|TRUE|yes|YES|y|Y|on|ON) DRY_RUN="true" ;;
  0|false|FALSE|no|NO|n|N|off|OFF) DRY_RUN="false" ;;
  *) echo "ERROR: DRY_RUN must be true/false"; exit 1 ;;
esac

if [[ "${DRY_RUN}" != "true" && -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) required for write mode"
  exit 1
fi

if [[ "${PSQL_BIN}" == */* ]]; then
  [[ -x "${PSQL_BIN}" ]] || { echo "ERROR: psql not found at ${PSQL_BIN}"; exit 1; }
else
  command -v "${PSQL_BIN}" >/dev/null 2>&1 || { echo "ERROR: psql not found on PATH"; exit 1; }
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

FILES=()
while IFS= read -r line; do
  FILES+=("$line")
done < <(
  find "${RAW_ROOT}" -type f -name '*.ndjson' | sort | \
    awk -v s="${START_MONTH}" -v e="${END_MONTH}" '
      {
        n=split($0,a,"/");
        f=a[n];
        gsub(/\.ndjson$/,"",f);
        if (f >= s && f <= e) print $0;
      }'
)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No staged TED files found for ${START_MONTH}..${END_MONTH}"
  exit 0
fi

echo "== TED baseline merge =="
echo "tenant_id=${TENANT_ID} window=${START_MONTH}..${END_MONTH} files=${#FILES[@]} dry_run=${DRY_RUN}"

echo "== Staged quick counts =="
TOTAL_LINES=0
for f in "${FILES[@]}"; do
  c=$(wc -l < "$f" | tr -d ' ')
  TOTAL_LINES=$((TOTAL_LINES + c))
done
echo "total_lines=${TOTAL_LINES}"

if [[ "${DRY_RUN}" == "true" ]]; then
  TMP_LIST="$(mktemp /tmp/ted_merge_dry.XXXXXX)"
  printf '%s\n' "${FILES[@]}" > "${TMP_LIST}"

  cat "${TMP_LIST}" | xargs cat | jq -r '.notice.ND // empty' | sort -u | wc -l | awk '{print "unique_tender_ids=" $1}'
  cat "${TMP_LIST}" | xargs cat | jq -r '.notice.PD // empty' | sed '/^$/d' | sort | tail -n 1 | awk '{print "max_publication_date=" $1}'

  echo "sample_files:"
  head -n 5 "${TMP_LIST}"
  rm -f "${TMP_LIST}"

  echo "== DRY RUN complete =="
  exit 0
fi

if ! "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA -c "select 1;" >/dev/null; then
  echo "ERROR: database connection preflight failed"
  exit 1
fi

TMP_SQL="$(mktemp /tmp/ted_merge_copy.XXXXXX.sql)"
{
  cat <<'SQL'
create temp table tmp_ted_line_raw (
  line text
);
SQL

  for f in "${FILES[@]}"; do
    esc=${f//\'/\'\'}
    printf "\\copy tmp_ted_line_raw(line) from '%s' with (format csv, delimiter E'\\x1f', quote E'\\x1e', escape E'\\x1d');\n" "$esc"
  done

  cat <<'SQL'
create temp table tmp_ted_line as
select line::jsonb as doc
from tmp_ted_line_raw
where coalesce(line, '') <> ''
  and pg_input_is_valid(line, 'jsonb');
SQL
} > "${TMP_SQL}"

"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off <<SQL >/dev/null
insert into public."ConnectorConfig" (tenant_id, connector_key, enabled, config, updated_at)
values ('${TENANT_ID}', '${CONNECTOR_KEY}', true, '{}'::jsonb, now())
on conflict (connector_key) do update
  set tenant_id = excluded.tenant_id,
      enabled = true,
      updated_at = now();
SQL

RUN_ID="$(${PSQL_BIN} "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -qtA <<SQL
insert into public."ConnectorRuns" (tenant_id, connector_key, status, started_at, metadata)
values (
  '${TENANT_ID}',
  '${CONNECTOR_KEY}',
  'running',
  now(),
  jsonb_build_object(
    'source', 'TED',
    'mode', 'baseline_merge',
    'start_month', '${START_MONTH}',
    'end_month', '${END_MONTH}',
    'input_files', ${#FILES[@]},
    'input_lines', ${TOTAL_LINES}
  )
)
returning id;
SQL
)"

cat >> "${TMP_SQL}" <<'SQL'
begin;

create temp table tmp_ted_parsed as
with base as (
  select
    doc,
    coalesce(doc->'notice', '{}'::jsonb) as notice
  from tmp_ted_line
), mapped as (
  select
    :'tenant_id'::text as tenant_id,
    ('TED:' || coalesce(nullif(notice->>'ND',''), nullif(notice->>'publication-number',''))) as tender_id,
    'TED'::text as source,
    coalesce(
      nullif(notice->>'PD','')::timestamptz,
      nullif(notice->>'DS','')::timestamptz,
      now()
    ) as published_at,
    jsonb_strip_nulls(
      jsonb_build_object(
        'source', 'TED',
        'country', coalesce((notice->'CY'->>0), null),
        'countries', coalesce(notice->'CY', '[]'::jsonb),
        'source_notice_id', coalesce(nullif(notice->>'ND',''), nullif(notice->>'publication-number','')),
        'title', (
          select
            case
              when jsonb_typeof(e.value) = 'string' then e.value #>> '{}'
              when jsonb_typeof(e.value) = 'array' then nullif(e.value->>0,'')
              else null
            end
          from jsonb_each(coalesce(notice->'title-proc', '{}'::jsonb)) e
          order by
            case when upper(e.key) = 'ENG' then 0 when upper(e.key) = 'FRA' then 1 when upper(e.key) = 'SPA' then 2 else 9 end,
            e.key
          limit 1
        ),
        'buyer_name', (
          select
            case
              when jsonb_typeof(e.value) = 'string' then e.value #>> '{}'
              when jsonb_typeof(e.value) = 'array' then nullif(e.value->>0,'')
              else null
            end
          from jsonb_each(coalesce(notice->'buyer-name', '{}'::jsonb)) e
          order by
            case when upper(e.key) = 'ENG' then 0 when upper(e.key) = 'FRA' then 1 when upper(e.key) = 'SPA' then 2 else 9 end,
            e.key
          limit 1
        ),
        'notice_type', nullif(notice->>'notice-type',''),
        'publication_date', left(coalesce(notice->>'PD',''),10),
        'dispatch_date', left(coalesce(notice->>'DS',''),10),
        'lot_count', case when jsonb_typeof(notice->'BT-137-Lot')='array' then jsonb_array_length(notice->'BT-137-Lot') else null end,
        'source_url', coalesce(
          notice#>>'{links,html,ENG}',
          notice#>>'{links,html,FRA}',
          notice#>>'{links,html,SPA}',
          notice#>>'{links,xml,MUL}'
        ),
        'raw_notice', notice
      )
    ) as data
  from base
)
select
  tenant_id,
  tender_id,
  source,
  published_at,
  data,
  md5(coalesce(data::text,'')) as version_hash
from mapped
where tender_id is not null
  and tender_id <> 'TED:';

create temp table tmp_ted_current as
select distinct on (tender_id)
  tenant_id,
  tender_id,
  source,
  published_at,
  data,
  version_hash
from tmp_ted_parsed
order by tender_id, published_at desc nulls last;

create temp table tmp_ted_counts as
with
v as (
  insert into public."TenderVersions" (tenant_id, tender_id, version_hash, data, created_at)
  select distinct tenant_id, tender_id, version_hash, data, now()
  from tmp_ted_parsed
  on conflict (tender_id, version_hash) do nothing
  returning tender_id
),
up as (
  insert into public."TendersCurrent" (tenant_id, tender_id, source, published_at, data, updated_at)
  select tenant_id, tender_id, source, published_at, data, now()
  from tmp_ted_current
  on conflict (tender_id) do update set
    tenant_id = excluded.tenant_id,
    source = excluded.source,
    published_at = excluded.published_at,
    data = excluded.data,
    updated_at = now()
  where public."TendersCurrent".tenant_id is distinct from excluded.tenant_id
     or public."TendersCurrent".source is distinct from excluded.source
     or public."TendersCurrent".published_at is distinct from excluded.published_at
     or public."TendersCurrent".data is distinct from excluded.data
  returning (xmax = 0) as inserted
)
select
  (select count(*)::int from tmp_ted_parsed) as fetched_count,
  (select count(*)::int from tmp_ted_current) as distinct_tenders,
  (select count(*)::int from up where inserted) as inserted_count,
  (select count(*)::int from up where not inserted) as updated_count,
  ((select count(*)::int from tmp_ted_current) - (select count(*)::int from up where inserted) - (select count(*)::int from up where not inserted)) as noop_count,
  (select count(*)::int from v) as versioned_count,
  (select max(published_at) from tmp_ted_current) as max_published_at;

select * from tmp_ted_counts;

update public."ConnectorConfig" cfg
set
  config = jsonb_set(
    coalesce(cfg.config, '{}'::jsonb),
    '{cursor}',
    jsonb_build_object(
      'type', 'published',
      'value', coalesce((select (max_published_at at time zone 'UTC')::text from tmp_ted_counts), cfg.config->'cursor'->>'value', now()::text),
      'last_success_at', now()::text
    ),
    true
  ),
  updated_at = now(),
  enabled = true
where cfg.tenant_id = :'tenant_id'
  and cfg.connector_key = :'connector_key';

update public."ConnectorRuns" r
set
  status = 'success',
  finished_at = now(),
  metadata = coalesce(r.metadata, '{}'::jsonb) || jsonb_build_object(
    'source', 'TED',
    'mode', 'baseline_merge',
    'fetched_count', (select fetched_count from tmp_ted_counts),
    'distinct_tenders', (select distinct_tenders from tmp_ted_counts),
    'inserted_count', (select inserted_count from tmp_ted_counts),
    'updated_count', (select updated_count from tmp_ted_counts),
    'noop_count', (select noop_count from tmp_ted_counts),
    'versioned_count', (select versioned_count from tmp_ted_counts),
    'cursor', (select cfg.config->'cursor' from public."ConnectorConfig" cfg where cfg.tenant_id=:'tenant_id' and cfg.connector_key=:'connector_key' limit 1)
  )
where r.id = :'run_id';

commit;
SQL

if ! "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -v run_id="${RUN_ID}" -v connector_key="${CONNECTOR_KEY}" -f "${TMP_SQL}"
then
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v run_id="${RUN_ID}" <<'SQL' >/dev/null
update public."ConnectorRuns"
set
  status = 'failed',
  finished_at = now(),
  metadata = coalesce(metadata, '{}'::jsonb) || jsonb_build_object('error', 'TED baseline merge failed')
where id = :'run_id';
SQL
  rm -f "${TMP_SQL}"
  echo "ERROR: TED baseline merge failed"
  exit 1
fi

rm -f "${TMP_SQL}"

echo "== Done =="
