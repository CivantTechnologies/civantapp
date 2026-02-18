#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./scripts/offload-payloads-to-storage.sh --table notices|TendersCurrent|canonical_tenders [--tenant civant_default] [--batch-size 1000] [--dry-run] [--no-resume] [--null-after-upload true|false]

Env required:
  SUPABASE_DB_URL
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
  SUPABASE_STORAGE_BUCKET

Notes:
  - Default: NULL notices.raw_payload after upload.
  - Default: KEEP canonical_tenders.normalized_json and TendersCurrent.data.
  - Uses payload_offload_state table for resumable checkpoint.
USAGE
}

TABLE=""
TENANT="civant_default"
BATCH_SIZE=1000
DRY_RUN="false"
RESUME="true"
NULL_AFTER_UPLOAD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --table) TABLE="$2"; shift 2 ;;
    --tenant) TENANT="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --dry-run) DRY_RUN="true"; shift ;;
    --no-resume) RESUME="false"; shift ;;
    --null-after-upload) NULL_AFTER_UPLOAD="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

if [[ -z "$TABLE" ]]; then
  echo "Missing --table"; usage; exit 1
fi

SUPABASE_DB_URL="${SUPABASE_DB_URL:-}"
SUPABASE_URL="${SUPABASE_URL:-${VITE_SUPABASE_URL:-}}"
SUPABASE_SERVICE_ROLE_KEY="${SUPABASE_SERVICE_ROLE_KEY:-}"
SUPABASE_STORAGE_BUCKET="${SUPABASE_STORAGE_BUCKET:-}"

if [[ -z "$SUPABASE_DB_URL" || -z "$SUPABASE_URL" || -z "$SUPABASE_SERVICE_ROLE_KEY" || -z "$SUPABASE_STORAGE_BUCKET" ]]; then
  echo "Missing required env vars: SUPABASE_DB_URL, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_STORAGE_BUCKET"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "Missing dependency: jq"
  exit 1
fi
if ! command -v gzip >/dev/null 2>&1; then
  echo "Missing dependency: gzip"
  exit 1
fi
if ! command -v openssl >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
  echo "Missing dependency: openssl or shasum"
  exit 1
fi

TABLE_SQL=""
PK_COL=""
PAYLOAD_COL=""

case "$TABLE" in
  notices)
    TABLE_SQL="public.notices"
    PK_COL="notice_id"
    PAYLOAD_COL="raw_payload"
    ;;
  TendersCurrent)
    TABLE_SQL="public.\"TendersCurrent\""
    PK_COL="tender_id"
    PAYLOAD_COL="data"
    ;;
  canonical_tenders)
    TABLE_SQL="public.canonical_tenders"
    PK_COL="canonical_id"
    PAYLOAD_COL="normalized_json"
    ;;
  *)
    echo "Unsupported table: $TABLE"; exit 1 ;;
esac

if [[ -z "$NULL_AFTER_UPLOAD" ]]; then
  if [[ "$TABLE" == "notices" ]]; then
    NULL_AFTER_UPLOAD="true"
  else
    NULL_AFTER_UPLOAD="false"
  fi
fi

PSQL_BIN="psql"
if [[ -x "/opt/homebrew/opt/libpq/bin/psql" ]]; then
  PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
fi

psql_exec() {
  "$PSQL_BIN" "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -At -c "$1"
}

safe_segment() {
  echo "$1" | tr '/' '_'
}

sha256_hex() {
  if command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 -binary | xxd -p -c 256
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

last_pk=""
if [[ "$RESUME" == "true" ]]; then
  last_pk="$(psql_exec "select coalesce(last_pk,'') from public.payload_offload_state where tenant_id='${TENANT}' and table_name='${TABLE}' limit 1;")"
fi

total_processed=0
total_failures=0

while true; do
  where_clause="tenant_id='${TENANT}' and ${PAYLOAD_COL} is not null and raw_object_key is null"
  if [[ -n "$last_pk" ]]; then
    where_clause="${where_clause} and ${PK_COL}::text > '${last_pk}'"
  fi

  query="select ${PK_COL}::text as pk, tenant_id::text as tenant_id, encode(convert_to(${PAYLOAD_COL}::text,'UTF8'),'base64') as payload_b64 from ${TABLE_SQL} where ${where_clause} order by ${PK_COL} asc limit ${BATCH_SIZE};"
  rows="$("$PSQL_BIN" "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -At -F $'\t' -c "$query" || true)"
  if [[ -z "$rows" ]]; then
    break
  fi

while IFS=$'\t' read -r pk tenant payload_b64; do
    if [[ -z "$pk" ]]; then
      continue
    fi
    if base64 --decode </dev/null >/dev/null 2>&1; then
      payload_json="$(echo "$payload_b64" | base64 --decode)"
    else
      payload_json="$(echo "$payload_b64" | base64 -D)"
    fi
    canonical="$(echo "$payload_json" | jq -cS '.' 2>/dev/null || echo "$payload_json")"
    hash="$(printf '%s' "$canonical" | sha256_hex)"
    payload_bytes="$(printf '%s' "$canonical" | wc -c | tr -d ' ')"
    safe_pk="$(safe_segment "$pk")"
    safe_tenant="$(safe_segment "$tenant")"
    object_key="${safe_tenant}/${TABLE}/${safe_pk}/${hash}.json.gz"

    if [[ "$DRY_RUN" == "true" ]]; then
      echo "[dry-run] ${TABLE} pk=${pk} hash=${hash} bytes=${payload_bytes} key=${object_key}"
      last_pk="$pk"
      total_processed=$((total_processed + 1))
      continue
    fi

    object_url="${SUPABASE_URL}/storage/v1/object/${SUPABASE_STORAGE_BUCKET}/${object_key}"
    status="$(curl -s -o /dev/null -w "%{http_code}" -I -H "Authorization: Bearer ${SUPABASE_SERVICE_ROLE_KEY}" "$object_url")"
    if [[ "$status" != "200" ]]; then
      tmpfile="$(mktemp)"
      printf '%s' "$canonical" | gzip -c > "$tmpfile"
      upload_status="$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        -H "Authorization: Bearer ${SUPABASE_SERVICE_ROLE_KEY}" \
        -H "Content-Type: application/gzip" \
        -H "x-upsert: true" \
        --data-binary @"$tmpfile" \
        "$object_url")"
      rm -f "$tmpfile"
      if [[ "$upload_status" != "200" && "$upload_status" != "201" ]]; then
        total_failures=$((total_failures + 1))
        psql_exec "insert into public.payload_offload_failures (tenant_id, table_name, primary_key, payload_hash_sha256, raw_object_key, error) values ('${TENANT}', '${TABLE}', '${pk}', '${hash}', '${object_key}', 'upload_failed_${upload_status}');" >/dev/null
        echo "[error] upload failed for ${TABLE} pk=${pk} status=${upload_status}"
        last_pk="$pk"
        continue
      fi
    fi

    null_clause=""
    if [[ "$TABLE" == "notices" && "$NULL_AFTER_UPLOAD" == "true" ]]; then
      null_clause=", ${PAYLOAD_COL} = null"
    fi

    psql_exec "update ${TABLE_SQL} set raw_object_key='${object_key}', payload_hash_sha256='${hash}', payload_bytes=${payload_bytes}, payload_stored_at=now()${null_clause} where tenant_id='${TENANT}' and ${PK_COL}::text='${pk}';" >/dev/null

    last_pk="$pk"
    total_processed=$((total_processed + 1))
  done <<< "$rows"

  "$PSQL_BIN" "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c "insert into public.payload_offload_state (tenant_id, table_name, last_pk, processed_rows, updated_at) values ('${TENANT}', '${TABLE}', '${last_pk}', ${total_processed}, now()) on conflict (tenant_id, table_name) do update set last_pk=excluded.last_pk, processed_rows=excluded.processed_rows, updated_at=now();" >/dev/null
done

echo "Done. processed=${total_processed} failures=${total_failures} table=${TABLE} tenant=${TENANT}"
