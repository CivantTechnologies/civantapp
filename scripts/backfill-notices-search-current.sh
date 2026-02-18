#!/usr/bin/env bash
set -euo pipefail

# Resumable backfill for public.notices_search_current (serving table).
#
# Usage:
#   ./scripts/backfill-notices-search-current.sh TENANT_ID [BATCH_SIZE]
#
# Defaults:
#   BATCH_SIZE=2000 (conservative)
#
# Requires:
#   SUPABASE_DB_URL or DATABASE_URL

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
if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required."
  exit 1
fi

RETRIES="${RETRIES:-8}"
RETRY_SLEEP_SEC="${RETRY_SLEEP_SEC:-5}"
BATCH_SLEEP_MS="${BATCH_SLEEP_MS:-0}"

is_transient_psql_error() {
  echo "${1:-}" | grep -Eqi "(timeout expired|operation timed out|could not connect|server closed the connection|terminating connection|connection timed out)"
}

psql_eval() {
  local extra_args="${1:-}"; shift || true
  local sql="${1:-}"
  local attempt=1
  local out rc

  while true; do
    out="$("${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 ${extra_args} -c "${sql}" 2>&1)" && {
      printf "%s" "${out}"
      return 0
    }
    rc=$?

    if is_transient_psql_error "${out}"; then
      if [[ "${attempt}" -ge "${RETRIES}" ]]; then
        echo "[backfill] ERROR: psql failed after ${RETRIES} attempts" >&2
        echo "${out}" >&2
        return "${rc}"
      fi
      echo "[backfill] transient psql error (attempt ${attempt}/${RETRIES}); retrying in $((RETRY_SLEEP_SEC * attempt))s" >&2
      sleep "$((RETRY_SLEEP_SEC * attempt))"
      attempt="$((attempt + 1))"
      continue
    fi

    echo "${out}" >&2
    return "${rc}"
  done
}

TENANT_ID_RAW="${1:-${TENANT_ID:-}}"
if [[ -z "${TENANT_ID_RAW}" ]]; then
  echo "ERROR: TENANT_ID is required."
  echo "Usage: $0 TENANT_ID [BATCH_SIZE]"
  exit 1
fi
TENANT_ID="$(echo "${TENANT_ID_RAW}" | tr '[:upper:]' '[:lower:]' | xargs)"

BATCH_SIZE_RAW="${2:-${BATCH_SIZE:-2000}}"
if [[ ! "${BATCH_SIZE_RAW}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: BATCH_SIZE must be an integer. Got: ${BATCH_SIZE_RAW}"
  exit 1
fi
BATCH_SIZE="${BATCH_SIZE_RAW}"

echo "[backfill] tenant=${TENANT_ID} batch_size=${BATCH_SIZE}"

ensure_state_sql=$(
  cat <<SQL
do \$\$
declare
  v_exists boolean;
  v_total bigint;
begin
  select exists(select 1 from public.notices_search_backfill_state where tenant_id = '${TENANT_ID}') into v_exists;
  if not v_exists then
    select count(*)::bigint into v_total
    from public.canonical_tenders
    where tenant_id = '${TENANT_ID}';
    insert into public.notices_search_backfill_state(tenant_id, total_rows, processed_rows)
    values ('${TENANT_ID}', v_total, 0);
  end if;
end
\$\$;
SQL
)

psql_eval "-q" "${ensure_state_sql}" >/dev/null

while true; do
  state_line="$(psql_eval "-qAt -F $'\t'" \
    "select coalesce(cursor_updated_at::text,''), coalesce(cursor_canonical_id,''), processed_rows::text, coalesce(total_rows::text,'') , coalesce(completed_at::text,'') from public.notices_search_backfill_state where tenant_id='${TENANT_ID}' limit 1;")"

  cursor_updated_at="$(echo "${state_line}" | awk -F $'\t' '{print $1}')"
  cursor_canonical_id="$(echo "${state_line}" | awk -F $'\t' '{print $2}')"
  processed_rows="$(echo "${state_line}" | awk -F $'\t' '{print $3}')"
  total_rows="$(echo "${state_line}" | awk -F $'\t' '{print $4}')"
  completed_at="$(echo "${state_line}" | awk -F $'\t' '{print $5}')"

  if [[ -n "${completed_at}" ]]; then
    echo "[backfill] already completed_at=${completed_at} processed_rows=${processed_rows} total_rows=${total_rows}"
    exit 0
  fi

  cursor_updated_at_sql="null"
  cursor_canonical_id_sql="null"
  if [[ -n "${cursor_updated_at}" ]]; then
    cursor_updated_at_sql="'${cursor_updated_at}'::timestamptz"
  fi
  if [[ -n "${cursor_canonical_id}" ]]; then
    cursor_canonical_id_sql="'${cursor_canonical_id}'"
  fi

  started_ms="$(python3 -c 'import time; print(int(time.time()*1000))')"

  batch_line="$(psql_eval "-qAt -F $'\t'" \
    "select processed, inserted, updated, coalesce(next_cursor_updated_at::text,''), coalesce(next_cursor_canonical_id,''), done from public.backfill_notices_search_current_batch('${TENANT_ID}', ${BATCH_SIZE}, ${cursor_updated_at_sql}, ${cursor_canonical_id_sql});")"

  processed="$(echo "${batch_line}" | awk -F $'\t' '{print $1}')"
  inserted="$(echo "${batch_line}" | awk -F $'\t' '{print $2}')"
  updated="$(echo "${batch_line}" | awk -F $'\t' '{print $3}')"
  next_cursor_updated_at="$(echo "${batch_line}" | awk -F $'\t' '{print $4}')"
  next_cursor_canonical_id="$(echo "${batch_line}" | awk -F $'\t' '{print $5}')"
  done_flag="$(echo "${batch_line}" | awk -F $'\t' '{print $6}')"

  ended_ms="$(python3 -c 'import time; print(int(time.time()*1000))')"
  elapsed_ms="$((ended_ms - started_ms))"

  if [[ -z "${processed}" ]]; then processed="0"; fi
  if [[ -z "${inserted}" ]]; then inserted="0"; fi
  if [[ -z "${updated}" ]]; then updated="0"; fi

  next_cursor_updated_at_sql="null"
  next_cursor_canonical_id_sql="null"
  if [[ -n "${next_cursor_updated_at}" ]]; then
    next_cursor_updated_at_sql="'${next_cursor_updated_at}'::timestamptz"
  fi
  if [[ -n "${next_cursor_canonical_id}" ]]; then
    next_cursor_canonical_id_sql="'${next_cursor_canonical_id}'"
  fi

  done_sql="false"
  if [[ "${done_flag}" == "t" || "${done_flag}" == "true" ]]; then
    done_sql="true"
  fi

  # Update checkpoint.
  update_sql=$(
    cat <<SQL
update public.notices_search_backfill_state
set
  cursor_updated_at = ${next_cursor_updated_at_sql},
  cursor_canonical_id = ${next_cursor_canonical_id_sql},
  processed_rows = processed_rows + ${processed},
  updated_at = now(),
  completed_at = case when ${done_sql} then now() else completed_at end
where tenant_id = '${TENANT_ID}';
SQL
  )

  psql_eval "-q" "${update_sql}" >/dev/null

  # Refresh progress values for logging.
  progress_line="$(psql_eval "-qAt -F $'\t'" \
    "select processed_rows::text, coalesce(total_rows::text,'') from public.notices_search_backfill_state where tenant_id='${TENANT_ID}' limit 1;")"
  processed_rows_now="$(echo "${progress_line}" | awk -F $'\t' '{print $1}')"
  total_rows_now="$(echo "${progress_line}" | awk -F $'\t' '{print $2}')"

  pct="?"
  if [[ -n "${total_rows_now}" && "${total_rows_now}" != "0" ]]; then
    pct="$(python3 -c "p=int('${processed_rows_now}'); t=int('${total_rows_now}'); print(f'{(p/t)*100:.2f}')")"
  fi

  echo "[backfill] processed=${processed} inserted=${inserted} updated=${updated} elapsed_ms=${elapsed_ms} total_processed=${processed_rows_now}/${total_rows_now:-?} (${pct}%)"

  if [[ "${BATCH_SLEEP_MS}" =~ ^[0-9]+$ ]] && [[ "${BATCH_SLEEP_MS}" -gt 0 ]]; then
    sleep "$(python3 -c "print(${BATCH_SLEEP_MS}/1000)")"
  fi

  if [[ "${done_flag}" == "t" || "${done_flag}" == "true" || "${processed}" == "0" ]]; then
    echo "[backfill] done."
    exit 0
  fi
done
