#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONNECTOR="${1:-}"
TENANT_ID="${2:-civant_default}"

if [[ -z "${CONNECTOR}" ]]; then
  echo "ERROR: connector is required (boamp|etenders|placsp)."
  echo "Usage: $0 CONNECTOR [TENANT_ID]"
  exit 1
fi

RUN_HOURLY_FLOOR="${RUN_HOURLY_FLOOR:-true}"
FORCE_HEAVY="${FORCE_HEAVY:-false}"
DETECT_ONLY="${DETECT_ONLY:-false}"

# Detector window: last 1 day keeps polling light while still safe around UTC day boundaries.
if date -u -v-1d +"%Y-%m-%d" >/dev/null 2>&1; then
  DETECT_START_DATE="$(date -u -v-1d +"%Y-%m-%d")"
else
  DETECT_START_DATE="$(date -u -d "1 day ago" +"%Y-%m-%d")"
fi

MINUTE_UTC="$(date -u +"%M")"
IS_HOURLY_FLOOR="false"
if [[ "${RUN_HOURLY_FLOOR}" == "true" && "${MINUTE_UTC}" == "00" ]]; then
  IS_HOURLY_FLOOR="true"
fi

TMP_DIR="${TMPDIR:-/tmp}"
LOG_FILE="$(mktemp "${TMP_DIR%/}/civant_${CONNECTOR}_detect_XXXXXX" 2>/dev/null || mktemp -t "civant_${CONNECTOR}_detect")"
cleanup() {
  rm -f "${LOG_FILE}" 2>/dev/null || true
}
trap cleanup EXIT

parse_staged_rows() {
  local file="$1"
  awk -F= '/^staged_rows=/{v=$2} END{if (v == "") v=0; gsub(/[^0-9]/, "", v); if (v == "") v=0; print v+0}' "${file}"
}

connector_key() {
  case "${CONNECTOR}" in
    boamp) echo "boamp_fr_incremental:${TENANT_ID}" ;;
    etenders) echo "etenders_ie_incremental:${TENANT_ID}" ;;
    placsp) echo "placsp_es_incremental:${TENANT_ID}" ;;
    *) return 1 ;;
  esac
}

fetch_cursor_value() {
  if [[ -z "${SUPABASE_DB_URL:-${DATABASE_URL:-}}" ]]; then
    echo ""
    return 0
  fi
  local db_url="${DATABASE_URL:-${SUPABASE_DB_URL}}"
  local key
  key="$(connector_key)"
  psql "${db_url}" -v ON_ERROR_STOP=1 -P pager=off -qtA <<SQL
select coalesce(config->'cursor'->>'value','')
from public."ConnectorConfig"
where tenant_id = '${TENANT_ID}'
  and connector_key = '${key}'
limit 1;
SQL
}

to_epoch() {
  local ts="$1"
  local node_epoch=""
  local normalized=""
  local bsd_fmt="%Y-%m-%dT%H:%M:%S%z"
  local bsd_input=""
  ts="$(printf '%s' "${ts}" | tr -d '\r' | xargs)"
  if [[ -z "${ts}" || "${ts}" == "<none>" ]]; then
    echo 0
    return 0
  fi
  normalized="${ts}"
  normalized="${normalized/ /T}"
  normalized="$(printf '%s' "${normalized}" | sed -E 's/\\.([0-9]+)(Z|[+-][0-9]{2}:?[0-9]{2})$/\\2/')"
  if [[ "${normalized}" =~ Z$ ]]; then
    normalized="${normalized%Z}+00:00"
  fi
  if [[ ! "${normalized}" =~ [+-][0-9]{2}:?[0-9]{2}$ ]]; then
    normalized="${normalized}+00:00"
  fi

  if command -v node >/dev/null 2>&1; then
    node_epoch="$(node -e "const v=Date.parse(process.argv[1]); if (!Number.isFinite(v)) process.exit(1); console.log(Math.floor(v/1000));" "${ts}" 2>/dev/null || true)"
    if [[ "${node_epoch}" =~ ^[0-9]+$ ]]; then
      echo "${node_epoch}"
      return 0
    fi
    node_epoch="$(node -e "const v=Date.parse(process.argv[1]); if (!Number.isFinite(v)) process.exit(1); console.log(Math.floor(v/1000));" "${normalized}" 2>/dev/null || true)"
    if [[ "${node_epoch}" =~ ^[0-9]+$ ]]; then
      echo "${node_epoch}"
      return 0
    fi
  fi

  if date -u -d "${normalized}" +%s >/dev/null 2>&1; then
    date -u -d "${normalized}" +%s
    return 0
  fi

  bsd_input="$(printf '%s' "${normalized}" | sed -E 's/([+-][0-9]{2}):([0-9]{2})$/\\1\\2/')"
  if date -u -j -f "${bsd_fmt}" "${bsd_input}" +%s >/dev/null 2>&1; then
    date -u -j -f "${bsd_fmt}" "${bsd_input}" +%s
    return 0
  fi
  echo 0
}

extract_watermark() {
  local file="$1"
  case "${CONNECTOR}" in
    boamp|etenders)
      awk -F'max_published_at=' '/max_published_at=/{split($2,a," "); v=a[1]} END{if (v=="") v="<none>"; print v}' "${file}"
      ;;
    placsp)
      awk -F'max_entry_updated=' '/max_entry_updated=/{split($2,a," "); v=a[1]} END{if (v=="") v="<none>"; print v}' "${file}"
      ;;
    *)
      echo "<none>"
      ;;
  esac
}

run_with_log() {
  : >"${LOG_FILE}"
  if ! "$@" >"${LOG_FILE}" 2>&1; then
    cat "${LOG_FILE}"
    return 1
  fi
}

run_detect() {
  case "${CONNECTOR}" in
    boamp)
      echo "== Detector: BOAMP (max_pages=1, start_date=${DETECT_START_DATE}) =="
      run_with_log env MAX_PAGES="${DETECT_MAX_PAGES_BOAMP:-1}" \
        "${REPO_ROOT}/scripts/rollout-boamp-fr-incremental.sh" "${TENANT_ID}" "${DETECT_START_DATE}" "true"
      ;;
    etenders)
      echo "== Detector: eTenders (start_date=${DETECT_START_DATE}) =="
      run_with_log "${REPO_ROOT}/scripts/rollout-etenders-ie-incremental.sh" "${TENANT_ID}" "${DETECT_START_DATE}" "true"
      ;;
    placsp)
      echo "== Detector: PLACSP (max_pages=1, lookback_minutes=30, start_date=${DETECT_START_DATE}) =="
      run_with_log env MAX_PAGES="${DETECT_MAX_PAGES_PLACSP:-1}" LOOKBACK_MINUTES="${DETECT_LOOKBACK_MINUTES_PLACSP:-30}" BATCH_SIZE="${DETECT_BATCH_SIZE_PLACSP:-60}" \
        "${REPO_ROOT}/scripts/rollout-placsp-es-incremental.sh" "${TENANT_ID}" "${DETECT_START_DATE}" "true"
      ;;
    *)
      echo "ERROR: unsupported connector '${CONNECTOR}'. Use boamp|etenders|placsp."
      exit 1
      ;;
  esac
}

run_heavy() {
  if [[ -z "${SUPABASE_DB_URL:-${DATABASE_URL:-}}" ]]; then
    echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required for heavy ingestion."
    exit 1
  fi

  case "${CONNECTOR}" in
    boamp)
      echo "== Heavy ingest: BOAMP =="
      MAX_PAGES="${HEAVY_MAX_PAGES_BOAMP:-30}" \
        "${REPO_ROOT}/scripts/rollout-boamp-fr-incremental.sh" "${TENANT_ID}" "" "false"
      ;;
    etenders)
      echo "== Heavy ingest: eTenders =="
      "${REPO_ROOT}/scripts/rollout-etenders-ie-incremental.sh" "${TENANT_ID}" "" "false"
      ;;
    placsp)
      echo "== Heavy ingest: PLACSP =="
      MAX_PAGES="${HEAVY_MAX_PAGES_PLACSP:-40}" LOOKBACK_MINUTES="${HEAVY_LOOKBACK_MINUTES_PLACSP:-180}" BATCH_SIZE="${HEAVY_BATCH_SIZE_PLACSP:-120}" \
        "${REPO_ROOT}/scripts/rollout-placsp-es-incremental.sh" "${TENANT_ID}" "" "false"
      ;;
    *)
      echo "ERROR: unsupported connector '${CONNECTOR}'."
      exit 1
      ;;
  esac
}

CURSOR_VALUE="$(fetch_cursor_value || true)"
run_detect
DETECT_ROWS="$(parse_staged_rows "${LOG_FILE}")"
SOURCE_WATERMARK="$(extract_watermark "${LOG_FILE}")"
CURSOR_EPOCH="$(to_epoch "${CURSOR_VALUE}")"
WATERMARK_EPOCH="$(to_epoch "${SOURCE_WATERMARK}")"

HAS_CHANGE="false"
if [[ "${WATERMARK_EPOCH}" -gt 0 && "${WATERMARK_EPOCH}" -gt "${CURSOR_EPOCH}" ]]; then
  HAS_CHANGE="true"
elif [[ -z "${CURSOR_VALUE}" && "${DETECT_ROWS}" -gt 0 ]]; then
  HAS_CHANGE="true"
fi

SHOULD_RUN_HEAVY="false"
if [[ "${FORCE_HEAVY}" == "true" || "${HAS_CHANGE}" == "true" || "${IS_HOURLY_FLOOR}" == "true" ]]; then
  SHOULD_RUN_HEAVY="true"
fi

if [[ "${DETECT_ONLY}" == "true" ]]; then
  SHOULD_RUN_HEAVY="false"
fi

echo "detector_rows=${DETECT_ROWS} source_watermark=${SOURCE_WATERMARK} watermark_epoch=${WATERMARK_EPOCH} cursor_value=${CURSOR_VALUE:-<none>} cursor_epoch=${CURSOR_EPOCH} has_change=${HAS_CHANGE} hourly_floor=${IS_HOURLY_FLOOR} force_heavy=${FORCE_HEAVY} detect_only=${DETECT_ONLY} run_heavy=${SHOULD_RUN_HEAVY} connector=${CONNECTOR} tenant=${TENANT_ID}"

if [[ "${SHOULD_RUN_HEAVY}" == "true" ]]; then
  run_heavy
else
  if [[ "${DETECT_ONLY}" == "true" ]]; then
    echo "Skipping heavy ingest (detect_only=true)."
  else
    echo "Skipping heavy ingest (no source change detected and not hourly floor)."
  fi
fi
