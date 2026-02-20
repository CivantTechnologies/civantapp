#!/usr/bin/env bash
set -euo pipefail

# 48h rollout watch for buyer_category_stats v2.1
# Fails when key quality signals regress beyond configured thresholds.

DEFAULT_PSQL_BIN="/opt/homebrew/opt/libpq/bin/psql"
PSQL_BIN="${PSQL_BIN:-}"
if [[ -z "${PSQL_BIN}" ]]; then
  if [[ -x "${DEFAULT_PSQL_BIN}" ]]; then
    PSQL_BIN="${DEFAULT_PSQL_BIN}"
  else
    PSQL_BIN="psql"
  fi
fi
if [[ "${PSQL_BIN}" == */* ]]; then
  [[ -x "${PSQL_BIN}" ]] || { echo "ERROR: psql not found at ${PSQL_BIN}" >&2; exit 1; }
else
  command -v "${PSQL_BIN}" >/dev/null 2>&1 || { echo "ERROR: psql not found on PATH: ${PSQL_BIN}" >&2; exit 1; }
  PSQL_BIN="$(command -v "${PSQL_BIN}")"
fi

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
[[ -n "${DATABASE_URL}" ]] || { echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required." >&2; exit 1; }

TENANT_ID="${TENANT_ID:-civant_default}"
BASELINE_FILE="${BASELINE_FILE:-.github/rollout/buyer_stats_v2_1_baseline.tsv}"
ROLL_OUT_START_UTC="${ROLLOUT_START_UTC:-2026-02-20T17:25:32Z}"
ROLL_OUT_END_UTC="${ROLLOUT_END_UTC:-2026-02-22T17:25:32Z}"
WATCH_REGIONS="${WATCH_REGIONS:-FR,IE}"
MAX_CADENCE_DROP_PCT="${MAX_CADENCE_DROP_PCT:-10}"
MAX_COVERAGE_DROP_PCT="${MAX_COVERAGE_DROP_PCT:-10}"
MAX_LOW_CONF_INCREASE_PP="${MAX_LOW_CONF_INCREASE_PP:-10}"
MIN_PREDICTIONS_FOR_LOW_RATIO="${MIN_PREDICTIONS_FOR_LOW_RATIO:-200}"

[[ -f "${BASELINE_FILE}" ]] || { echo "ERROR: baseline file not found: ${BASELINE_FILE}" >&2; exit 1; }

iso_to_epoch() {
  local iso_ts="$1"
  python3 - "$iso_ts" <<'PY'
import sys
from datetime import datetime, timezone

ts = sys.argv[1]
if ts.endswith("Z"):
    ts = ts[:-1] + "+00:00"
dt = datetime.fromisoformat(ts)
if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)
print(int(dt.timestamp()))
PY
}

now_epoch="$(date -u +%s)"
start_epoch="$(iso_to_epoch "${ROLL_OUT_START_UTC}")"
end_epoch="$(iso_to_epoch "${ROLL_OUT_END_UTC}")"

if (( now_epoch < start_epoch )); then
  echo "INFO: rollout watch window has not started yet."
  exit 0
fi
if (( now_epoch > end_epoch )); then
  echo "INFO: rollout watch window ended; no-op."
  exit 0
fi

base_ie_cadence=""
base_ie_coverage=""
base_ie_low_conf=""
base_fr_cadence=""
base_fr_coverage=""
base_fr_low_conf=""
base_es_cadence=""
base_es_coverage=""
base_es_low_conf=""
while IFS=$'\t' read -r region cadence coverage low_conf _; do
  [[ -z "${region}" || "${region}" =~ ^# ]] && continue
  case "${region}" in
    IE)
      base_ie_cadence="${cadence}"
      base_ie_coverage="${coverage}"
      base_ie_low_conf="${low_conf}"
      ;;
    FR)
      base_fr_cadence="${cadence}"
      base_fr_coverage="${coverage}"
      base_fr_low_conf="${low_conf}"
      ;;
    ES)
      base_es_cadence="${cadence}"
      base_es_coverage="${coverage}"
      base_es_low_conf="${low_conf}"
      ;;
  esac
done < "${BASELINE_FILE}"

query_out="$(${PSQL_BIN} "${DATABASE_URL}" -v ON_ERROR_STOP=1 -At <<SQL
with regions(region) as (
  values ('IE'),('FR'),('ES')
),
stats as (
  select
    region,
    avg(cadence_reliability_score)::numeric as avg_cadence,
    avg(data_coverage_score)::numeric as avg_coverage
  from public.buyer_category_stats
  where tenant_id = '${TENANT_ID}'
  group by region
),
preds as (
  select
    region,
    count(*)::numeric as n,
    sum((confidence_band = 'Low')::int)::numeric as low_n
  from public.predictions_current
  where tenant_id = '${TENANT_ID}'
  group by region
)
select
  r.region,
  coalesce(round(s.avg_cadence, 6), 0),
  coalesce(round(s.avg_coverage, 6), 0),
  coalesce(round((p.low_n / nullif(p.n,0)), 6), 0),
  coalesce(p.n::int, 0)
from regions r
left join stats s using(region)
left join preds p using(region)
order by r.region;
SQL
)"

echo "Rollout watch snapshot (tenant=${TENANT_ID}, utc=$(date -u +%Y-%m-%dT%H:%M:%SZ))"
echo "region|cadence|coverage|low_conf_ratio|prediction_count"
echo "${query_out}"

failures=0
IFS=',' read -r -a watch_region_arr <<< "${WATCH_REGIONS}"

is_watch_region() {
  local r="$1"
  for wr in "${watch_region_arr[@]}"; do
    if [[ "${r}" == "${wr}" ]]; then
      return 0
    fi
  done
  return 1
}

while IFS='|' read -r region cur_cadence cur_coverage cur_low_conf cur_pred_count; do
  [[ -z "${region}" ]] && continue

  b_cadence=""
  b_coverage=""
  b_low_conf=""
  case "${region}" in
    IE)
      b_cadence="${base_ie_cadence}"
      b_coverage="${base_ie_coverage}"
      b_low_conf="${base_ie_low_conf}"
      ;;
    FR)
      b_cadence="${base_fr_cadence}"
      b_coverage="${base_fr_coverage}"
      b_low_conf="${base_fr_low_conf}"
      ;;
    ES)
      b_cadence="${base_es_cadence}"
      b_coverage="${base_es_coverage}"
      b_low_conf="${base_es_low_conf}"
      ;;
  esac

  if [[ -z "${b_cadence}" || -z "${b_coverage}" || -z "${b_low_conf}" ]]; then
    echo "WARN: missing baseline for region=${region}; skipping checks for this region"
    continue
  fi

  cadence_drop_pct="$(awk -v b="${b_cadence}" -v c="${cur_cadence}" 'BEGIN{ if (b<=0) printf "0"; else printf "%.4f", ((b-c)/b)*100 }')"
  coverage_drop_pct="$(awk -v b="${b_coverage}" -v c="${cur_coverage}" 'BEGIN{ if (b<=0) printf "0"; else printf "%.4f", ((b-c)/b)*100 }')"
  low_conf_increase_pp="$(awk -v b="${b_low_conf}" -v c="${cur_low_conf}" 'BEGIN{ printf "%.4f", (c-b)*100 }')"

  echo "region=${region} cadence_drop_pct=${cadence_drop_pct} coverage_drop_pct=${coverage_drop_pct} low_conf_increase_pp=${low_conf_increase_pp} pred_count=${cur_pred_count}"

  if is_watch_region "${region}"; then
    cadence_fail="$(awk -v v="${cadence_drop_pct}" -v t="${MAX_CADENCE_DROP_PCT}" 'BEGIN{print (v>t)?1:0}')"
    coverage_fail="$(awk -v v="${coverage_drop_pct}" -v t="${MAX_COVERAGE_DROP_PCT}" 'BEGIN{print (v>t)?1:0}')"
    low_conf_fail="0"
    if [[ "${cur_pred_count}" =~ ^[0-9]+$ ]] && (( cur_pred_count >= MIN_PREDICTIONS_FOR_LOW_RATIO )); then
      low_conf_fail="$(awk -v v="${low_conf_increase_pp}" -v t="${MAX_LOW_CONF_INCREASE_PP}" 'BEGIN{print (v>t)?1:0}')"
    fi

    if [[ "${cadence_fail}" == "1" || "${coverage_fail}" == "1" || "${low_conf_fail}" == "1" ]]; then
      echo "ALERT: rollout regression threshold exceeded for region=${region}"
      failures=$((failures + 1))
    fi
  fi
done <<< "${query_out}"

if (( failures > 0 )); then
  echo "ERROR: buyer stats v2.1 rollout watch failed with ${failures} regional regression(s)."
  exit 1
fi

echo "OK: buyer stats v2.1 rollout watch passed."
