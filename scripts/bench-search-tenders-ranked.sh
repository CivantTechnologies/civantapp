#!/usr/bin/env bash
set -euo pipefail

# Benchmarks search_tenders_ranked via psql, reporting p50/p95 latency (ms).
#
# Usage:
#   ./scripts/bench-search-tenders-ranked.sh TENANT_ID [N]
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

TENANT_ID_RAW="${1:-${TENANT_ID:-}}"
if [[ -z "${TENANT_ID_RAW}" ]]; then
  echo "ERROR: TENANT_ID is required."
  echo "Usage: $0 TENANT_ID [N]"
  exit 1
fi
TENANT_ID="$(echo "${TENANT_ID_RAW}" | tr '[:upper:]' '[:lower:]' | xargs)"

N_RAW="${2:-${N:-50}}"
if [[ ! "${N_RAW}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: N must be an integer. Got: ${N_RAW}"
  exit 1
fi
N="${N_RAW}"

tmp_file="$(mktemp -t civant_search_bench.XXXXXX)"
trap 'rm -f "${tmp_file}"' EXIT

echo "[bench] tenant=${TENANT_ID} n=${N}"

for ((i=1;i<=N;i++)); do
  # Rotate a small set of realistic filter patterns.
  mod=$((i % 5))
  keyword="null"
  country="null"
  source="null"
  deadline_within="null"
  last_tendered="90"

  case "${mod}" in
    0) keyword="'it'"; country="'IE'"; source="'ETENDERS_IE'"; deadline_within="30" ;;
    1) keyword="'services'"; country="'FR'"; source="'BOAMP_FR'"; deadline_within="90" ;;
    2) keyword="'digital'"; country="'ES'"; source="'PLACSP_ES'"; deadline_within="60" ;;
    3) keyword="null"; country="null"; source="null"; deadline_within="90" ;;
    4) keyword="'grant'"; country="'IE'"; source="'TED'"; deadline_within="120" ;;
  esac

  start_ms="$(python3 - <<'PY'\nimport time\nprint(int(time.time()*1000))\nPY)"

  # Count rows to avoid transferring full payload.
  "${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -qAt -c \
    "select count(*) from public.search_tenders_ranked('${TENANT_ID}'::text, 200, ${keyword}, ${country}, ${source}, null, null, ${deadline_within}, null, null, ${last_tendered});" \
    >/dev/null

  end_ms="$(python3 - <<'PY'\nimport time\nprint(int(time.time()*1000))\nPY)"
  echo $((end_ms - start_ms)) >> "${tmp_file}"
done

python3 - <<PY
import statistics
xs=[int(x.strip()) for x in open("${tmp_file}") if x.strip()]
xs.sort()
def pct(p):
    if not xs: return None
    k=(len(xs)-1)*p
    f=int(k)
    c=min(f+1,len(xs)-1)
    if f==c: return xs[f]
    return xs[f]+(xs[c]-xs[f])*(k-f)
print(f"[bench] samples={len(xs)} min={xs[0]} p50={pct(0.50):.1f} p95={pct(0.95):.1f} max={xs[-1]}")
print(f"[bench] mean={statistics.mean(xs):.1f} stdev={statistics.pstdev(xs):.1f}")
PY

