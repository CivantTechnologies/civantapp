#!/usr/bin/env bash
set -euo pipefail

# TED baseline staging runner on external SSD.
#
# Writes raw monthly/country chunks to SSD (no DB writes).
# Safe resume via checkpoint JSON.
#
# Usage:
#   ./scripts/rollout-ted-baseline-ssd.sh [mode] [start_month] [end_month] [max_seconds]
#
# Examples:
#   ./scripts/rollout-ted-baseline-ssd.sh size 2024-08 2026-02
#   ./scripts/rollout-ted-baseline-ssd.sh fetch 2024-08 2026-02 28800

MODE="${1:-size}"
START_MONTH="${2:-2024-08}"
END_MONTH="${3:-$(date -u +%Y-%m)}"
MAX_SECONDS="${4:-28800}"
COUNTRIES="${COUNTRIES:-IRL,FRA,ESP}"
SLEEP_MS="${SLEEP_MS:-150}"
LIMIT="${LIMIT:-250}"

SSD_ROOT="${SSD_ROOT:-/Volumes/Civant}"
OUT_ROOT="${OUT_ROOT:-${SSD_ROOT}/_system/raw/ted_baseline}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-${SSD_ROOT}/_system/checkpoints/ted_baseline}"
REPORT_ROOT="${REPORT_ROOT:-${SSD_ROOT}/_system/reports/ted_baseline}"
LOG_ROOT="${LOG_ROOT:-${SSD_ROOT}/_system/logs/ted_baseline}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TED_SCRIPT="${REPO_ROOT}/scripts/ted/ted-baseline-ie-fr-es.mjs"

if [[ ! -d "${SSD_ROOT}" ]]; then
  echo "ERROR: SSD_ROOT not found: ${SSD_ROOT}"
  exit 1
fi

if [[ ! -x "${TED_SCRIPT}" ]]; then
  echo "ERROR: TED script not executable: ${TED_SCRIPT}"
  exit 1
fi

mkdir -p "${OUT_ROOT}" "${CHECKPOINT_ROOT}" "${REPORT_ROOT}" "${LOG_ROOT}"

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_FILE="${LOG_ROOT}/ted_${MODE}_${START_MONTH}_${END_MONTH}_${RUN_TS}.log"

echo "== TED baseline SSD rollout =="
echo "mode=${MODE} countries=${COUNTRIES} start_month=${START_MONTH} end_month=${END_MONTH}"
echo "ssd_root=${SSD_ROOT}"
echo "out_root=${OUT_ROOT}"
echo "checkpoint_root=${CHECKPOINT_ROOT}"
echo "report_root=${REPORT_ROOT}"
echo "log_file=${LOG_FILE}"

set -o pipefail
node "${TED_SCRIPT}" \
  --mode "${MODE}" \
  --countries "${COUNTRIES}" \
  --start-month "${START_MONTH}" \
  --end-month "${END_MONTH}" \
  --limit "${LIMIT}" \
  --sleep-ms "${SLEEP_MS}" \
  --max-seconds "${MAX_SECONDS}" \
  --out-root "${OUT_ROOT}" \
  --checkpoint-root "${CHECKPOINT_ROOT}" \
  --report-root "${REPORT_ROOT}" \
  2>&1 | tee "${LOG_FILE}"
