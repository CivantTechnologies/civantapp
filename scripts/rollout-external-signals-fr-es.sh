#!/usr/bin/env bash
set -euo pipefail

# Civant: FR + ES external funding rollout orchestration
# Usage:
#   ./scripts/rollout-external-signals-fr-es.sh TENANT_ID [START_DATE] [DRY_RUN]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TENANT_ID="${1:-${TENANT_ID:-}}"
START_DATE="${2:-${START_DATE:-}}"
DRY_RUN="${3:-${DRY_RUN:-true}}"

if [[ -z "${TENANT_ID}" ]]; then
  echo "ERROR: TENANT_ID is required."
  echo "Usage: $0 TENANT_ID [START_DATE] [DRY_RUN]"
  exit 1
fi

echo "== Running FR rollout =="
"${SCRIPT_DIR}/rollout-external-signals-fr.sh" "${TENANT_ID}" "${START_DATE}" "${DRY_RUN}"

echo

echo "== Running ES rollout =="
"${SCRIPT_DIR}/rollout-external-signals-es.sh" "${TENANT_ID}" "${START_DATE}" "${DRY_RUN}"

echo

echo "== Completed FR + ES rollout =="
