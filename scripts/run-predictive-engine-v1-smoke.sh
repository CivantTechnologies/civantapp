#!/usr/bin/env bash
set -euo pipefail

# Runs QA SQL assertions for Predictive Engine V1 and validates UI remains read-only.
# Usage:
#   ./scripts/run-predictive-engine-v1-smoke.sh [TENANT_ID]

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

DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: SUPABASE_DB_URL (or DATABASE_URL) is required."
  exit 1
fi

TENANT_ID="${1:-${TENANT_ID:-qa_predictive_v1}}"

echo "== Guardrail: no UI-triggered V1 compute path =="
if rg -n "run_predictive_engine_v1|normalize_all_signals\(|compute_prediction_for_pair_v1\(" src api >/dev/null 2>&1; then
  echo "FAIL: Found predictive engine compute calls in UI/API paths."
  rg -n "run_predictive_engine_v1|normalize_all_signals\(|compute_prediction_for_pair_v1\(" src api
  exit 1
fi

echo "PASS: No UI/API compute calls found for V1 forecast engine."

echo "== Running SQL QA pack =="
"${PSQL_BIN}" "${DATABASE_URL}" -v ON_ERROR_STOP=1 -P pager=off -v tenant_id="${TENANT_ID}" -f scripts/qa-predictive-engine-v1.sql

echo "== Smoke check complete =="
