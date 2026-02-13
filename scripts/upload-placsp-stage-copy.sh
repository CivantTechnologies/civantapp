#!/usr/bin/env bash
set -euo pipefail

# Stage-only loader for Spain (PLACSP_ES):
# - Loads local NDJSON into unlogged staging tables via psql \copy
# - DOES NOT merge into canonical_tenders / TendersCurrent
# - Intended for QA gating before running merge-only

PSQL_BIN="${PSQL_BIN:-/opt/homebrew/opt/libpq/bin/psql}"
DATABASE_URL="${DATABASE_URL:-${SUPABASE_DB_URL:-}}"
# Default points at a "latest" symlink so you can't accidentally upload an old/corrupted run.
# Override via NDJSON_DIR if needed.
NDJSON_DIR="${NDJSON_DIR:-/Users/davidmanrique/projects/Historical_data_spain/processed_ndjson_latest}"
CANONICAL_FILE="${CANONICAL_FILE:-$NDJSON_DIR/canonical_tenders.ndjson}"
CURRENT_FILE="${CURRENT_FILE:-$NDJSON_DIR/TendersCurrent.ndjson}"

if [[ -z "${DATABASE_URL}" ]]; then
  echo "ERROR: DATABASE_URL (or SUPABASE_DB_URL) is required."
  exit 1
fi

if [[ ! -x "${PSQL_BIN}" ]]; then
  echo "ERROR: psql not found at ${PSQL_BIN}"
  exit 1
fi

export PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-60}"

if [[ ! -f "${CANONICAL_FILE}" ]]; then
  echo "ERROR: missing canonical file: ${CANONICAL_FILE}"
  echo "Tip: set NDJSON_DIR=/path/to/processed_ndjson_<run> or repoint processed_ndjson_latest symlink."
  exit 1
fi

if [[ ! -f "${CURRENT_FILE}" ]]; then
  echo "ERROR: missing current file: ${CURRENT_FILE}"
  echo "Tip: set NDJSON_DIR=/path/to/processed_ndjson_<run> or repoint processed_ndjson_latest symlink."
  exit 1
fi

# macOS/BSD mktemp requires trailing XXXXXX; avoid deterministic filenames.
SQL_FILE="$(mktemp /tmp/placsp-stage-copy.XXXXXX)"
trap 'rm -f "${SQL_FILE}"' EXIT

cat > "${SQL_FILE}" <<'SQL'
\set ON_ERROR_STOP on
\timing on

set statement_timeout = 0;
set lock_timeout = 0;
set idle_in_transaction_session_timeout = 0;

\echo '[1/2] Preparing staging tables...'
create unlogged table if not exists _stg_placsp_canonical (
  doc jsonb not null
);

create unlogged table if not exists _stg_placsp_current (
  doc jsonb not null
);

truncate table _stg_placsp_canonical;
truncate table _stg_placsp_current;
SQL

{
  # Use CSV with non-printable quote/escape/delimiter characters so JSON content is preserved
  # byte-for-byte (text COPY would interpret backslash escapes like \n).
  printf "\\copy _stg_placsp_canonical(doc) FROM '%s' WITH (FORMAT csv, DELIMITER E'\\\\x1f', QUOTE E'\\\\x1e', ESCAPE E'\\\\x1d');\n" "${CANONICAL_FILE}"
  printf "\\copy _stg_placsp_current(doc) FROM '%s' WITH (FORMAT csv, DELIMITER E'\\\\x1f', QUOTE E'\\\\x1e', ESCAPE E'\\\\x1d');\n" "${CURRENT_FILE}"
} >> "${SQL_FILE}"

cat >> "${SQL_FILE}" <<'SQL'
\echo '[2/2] Staging counts'
select
  (select count(*) from _stg_placsp_canonical) as staging_canonical_rows,
  (select count(*) from _stg_placsp_current) as staging_current_rows;
SQL

echo "Staging copy only:"
echo "  canonical: ${CANONICAL_FILE}"
echo "  current  : ${CURRENT_FILE}"

"${PSQL_BIN}" "${DATABASE_URL}" -f "${SQL_FILE}"

echo "Done: staging copy completed (no merge performed)."
