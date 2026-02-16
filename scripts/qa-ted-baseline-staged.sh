#!/usr/bin/env bash
set -euo pipefail

# QA staged TED baseline files before DB merge.
# Usage:
#   ./scripts/qa-ted-baseline-staged.sh [START_MONTH] [END_MONTH] [RAW_ROOT]
# Example:
#   ./scripts/qa-ted-baseline-staged.sh 2025-09 2026-02 /Volumes/Civant/_system/raw/ted_baseline

START_MONTH="${1:-2025-09}"
END_MONTH="${2:-2026-02}"
RAW_ROOT="${3:-/Volumes/Civant/_system/raw/ted_baseline}"

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
  echo "No staged files found for ${START_MONTH}..${END_MONTH}"
  exit 0
fi

echo "== TED staged QA =="
echo "window=${START_MONTH}..${END_MONTH}"
echo "raw_root=${RAW_ROOT}"
echo "files=${#FILES[@]}"

TMP_LIST="$(mktemp /tmp/ted_qa_files.XXXXXX)"
printf '%s\n' "${FILES[@]}" > "${TMP_LIST}"

echo "\n== Files by country/month =="
awk -F'/' '{country=$(NF-2); file=$NF; sub(/\.ndjson$/,"",file); print country"\t"file}' "${TMP_LIST}" | sort

echo "\n== Line counts by file =="
while IFS= read -r f; do
  c=$(wc -l < "$f" | tr -d ' ')
  echo "${c}  ${f}"
done < "${TMP_LIST}"

echo "\n== Core integrity checks =="
cat "${TMP_LIST}" | xargs cat | jq -r '
  . as $row |
  {
    tender_id: ($row.notice.ND // ""),
    publication_date: ($row.notice.PD // ""),
    country_count: (($row.notice.CY // []) | length),
    title_present: ((($row.notice["title-proc"] // {}) | length) > 0),
    buyer_present: ((($row.notice["buyer-name"] // {}) | length) > 0)
  }
' | jq -s '
  {
    rows: length,
    tender_id_null: map(select(.tender_id == "")) | length,
    publication_date_null: map(select(.publication_date == "")) | length,
    country_missing: map(select(.country_count == 0)) | length,
    title_missing: map(select(.title_present == false)) | length,
    buyer_missing: map(select(.buyer_present == false)) | length
  }
'

echo "\n== Duplicate tender IDs in window (top 20) =="
cat "${TMP_LIST}" | xargs cat | jq -r '.notice.ND // empty' | sort | uniq -c | sort -nr | awk '$1>1{print; if (++n>=20) exit}' || true

echo "\n== Sample 10 rows =="
cat "${TMP_LIST}" | xargs cat | jq -c '{nd:(.notice.ND // null), pd:(.notice.PD // null), cy:(.notice.CY // []), notice_type:(.notice["notice-type"] // null)}' | awk 'NR<=10 {print}'

rm -f "${TMP_LIST}"

echo "\n== Done =="
