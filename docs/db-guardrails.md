# DB Guardrails: Stop-the-Bleeding Search

These guardrails prevent regressions back to heavy JSON payload search and detect silent failures in the serving-path search architecture.

## What Guardrails Exist

Single source of truth SQL:
- `scripts/guardrails-stop-bleeding.sql`

Automated workflows:
- Daily full guardrails: `.github/workflows/db-guardrails-stop-bleeding.yml`
- Hourly cheap canary: `.github/workflows/db-guardrails-stop-bleeding-canary.yml`

### Checks (Full Mode)
1. Heavy index is absent
   - Fails if `public.idx_notices_raw_payload_gin` exists.
   - Fails if any `public.notices` index definition contains both `USING gin` and `raw_payload`.
2. Search uses serving path
   - Calls `public.search_tenders_ranked(<tenant>, 20, ...)` and checks `data.engine`.
   - Full mode:
     - If `serving_rows > 0`, engine must be `serving`.
     - If `serving_rows = 0`, `legacy` is allowed (emergency fallback).
   - Canary mode: engine must be `serving`.
3. Serving table coverage
   - For each tenant:
     - Canary: fails if `notices_search_current` rows = 0.
     - Full: checks coverage drift only when `serving_rows > 0`.
     - Coverage drift tolerance defaults to 5% (see below).
4. Notices index size ceiling
   - Computes `pg_indexes_size('public.notices')` and fails if > 900 MB (default).
5. Health summary output
   - Prints a compact row set for logs:
     - timestamp, tenant_id, notices_index_size_mb, serving_rows, canonical_rows, serving_ratio, engine_mode

### Checks (Canary Mode)
Canary runs a cheap subset:
1. Heavy index is absent
2. Search uses serving path (engine must be `serving`)
3. Serving rows > 0

## Run Locally

Full:
```bash
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v guardrail_mode=full -f scripts/guardrails-stop-bleeding.sql
```

Canary:
```bash
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v guardrail_mode=canary -f scripts/guardrails-stop-bleeding.sql
```

## Changing Thresholds Safely

Change thresholds via `psql -v ...` overrides (preferred), or by editing defaults in `scripts/guardrails-stop-bleeding.sql`.

- Index size ceiling (MB):
```bash
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v guardrail_notices_index_size_mb_limit=950 -f scripts/guardrails-stop-bleeding.sql
```

- Serving coverage tolerance (fraction, default 0.05 = 5%):
```bash
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v guardrail_serving_ratio_tolerance=0.03 -f scripts/guardrails-stop-bleeding.sql
```

Guideline:
- Start at 5% for one week to avoid false fails, then tighten to 2% once stable.
- If you need to raise thresholds, do it temporarily and pair it with a concrete plan to bring the metric back down (e.g., dropping accidental indexes, repairing serving refresh).

## Temporarily Disable (Without Deleting)

Both workflows respect a repo variable:
- `DB_GUARDRAILS_STOP_BLEEDING_DISABLED=true`

Set it in GitHub:
- Settings -> Secrets and variables -> Actions -> Variables

To re-enable:
- Remove the variable or set it to anything other than `true`.

## GitHub Actions Secrets

Required secret (only):
- `SUPABASE_DB_URL`

## Rollback / Disable Plan

If guardrails start failing and you need breathing room:
1. Disable runs quickly by setting `DB_GUARDRAILS_STOP_BLEEDING_DISABLED=true` (see above).
2. Investigate and fix the underlying regression (preferred).
3. If a threshold needs temporary relaxation, override the threshold via `-v ...` in the workflow (code change) and revert once stable.
