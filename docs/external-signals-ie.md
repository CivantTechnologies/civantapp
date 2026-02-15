# External Signals (Ireland)

Civant ingests Ireland-only external signals into `public.external_signals_ie` using the database helper function:

- `public.upsert_external_signal_ie(...)`

This is additive and does not modify any canonical tender tables.

## Connectors

### Hiring: PublicJobs

- Script: `scripts/external-signals/publicjobs-ie.mjs`
- Output: tab-separated rows for ingestion (via `psql \copy` + `public.upsert_external_signal_ie`)
- Source: the public job board hosted on `publicjobs.tal.net` (no embedded credentials, no Authorization header)

Environment variables:

- `PUBLICJOBS_IE_JOBBOARD_URL` (optional)
  - Default: `https://publicjobs.tal.net/candidate/jobboard/vacancy/3/adv/`
- `PUBLICJOBS_FETCH_DETAILS` (optional)
  - If true, fetches each vacancy detail page to enrich extracted fields (still no auth)

### Funding: data.gov.ie (CKAN)

- Script: `scripts/external-signals/funding-ckan-ie.mjs`
- Output: tab-separated rows for ingestion (via `psql \copy` + `public.upsert_external_signal_ie`)
- Source: CKAN Action API (`package_search`)

Environment variables:

- `CKAN_IE_BASE_URL` (optional)
  - Default: `https://data.gov.ie`
- `CKAN_ROWS` (optional)
- `CKAN_MAX_PAGES` (optional)
- `CKAN_MAX_EVENTS` (optional)

## Rollout (runs both)

Runner:

- `scripts/rollout-external-signals-ie.sh`

Usage:

```bash
./scripts/rollout-external-signals-ie.sh TENANT_ID [START_DATE] [DRY_RUN]

# Example (dry run):
./scripts/rollout-external-signals-ie.sh civant_default 2026-02-01 true

# Example (write to Supabase):
export SUPABASE_DB_URL='postgresql://postgres:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require'
./scripts/rollout-external-signals-ie.sh civant_default 2026-02-01 false
```

Notes:

- No secrets are committed to the repo.
- For writes, you must provide `SUPABASE_DB_URL` (or `DATABASE_URL`) at runtime.

## QA packs

- Hiring: `scripts/qa-external-signals-publicjobs-ie.sql`
- Funding: `scripts/qa-external-signals-funding-ckan-ie.sql`

Both include:

- counts by source
- last 30 days sample
- duplicate checks on `(tenant_id, signal_source, source_event_id)`
