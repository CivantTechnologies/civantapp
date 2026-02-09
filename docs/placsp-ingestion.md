# PLACSP (Spain) Ingestion Runbook

This runbook defines how to ingest and maintain Spanish procurement data from the CODICE/PLACSP Atom feed and archives.

## Script

Use:

```bash
node scripts/import-placsp-es.mjs --help
```

Or npm shortcuts:

```bash
npm run import:placsp
npm run import:placsp:backfill
npm run import:placsp:incremental
npm run import:placsp:check
```

## Data targets

The importer writes tenant-scoped data into:

- `raw_documents`
- `canonical_tenders`
- `TendersCurrent`
- `ingestion_runs` (run tracking)

Source is normalized as:

- `source = PLACSP_ES`
- `country = ES`
- canonical key: `PLACSP_ES:<ContractFolderID>`

## One-time historical backfill

Run from 2012 to current year:

```bash
npm run import:placsp:backfill -- \
  --api-base https://civantapp.vercel.app \
  --app-id civantapp \
  --tenant-id civant_default \
  --historical-from-year 2012 \
  --historical-to-year 2026 \
  --include-monthly-current-year true \
  --batch-size 120 \
  --status-file /tmp/placsp-es-backfill-status.json
```

Resume an interrupted backfill from a parsed record number:

```bash
npm run import:placsp:backfill -- \
  --api-base https://civantapp.vercel.app \
  --app-id civantapp \
  --tenant-id civant_default \
  --historical-from-year 2012 \
  --historical-to-year 2026 \
  --start-record 356280 \
  --batch-size 120 \
  --status-file /tmp/placsp-es-backfill-status.json
```

## Incremental sync (repeatable checker)

Use checkpoint lookback:

```bash
npm run import:placsp:incremental -- \
  --api-base https://civantapp.vercel.app \
  --app-id civantapp \
  --tenant-id civant_default \
  --lookback-minutes 90 \
  --max-pages 120 \
  --checkpoint-file /tmp/placsp-es-checkpoint.json \
  --status-file /tmp/placsp-es-incremental-status.json
```

Run from a specific timestamp:

```bash
npm run import:placsp:incremental -- \
  --api-base https://civantapp.vercel.app \
  --app-id civantapp \
  --tenant-id civant_default \
  --since-iso 2026-01-01T00:00:00.000Z \
  --max-pages 120
```

## Current-data verification and repair

Check last 14 days:

```bash
npm run import:placsp:check -- \
  --api-base https://civantapp.vercel.app \
  --app-id civantapp \
  --tenant-id civant_default \
  --lookback-days 14 \
  --max-pages 120 \
  --repair false \
  --status-file /tmp/placsp-es-check-status.json
```

Check and repair missing canonical/current records:

```bash
npm run import:placsp:check -- \
  --api-base https://civantapp.vercel.app \
  --app-id civantapp \
  --tenant-id civant_default \
  --lookback-days 30 \
  --repair true \
  --batch-size 120
```

## Notes

- Backfill is archive-driven and dedupes by `(canonical_id, version_fingerprint)` in-process.
- Canonical/current duplicates are handled with id-based update fallback.
- Use `--insecure-tls true` only for temporary TLS issues in controlled environments.
- Keep backfill and heavy BOAMP imports separate to avoid local resource contention.
