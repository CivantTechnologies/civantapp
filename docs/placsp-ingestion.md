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

## Recommended block strategy (safer restart model)

For large historical runs, use the block manager. It runs multiple year ranges in sequence with retries, per-block checkpoint files, and a health report every 5 minutes.
Default mode is now local-only (`backfill-local`), so no API/Supabase writes happen during historical preprocessing.
By default it now raises a stall warning after 5 minutes of no status activity, then triggers a safe stop after 15 minutes. Stall-triggered stops auto-restart from the latest checkpoint within the same manager run.
If the importer does not exit after stall-stop, the manager force-kills it after a 3-minute grace period and retries from checkpoint.

Start (detached):

```bash
npm run import:placsp:block:start -- \
  --api-base https://civantapp.vercel.app \
  --app-id civantapp \
  --tenant-id civant_default \
  --import-mode backfill-local \
  --download-dir /Users/davidmanrique/Downloads/placsp_zips \
  --local-output-dir /Users/davidmanrique/projects/Historical_data_spain/processed_ndjson \
  --batch-size 120 \
  --warning-threshold-minutes 5 \
  --stall-threshold-minutes 15 \
  --stall-kill-grace-minutes 3
```

Notes:

- Prefer writing each run to a fresh timestamped folder, then repoint the `processed_ndjson_latest` symlink:
  - `ln -sfn /path/to/processed_ndjson_<run_id> /Users/davidmanrique/projects/Historical_data_spain/processed_ndjson_latest`
- Spain upload scripts default `NDJSON_DIR` to `processed_ndjson_latest` so you can't accidentally upload an older/corrupted run.

Monitor:

```bash
npm run import:placsp:block:monitor
```

Live monitor:

```bash
npm run import:placsp:block:monitor -- --watch true --watch-seconds 15
```

Pause safely:

```bash
npm run import:placsp:block:pause -- --reason "network maintenance"
```

Stop safely:

```bash
npm run import:placsp:block:stop -- --reason "manual stop"
```

Restart from saved progress:

```bash
npm run import:placsp:block:restart -- --reason "resume after interruption"
```

Operational files written by the block manager:

- `/tmp/placsp-es-block-manager-status.json` (global state)
- `/tmp/placsp-es-block-reports.log` (5-minute health reports)
- `/tmp/placsp-es-block-manager.log` (manager runtime log)
- `/tmp/placsp-es-backfill-block-<block>-*.json` (per-block status/checkpoint/control)

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
- ZIP archives are the preferred ingestion input for PLACSP in this implementation.
- Local preprocessing (`backfill-local`) writes NDJSON only; use `upload-local` later for controlled API upload.
- Use `--insecure-tls true` only for temporary TLS issues in controlled environments.
- Keep backfill and heavy BOAMP imports separate to avoid local resource contention.

## Post-Run TODO (Quality Gates)

- Extend the cross-country semantic QA gate beyond dates (null-rate bounds, field-source distribution, and by-status sanity checks).
- Add a country-level gold sample validation set and require it on parser changes before production merge.

## QA Gate (Run Every Time)

Before shipping data (or immediately after a bulk merge), run:

```bash
./scripts/qa-gate-tender-dates.sh SOURCE=PLACSP_ES
./scripts/qa-gate-tender-dates.sh SOURCE=BOAMP_FR
./scripts/qa-gate-tender-dates.sh SOURCE=ETENDERS_IE
```
