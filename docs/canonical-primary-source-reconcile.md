# Canonical Primary Source + TED/National Reconciliation

This runbook keeps canonical tender source evidence aligned with canonical links and improves TED↔national linking quality (IE/FR/ES) without creating duplicate canonicals.

## What it does

1. Derives canonical `primary_source_*` fields from linked notices.
2. Keeps `source_url` aligned to the selected primary linked notice when available.
3. Reconciles TED notices against national canonical tenders (`ETENDERS_IE`, `BOAMP_FR`, `PLACSP_ES`) with deterministic scoring:
   - auto-link if `score >= 0.90`
   - queue for review if `0.75 <= score < 0.90`
   - otherwise leave unchanged

## Required migration

Apply:

- `database/migrations/20260218_canonical_primary_source_ted_etenders_reconcile_v1.sql`
- `database/migrations/20260218_canonical_reconcile_multi_country_v1.sql`
- `database/migrations/20260218_canonical_reconcile_country_norm_perf_v1.sql`
- `database/migrations/20260218_canonical_reconcile_country_norm_perf_v2.sql`

## Rollout command

Dry-run reconciliation (no relink writes):

```bash
./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 10000 500 false IE,FR,ES
```

Apply reconciliation:

```bash
./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 0 0 true IE,FR,ES
```

Arguments:

1. `TENANT_ID` (required)
2. `BACKFILL_LIMIT` (default `10000`)
3. `RECON_LIMIT` (default `500`)
4. `APPLY_RECONCILE` (`true|false`, default `false`)
5. `COUNTRY_LIST` (default `IE`; accepts CSV like `IE,FR,ES`)

Notes:
- `BACKFILL_LIMIT=0` means full tenant backfill scan.
- `RECON_LIMIT=0` means full TED notice scan per country.
- Batching is controlled by:
  - `BACKFILL_BATCH_SIZE` (default `5000`)
  - `RECON_BATCH_SIZE` (default `20`)

## Ingestion-time automation

Each connector rollout now runs a post-ingestion reconciliation step automatically:

- `scripts/rollout-etenders-ie-incremental.sh`
- `scripts/rollout-boamp-fr-incremental.sh`
- `scripts/rollout-placsp-es-incremental.sh`
- `scripts/rollout-ted-incremental.sh`

Shared helper:

- `scripts/reconcile-ted-national.sh`

Toggles:

- `RECONCILE_AFTER_INGEST` (`true`/`false`, default `true`)
- `RECONCILE_LIMIT` (default `25` for IE/FR, `20` for ES, `20` for TED)
- `RECONCILE_STRICT` (`true` to fail connector run on reconcile error, default `false`)

## Daily sweep automation

GitHub Actions workflow:

- `.github/workflows/canonical-reconcile-daily.yml`

Runs daily and can also be invoked manually to do full-tenant reconciliation sweeps.

Requires `SUPABASE_DB_URL` (or `DATABASE_URL`) in environment.

## QA checks

Run:

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-canonical-primary-source-reconcile.sql
```

Additional canonical QA pack:

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-canonical-notice-linking.sql
```

## Expected outcomes

- `primary_source_url` reflects linked notice URL when linked notice exists.
- `coverage_status` and `verification_level` remain invariant-compliant.
- TED/national high-confidence pairs move to shared canonical rows.
- Search remains one-row-per-canonical (no duplicates).
