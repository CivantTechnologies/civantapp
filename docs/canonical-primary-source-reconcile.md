# Canonical Primary Source + TED/eTenders Reconciliation

This runbook keeps canonical tender source evidence aligned with canonical links and improves TED↔eTenders linking quality without creating duplicate canonicals.

## What it does

1. Derives canonical `primary_source_*` fields from linked notices.
2. Keeps `source_url` aligned to the selected primary linked notice when available.
3. Reconciles TED notices against `ETENDERS_IE` canonical tenders with deterministic scoring:
   - auto-link if `score >= 0.90`
   - queue for review if `0.75 <= score < 0.90`
   - otherwise leave unchanged

## Required migration

Apply:

- `database/migrations/20260218_canonical_primary_source_ted_etenders_reconcile_v1.sql`

## Rollout command

Dry-run reconciliation (no relink writes):

```bash
./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 10000 500 false IE
```

Apply reconciliation:

```bash
./scripts/rollout-canonical-primary-source-reconcile.sh civant_default 10000 500 true IE
```

Arguments:

1. `TENANT_ID` (required)
2. `BACKFILL_LIMIT` (default `10000`)
3. `RECON_LIMIT` (default `500`)
4. `APPLY_RECONCILE` (`true|false`, default `false`)
5. `COUNTRY` (default `IE`)

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
- TED/eTenders high-confidence pairs move to shared canonical rows.
- Search remains one-row-per-canonical (no duplicates).
