# FR/ES External Funding Signals

This rollout adds funding-oriented external signals for France and Spain using official open-data catalogs.

## Sources

- France: `data.gouv.fr` datasets/resources API (`/api/1/datasets/`)
- Spain: `datos.gob.es` APIDATA catalog (`/apidata/catalog/dataset.json`)

## What gets written

- `public.external_signals_fr`
- `public.external_signals_es`
- `public.external_signal_rollup_fr`
- `public.external_signal_rollup_es`

## Migrations

Apply:

- `database/migrations/20260218_external_signals_fr_es_v1.sql`
- `database/migrations/20260218_external_signal_rollup_fr_es_v1.sql`

## Dry run

```bash
./scripts/rollout-external-signals-fr.sh civant_default 2026-01-01 true
./scripts/rollout-external-signals-es.sh civant_default 2026-01-01 true
```

Or both:

```bash
./scripts/rollout-external-signals-fr-es.sh civant_default 2026-01-01 true
```

## Write run

```bash
export SUPABASE_DB_URL='postgresql://postgres:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require'
./scripts/rollout-external-signals-fr.sh civant_default 2026-01-01 false
./scripts/rollout-external-signals-es.sh civant_default 2026-01-01 false
```

## QA

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-external-signals-fr.sql
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-external-signals-es.sql
```

## Forecast usage

Forecast (`Tender Panorama`) now reads rollups from IE, FR, and ES and blends the external signal strength into cadence scoring.
