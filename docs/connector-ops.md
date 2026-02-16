# Connector Ops Runbook (IE + FR + ES)

This runbook is the quick, repeatable way to validate connector health and data quality in production/staging.

Scope today:
- `ETENDERS_IE` incremental connector (Ireland)
- `BOAMP_FR` incremental connector (France)
- `PLACSP_ES` incremental connector (Spain)
- `TED` incremental connector (IE/FR/ES scoped)

## 1) Prerequisites

From repo root:

```bash
cd /Users/davidmanrique/projects/Civant
```

Set DB URL (do not commit this):

```bash
export SUPABASE_DB_URL='postgresql://postgres:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require'
```

Optional DB connectivity check:

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -c "select now();"
```

## 2) eTenders IE Incremental

Dry-run first:

```bash
./scripts/rollout-etenders-ie-incremental.sh civant_default 2026-02-01 true
```

Real write run:

```bash
./scripts/rollout-etenders-ie-incremental.sh civant_default 2026-02-01 false
```

QA pack:

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-etenders-ie-incremental.sql
```

Expected healthy pattern:
- `status=success` in `ConnectorRuns`
- `fetched_count > 0` for active windows
- replay runs may show `inserted_count=0`, `noop_count>0` (this is normal)

## 3) BOAMP FR Incremental

Dry-run first:

```bash
./scripts/rollout-boamp-fr-incremental.sh civant_default 2026-02-01 true
```

Real write run:

```bash
./scripts/rollout-boamp-fr-incremental.sh civant_default 2026-02-01 false
```

QA pack:

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-boamp-fr-incremental.sql
```

Expected healthy pattern:
- first scoped run: `inserted_count > 0`
- immediate replay: mostly `noop_count > 0`, `inserted_count=0`, `updated_count=0`
- cursor in `ConnectorConfig.config.cursor.value` advances to latest `published_at`

GitHub Actions automation:
- workflow file: `.github/workflows/boamp-fr-incremental.yml`
- schedule: daily `05:45 UTC`
- manual dispatch supports `tenant_id`, `start_date`, `dry_run`, `max_pages`

## 4) PLACSP ES Incremental

Dry-run first:

```bash
./scripts/rollout-placsp-es-incremental.sh civant_default 2026-02-01 true
```

Real write run:

```bash
./scripts/rollout-placsp-es-incremental.sh civant_default 2026-02-01 false
```

QA pack:

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-placsp-es-incremental.sql
```

Expected healthy pattern:
- first scoped run: `inserted_count > 0`
- immediate replay: mostly `noop_count > 0`, `inserted_count=0`, `updated_count=0`
- cursor in `ConnectorConfig.config.cursor.value` advances to latest feed update

GitHub Actions automation:
- workflow file: `.github/workflows/placsp-es-incremental.yml`
- schedule: daily `06:15 UTC`
- manual dispatch supports `tenant_id`, `start_date`, `dry_run`, `max_pages`

## 5) TED Incremental

Dry-run first:

```bash
COUNTRIES=IRL,FRA,ESP MAX_PAGES=10 ./scripts/rollout-ted-incremental.sh civant_default 2026-02-01 true
```

Real write run:

```bash
COUNTRIES=IRL,FRA,ESP MAX_PAGES=40 ./scripts/rollout-ted-incremental.sh civant_default 2026-02-01 false
```

QA pack:

```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-ted-incremental.sql
```

Expected healthy pattern:
- first scoped run: `inserted_count > 0`
- immediate replay: mostly `noop_count > 0`, `inserted_count=0`, `updated_count=0`
- cursor in `ConnectorConfig.config.cursor.value` advances to latest TED `published_at`
- if no cursor exists and no `START_DATE` is passed, rollout bootstraps with a 30-day window

GitHub Actions automation:
- workflow file: `.github/workflows/ted-incremental.yml`
- schedule: daily `04:45 UTC`
- manual dispatch supports `tenant_id`, `start_date`, `dry_run`, `countries`, `max_pages`

## 6) Unified Health Checks

Connector runs summary:

```sql
select
  connector_key,
  status,
  started_at,
  finished_at,
  metadata->>'fetched_count'  as fetched_count,
  metadata->>'inserted_count' as inserted_count,
  metadata->>'updated_count'  as updated_count,
  metadata->>'noop_count'     as noop_count
from public."ConnectorRuns"
where tenant_id = 'civant_default'
  and connector_key in (
    'ted_incremental:civant_default',
    'etenders_ie_incremental:civant_default',
    'boamp_fr_incremental:civant_default',
    'placsp_es_incremental:civant_default'
  )
order by started_at desc
limit 20;
```

Source counts in current table:

```sql
select source, count(*)
from public."TendersCurrent"
where tenant_id = 'civant_default'
group by source
order by source;
```

Recent sample rows for visual sanity:

```sql
select
  tender_id,
  source,
  published_at,
  data->>'title' as title,
  data->>'buyer_name' as buyer_name,
  data->>'deadline_date' as deadline_date,
  data->>'source_url' as source_url
from public."TendersCurrent"
where tenant_id = 'civant_default'
  and source in ('TED', 'ETENDERS_IE', 'BOAMP_FR', 'PLACSP_ES')
order by published_at desc nulls last
limit 25;
```

## 7) Interpreting Results

Healthy:
- Run status is `success`
- No duplicate explosions in QA
- `source_url` is populated for most rows
- Replay runs produce mostly `noop_count`

Needs investigation:
- `status=failed/partial` repeatedly
- `fetched_count=0` unexpectedly for active windows
- sudden drop in source row counts
- high null-rate on key fields (`title`, `publication_date`, `source_url`)

## 8) Current known constraints

- TED incremental is scoped to `IRL,FRA,ESP` by default to keep runtime/cost predictable.
- For connector implementation details and cursor logic, see:
  - `docs/ted-incremental.md`
  - `docs/etenders-ie-incremental.md`
