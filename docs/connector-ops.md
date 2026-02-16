# Connector Ops Runbook (IE + FR)

This runbook is the quick, repeatable way to validate connector health and data quality in production/staging.

Scope today:
- `ETENDERS_IE` incremental connector (Ireland)
- `BOAMP_FR` import connector (France)

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

## 4) Unified Health Checks

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
  and connector_key in ('etenders_ie_incremental:civant_default', 'boamp_fr_incremental:civant_default')
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
  and source in ('ETENDERS_IE', 'BOAMP_FR')
order by published_at desc nulls last
limit 25;
```

## 5) Interpreting Results

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

## 6) Current known constraints

- `TED` is intentionally deferred due to EU-wide volume.
- `PLACSP_ES` not covered by this runbook section.
- For eTenders implementation details and cursor logic, see:
  - `docs/etenders-ie-incremental.md`
