# TED Incremental Connector (IE/FR/ES Scope)

## Goal
Keep TED data fresh for Civant markets by incrementally ingesting only **new/updated** notices for:
- Ireland (`IRL`)
- France (`FRA`)
- Spain (`ESP`)

Writes are additive and replay-safe into:
- `public."TendersCurrent"`
- `public."TenderVersions"`

## Source (Official)
TED API v3 notice search endpoint:
- `https://api.ted.europa.eu/v3/notices/search`

Incremental filter pattern (per country):
- `CY = <ISO3> AND (PD >= <YYYYMMDD> OR DS >= <YYYYMMDD>)`

This captures newly published notices and notices with updated dispatch metadata.

## Cursor / Watermark
Cursor is stored per tenant in `public."ConnectorConfig".config` under:
- `connector_key = 'ted_incremental:<TENANT_ID>'`

Shape:
```json
{
  "cursor": {
    "type": "published",
    "value": "2026-02-16 10:20:00+00",
    "last_success_at": "2026-02-16 10:25:00+00"
  }
}
```

When `START_DATE` is omitted, rollout uses stored cursor with a **3-day overlap** for replay safety.
If no cursor exists yet, it bootstraps from the last **30 days**.

## Local Run (Dry-Run)
```bash
COUNTRIES=IRL,FRA,ESP MAX_PAGES=10 ./scripts/rollout-ted-incremental.sh civant_default 2026-02-01 true
```

## Local Run (Write Mode)
Requires `SUPABASE_DB_URL` (or `DATABASE_URL`) at runtime:
```bash
export SUPABASE_DB_URL='postgresql://postgres:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require'
COUNTRIES=IRL,FRA,ESP MAX_PAGES=40 ./scripts/rollout-ted-incremental.sh civant_default 2026-02-01 false
```

## Verify in Supabase
Counts:
```sql
select source, count(*)
from public."TendersCurrent"
where tenant_id = 'civant_default'
  and source = 'TED'
group by 1;
```

Run QA pack:
```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-ted-incremental.sql
```

## GitHub Actions (Daily)
Workflow file:
- `.github/workflows/ted-incremental.yml`

Dispatch inputs:
- `tenant_id`
- `start_date`
- `dry_run`
- `countries`
- `max_pages`

Repo secrets:
- `SUPABASE_DB_URL` (required)
- `CIVANT_TENANT_ID` (optional)
