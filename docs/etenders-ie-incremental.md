# eTenders IE Incremental Connector (Current/Delta Only)

## Goal
Keep Ireland eTenders data fresh by **incrementally** ingesting only **new/updated CfTs** into the app's "current" store (`public."TendersCurrent"`) without reloading canonical history.

This connector is intentionally **additive and replay-safe**.

## Source (Official)
We use the official eTenders portal HTML listing for "Latest CfTs":
- `https://www.etenders.gov.ie/epps/quickSearchAction.do?searchType=cftFTS&latest=true`

Each row includes a stable `resourceId` and a public detail URL:
- `https://www.etenders.gov.ie/epps/cft/prepareViewCfTWS.do?resourceId=<resourceId>`

## Cursor / Watermark
Cursor is stored per tenant in `public."ConnectorConfig".config` under:
- `connector_key = 'etenders_ie_incremental:<TENANT_ID>'`

Shape:
```json
{
  "cursor": {
    "type": "published",
    "value": "2026-02-13 22:46:25+00",
    "last_success_at": "2026-02-15 18:30:00+00"
  }
}
```

We apply a built-in overlap of **2 days** when using the stored cursor to reduce the risk of missing late updates.

## Local Run (Dry-Run)
```bash
./scripts/rollout-etenders-ie-incremental.sh civant_default 2026-02-01 true
```

## Local Run (Write Mode)
Requires `SUPABASE_DB_URL` (or `DATABASE_URL`) at runtime:
```bash
export SUPABASE_DB_URL='postgresql://postgres:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require'
./scripts/rollout-etenders-ie-incremental.sh civant_default 2026-02-01 false
```

If `START_DATE` is omitted, the script will use the stored cursor (minus 2 days overlap).

## Verify In Supabase
Counts:
```sql
select source, count(*)
from public."TendersCurrent"
where tenant_id = 'civant_default'
  and tender_id like 'ETENDERS_IE:%'
group by 1;
```

Run QA pack:
```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-etenders-ie-incremental.sql
```
