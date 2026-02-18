# DB Payload Offload

Goal: reduce DB size and IO by offloading large JSON payloads to object storage while preserving deterministic access and auditability.

## Architecture Overview
- Payloads are compressed (`gzip`) and uploaded to Supabase Storage bucket `civant-payloads` (configurable).
- DB stores pointer metadata:
  - `raw_object_key`
  - `payload_hash_sha256`
  - `payload_bytes`
  - `payload_stored_at`
- Payload columns remain present for backward compatibility.
- Default behavior:
  - `notices.raw_payload` is NULLed after successful offload.
  - `TendersCurrent.data` and `canonical_tenders.normalized_json` remain intact for now.

## Phase 0: Write Paths (Located)

Primary write points (payload sources):
- `/Users/davidmanrique/Documents/Civant/database/migrations/20260217_notices_canonical_sync_v1.sql`
  - `public.process_tenderscurrent_record(...)` parses `TendersCurrent.data`
  - `public.upsert_notice_and_link(...)` writes `notices.raw_payload` and `canonical_tenders.normalized_json`
- `/Users/davidmanrique/Documents/Civant/functions/fetchEtendersIeIncremental.ts`
  - Writes `public."TendersCurrent".data`
- Legacy / optional ingestion:
  - `/Users/davidmanrique/Documents/Civant/functions/fetchIreland.ts`
  - `/Users/davidmanrique/Documents/Civant/functions/fetchTed.ts`
  - `/Users/davidmanrique/Documents/Civant/functions/fetchBoampFr.ts`
  - `/Users/davidmanrique/Documents/Civant/functions/importCsvData.ts`

## Required Env Vars

For ingestion offload and backfill:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `SUPABASE_STORAGE_BUCKET` (bucket name, default target is `civant-payloads`)
- `SUPABASE_DB_URL` (for backfill + verification SQL)

Optional:
- `OFFLOAD_FALLBACK_ALLOW_DB_PAYLOAD=true`
  - If set, ingestion can temporarily store payload JSON in DB when storage upload fails.
  - If not set, ingestion fails loudly and logs to `payload_offload_failures`.

## How Offload Works (NEW writes)

1. Ingestion code computes a canonical JSON string (sorted keys, deterministic).
2. `sha256` hash is computed from canonical bytes.
3. Gzipped payload uploaded to:
   ```
   {tenant}/{table}/{primary_key}/{sha256}.json.gz
   ```
4. Pointer metadata is written to the row.
5. For `notices.raw_payload`, payload is NULLed after successful offload.

Failures are recorded in `public.payload_offload_failures` and the operation fails unless fallback is explicitly enabled.

## Backfill (Resumable)

Script:
- `/Users/davidmanrique/Documents/Civant/scripts/offload-payloads-to-storage.sh`

Examples:
```bash
# Notices (default: NULL raw_payload after upload)
./scripts/offload-payloads-to-storage.sh --table notices --tenant civant_default --batch-size 1000

# TendersCurrent (keep data column)
./scripts/offload-payloads-to-storage.sh --table TendersCurrent --tenant civant_default --batch-size 1000 --null-after-upload false

# Canonical tenders (keep normalized_json)
./scripts/offload-payloads-to-storage.sh --table canonical_tenders --tenant civant_default --batch-size 1000 --null-after-upload false

# Dry run
./scripts/offload-payloads-to-storage.sh --table notices --tenant civant_default --batch-size 1000 --dry-run

# Restart from scratch (ignore checkpoint)
./scripts/offload-payloads-to-storage.sh --table notices --tenant civant_default --batch-size 1000 --no-resume
```

Checkpointing:
- Stored in `public.payload_offload_state` keyed by `(tenant_id, table_name)`.

## Verification

SQL verification:
```bash
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v tenant_id='civant_default' -f scripts/verify-payload-offload.sql
```

The script reports:
- Offloaded % per table
- Average payload size
- Failure count
- Sample hash verification (50 rows)
- Table sizes (table + toast + indexes)
- search engine mode check
- prediction smoke check

## What NOT to Do
- Do not run `VACUUM FULL`.
- Do not drop or remove payload columns yet.
- Do not reintroduce JSON payload search.

## Rollback / Disable

To disable offload:
- Unset `SUPABASE_STORAGE_BUCKET` or `SUPABASE_SERVICE_ROLE_KEY` in ingestion runtime.
- Optionally set `OFFLOAD_FALLBACK_ALLOW_DB_PAYLOAD=true` for temporary fallback.

To keep payloads in DB:
- Stop running the backfill script.
- Ensure `notices.raw_payload` remains non-NULL for new writes by disabling offload.

## VACUUM Guidance
- After large backfills, use `VACUUM (ANALYZE)` (not `FULL`) during low traffic.

