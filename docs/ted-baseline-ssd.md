# TED Baseline Pilot on SSD (IE/FR/ES)

Last updated: 2026-02-16

This runbook stages TED baseline data on an external SSD before any DB ingestion.

## Scope
- Countries: `IRL`, `FRA`, `ESP`
- Window: `2024-08` through `2026-02` (adjustable)
- Mode: staged raw extraction first, DB merge later

## SSD layout
- Volume: `/Volumes/Civant`
- Shared system paths:
  - Raw data: `/Volumes/Civant/_system/raw/ted_baseline`
  - Checkpoints: `/Volumes/Civant/_system/checkpoints/ted_baseline`
  - Reports: `/Volumes/Civant/_system/reports/ted_baseline`
  - Logs: `/Volumes/Civant/_system/logs/ted_baseline`

## What runs
Script: `/Users/davidmanrique/projects/Civant/scripts/ted/ted-baseline-ie-fr-es.mjs`
Wrapper: `/Users/davidmanrique/projects/Civant/scripts/rollout-ted-baseline-ssd.sh`

### Modes
- `size`: counts notices per country/month (no data download)
- `fetch`: downloads notice payloads as NDJSON chunk files (country + month)

## Daily restart-safe flow
The fetch runner checkpoints chunk state after each API page and marks a chunk done only when complete.

Recommended daily cadence:
1. Start a bounded run (`max_seconds` 6-10h)
2. Let it stop naturally or Ctrl+C near end of day
3. Reboot laptop
4. Start same command next day (auto-resume from checkpoint)

## Commands

### 1) Sizing pass
```bash
cd /Users/davidmanrique/projects/Civant
./scripts/rollout-ted-baseline-ssd.sh size 2024-08 2026-02
```

### 2) Fetch baseline (8-hour window)
```bash
cd /Users/davidmanrique/projects/Civant
./scripts/rollout-ted-baseline-ssd.sh fetch 2024-08 2026-02 28800
```

### 3) Resume next day
Run the same fetch command again. It resumes from checkpoint.

## Throughput and time estimate
Sizing totals for 2024-08..2026-02:
- IRL: 15,701
- FRA: 147,149
- ESP: 104,405
- Total: 267,255 notices

With `limit=250`, expected page requests are roughly:
- ~1,069 API pages total

Expected wall-clock for extraction on a laptop:
- Typical: 4-10 hours
- Conservative with retries/throttling: up to 1-2 days split across daily windows

## Cost and performance guidance
- Use SSD writes only (already configured) to avoid internal disk pressure.
- Keep `sleep-ms` at 100-200ms to reduce API throttling.
- Prefer wired network when possible.
- Avoid running heavy local builds while fetch is active.
- Keep extraction staged; do not ingest to DB until QA passes.

## Outputs
- NDJSON chunk files: `/Volumes/Civant/_system/raw/ted_baseline/<COUNTRY>/<YEAR>/<YYYY-MM>.ndjson`
- Checkpoint: `/Volumes/Civant/_system/checkpoints/ted_baseline/ted_baseline_<start>_<end>.checkpoint.json`
- Reports: `/Volumes/Civant/_system/reports/ted_baseline/`
- Logs: `/Volumes/Civant/_system/logs/ted_baseline/`

## Next step after baseline
1. Run QA over staged files (duplicates, null rates, date sanity)
2. Merge a bounded first phase (last 6 months) into `TendersCurrent`/`TenderVersions`
3. Validate DB QA pack and connector health
4. Enable `ted_incremental:<tenant_id>` daily sync

## Phase-1 merge (last 6 months)

### 1) Staged QA (local SSD files)
```bash
cd /Users/davidmanrique/projects/Civant
./scripts/qa-ted-baseline-staged.sh 2025-09 2026-02 /Volumes/Civant/_system/raw/ted_baseline
```

### 2) Dry run merge
```bash
cd /Users/davidmanrique/projects/Civant
./scripts/rollout-ted-baseline-merge.sh civant_default 2025-09 2026-02 true
```

### 3) Real merge (write mode)
```bash
cd /Users/davidmanrique/projects/Civant
export SUPABASE_DB_URL='postgresql://postgres:<PASSWORD>@db.<PROJECT_REF>.supabase.co:5432/postgres?sslmode=require'
./scripts/rollout-ted-baseline-merge.sh civant_default 2025-09 2026-02 false
```

### 4) Post-merge DB QA
```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v tenant_id='civant_default' -f scripts/qa-ted-baseline.sql
```
