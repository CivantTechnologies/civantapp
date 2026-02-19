# Predictive Confidence Report (civant_default)

Date: 2026-02-18

## Summary
A batch recompute was executed using the scoped runner (recent-activity strategy, 10 batches of 2,000). Coverage increased, but high-confidence forecasts (>= 60) are still not produced. Diagnostics show missing core inputs for resolved pairs: `buyer_category_stats` lacks `avg_cycle_days` and `last_award_at` for nearly all rows, and `evidence_count` is zero for all resolved predictions.

## Before vs After (Key Metrics)

**Before batch run**
- total predictions: 14,626
- resolved predictions: 2,493
- unresolved predictions: 12,133
- max confidence (resolved): 17
- confidence >= 60: 0
- fallback tier (resolved): tier 4 = 2,488, tier 3 = 5
- evidence_count (resolved): 0 for all
- buyer_category_stats completeness: avg_cycle_days present = 5; last_award_at present = 5

**After batch run**
- total predictions: 23,624
- resolved predictions: 9,135
- unresolved predictions: 14,489
- max confidence (resolved): 17
- confidence >= 60: 0
- fallback tier (resolved): tier 4 = 9,130, tier 3 = 5
- evidence_count (resolved): 0 for all
- buyer_category_stats completeness: avg_cycle_days present = 5; last_award_at present = 5

## Root Causes (Evidence-Based)
1) **buyer_category_stats is incomplete**
   - `avg_cycle_days` and `last_award_at` are populated for only 5 rows, while the table has 24,189 rows.
   - These stats drive cadence/seasonality and are prerequisites for higher confidence.

2) **evidence_count is zero for all resolved predictions**
   - Evidence-based boosting appears to be missing entirely (either no evidence is being computed or a join/write bug is preventing persistence).

3) **Top forecast candidates lack core signal types**
   - Top 20 resolved pairs with evidence_count=0 are missing `award_published`, `framework_established`, `cancellation`, `amendment`, and often `notice_published`.

## Recommended Next Deterministic Fixes (Top 3)
1) **Backfill or rebuild buyer_category_stats**
   - Investigate and run the stats refresh for all resolved buyer+cpv+region pairs.
   - Validate that `avg_cycle_days` and `last_award_at` are populated.

2) **Audit evidence pipeline in compute_prediction_for_pair_v1**
   - Verify evidence generation, especially joins to signal sources and persistence to prediction_drivers/evidence_count.
   - Confirm evidence_refs are written and the count increments for resolved predictions.

3) **Improve signal coverage for top candidates**
   - Ensure award and notice signals are mapped to buyer+cpv+region for top forecast-score pairs.

## Batch Run Details
- Strategy: recent activity
- Batches: 10 (offsets 0–18,000)
- First 5 batches processed 10,000 pairs total
- Remaining batches inserted 0 pairs (no additional recent pairs beyond offset 10,000)

---

## Update (2026-02-19)

### Scope
- Applied stats/evidence fixes and re-ran batch recompute in smaller batches due to connection resets on larger scopes.
- Total processed in latest run: 2,000 pairs (20 batches of 100).

### After Latest Run (Key Metrics)
- total predictions: 23,716
- resolved predictions: 9,194
- unresolved predictions: 14,522
- max confidence (resolved): 17
- confidence >= 60: 0
- fallback tier (resolved): tier 4 = 9,189, tier 3 = 5
- evidence_count (resolved): non-zero now (1–6 evidence refs for 1,497 resolved predictions)
- buyer_category_stats completeness: avg_cycle_days present = 5; last_award_at present = 5 (table size 29,152)

### Delta Summary
- Evidence persistence is now working (evidence_count > 0 appears).
- Confidence ceiling remains low (max 17), tied to missing cadence/award stats.
- `buyer_category_stats` remains the dominant blocker for high confidence.

---

## Deterministic Confidence Unlock Patch (2026-02-19)

### Root cause (proven)
1) Mapping coverage exists, but usable pair-level inputs are under-consumed in stats:
- `award_published` and `notice_published` signals are present at scale.
- `buyer_category_stats.avg_cycle_days` and `buyer_category_stats.last_award_at` remain near-empty, which keeps most resolved rows in fallback tiers.

2) Evidence persistence was not deterministic across all scoring driver rows:
- Drivers could exist while `evidence_refs` remained empty on key types.
- `evidence_count` aggregation could double-count duplicates or under-represent mixed driver evidence.

### Applied deterministic fixes
1) `database/migrations/20260222_buyer_category_stats_backfill_fix_v1.sql`
- Reworked `public.recompute_buyer_category_stats_v1(...)` to:
  - derive `last_tender_at` from `notice_published`
  - derive `last_award_at` from `award_published`
  - compute cadence using both notice->notice and award->next-notice intervals
  - require at least 3 intervals before setting `avg_cycle_days` / `std_cycle_days`
  - score `data_coverage_score` and `cadence_reliability_score` from deterministic counts/stability
  - avoid canonical table dependencies for buyer mapping

2) `database/migrations/20260222_prediction_evidence_persist_fix_v1.sql`
- Hardened `public.attach_prediction_evidence_v1(...)`:
  - dedupe evidence refs deterministically
  - attach notice evidence to both `cadence_pattern` and `coverage_penalty`
  - compute `predictions_current.evidence_count` as unique refs across all drivers

3) `scripts/backfill-buyer-category-stats.sh`
- Resumable, chunked backfill for eligible pairs from `signals`:
  - resolved buyer only, non-null CPV, ISO2 region, procurement signal types
  - supports `recent | region | bucket`
  - respects `MAX_BATCHES` for safe progressive runs
  - state persisted to `scripts/.state/buyer-category-stats-<tenant>-<strategy>.state`

4) `scripts/qa-predictive-confidence-diagnostics.sql`
- Added Phase 0 mapping-vs-computation proof pack:
  - award/notice usable coverage counts + percents
  - top-50 resolved (evidence=0) exact-pair signal presence checks
- Added Phase 2 evidence isolation sample (20 resolved predictions):
  - relevant signal presence vs `prediction_drivers` rows vs non-empty `evidence_refs`

### Exact run sequence
```bash
# 1) Apply deterministic patch migrations
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f database/migrations/20260222_buyer_category_stats_backfill_fix_v1.sql
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f database/migrations/20260222_prediction_evidence_persist_fix_v1.sql

# 2) Baseline diagnostics before backfill
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v tenant_id='civant_default' -f scripts/qa-predictive-confidence-diagnostics.sql

# 3) Backfill stats in resumable chunks (example: 10k pairs total)
./scripts/backfill-buyer-category-stats.sh civant_default 1000 recent 10

# 4) Recompute predictions for same scope (progressive batches)
./scripts/run-predictive-engine-v1-batch.sh civant_default 1000 10 recent false v1.0.0

# 5) Post-fix diagnostics
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v tenant_id='civant_default' -f scripts/qa-predictive-confidence-diagnostics.sql
```

### Post-fix validation checklist
- `buyer_category_stats`: meaningful increase in
  - `avg_cycle_days is not null`
  - `last_award_at is not null`
- `predictions_current` (resolved):
  - `max(confidence)` increases from prior ceiling
  - `count(confidence >= 60)` becomes non-zero when warranted
  - fallback tier shifts out of tier 4 concentration
  - `evidence_count` distribution shows non-zero values
- top confidence sample should include non-empty driver evidence refs.

### Rollback plan
```bash
# Revert function definitions by reapplying prior known-good migrations
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f database/migrations/20260220_predictive_engine_v1_reliability_fix.sql
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f database/migrations/20260222_prediction_runs_status_fix_v1.sql

# Optional: clear run state for fresh retry
rm -f scripts/.state/buyer-category-stats-civant_default-*.state

# Re-run full deterministic recompute
./scripts/run-predictive-engine-v1-batch.sh civant_default 1000 10 recent false v1.0.0
```
