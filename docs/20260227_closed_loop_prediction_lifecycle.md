# Closed-Loop Prediction Lifecycle (FR/IE/ES)

## Goal
Implement a deterministic loop:
`prediction -> notice reality -> reconciliation outcome -> canonical history -> next prediction`

## Core Flow
1. Seed lifecycle rows from `predictions_current` via `bootstrap_prediction_lifecycle_from_current`.
2. New canonical notice links (`canonical_notice_links`) trigger `reconcile_prediction_for_canonical_notice`.
3. Reconciler marks prediction as:
   - `Hit` when score is high and notice is in-window.
   - `NeedsReview` when plausible but outside exact window.
   - no state change when low confidence.
4. Outcomes are logged in:
   - `prediction_reconciliation_log` (QA trace)
   - `prediction_cycle_history` (historical lifecycle ledger)
5. On `Hit`, the system refreshes buyer+cluster stats and computes next prediction.
6. Misses are advanced with `evaluate_prediction_lifecycle_misses`.

## Matching Logic (Pseudo-code)
```text
INPUT: tenant_id, canonical_id, notice_id?

notice = load canonical_tenders + primary notice context
if missing buyer_entity_id or publication_date: return insufficient_context

candidates = open lifecycle rows for same tenant + buyer + region + cluster
for each candidate:
  within_window = publication_date in [window_start, window_end]
  within_grace = publication_date in [window_start-grace, window_end+grace]
  score = 0
  score += 0.55 if buyer match
  score += 0.25 if cluster exact (or 0.05 for unknown fallback)
  score += 0.20 if within_window
  score += 0.10 if within_grace but outside window

best = max(score, closest to window midpoint)

if best.score >= 0.80 and best.within_window:
  mark lifecycle Hit
  set linked canonical/notice
  write reconciliation log + cycle history
  recompute pair prediction (next cycle)
  upsert next lifecycle draft row
else if best.score >= 0.60 and best.within_grace:
  mark lifecycle NeedsReview
  enqueue candidate(s) for manual review
  write reconciliation log + cycle history
else:
  write notice_seen QA log only
```

## Manual Review Queue
- Table: `prediction_reconciliation_candidates`
- Resolver: `resolve_prediction_reconciliation_candidate`
  - `approve` -> marks lifecycle `Hit`
  - `reject` -> candidate rejected and lifecycle returns to `Monitoring` when no pending candidates remain

## Reporting
- Timeline rows: `get_buyer_category_timeline_rows`
- Full report JSON: `get_buyer_category_cycle_trend_report`
  - timeline table
  - cycle metrics (avg/median/variance/stddev/trend/confidence)
  - leading indicators (mods/support tenders/extensions)
  - next prediction window + top drivers + evidence links

## Operational Sequence
1. Run predictive engine (`run_predictive_engine_v1`).
2. Seed lifecycle (`bootstrap_prediction_lifecycle_from_current`).
3. Ingestion continues; canonical link trigger auto-reconciles.
4. Run miss advancement periodically:
   `evaluate_prediction_lifecycle_misses(tenant_id, now(), grace_days)`
