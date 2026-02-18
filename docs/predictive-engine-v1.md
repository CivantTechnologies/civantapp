# Predictive Engine V1

Deterministic, explainable, tenant-scoped forecast pipeline for Civant.

## Architecture
- Ingestion connectors write source data to canonical and external-signal tables.
- Normalization functions map procurement + external events into `public.signals` using deterministic mapping tables.
- `run_predictive_engine_v1` computes/updates one forecast row per `(tenant_id, buyer_entity_id, cpv_cluster_id, region)` in `public.predictions_current`.
- Explainability and reproducibility are persisted in:
  - `public.prediction_drivers`
  - `public.prediction_scorecard`
  - `public.prediction_inputs_snapshot`
- UI is read-only against stored predictions (no browser-side compute path).

## Mapping Layer
- `public.buyer_aliases`: raw buyer key + source -> canonical buyer entity.
- `public.cpv_cluster`: canonical category taxonomy.
- `public.cpv_cluster_map`: deterministic CPV prefix -> cluster.
- `public.external_signal_to_cpv`: role keyword -> cluster.
- `public.signal_mappings`: override rules for source/signal/buyer/keyword to buyer + cluster + region.
- `public.external_signal_raw`: immutable-ish as-ingested store for traceability.

## Probability Formula
`probability =`
- `0.40 * cycle_proximity`
- `0.15 * seasonality_alignment`
- `0.15 * framework_expiry_proximity`
- `0.15 * authority_cadence_strength`
- `0.15 * external_signal_timing_boost`

Caps:
- probability `<= 0.95`
- external absolute contribution is bounded by weight (`<= 0.15`)

## Confidence Formula
`confidence = 100 * (`
- `0.30 * historical_depth`
- `0.20 * pattern_stability`
- `0.20 * data_coverage`
- `0.15 * behavioural_reliability`
- `0.15 * external_signal_corroboration`
`)`

Caps:
- unresolved/fallback buyer ids -> confidence capped to Medium (`<= 59`)
- default cap `<= 95` unless `tender_count_24m >= 5` and `data_coverage > 0.9`

Bands:
- 0-39 Low
- 40-59 Medium
- 60-79 High
- 80-100 Very High

## External Signal Decay
`effective_strength = signal_strength * exp(-days_since / half_life_days)`

Half-life defaults:
- `job_posting`: 45
- `hiring_spike`/`role_category_detected`: 30
- `budget_approval`/`budget_increase`: 120
- `grant_awarded`/`eu_funding_allocation`: 180
- policy/compliance/sustainability family: 365

## Fallback Tiers
- Tier 0: buyer + cpv cadence exists.
- Tier 1: buyer-level history + CPV baseline adjustment.
- Tier 2: behavioural twins in same region + cluster + similar volume.
- Tier 3: country-level CPV cadence baseline.
- Tier 4: sparse fallback with conservative defaults and capped confidence.
  - Hard guard: if no behavioural fingerprint exists, no prediction row is emitted.
  - External-only paths remain blocked unless behavioural evidence exists.

## Windowing
Forecast window labels (`next_window_label`):
- `next_30`
- `m1_3`
- `m3_6`
- `m6_12`

## Schedules
- `predictive-signals-v1.yml`: every 15 minutes (`signals_only`)
- `predictive-engine-incremental-v1.yml`: hourly incremental compute
- `predictive-engine-nightly-v1.yml`: nightly full recompute

## Manual Commands
Normalize + incremental run:
```bash
./scripts/rollout-predictive-engine-v1.sh civant_default incremental "" 5000 false v1.0.0
```

Signals-only normalization:
```bash
./scripts/rollout-predictive-engine-v1.sh civant_default signals_only "" 4000 true v1.0.0
```

Nightly/full equivalent:
```bash
./scripts/rollout-predictive-engine-v1.sh civant_default full "" 25000 true v1.0.0
```

Run health diagnostics for a real tenant:
```bash
/opt/homebrew/opt/libpq/bin/psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v tenant_id="civant_default" -f scripts/qa-predictive-engine-health.sql
```

`rollout-predictive-engine-v1.sh` now prints per-run metadata:
- `computed_prediction_rows`
- `skipped_pairs`

QA smoke pack:
```bash
./scripts/run-predictive-engine-v1-smoke.sh qa_predictive_v1
```
