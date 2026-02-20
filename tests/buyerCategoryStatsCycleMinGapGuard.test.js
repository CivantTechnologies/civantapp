import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260223_buyer_category_stats_cycle_min_gap_v1.sql', import.meta.url),
  'utf8',
);

test('recompute_buyer_category_stats_v2 uses 1-day min gap with same-day dedupe', () => {
  assert.match(source, /create or replace function public\.recompute_buyer_category_stats_v2\(/);
  assert.match(source, /select distinct occurred_at::date as event_day/);
  assert.match(source, /where gap_days between 1 and 2555/);
  assert.doesNotMatch(source, /where gap_days between 7 and 2555/);
});

test('migration recomputes scoped IE rows with missing avg cycle stats', () => {
  assert.match(source, /for r in \(\s*with eligible_pairs as \(/s);
  assert.match(source, /join public\.signals s/);
  assert.match(source, /s\.cpv_cluster_id = bcs\.cpv_cluster_id/);
  assert.match(source, /coalesce\(public\.normalize_prediction_region\(s\.region, null\), 'IE'\) = bcs\.region/);
  assert.match(source, /group by 1, 2, 3, 4/);
  assert.match(source, /having count\(distinct s\.occurred_at::date\) >= 3/);
  assert.match(source, /count\(distinct s\.occurred_at::date\)::int as notice_day_count/);
  assert.match(source, /s\.occurred_at >= v_since/);
  assert.match(source, /s\.occurred_at <= v_as_of/);
  assert.match(source, /where bcs\.tenant_id = 'civant_default'/);
  assert.match(source, /and bcs\.region = 'IE'/);
  assert.match(source, /and bcs\.avg_cycle_days is null/);
  assert.match(source, /perform public\.recompute_buyer_category_stats_v2\(/);
});

test('validation and assertion use eligible pair set and prove zero remain', () => {
  assert.match(source, /with eligible_pairs as \(/);
  assert.match(source, /select count\(\*\)::int as still_missing/);
  assert.match(source, /from eligible_pairs/);
  assert.match(source, /v_still_missing <> 0/);
  assert.match(source, /still_missing, \(still_missing = 0\) as assert_zero/);
});
