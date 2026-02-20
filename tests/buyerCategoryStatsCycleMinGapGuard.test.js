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
  assert.match(source, /for r in \(\s*select[\s\S]*from public\.buyer_category_stats bcs/s);
  assert.match(source, /where bcs\.tenant_id = 'civant_default'/);
  assert.match(source, /and bcs\.region = 'IE'/);
  assert.match(source, /and bcs\.tender_count_24m >= 3/);
  assert.match(source, /and bcs\.avg_cycle_days is null/);
  assert.match(source, /perform public\.recompute_buyer_category_stats_v2\(/);
});

test('validation SQL includes should_have_cycle_but_missing metric for IE', () => {
  assert.match(source, /should_have_cycle_but_missing/);
  assert.match(source, /p\.notice_days >= 2/);
  assert.match(source, /v_still_missing <> 0/);
  assert.match(source, /still_missing, \(still_missing = 0\) as assert_zero/);
});
