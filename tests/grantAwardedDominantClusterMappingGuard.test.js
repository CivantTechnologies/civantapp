import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260223_grant_awarded_dominant_cluster_map_v1.sql', import.meta.url),
  'utf8',
);

test('grant dominant cluster migration exists with strict tenant/signal scope', () => {
  assert.match(source, /Migration: 20260223_grant_awarded_dominant_cluster_map_v1\.sql/);
  assert.match(source, /where s\.tenant_id = 'civant_default'/);
  assert.match(source, /and s\.signal_type = 'grant_awarded'/);
  assert.match(source, /and \(s\.cpv_cluster_id is null or s\.cpv_cluster_id = 'cluster_unknown'\)/);
});

test('dominant cluster rule uses >= 40% threshold with region normalization', () => {
  assert.match(source, /\(dc\.tender_count_24m::numeric \/ dc\.buyer_total_tenders::numeric\) >= 0\.40/);
  assert.match(source, /coalesce\(public\.normalize_prediction_region\(s\.region, s\.source\), 'IE'\) as normalized_region/);
  assert.match(source, /dom\.region = t\.normalized_region/);
});

test('migration includes rollback and validation comments', () => {
  assert.match(source, /Rollback \(best-effort\):/);
  assert.match(source, /method', 'dominant_cluster_v1'/);
  assert.match(source, /Validation Queries/);
  assert.match(source, /count\(\*\) filter \(where s\.external_signal_score > 0\) as with_external/);
});
