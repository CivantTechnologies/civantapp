import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260223_predictive_external_signal_buyer_level_v1.sql', import.meta.url),
  'utf8',
);

test('external signal selection applies buyer-level cpv signals to all pairs and keeps exact cluster match', () => {
  assert.match(source, /create or replace function public\.compute_prediction_for_pair_v1\(/);
  assert.match(
    source,
    /and \(\(s\.cpv_cluster_id is null or s\.cpv_cluster_id = 'cluster_unknown'\) or s\.cpv_cluster_id = coalesce\(p_cpv_cluster_id, 'cluster_unknown'\)\)/,
  );
});

test('external scoring caps remain unchanged', () => {
  assert.match(source, /if not v_non_external_support then\s*v_external_score := least\(v_external_score, 18\);/s);
  assert.match(source, /v_total_score := greatest\(0, least\(100, v_total_score\)\);/);
});
