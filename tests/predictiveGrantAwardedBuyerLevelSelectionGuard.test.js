import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260224_predictive_grant_awarded_buyer_level_selection_v1.sql', import.meta.url),
  'utf8',
);

test('grant_awarded bypasses cpv cluster match in external signal selection', () => {
  assert.match(source, /create or replace function public\.compute_prediction_for_pair_v1\(/);
  assert.match(source, /into v_effective_external, v_external_count[\s\S]*\(s\.signal_type = 'grant_awarded'\)/s);
});

test('non-grant external signal types still require cpv match or null cpv', () => {
  assert.match(
    source,
    /s\.signal_type <> 'grant_awarded'[\s\S]*s\.cpv_cluster_id = coalesce\(p_cpv_cluster_id, 'cluster_unknown'\)[\s\S]*or s\.cpv_cluster_id is null/s,
  );
  assert.match(source, /and s\.occurred_at >= \(p_as_of - interval '24 months'\)/);
});

test('external caps remain unchanged', () => {
  assert.match(source, /if not v_non_external_support then\s*v_external_score := least\(v_external_score, 18\);/s);
});
