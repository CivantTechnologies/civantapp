import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260223_predictive_include_grant_awarded_external_v1.sql', import.meta.url),
  'utf8',
);

test('compute_prediction_for_pair_v1 external signal list includes grant_awarded', () => {
  assert.match(source, /create or replace function public\.compute_prediction_for_pair_v1\(/);
  assert.match(source, /into v_effective_external, v_external_count[\s\S]*and s\.signal_type in \([\s\S]*'grant_awarded'/s);
});

test('external caps remain unchanged', () => {
  assert.match(source, /if not v_non_external_support then\s*v_external_score := least\(v_external_score, 18\);/s);
});
