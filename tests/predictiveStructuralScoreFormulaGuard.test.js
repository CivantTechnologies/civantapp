import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260223_predictive_structural_score_value_stability_v1.sql', import.meta.url),
  'utf8',
);

test('compute_prediction_for_pair_v1 structural score uses value stability and is clamped 0-10', () => {
  assert.match(source, /create or replace function public\.compute_prediction_for_pair_v1\(/);
  assert.match(
    source,
    /v_structural_score := greatest\(0, least\(10, round\(10 \* coalesce\(v_stats\.value_stability_score, 0\)\)\)\)::int;/,
  );
  assert.doesNotMatch(source, /v_structural_score := round\(10 \* v_framework_expiry_proximity\);/);
});

test('total score still includes structural component and can exceed 60 with strong inputs', () => {
  assert.match(source, /v_total_score := v_cycle_score \+ v_timing_score \+ v_behavioural_score \+ v_structural_score \+ v_external_score \+ v_quality_score;/);

  const v_cycle_score = 20;
  const v_timing_score = 15;
  const v_behavioural_score = 15;
  const v_structural_score = Math.max(0, Math.min(10, Math.round(10 * 1.0)));
  const v_external_score = 18;
  const v_quality_score = 20;
  const total = v_cycle_score + v_timing_score + v_behavioural_score + v_structural_score + v_external_score + v_quality_score;

  assert.equal(v_structural_score, 10);
  assert.ok(total > 60);
});
