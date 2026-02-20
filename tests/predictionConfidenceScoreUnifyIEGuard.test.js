import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260223_predictions_confidence_scorecard_unify_ie_v1.sql', import.meta.url),
  'utf8',
);

test('trigger sync is scoped to civant_default + IE only', () => {
  assert.match(source, /if new\.tenant_id <> 'civant_default' then\s*return new;/s);
  assert.match(source, /where pc\.tenant_id = new\.tenant_id[\s\S]*pc\.region = 'IE'/s);
});

test('prediction_scorecard writes drive confidence sync', () => {
  assert.match(source, /create trigger trg_sync_ie_confidence_from_scorecard_v1/);
  assert.match(source, /after insert or update of total_score\s*on public\.prediction_scorecard/s);
  assert.match(source, /confidence = new\.total_score/);
});

test('backfill is tenant- and prediction-scoped via scorecard join', () => {
  assert.match(source, /update public\.predictions_current pc/);
  assert.match(source, /where pc\.tenant_id = 'civant_default'/);
  assert.match(source, /pc\.region = 'IE'/);
  assert.match(source, /pc\.tenant_id = scored\.tenant_id/);
  assert.match(source, /pc\.prediction_id = scored\.prediction_id/);
  assert.match(source, /confidence = scored\.total_score/);
});
