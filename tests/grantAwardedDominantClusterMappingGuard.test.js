import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260224_grant_awarded_dominant_cluster_map_v2.sql', import.meta.url),
  'utf8',
);

test('grant mapping migration is tenant-scoped and targets unknown grant cpv rows', () => {
  assert.match(source, /Migration: 20260224_grant_awarded_dominant_cluster_map_v2\.sql/);
  assert.match(source, /where s\.tenant_id = 'civant_default'/);
  assert.match(source, /and s\.signal_type = 'grant_awarded'/);
  assert.match(source, /and \(s\.cpv_cluster_id is null or s\.cpv_cluster_id = 'cluster_unknown'\)/);
});

test('dominant cluster derivation uses notice_published over 15 years without share threshold gating', () => {
  assert.match(source, /from public\.signals s/);
  assert.match(source, /and s\.signal_type = 'notice_published'/);
  assert.match(source, /and s\.occurred_at >= now\(\) - interval '15 years'/);
  assert.match(source, /and s\.cpv_cluster_id <> 'cluster_unknown'/);
  assert.match(source, /coalesce\(public\.normalize_prediction_region\(s\.region, s\.source\), 'IE'\) as region/);
  assert.match(source, /where dc\.rn = 1[\s\S]*and dc\.buyer_total_notice_count > 0/s);
  assert.doesNotMatch(source, /dominant share must be >= 40%/i);
});

test('mapping metadata and signal strength scaling are persisted', () => {
  assert.match(source, /'method', 'dominant_cluster_v2_top_cluster'/);
  assert.match(source, /'dominant_cluster_pct', round\(dom\.dominant_cluster_pct, 4\)/);
  assert.match(source, /'confidence_band', dom\.confidence_band/);
  assert.match(source, /coalesce\(s\.signal_strength, 0\.5\) \* greatest\(0\.10::numeric, least\(1::numeric, dom\.dominant_cluster_pct\)\)/);
});

test('migration includes rollback and material increase validation query', () => {
  assert.match(source, /Rollback \(best-effort\):/);
  assert.match(source, /create temp table _grant_map_before as/);
  assert.match(source, /now_mapped_delta/);
});
