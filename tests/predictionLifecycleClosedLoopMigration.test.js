import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(
  new URL('../database/migrations/20260227_prediction_closed_loop_cycle_report_v1.sql', import.meta.url),
  'utf8'
);

test('closed-loop migration defines lifecycle statuses and reconciliation artifacts', () => {
  assert.match(source, /create table if not exists public\.prediction_lifecycle/i);
  assert.match(source, /status text not null default 'Draft' check \(status in \('Draft', 'Published', 'Monitoring', 'Hit', 'Miss', 'Withdrawn', 'NeedsReview'\)\)/i);
  assert.match(source, /contract_status text not null default 'Active' check \(contract_status in \('Active', 'EndingSoon', 'Extended', 'RetenderInProgress', 'Replaced'\)\)/i);
  assert.match(source, /create table if not exists public\.prediction_reconciliation_candidates/i);
  assert.match(source, /create table if not exists public\.prediction_reconciliation_log/i);
  assert.match(source, /create table if not exists public\.prediction_cycle_history/i);
});

test('closed-loop migration provides report and lifecycle functions', () => {
  assert.match(source, /create or replace function public\.get_buyer_category_timeline_rows\(/i);
  assert.match(source, /create or replace function public\.get_buyer_category_cycle_trend_report\(/i);
  assert.match(source, /create or replace function public\.reconcile_prediction_for_canonical_notice\(/i);
  assert.match(source, /create or replace function public\.evaluate_prediction_lifecycle_misses\(/i);
  assert.match(source, /create trigger trg_prediction_reconcile_on_canonical_link/i);
});
