import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../functions/getPipelineAdmin.ts', import.meta.url), 'utf8');

test('pipeline admin lifecycle queue queries remain tenant-scoped', () => {
  assert.match(source, /predictionLifecycle\]\.filter\(\{ tenant_id: tenantId \}, '-updated_at', 250\)/);
  assert.match(source, /predictionReconciliationCandidates\]\.filter\(\{ tenant_id: tenantId, status: 'pending' \}, '-created_at', 250\)/);
});

test('pipeline admin lifecycle decisions pass explicit tenant context to RPC', () => {
  assert.match(source, /action === 'lifecycle_review_decision'/);
  assert.match(source, /p_tenant_id: tenantId/);
  assert.match(source, /p_candidate_id: candidateId/);
});
