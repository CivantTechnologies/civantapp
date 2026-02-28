import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const todaySource = readFileSync(new URL('../api/mobile/today.ts', import.meta.url), 'utf8');
const triageSource = readFileSync(new URL('../api/mobile/triage.ts', import.meta.url), 'utf8');

test('mobile today feed derives tenant from authenticated user context', () => {
  assert.match(todaySource, /const user = await getCurrentUser\(req\);/);
  assert.match(todaySource, /const tenantId = String\(user\.tenantId \|\| ''\)\.trim\(\)\.toLowerCase\(\);/);
  assert.doesNotMatch(todaySource, /getTenantFromHeader/);
  assert.doesNotMatch(todaySource, /body\.tenant_id/);
  assert.doesNotMatch(todaySource, /body\.tenantId/);
});

test('mobile triage validates prediction ownership in tenant before write', () => {
  assert.match(triageSource, /const user = await getCurrentUser\(req\);/);
  assert.match(triageSource, /\.from\('predictions_current'\)/);
  assert.match(triageSource, /\.eq\('prediction_id', predictionId\)/);
  assert.match(triageSource, /\.eq\('tenant_id', tenantId\)/);
  assert.match(triageSource, /\.from\('prediction_extensions'\)/);
  assert.doesNotMatch(triageSource, /getTenantFromHeader/);
  assert.doesNotMatch(triageSource, /body\.tenant_id/);
  assert.doesNotMatch(triageSource, /body\.tenantId/);
});
