import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../functions/getPipelineAdmin.ts', import.meta.url), 'utf8');

test('pipeline admin derives tenant from validated header and enforces tenant admin auth', () => {
  assert.match(source, /const tenantId = getTenantFromHeader\(req\);/);
  assert.match(source, /await requireAdminForTenant\(\{ civant, req, tenantId \}\);/);
  assert.doesNotMatch(source, /civant\.auth\.me\(/);
});

test('pipeline admin scopes queue, prediction, and runs queries by tenant_id', () => {
  assert.match(source, /reconciliationQueue\]\.filter\(\{\s*id: queueId,\s*tenant_id: tenantId\s*\}\)/s);
  assert.match(source, /predictions\]\.filter\(\{\s*id: predictionId,\s*tenant_id: tenantId\s*\}\)/s);
  assert.match(source, /ingestionRuns\]\.filter\(\{\s*tenant_id: tenantId\s*\}, '-started_at', 100\)/);
  assert.match(source, /reconciliationQueue\]\.filter\(\{\s*tenant_id: tenantId,\s*status: 'pending'\s*\}, '-created_at', 200\)/s);
  assert.match(source, /predictions\]\.filter\(\{\s*tenant_id: tenantId\s*\}, '-generated_at', 200\)/);
  assert.doesNotMatch(source, /ingestionRuns\]\.list\(/);
  assert.doesNotMatch(source, /predictions\]\.list\(/);
});
