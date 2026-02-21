import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../functions/analyzeCompetitor.ts', import.meta.url), 'utf8');

test('analyzeCompetitor derives tenant from authenticated user context', () => {
  assert.match(source, /requireAuthenticatedUser\(\{\s*civant,\s*req\s*\}\)/);
  assert.match(source, /const tenantId = String\(user\.tenantId \|\| ''\)\.trim\(\)\.toLowerCase\(\);/);
  assert.match(source, /p_tenant_id:\s*tenantId/);
  assert.doesNotMatch(source, /p_tenant_id:\s*'civant_default'/);
});

test('analyzeCompetitor fallback query remains tenant-scoped', () => {
  assert.match(source, /TendersCurrent\.filter\(\s*\{\s*tenant_id:\s*tenantId\s*\}/s);
});
