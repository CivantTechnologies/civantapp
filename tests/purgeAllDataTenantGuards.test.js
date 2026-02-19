import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../functions/purgeAllData.ts', import.meta.url), 'utf8');

test('purgeAllData requires destructive ops env gate and tenant admin guard', () => {
  assert.match(source, /Deno\.env\.get\('ENABLE_DESTRUCTIVE_OPS'\)/);
  assert.match(source, /if \(!destructiveOpsEnabled\(\)\)/);
  assert.match(source, /const tenantId = getTenantFromHeader\(req\);/);
  assert.match(source, /await requireAdminForTenant\(\{ civant, req, tenantId \}\);/);
  assert.match(source, /const expectedConfirm = `PURGE_\$\{tenantId\}`;/);
  assert.match(source, /if \(confirm !== expectedConfirm\)/);
  assert.doesNotMatch(source, /civant\.auth\.me\(/);
});

test('purgeAllData uses tenant_id filter and avoids global list-based deletes', () => {
  assert.match(source, /entities\[tableName\]\.filter\(\{\s*tenant_id: tenantId\s*\}\)/s);
  assert.doesNotMatch(source, /\.list\(/);
  assert.match(source, /const skippedTables = \['TenderEnrichment', 'PredictionFeedback'\];/);
});
