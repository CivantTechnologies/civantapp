import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const files = [
  '../functions/fetchTed.ts',
  '../functions/fetchIreland.ts',
  '../functions/fetchBoampFr.ts',
  '../functions/fetchEtendersIeIncremental.ts',
  '../functions/runAllConnectors.ts',
  '../functions/runAgenticPipeline.ts'
];

for (const file of files) {
  const source = readFileSync(new URL(file, import.meta.url), 'utf8');

  test(`${file} derives tenant from validated header and enforces tenant admin`, () => {
    assert.match(source, /const tenantId = getTenantFromHeader\(req\);/);
    assert.match(source, /await requireAdminForTenant\(\{ civant, req, tenantId \}\);/);
  });

  test(`${file} does not accept tenant override from body or default fallback`, () => {
    assert.doesNotMatch(source, /body\.tenant_id/);
    assert.doesNotMatch(source, /body\.tenantId/);
    assert.doesNotMatch(source, /DEFAULT_TENANT_ID/);
    assert.doesNotMatch(source, /req\.headers\.get\('X-Tenant-Id'\)/);
  });
}
