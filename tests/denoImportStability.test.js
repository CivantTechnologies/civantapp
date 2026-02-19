import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const importCsvSource = readFileSync(new URL('../functions/importCsvData.ts', import.meta.url), 'utf8');
const payloadOffloadSource = readFileSync(new URL('../functions/payloadOffload.ts', import.meta.url), 'utf8');

test('CSV import resolves tenant id safely and uses canonical tender key fields', () => {
  assert.match(importCsvSource, /const tenantId = getTenantFromHeader\(req\);/);
  assert.match(importCsvSource, /await requireAdminForTenant\(\{ civant, req, tenantId \}\);/);
  assert.match(importCsvSource, /entities\.TendersCurrent\.filter\(\{\s*tenant_id: tenantId,\s*tender_id: tenderData\.tender_uid/s);
  assert.doesNotMatch(importCsvSource, /entities\.TendersCurrent\.filter\(\{\s*tender_id: tenderData\.tender_uid\s*\}\)/s);
  assert.match(importCsvSource, /Tenant-scoped tender lookup mismatch/);
  assert.match(importCsvSource, /entities\.TendersCurrent\.update\(existingRow\.id, \{\s*tenant_id: tenantId,\s*tender_id: tenderData\.tender_uid/s);
  assert.match(importCsvSource, /tender_id: tenderData\.tender_uid/);
  assert.match(importCsvSource, /if \(existingRow\?\.data\?\.fingerprint !== fingerprint\)/);
});

test('payload offload normalizes byte buffers and unknown errors', () => {
  assert.match(payloadOffloadSource, /function toArrayBuffer\(bytes: Uint8Array\): ArrayBuffer/);
  assert.match(payloadOffloadSource, /crypto\.subtle\.digest\('SHA-256', toArrayBuffer\(bytes\)\)/);
  assert.match(payloadOffloadSource, /function getErrorMessage\(error: unknown\): string/);
});
