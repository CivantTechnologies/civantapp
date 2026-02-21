import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../functions/processAlerts.ts', import.meta.url), 'utf8');

test('processAlerts fetches tenant-scoped alerts and tenders', () => {
  assert.match(source, /const tenantId = getTenantFromHeader\(req\);/);
  assert.match(source, /entities\.Alerts\.filter\(\{\s*tenant_id: tenantId,\s*active: true\s*\}\)/s);
  assert.match(source, /entities\.TendersCurrent\.filter\(\{\s*tenant_id: tenantId\s*\}, '-first_seen_at', 500\)/s);
  assert.doesNotMatch(source, /entities\.tenants\.list\(/);
  assert.doesNotMatch(source, /entities\.Alerts\.filter\(\{\s*active: true\s*\}\)/s);
  assert.doesNotMatch(source, /entities\.TendersCurrent\.list\(/);
});

test('processAlerts enforces tenant match before sending email', () => {
  assert.match(source, /const alertTenantId = normalizeTenantId\(alert\.tenant_id\);/);
  assert.match(source, /const tenderTenantId = normalizeTenantId\(tender\.tenant_id\);/);
  assert.match(source, /Tender tenant mismatch before email send/);
  assert.match(source, /entities\.AlertEvents\.filter\(\{\s*tenant_id: alertTenantId,\s*alert_id: alert\.id,\s*tender_uid: tender\.tender_uid\s*\}\)/s);
});
