import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../api/_lib/entities.ts', import.meta.url), 'utf8');

test('company_profiles is included in tenant-scoped entity allowlist', () => {
  assert.match(source, /'company_profiles'/);
});

test('company_profiles entity writes use tenant_id upsert', () => {
  assert.match(
    source,
    /if \(tableName === 'company_profiles'\) \{[\s\S]*?upsert\(bodyWithTenant, \{ onConflict: 'tenant_id' \}\);/
  );
});

test('company_profiles id filter resolves to tenant_id key', () => {
  assert.match(
    source,
    /if \(tableName === 'company_profiles'\) \{[\s\S]*?return qb\.eq\('tenant_id', id\);/
  );
});
