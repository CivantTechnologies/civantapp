import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const AUTH_PATH = new URL('../api/_lib/auth.ts', import.meta.url);
const source = readFileSync(AUTH_PATH, 'utf8');

test('new user bootstrap is pinned to safe defaults', () => {
  assert.match(source, /const bootstrapTenantId = 'civant_default';/);
  assert.match(source, /const bootstrapRole = 'user';/);
  assert.match(source, /tenant_id: bootstrapTenantId/);
  assert.match(source, /role: bootstrapRole/);
  assert.match(source, /role: normalizeRole\(inlineRole \|\| bootstrapRole\)/);
});

test('auth bootstrap no longer trusts mutable user metadata for tenant or role', () => {
  assert.doesNotMatch(source, /metadata\.tenant_id/);
  assert.doesNotMatch(source, /metadata\.role/);
  assert.doesNotMatch(source, /requestedRole/);
  assert.doesNotMatch(source, /tenantIdFromMetadata/);
});
