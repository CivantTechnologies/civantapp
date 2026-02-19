import test from 'node:test';
import assert from 'node:assert/strict';
import {
  isSupportGrantActive,
  normalizeTenantId,
  requiresSupportGrant
} from '../shared/supportAccessPolicy.js';

test('requiresSupportGrant only when requested tenant differs from user tenant', () => {
  assert.equal(requiresSupportGrant('tenant_a', 'tenant_a'), false);
  assert.equal(requiresSupportGrant('tenant_a', 'tenant_b'), true);
  assert.equal(requiresSupportGrant('', 'tenant_b'), true);
  assert.equal(requiresSupportGrant('tenant_a', ''), false);
});

test('isSupportGrantActive enforces enabled, non-revoked, and non-expired grant', () => {
  const future = new Date(Date.now() + 60_000).toISOString();
  const past = new Date(Date.now() - 60_000).toISOString();

  assert.equal(isSupportGrantActive({ enabled: true, revoked_at: null, expires_at: future }), true);
  assert.equal(isSupportGrantActive({ enabled: true, revoked_at: null, expires_at: past }), false);
  assert.equal(isSupportGrantActive({ enabled: true, revoked_at: '2026-01-01T00:00:00.000Z', expires_at: future }), false);
  assert.equal(isSupportGrantActive({ enabled: false, revoked_at: null, expires_at: future }), false);
});

test('normalizeTenantId validates allowed tenant ids', () => {
  assert.equal(normalizeTenantId('Tenant_ABC'), 'tenant_abc');
  assert.equal(normalizeTenantId(''), '');
  assert.equal(normalizeTenantId('bad-tenant-id'), '');
});
