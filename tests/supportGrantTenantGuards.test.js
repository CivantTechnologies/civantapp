import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const apiAuthSource = readFileSync(new URL('../api/_lib/auth.ts', import.meta.url), 'utf8');
const denoAuthSource = readFileSync(new URL('../functions/requireAdmin.ts', import.meta.url), 'utf8');
const enableSupportSource = readFileSync(new URL('../functions/enableSupportAccess.ts', import.meta.url), 'utf8');
const revokeSupportSource = readFileSync(new URL('../functions/revokeSupportAccess.ts', import.meta.url), 'utf8');

test('api auth removes creator bypass and requires support grants for cross-tenant access', () => {
  assert.doesNotMatch(apiAuthSource, /if \(hasRole\(user, 'creator'\)\) return true;/);
  assert.match(apiAuthSource, /export async function requireTenantAccessWithSupportGrant/);
  assert.match(apiAuthSource, /from\('support_access_grants'\)/);
  assert.match(apiAuthSource, /\.eq\('support_user_id', supportUserId\)/);
  assert.match(apiAuthSource, /from\('support_access_audit'\)/);
  assert.match(apiAuthSource, /throw forbidden\('Support access grant required for tenant'\);/);
});

test('deno auth removes creator bypass and adds support grant + audit checks', () => {
  assert.doesNotMatch(denoAuthSource, /if \(hasRole\(user, 'creator'\)\) return true;/);
  assert.match(denoAuthSource, /export async function requireSupportAccessForTenant/);
  assert.match(denoAuthSource, /support_access_grants\.filter\(\{\s*tenant_id: tenantId,\s*support_user_id: supportUserId,\s*enabled: true/s);
  assert.match(denoAuthSource, /support_access_audit\.create\(/);
  assert.match(denoAuthSource, /throw forbidden\('Support access grant required for tenant'\);/);
});

test('support access grant management requires explicit support user scope', () => {
  assert.match(enableSupportSource, /support_user_id is required/);
  assert.match(enableSupportSource, /support_user_id: supportUserId/);
  assert.match(revokeSupportSource, /support_user_id is required/);
  assert.match(revokeSupportSource, /getActiveSupportGrant\(civant, tenantId, supportUserId\)/);
});
