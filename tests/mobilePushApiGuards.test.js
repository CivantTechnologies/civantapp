import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const registerSource = readFileSync(new URL('../api/mobile/push/register.ts', import.meta.url), 'utf8');
const dispatchSource = readFileSync(new URL('../api/mobile/push/dispatch.ts', import.meta.url), 'utf8');

test('mobile push register derives tenant from authenticated user context', () => {
  assert.match(registerSource, /const user = await getCurrentUser\(req\);/);
  assert.match(registerSource, /const tenantId = String\(user\.tenantId \|\| ''\)\.trim\(\)\.toLowerCase\(\);/);
  assert.doesNotMatch(registerSource, /getTenantFromHeader/);
  assert.doesNotMatch(registerSource, /body\.tenant_id/);
  assert.doesNotMatch(registerSource, /body\.tenantId/);
});

test('mobile push dispatch requires internal request auth', () => {
  assert.match(dispatchSource, /assertInternalRequest\(req, 'mobilePushDispatch'\);/);
  assert.doesNotMatch(dispatchSource, /getCurrentUser\(req\)/);
});
