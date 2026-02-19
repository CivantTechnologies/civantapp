import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../functions/createSession.ts', import.meta.url), 'utf8');

test('createSession is disabled unless explicitly enabled by env flag', () => {
  assert.match(source, /ENABLE_CREATE_SESSION_DEV_ONLY/);
  assert.match(source, /if \(!isCreateSessionEnabled\(\)\) \{[\s\S]*status: 404/);
});

test('createSession requires authenticated caller and self-only minting', () => {
  assert.match(source, /const user = await getCurrentUserFromRequest\(civant, req\);/);
  assert.match(source, /if \(requestedEmail && requestedEmail !== user\.email\) \{[\s\S]*status: 403/);
  assert.match(source, /user_id: user\.userId/);
  assert.doesNotMatch(source, /entities\.users\.create\(/);
});

test('createSession includes basic per-user throttle', () => {
  assert.match(source, /MIN_SESSION_MINT_INTERVAL_MS/);
  assert.match(source, /status: 429/);
});
