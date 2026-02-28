import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const appSource = readFileSync(new URL('../apps/mobile/App.tsx', import.meta.url), 'utf8');

test('mobile app queues triage actions when network errors are retryable', () => {
  assert.match(appSource, /import \{\s*enqueueTriageAction,\s*flushQueuedTriageActions,\s*getQueuedTriageCount,\s*\} from "\.\/src\/lib\/offlineQueue";/s);
  assert.match(appSource, /if \(isRetryableNetworkError\(error\)\) \{\s*const queuedCount = await enqueueTriageAction\(\{ predictionId, action \}\);/s);
  assert.match(appSource, /Offline\. Action queued/);
});

test('mobile app attempts queued triage sync after loading today feed', () => {
  assert.match(appSource, /async function syncQueuedTriageActions\(activeSession: Session\)/);
  assert.match(appSource, /await flushQueuedTriageActions\(\{\s*accessToken: activeSession\.access_token,/s);
  assert.match(appSource, /await syncQueuedTriageActions\(activeSession\);/);
});
