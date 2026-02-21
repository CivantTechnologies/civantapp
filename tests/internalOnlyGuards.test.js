import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const schedulerSource = readFileSync(new URL('../functions/scheduledConnectorSync.ts', import.meta.url), 'utf8');
const alertsSource = readFileSync(new URL('../functions/processAlerts.ts', import.meta.url), 'utf8');

test('scheduledConnectorSync requires internal-only guard', () => {
  assert.match(schedulerSource, /assertInternalRequest\(req,\s*'scheduledConnectorSync'\)/);
});

test('processAlerts requires internal-only guard', () => {
  assert.match(alertsSource, /assertInternalRequest\(req,\s*'processAlerts'\)/);
});
