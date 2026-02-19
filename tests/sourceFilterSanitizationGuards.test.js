import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../api/_lib/handlers.ts', import.meta.url), 'utf8');

test('source filter is allowlisted before query composition', () => {
  assert.match(source, /const ALLOWED_SOURCE_FILTERS = new Set\(/);
  assert.match(source, /if \(!ALLOWED_SOURCE_FILTERS\.has\(normalized\)\) \{\s*throw badRequest\('Invalid source filter'\);/s);
  assert.match(source, /source: normalizeSourceFilterValue\(raw\.source \|\| 'all'\)/);
});

test('or clause uses sanitized filters.source and not raw source input', () => {
  assert.match(source, /qb = qb\.or\(`source\.eq\.\$\{filters\.source\},verification_sources\.cs\.\{\$\{filters\.source\}\}`\);/);
  assert.doesNotMatch(source, /sourceValue = String\(filters\.source/);
  assert.doesNotMatch(source, /qb = qb\.or\(`source\.eq\.\$\{sourceValue\}/);
});
