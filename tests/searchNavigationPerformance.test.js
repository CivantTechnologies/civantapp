import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../src/pages/Search.jsx', import.meta.url), 'utf8');

test('search page uses lower initial and interactive load limits', () => {
  assert.match(source, /const DEFAULT_LOAD_LIMIT = 150;/);
  assert.match(source, /const APPLY_LOAD_LIMIT = 250;/);
  assert.match(source, /loadTenders\(appliedFilters, DEFAULT_LOAD_LIMIT\)/);
  assert.match(source, /loadTenders\(nextFilters, APPLY_LOAD_LIMIT\)/);
});

test('search page does not block full route render while loading', () => {
  assert.match(source, /Refreshing\.\.\./);
  assert.match(source, /Loading tenders\.\.\./);
  assert.doesNotMatch(source, /if \(loading\) \{[\s\S]*return \(\s*<div className="flex items-center justify-center h-64">/);
});
