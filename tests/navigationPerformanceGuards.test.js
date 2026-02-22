import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const appSource = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');
const pagesConfigSource = readFileSync(new URL('../src/pages.config.js', import.meta.url), 'utf8');
const onboardingSource = readFileSync(new URL('../src/lib/OnboardingGate.jsx', import.meta.url), 'utf8');

test('app prefetches core page bundles after auth settles', () => {
  assert.match(appSource, /prefetchCorePages/);
  assert.match(appSource, /requestIdleCallback/);
});

test('pages config exports a core page prefetch function', () => {
  assert.match(pagesConfigSource, /const CORE_PREFETCH_LOADERS = \[/);
  assert.match(pagesConfigSource, /export async function prefetchCorePages\(\)/);
});

test('onboarding gate no longer blocks render while status is loading', () => {
  assert.match(onboardingSource, /onboardingStatus: 'loading'/);
  assert.match(onboardingSource, /if \(onboardingStatus === 'loading'\) return children;/);
  assert.doesNotMatch(onboardingSource, /status === 'loading' \? null : children/);
});
