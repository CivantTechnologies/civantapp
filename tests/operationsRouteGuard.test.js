import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const source = readFileSync(new URL('../src/App.jsx', import.meta.url), 'utf8');

test('operations routes are protected by RequireOperationsRole', () => {
  assert.match(source, /function RequireOperationsRole\(/);
  assert.match(source, /roles\.includes\('super_admin'\)/);
  assert.match(source, /path="\/operations\/connectors"[\s\S]*<RequireOperationsRole>/);
  assert.match(source, /path="\/operations\/pipeline"[\s\S]*<RequireOperationsRole>/);
  assert.match(source, /path="\/operations\/architecture"[\s\S]*<RequireOperationsRole>/);
  assert.match(source, /path="\/operations\/system"[\s\S]*<RequireOperationsRole>/);
});

test('legacy infra routes redirect into operations namespace', () => {
  assert.match(source, /path="\/connectors" element={<Navigate to="\/operations\/connectors" replace \/>}/);
  assert.match(source, /path="\/pipeline" element={<Navigate to="\/operations\/pipeline" replace \/>}/);
  assert.match(source, /path="\/architecture" element={<Navigate to="\/operations\/architecture" replace \/>}/);
  assert.match(source, /path="\/system" element={<Navigate to="\/operations\/system" replace \/>}/);
});
