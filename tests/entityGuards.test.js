import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const ENTITIES_PATH = new URL('../api/_lib/entities.ts', import.meta.url);
const source = readFileSync(ENTITIES_PATH, 'utf8');

test('entity CRUD paths require authenticated tenant access', () => {
  assert.match(source, /async function requireEntityAccess\(req: DynamicRequest, tableName: string\)/);
  assert.match(source, /const tenantId = await requireEntityAccess\(req, tableName\);/);
  assert.match(source, /throw forbidden\(`Entity access is not allowed for table: \$\{tableName\}`\);/);

  const operations = [
    'listOrFilterEntity',
    'createEntity',
    'deleteManyEntity',
    'getEntityById',
    'updateEntityById',
    'deleteEntityById'
  ];

  for (const operationName of operations) {
    assert.match(
      source,
      new RegExp(`export async function ${operationName}\\(req: DynamicRequest\\)[\\s\\S]*const tenantId = await requireEntityAccess\\(req, tableName\\);`)
    );
  }
});

test('tenant override attempts are explicitly rejected', () => {
  assert.match(source, /function assertTenantQueryMatches\(tenantId: string, queryObj: Record<string, unknown>\)/);
  assert.match(source, /throw forbidden\('Cross-tenant query is not allowed'\);/);
  assert.match(source, /function assertTenantPayloadMatches\(tenantId: string, payload: unknown\)/);
  assert.match(source, /throw forbidden\('Cross-tenant write is not allowed'\);/);
});
