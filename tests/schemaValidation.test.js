import test from 'node:test';
import assert from 'node:assert/strict';
import { validateAgainstSchema } from '../src/lib/agentic/schemaValidator.js';
import { RECONCILER_SCHEMA } from '../src/lib/agentic/schemas.js';

test('valid reconciler output passes strict schema', () => {
  const output = {
    merge_decision: 'merge',
    canonical_name: 'Health Service Executive',
    confidence: 0.91,
    evidence: [{ reason: 'Exact VAT match', field: 'vat_number', score: 1 }]
  };

  const result = validateAgainstSchema(output, RECONCILER_SCHEMA);
  assert.equal(result.valid, true);
  assert.equal(result.errors.length, 0);
});

test('invalid reconciler output fails closed', () => {
  const output = {
    merge_decision: 'merge',
    canonical_name: 'Bad Entity',
    confidence: 1.2,
    extra: 'not allowed',
    evidence: []
  };

  const result = validateAgainstSchema(output, RECONCILER_SCHEMA);
  assert.equal(result.valid, false);
  assert.ok(result.errors.some((x) => x.includes('not allowed')));
  assert.ok(result.errors.some((x) => x.includes('<= 1')));
});
