export const RECONCILER_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['merge_decision', 'canonical_name', 'confidence', 'evidence'],
  properties: {
    merge_decision: {
      type: 'string',
      enum: ['merge', 'separate', 'needs_review']
    },
    canonical_name: { type: 'string' },
    confidence: { type: 'number', minimum: 0, maximum: 1 },
    evidence: {
      type: 'array',
      minItems: 1,
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['reason', 'field', 'score'],
        properties: {
          reason: { type: 'string' },
          field: { type: 'string' },
          score: { type: 'number', minimum: 0, maximum: 1 }
        }
      }
    }
  }
};
