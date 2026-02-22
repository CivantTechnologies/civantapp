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
} as const;

export const CLASSIFIER_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['category', 'subcategory', 'confidence', 'evidence_terms'],
  properties: {
    category: { type: 'string' },
    subcategory: { type: 'string' },
    confidence: { type: 'number', minimum: 0, maximum: 1 },
    evidence_terms: {
      type: 'array',
      minItems: 1,
      items: { type: 'string' }
    }
  }
} as const;

export const SIGNALS_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['signals'],
  properties: {
    signals: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: [
          'signal_type',
          'entity_hint',
          'start_date',
          'end_date',
          'strength',
          'source_quality',
          'evidence_snippet'
        ],
        properties: {
          signal_type: {
            type: 'string',
            enum: [
              'contract_end_date',
              'evaluation_project',
              'installed_base',
              'budget_signal',
              'policy_change',
              'other'
            ]
          },
          entity_hint: { type: 'string' },
          start_date: { type: 'string' },
          end_date: { type: 'string' },
          strength: { type: 'number', minimum: 0, maximum: 1 },
          source_quality: { type: 'number', minimum: 0, maximum: 1 },
          evidence_snippet: { type: 'string' },
          source_url: { type: 'string' }
        }
      }
    }
  }
} as const;

export const PIPELINE_NAMES = {
  rawDocuments: 'raw_documents',
  ingestionRuns: 'ingestion_runs',
  stagingRecords: 'staging_records',
  canonicalTenders: 'canonical_tenders',
  entities: 'entities',
  entityAliases: 'entity_aliases',
  tenderFeaturesWeekly: 'tender_features_weekly',
  predictions: 'predictions',
  predictionsCurrent: 'predictions_current',
  predictionLifecycle: 'prediction_lifecycle',
  predictionReconciliationCandidates: 'prediction_reconciliation_candidates',
  marketSignals: 'market_signals',
  reconciliationQueue: 'reconciliation_queue'
} as const;

export type PipelineStepResult<T = unknown> = {
  success: boolean;
  data?: T;
  error?: string;
};

export type ConfidenceBreakdown = {
  data_confidence: number;
  signal_confidence: number;
  model_confidence: number;
  overall_confidence: number;
  drivers: string[];
};
