import { CLASSIFIER_SCHEMA, RECONCILER_SCHEMA, SIGNALS_SCHEMA } from './models.ts';
import { callStructuredAgent } from './openaiResponses.ts';
import { parseAndValidateJson } from './schemaValidator.ts';

const SYSTEM_BASE = 'You are a procurement analytics agent. Return only strict JSON that matches schema.';

export async function runReconcilerAgent(input: {
  entityA: Record<string, unknown>;
  entityB?: Record<string, unknown>;
  candidates?: Array<Record<string, unknown>>;
}) {
  const result = await callStructuredAgent({
    systemPrompt: SYSTEM_BASE,
    userPrompt: `Task: Entity reconciliation.\nInput: ${JSON.stringify(input)}`,
    schemaName: 'reconciler_result',
    schema: RECONCILER_SCHEMA
  });

  const parsed = parseAndValidateJson(result, RECONCILER_SCHEMA);
  if (!parsed.ok) {
    throw new Error(parsed.error);
  }
  return parsed.value as {
    merge_decision: 'merge' | 'separate' | 'needs_review';
    canonical_name: string;
    confidence: number;
    evidence: Array<{ reason: string; field: string; score: number }>;
  };
}

export async function runClassifierAgent(input: {
  tender_text: string;
  cpvs: string[];
  title?: string;
}) {
  const result = await callStructuredAgent({
    systemPrompt: SYSTEM_BASE,
    userPrompt: `Task: Tender classification.\nInput: ${JSON.stringify(input)}`,
    schemaName: 'classifier_result',
    schema: CLASSIFIER_SCHEMA
  });

  const parsed = parseAndValidateJson(result, CLASSIFIER_SCHEMA);
  if (!parsed.ok) {
    throw new Error(parsed.error);
  }
  return parsed.value as {
    category: string;
    subcategory: string;
    confidence: number;
    evidence_terms: string[];
  };
}

export async function runSignalsAgent(input: {
  source_url?: string;
  content_text: string;
}) {
  const result = await callStructuredAgent({
    systemPrompt: SYSTEM_BASE,
    userPrompt: `Task: Extract market signals from source text.\nInput: ${JSON.stringify(input)}`,
    schemaName: 'signals_result',
    schema: SIGNALS_SCHEMA
  });

  const parsed = parseAndValidateJson(result, SIGNALS_SCHEMA);
  if (!parsed.ok) {
    throw new Error(parsed.error);
  }
  return parsed.value as {
    signals: Array<{
      signal_type: string;
      entity_hint: string;
      start_date: string;
      end_date: string;
      strength: number;
      source_quality: number;
      evidence_snippet: string;
      source_url?: string;
    }>;
  };
}
