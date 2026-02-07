import { createClientFromRequest } from '../civantSdk.ts';
import { PIPELINE_NAMES } from './models.ts';
import { runClassifierAgent, runReconcilerAgent, runSignalsAgent } from './agents.ts';
import { computeConfidence } from './scoring.ts';

type CivantClient = ReturnType<typeof createClientFromRequest>;
type AnyRow = Record<string, any>;

type RawInput = {
  source: string;
  source_url?: string;
  external_id?: string;
  document_type?: string;
  raw_text?: string;
  raw_json?: Record<string, unknown>;
  fetched_at?: string;
};

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

async function sha256(input: string) {
  const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(input));
  return Array.from(new Uint8Array(hash)).map((x) => x.toString(16).padStart(2, '0')).join('');
}

function normalizeCpvs(value: unknown): string[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.map((v) => String(v).trim()).filter(Boolean);
  return String(value)
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

function cpvFamily(cpvs: string[]) {
  if (!cpvs.length) return 'unknown';
  return cpvs[0].slice(0, 2).padEnd(2, '0');
}

function extractParsedRecord(raw: Record<string, unknown>) {
  const parsed = raw.raw_json && typeof raw.raw_json === 'object'
    ? raw.raw_json as Record<string, unknown>
    : {};

  const title = String(parsed.title || parsed.notice_title || parsed.subject || '').trim();
  const buyer_name = String(parsed.buyer_name || parsed.organisation || parsed.contracting_authority || '').trim();
  const external_id = String(parsed.source_notice_id || parsed.id || raw.external_id || '').trim();
  const cpv_codes = normalizeCpvs(parsed.cpv_codes || parsed.cpv || parsed.cpvCode);
  const publication_date = String(parsed.publication_date || parsed.published_date || '').slice(0, 10) || null;
  const deadline_date = String(parsed.deadline_date || parsed.closing_date || parsed.deadline || '').slice(0, 10) || null;
  const estimated_value = parsed.estimated_value ? Number(parsed.estimated_value) : null;

  return {
    external_id,
    title,
    buyer_name,
    cpv_codes,
    publication_date,
    deadline_date,
    estimated_value,
    currency: String(parsed.currency || 'EUR'),
    source_url: String(parsed.url || raw.source_url || ''),
    raw_text: String(raw.raw_text || title),
    parsed_json: parsed
  };
}

async function upsertIngestionRun(
  civant: CivantClient,
  tenantId: string,
  runId: string,
  payload: Record<string, unknown>
) {
  const existing = await civant.asServiceRole.entities[PIPELINE_NAMES.ingestionRuns].filter({
    tenant_id: tenantId,
    run_id: runId
  });
  if (existing.length > 0) {
    return civant.asServiceRole.entities[PIPELINE_NAMES.ingestionRuns].update(existing[0].id, {
      tenant_id: tenantId,
      ...payload
    });
  }
  return civant.asServiceRole.entities[PIPELINE_NAMES.ingestionRuns].create({
    tenant_id: tenantId,
    run_id: runId,
    ...payload
  });
}

async function getOrCreateEntity(
  civant: CivantClient,
  tenantId: string,
  entityType: 'buyer' | 'supplier',
  name: string
) {
  const normalized = name.trim();
  if (!normalized) return null;

  const exact = await civant.asServiceRole.entities[PIPELINE_NAMES.entities].filter({
    tenant_id: tenantId,
    entity_type: entityType,
    canonical_name: normalized
  });
  if (exact.length > 0) return exact[0];

  const aliasMatches = await civant.asServiceRole.entities[PIPELINE_NAMES.entityAliases].filter({
    tenant_id: tenantId,
    alias: normalized
  });
  if (aliasMatches.length > 0) {
    const aliasEntity = await civant.asServiceRole.entities[PIPELINE_NAMES.entities].filter({
      tenant_id: tenantId,
      entity_id: aliasMatches[0].entity_id
    });
    if (aliasEntity.length > 0) return aliasEntity[0];
  }

  const nearby: AnyRow[] = await civant.asServiceRole.entities[PIPELINE_NAMES.entities].filter({
    tenant_id: tenantId,
    entity_type: entityType
  }, '-updated_at', 20);

  if (nearby.length) {
    const agent = await runReconcilerAgent({
      entityA: { name: normalized, entity_type: entityType },
      candidates: nearby.map((x: AnyRow) => ({ entity_id: x.entity_id, canonical_name: x.canonical_name, metadata: x.metadata }))
    });

    if (agent.confidence >= 0.85 && agent.merge_decision === 'merge') {
      await civant.asServiceRole.entities[PIPELINE_NAMES.entityAliases].create({
        id: makeId('alias'),
        tenant_id: tenantId,
        entity_id: nearby.find((x: AnyRow) => x.canonical_name === agent.canonical_name)?.entity_id || nearby[0].entity_id,
        alias: normalized,
        source: 'ReconcilerAgent',
        confidence: agent.confidence,
        evidence: agent.evidence
      });
      return nearby.find((x: AnyRow) => x.canonical_name === agent.canonical_name) || nearby[0];
    }

    await civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].create({
      id: makeId('reconq'),
      tenant_id: tenantId,
      candidate_json: {
        input_name: normalized,
        entity_type: entityType,
        candidates: nearby
      },
      agent_output: agent,
      status: 'pending'
    });
  }

  const entity = await civant.asServiceRole.entities[PIPELINE_NAMES.entities].create({
    tenant_id: tenantId,
    entity_id: makeId('ent'),
    entity_type: entityType,
    canonical_name: normalized,
    metadata: {}
  });

  await civant.asServiceRole.entities[PIPELINE_NAMES.entityAliases].create({
    id: makeId('alias'),
    tenant_id: tenantId,
    entity_id: entity.entity_id,
    alias: normalized,
    source: 'deterministic',
    confidence: 1,
    evidence: [{ reason: 'Exact canonical create', field: 'canonical_name', score: 1 }]
  });

  return entity;
}

export async function ingest_raw(civant: CivantClient, params: {
  run_id: string;
  tenant_id: string;
  source: string;
  cursor?: string;
  documents: RawInput[];
}) {
  const errors: Array<{ external_id?: string; message: string }> = [];
  let inserted = 0;

  await upsertIngestionRun(civant, params.tenant_id, params.run_id, {
    source: params.source,
    cursor: params.cursor || null,
    status: 'running',
    started_at: new Date().toISOString(),
    metrics: { attempted: params.documents.length, inserted: 0, duplicates: 0 },
    errors: []
  });

  for (const doc of params.documents) {
    try {
      const rawPayload = JSON.stringify(doc.raw_json || {}) + (doc.raw_text || '');
      const checksum = await sha256(rawPayload);
      const existing = await civant.asServiceRole.entities[PIPELINE_NAMES.rawDocuments].filter({
        tenant_id: params.tenant_id,
        checksum
      });
      if (existing.length > 0) continue;

      await civant.asServiceRole.entities[PIPELINE_NAMES.rawDocuments].create({
        id: makeId('raw'),
        tenant_id: params.tenant_id,
        run_id: params.run_id,
        source: doc.source,
        source_url: doc.source_url || null,
        document_type: doc.document_type || 'tender',
        external_id: doc.external_id || null,
        raw_text: doc.raw_text || null,
        raw_json: doc.raw_json || null,
        fetched_at: doc.fetched_at || new Date().toISOString(),
        checksum
      });
      inserted += 1;
    } catch (error) {
      errors.push({
        external_id: doc.external_id,
        message: error instanceof Error ? error.message : 'Unknown ingest error'
      });
    }
  }

  await upsertIngestionRun(civant, params.tenant_id, params.run_id, {
    status: errors.length ? 'partial' : 'success',
    finished_at: new Date().toISOString(),
    metrics: {
      attempted: params.documents.length,
      inserted,
      duplicates: Math.max(params.documents.length - inserted - errors.length, 0),
      errors: errors.length
    },
    errors
  });

  return { inserted, errors };
}

export async function parse_to_staging(civant: CivantClient, params: { run_id: string; tenant_id: string }) {
  const docs = await civant.asServiceRole.entities[PIPELINE_NAMES.rawDocuments].filter({
    tenant_id: params.tenant_id,
    run_id: params.run_id
  }, '-created_at', 5000);
  let valid = 0;
  let invalid = 0;

  for (const doc of docs) {
    const parsed = extractParsedRecord(doc);
    const hasCoreFields = Boolean(parsed.external_id && parsed.title);
    const validation_status = hasCoreFields ? 'valid' : 'invalid';
    const errors = hasCoreFields ? [] : ['Missing external_id or title'];

    await civant.asServiceRole.entities[PIPELINE_NAMES.stagingRecords].create({
      id: makeId('stage'),
      tenant_id: params.tenant_id,
      run_id: params.run_id,
      external_id: parsed.external_id || makeId('missing'),
      parsed_json: parsed,
      validation_status,
      errors
    });

    if (hasCoreFields) valid += 1;
    else invalid += 1;
  }

  return { valid, invalid };
}

export async function normalise_to_canonical(civant: CivantClient, params: { run_id: string; tenant_id: string }) {
  const staging = await civant.asServiceRole.entities[PIPELINE_NAMES.stagingRecords].filter({
    tenant_id: params.tenant_id,
    run_id: params.run_id,
    validation_status: 'valid'
  }, '-created_at', 5000);

  let upserts = 0;
  let queuedReviews = 0;

  for (const record of staging) {
    const parsed = record.parsed_json || {};
    const title = String(parsed.title || '');
    const cpvs = Array.isArray(parsed.cpv_codes) ? parsed.cpv_codes : [];
    const classification = await runClassifierAgent({
      tender_text: `${title}\n${String(parsed.raw_text || '')}`,
      cpvs,
      title
    });

    if (classification.confidence < 0.85) {
      queuedReviews += 1;
      await civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].create({
        id: makeId('reconq'),
        tenant_id: params.tenant_id,
        run_id: params.run_id,
        candidate_json: {
          type: 'classification',
          staging_record_id: record.id,
          parsed_json: parsed
        },
        agent_output: classification,
        status: 'pending'
      });
      continue;
    }

    const buyerEntity = await getOrCreateEntity(civant, params.tenant_id, 'buyer', String(parsed.buyer_name || 'Unknown Buyer'));
    const cpvCodes = Array.isArray(parsed.cpv_codes) ? parsed.cpv_codes : [];

    const existing = await civant.asServiceRole.entities[PIPELINE_NAMES.canonicalTenders].filter({
      tenant_id: params.tenant_id,
      source: parsed.source || 'unknown',
      external_id: record.external_id
    });

    const canonicalPayload = {
      canonical_id: existing[0]?.canonical_id || makeId('ct'),
      tenant_id: params.tenant_id,
      source: parsed.source || 'unknown',
      external_id: record.external_id,
      buyer_entity_id: buyerEntity?.entity_id || null,
      title,
      category: classification.category,
      subcategory: classification.subcategory,
      cpv_codes: cpvCodes,
      publication_date: parsed.publication_date || null,
      deadline_date: parsed.deadline_date || null,
      estimated_value: parsed.estimated_value || null,
      currency: parsed.currency || 'EUR',
      source_url: parsed.source_url || null,
      dedupe_quality: 1,
      normalized_json: {
        ...parsed,
        classification,
        evidence_terms: classification.evidence_terms
      },
      updated_at: new Date().toISOString()
    };

    if (existing.length > 0) {
      await civant.asServiceRole.entities[PIPELINE_NAMES.canonicalTenders].update(existing[0].id, canonicalPayload);
    } else {
      await civant.asServiceRole.entities[PIPELINE_NAMES.canonicalTenders].create(canonicalPayload);
    }

    upserts += 1;
  }

  return { upserts, queuedReviews };
}

export async function build_weekly_features(civant: CivantClient, params: { weeks_back?: number; tenant_id: string }) {
  const canonical: AnyRow[] = await civant.asServiceRole.entities[PIPELINE_NAMES.canonicalTenders].filter({
    tenant_id: params.tenant_id
  }, '-publication_date', 10000);
  const weeksBack = params.weeks_back || 104;
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - weeksBack * 7);

  const groups: Record<string, any[]> = {};
  canonical.forEach((tender: AnyRow) => {
    if (!tender.publication_date) return;
    const pub = new Date(tender.publication_date);
    if (pub < cutoff) return;
    const weekStart = new Date(pub);
    weekStart.setDate(pub.getDate() - pub.getDay());
    const weekKey = weekStart.toISOString().slice(0, 10);
    const key = [weekKey, tender.buyer_entity_id || 'unknown', tender.category || 'unknown', cpvFamily(tender.cpv_codes || [])].join('|');
    groups[key] = groups[key] || [];
    groups[key].push(tender);
  });

  let written = 0;
  for (const [key, rows] of Object.entries(groups)) {
    const [week_start, buyer_entity_id, category, cpv_family] = key.split('|');
    const values = rows.map((r) => Number(r.estimated_value || 0)).filter((x) => x > 0).sort((a, b) => a - b);
    const avg_value = values.length ? values.reduce((s, n) => s + n, 0) / values.length : null;
    const median = values.length ? values[Math.floor(values.length / 2)] : null;

    const payload = {
      id: makeId('feat'),
      tenant_id: params.tenant_id,
      week_start,
      buyer_entity_id: buyer_entity_id === 'unknown' ? null : buyer_entity_id,
      category,
      cpv_family,
      tender_count: rows.length,
      avg_value,
      median_days_between: median,
      active_suppliers: 0
    };

    await civant.asServiceRole.entities[PIPELINE_NAMES.tenderFeaturesWeekly].create(payload);
    written += 1;
  }

  return { written };
}

export async function attach_market_signals(civant: CivantClient, params: { run_id: string; tenant_id: string }) {
  const raw = await civant.asServiceRole.entities[PIPELINE_NAMES.rawDocuments].filter({
    tenant_id: params.tenant_id,
    run_id: params.run_id
  }, '-created_at', 2000);
  let signalsInserted = 0;

  for (const doc of raw) {
    if (!(doc.raw_text || '').trim()) continue;

    const extracted = await runSignalsAgent({
      source_url: doc.source_url || undefined,
      content_text: String(doc.raw_text)
    });

    for (const signal of extracted.signals) {
      if (signal.strength < 0.85) {
        await civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].create({
          id: makeId('reconq'),
          tenant_id: params.tenant_id,
          run_id: params.run_id,
          candidate_json: { type: 'signal', source_url: doc.source_url, signal },
          agent_output: signal,
          status: 'pending'
        });
        continue;
      }

      const entity = await getOrCreateEntity(civant, params.tenant_id, 'buyer', signal.entity_hint || 'Unknown Entity');
      await civant.asServiceRole.entities[PIPELINE_NAMES.marketSignals].create({
        id: makeId('sig'),
        tenant_id: params.tenant_id,
        signal_type: signal.signal_type,
        entity_id: entity?.entity_id || null,
        source_url: signal.source_url || doc.source_url || null,
        source_quality: signal.source_quality,
        signal_strength: signal.strength,
        start_date: signal.start_date || null,
        end_date: signal.end_date || null,
        evidence_snippet: signal.evidence_snippet,
        extracted_json: signal
      });
      signalsInserted += 1;
    }
  }

  return { signalsInserted };
}

export async function generate_predictions(civant: CivantClient, params: {
  tenant_id: string;
  model_version?: string;
  time_window?: string;
}) {
  const features: AnyRow[] = await civant.asServiceRole.entities[PIPELINE_NAMES.tenderFeaturesWeekly].filter({
    tenant_id: params.tenant_id
  }, '-week_start', 20000);
  const signals: AnyRow[] = await civant.asServiceRole.entities[PIPELINE_NAMES.marketSignals].filter({
    tenant_id: params.tenant_id
  }, '-created_at', 5000);

  const grouped: Record<string, any[]> = {};
  features.forEach((row: AnyRow) => {
    const key = [row.buyer_entity_id || 'unknown', row.category || 'unknown', row.cpv_family || 'unknown'].join('|');
    grouped[key] = grouped[key] || [];
    grouped[key].push(row);
  });

  let created = 0;
  for (const [key, rows] of Object.entries(grouped)) {
    const [buyer_id, category, cpv_family] = key.split('|');
    const recent = rows.slice(0, 8);
    const avgVolume = recent.reduce((sum, r) => sum + Number(r.tender_count || 0), 0) / Math.max(recent.length, 1);
    const normalizedVolume = Math.min(avgVolume / 5, 1);
    const buyerSignals = signals.filter((s: AnyRow) => (s.entity_id || 'unknown') === buyer_id);

    const confidence = computeConfidence({
      data: {
        completeness: 0.95,
        recencyDays: 7,
        historyLengthWeeks: rows.length,
        dedupeQuality: 0.95
      },
      signals: {
        signals: buyerSignals.map((s: AnyRow) => ({ strength: Number(s.signal_strength || 0), source_quality: Number(s.source_quality || 0) })),
        agreement: buyerSignals.length > 1 ? 0.8 : buyerSignals.length ? 0.6 : 0
      },
      model: {
        calibration: 0.8,
        variance: 0.2,
        stability: 0.75
      },
      drivers: [
        `Recent weekly avg volume ${avgVolume.toFixed(2)}`,
        `Signal count ${buyerSignals.length}`,
        `History weeks ${rows.length}`
      ]
    });

    const probability = Math.min(0.95, Math.max(0.05, normalizedVolume * 0.6 + (confidence.overall_confidence / 100) * 0.4));
    const evidence = buyerSignals
      .slice(0, 5)
      .map((s: AnyRow) => ({ source_url: s.source_url, evidence_snippet: s.evidence_snippet, signal_type: s.signal_type }));

    await civant.asServiceRole.entities[PIPELINE_NAMES.predictions].create({
      id: makeId('pred'),
      tenant_id: params.tenant_id,
      buyer_id: buyer_id === 'unknown' ? null : buyer_id,
      category,
      cpv_family,
      time_window: params.time_window || 'next_12_months',
      probability,
      confidence: confidence.overall_confidence / 100,
      confidence_breakdown: confidence,
      top_drivers: confidence.drivers.slice(0, 5),
      evidence,
      model_version: params.model_version || 'agentic-v1',
      generated_at: new Date().toISOString()
    });
    created += 1;
  }

  return { created };
}

export function compute_confidence(input: Parameters<typeof computeConfidence>[0]) {
  return computeConfidence(input);
}

export async function runPipeline(civant: CivantClient, params: {
  run_id: string;
  tenant_id: string;
  source: string;
  cursor?: string;
  documents: RawInput[];
}) {
  const ingest = await ingest_raw(civant, {
    run_id: params.run_id,
    tenant_id: params.tenant_id,
    source: params.source,
    cursor: params.cursor,
    documents: params.documents
  });
  const staging = await parse_to_staging(civant, { run_id: params.run_id, tenant_id: params.tenant_id });
  const normalized = await normalise_to_canonical(civant, { run_id: params.run_id, tenant_id: params.tenant_id });
  const features = await build_weekly_features(civant, { tenant_id: params.tenant_id });
  const signals = await attach_market_signals(civant, { run_id: params.run_id, tenant_id: params.tenant_id });
  const predictions = await generate_predictions(civant, { tenant_id: params.tenant_id, model_version: 'agentic-v1' });

  return {
    run_id: params.run_id,
    tenant_id: params.tenant_id,
    ingest,
    staging,
    normalized,
    features,
    signals,
    predictions
  };
}
