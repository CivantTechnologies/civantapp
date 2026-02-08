#!/usr/bin/env node

import fs from 'node:fs';
import readline from 'node:readline';
import crypto from 'node:crypto';

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = 'true';
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function makeRunId(prefix = 'boamp_csv') {
  const stamp = new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14);
  const suffix = crypto.randomUUID().replace(/-/g, '').slice(0, 8);
  return `${prefix}_${stamp}_${suffix}`;
}

function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.toLowerCase() === 'null') return null;
  return text;
}

function parseCsvLine(line, delimiter = ';') {
  const out = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      const next = line[i + 1];
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === delimiter && !inQuotes) {
      out.push(current);
      current = '';
      continue;
    }
    current += char;
  }
  out.push(current);
  return { fields: out, inQuotes };
}

function parseDate(value) {
  const text = clean(value);
  if (!text) return { date: null, iso: null };
  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) {
    const date = parsed.toISOString().slice(0, 10);
    return { date, iso: parsed.toISOString() };
  }
  return { date: null, iso: null };
}

function parseNumber(value) {
  const text = clean(value);
  if (!text) return null;
  const normalized = text.replace(',', '.').replace(/[^0-9.-]/g, '');
  if (!normalized) return null;
  const parsed = Number(normalized);
  if (Number.isNaN(parsed)) return null;
  return parsed;
}

function makeFingerprint(parts) {
  return crypto.createHash('sha256').update(parts.filter(Boolean).join('|')).digest('hex');
}

function inferNoticeType(record) {
  const nature = clean(record.nature)?.toLowerCase() || '';
  if (nature.includes('attribution') || nature.includes('attrib')) return 'award';
  if (nature.includes('rectificatif') || nature.includes('rectif')) return 'corrigendum';
  return 'tender';
}

function collectCpvCodes(record) {
  const candidates = Object.keys(record)
    .filter((key) => key.toLowerCase().includes('cpv'))
    .map((key) => clean(record[key]))
    .filter(Boolean);

  const set = new Set();
  for (const candidate of candidates) {
    for (const token of String(candidate).split(/[,\s;|]+/)) {
      const code = clean(token);
      if (!code) continue;
      if (/^\d{6,9}$/.test(code)) set.add(code);
    }
  }
  return Array.from(set);
}

function mapRow(record) {
  const sourceNoticeId = clean(record.idweb) || clean(record.id);
  const title = clean(record.objet) || clean(record.intitule) || clean(record.designation);
  if (!sourceNoticeId || !title) return null;

  const source = 'BOAMP_FR';
  const canonicalId = `${source}:${sourceNoticeId}`;
  const publication = parseDate(record.dateparution);
  const deadline = parseDate(record.datelimitereponse);
  const cpvCodes = collectCpvCodes(record);
  const nowIso = new Date().toISOString();
  const buyerName = clean(record.nomacheteur) || clean(record.organisme) || clean(record.nomorganisme);
  const estimatedValue = parseNumber(record.montant) ?? parseNumber(record.montantmarche) ?? parseNumber(record.valeurestimee);
  const sourceUrl = clean(record.urlavis) || clean(record.url) || `https://www.boamp.fr/avis/detail/${sourceNoticeId}`;
  const noticeType = inferNoticeType(record);

  const fingerprint = makeFingerprint([
    canonicalId,
    title,
    buyerName,
    publication.date,
    deadline.date,
    cpvCodes.join(','),
    String(estimatedValue ?? '')
  ]);

  const normalizedJson = {
    source,
    country: 'FR',
    canonical_id: canonicalId,
    source_notice_id: sourceNoticeId,
    title,
    buyer_name: buyerName,
    notice_type: noticeType,
    publication_date: publication.date,
    deadline_date: deadline.date,
    cpv_codes: cpvCodes,
    estimated_value: estimatedValue,
    currency: 'EUR',
    source_url: sourceUrl,
    fingerprint,
    first_seen_at: publication.iso || nowIso,
    last_seen_at: nowIso,
    version_count: 1,
    raw: record
  };

  return {
    canonicalTender: {
      canonical_id: canonicalId,
      source,
      external_id: sourceNoticeId,
      title,
      category: clean(record.famille_libelle) || clean(record.famille),
      subcategory: clean(record.procedure_libelle) || clean(record.type_procedure),
      cpv_codes: cpvCodes,
      publication_date: publication.date,
      deadline_date: deadline.date,
      estimated_value: estimatedValue,
      currency: 'EUR',
      source_url: sourceUrl,
      dedupe_quality: 0.92,
      normalized_json: normalizedJson
    },
    currentTender: {
      tender_id: canonicalId,
      source,
      published_at: publication.iso || nowIso,
      data: normalizedJson
    }
  };
}

async function postRows({ baseUrl, appId, tenantId, table, rows, includeTenantHeader }) {
  const headers = { 'content-type': 'application/json' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;

  const response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(rows)
  });

  const text = await response.text();
  const payload = text
    ? (() => {
        try { return JSON.parse(text); } catch { return { raw: text }; }
      })()
    : null;

  if (!response.ok) {
    const message = payload?.error || payload?.message || `HTTP ${response.status}`;
    const error = new Error(String(message));
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function putEntityById({ baseUrl, appId, tenantId, table, id, payload, includeTenantHeader }) {
  const headers = { 'content-type': 'application/json' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;

  const response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}/${encodeURIComponent(id)}`, {
    method: 'PUT',
    headers,
    body: JSON.stringify(payload)
  });

  const text = await response.text();
  const parsed = text
    ? (() => {
        try { return JSON.parse(text); } catch { return { raw: text }; }
      })()
    : null;
  if (!response.ok) throw new Error(parsed?.error || parsed?.message || `HTTP ${response.status}`);
  return parsed;
}

function isDuplicateError(message) {
  const text = String(message || '').toLowerCase();
  return text.includes('duplicate key') || text.includes('already exists') || text.includes('conflict');
}

async function insertBatchWithFallback({ baseUrl, appId, tenantId, table, rows, metrics, includeTenantHeader }) {
  if (!rows.length) return;
  try {
    await postRows({ baseUrl, appId, tenantId, table, rows, includeTenantHeader });
    metrics.inserted += rows.length;
    return;
  } catch (error) {
    if (rows.length === 1) {
      if (isDuplicateError(error.message)) metrics.duplicates += 1;
      else {
        metrics.failed += 1;
        metrics.errors.push(`${table}: ${error.message}`);
      }
      return;
    }
  }

  for (const row of rows) {
    try {
      await postRows({ baseUrl, appId, tenantId, table, rows: [row], includeTenantHeader });
      metrics.inserted += 1;
    } catch (error) {
      if (isDuplicateError(error.message)) metrics.duplicates += 1;
      else {
        metrics.failed += 1;
        metrics.errors.push(`${table} ${row.canonical_id || row.tender_id || 'unknown'}: ${error.message}`);
      }
    }
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const file = args.file || '/Users/davidmanrique/Downloads/boamp.csv';
  const baseUrl = String(args['api-base'] || 'https://civantapp.vercel.app').replace(/\/$/, '');
  const appId = String(args['app-id'] || 'civantapp');
  const tenantId = String(args['tenant-id'] || 'civant_default');
  const includeTenantHeader = args['with-tenant-header'] !== 'false';
  const runId = String(args['run-id'] || makeRunId());
  const batchSize = Math.max(20, Number(args['batch-size'] || 120));
  const limit = args.limit ? Math.max(1, Number(args.limit)) : null;
  const startRecord = Math.max(1, Number(args['start-record'] || 1));
  const rawOnly = args['raw-only'] === 'true';
  const dryRun = args['dry-run'] === 'true';

  if (!fs.existsSync(file)) {
    console.error(`File not found: ${file}`);
    process.exit(1);
  }

  const source = 'BOAMP_FR';
  const sourceCursor = `file:${file}`;
  const startedAt = new Date().toISOString();
  const rawMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };
  const canonicalMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };
  const currentMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };
  let headers = [];
  let lineNumber = 0;
  let recordNumber = 0;
  let dataRecordNumber = 0;
  let processed = 0;
  let skipped = 0;
  let skippedBeforeStart = 0;
  let malformedRecords = 0;
  let pendingRecord = '';
  let dedupedInFile = 0;
  const seenCanonicalIds = new Set();
  const rawBatch = [];
  const canonicalBatch = [];
  const currentBatch = [];

  if (!dryRun) {
    await postRows({
      baseUrl,
      appId,
      tenantId,
      table: 'ingestion_runs',
      rows: [{
        run_id: runId,
        source,
        cursor: sourceCursor,
        status: 'running',
        metrics: {},
        errors: [],
        started_at: startedAt,
        finished_at: null,
        tenant_id: tenantId
      }],
      includeTenantHeader
    });
  }

  const stream = fs.createReadStream(file, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  const flush = async () => {
    if (!rawBatch.length && (rawOnly || !canonicalBatch.length)) return;
    if (dryRun) {
      rawMetrics.inserted += rawBatch.length;
      rawBatch.length = 0;
      canonicalMetrics.inserted += canonicalBatch.length;
      currentMetrics.inserted += currentBatch.length;
      canonicalBatch.length = 0;
      currentBatch.length = 0;
      return;
    }

    if (rawBatch.length) {
      await insertBatchWithFallback({ baseUrl, appId, tenantId, table: 'raw_documents', rows: rawBatch.splice(0, rawBatch.length), metrics: rawMetrics, includeTenantHeader });
    }
    if (!rawOnly && canonicalBatch.length) {
      await insertBatchWithFallback({ baseUrl, appId, tenantId, table: 'canonical_tenders', rows: canonicalBatch.splice(0, canonicalBatch.length), metrics: canonicalMetrics, includeTenantHeader });
      await insertBatchWithFallback({ baseUrl, appId, tenantId, table: 'TendersCurrent', rows: currentBatch.splice(0, currentBatch.length), metrics: currentMetrics, includeTenantHeader });
    } else {
      canonicalBatch.length = 0;
      currentBatch.length = 0;
    }
  };

  for await (const rawLine of rl) {
    lineNumber += 1;
    if (!pendingRecord && !rawLine.trim()) continue;
    pendingRecord = pendingRecord ? `${pendingRecord}\n${rawLine}` : rawLine;
    const parsed = parseCsvLine(pendingRecord, ';');
    if (parsed.inQuotes) continue;
    recordNumber += 1;
    const recordText = pendingRecord;
    pendingRecord = '';
    const values = parsed.fields;

    if (recordNumber === 1) {
      headers = values.map((value) => String(value || '').replace(/^\uFEFF/, '').trim());
      continue;
    }

    dataRecordNumber += 1;
    if (dataRecordNumber < startRecord) {
      skippedBeforeStart += 1;
      continue;
    }

    const row = {};
    headers.forEach((header, index) => { row[header] = values[index] ?? ''; });

    const contentHash = makeFingerprint([JSON.stringify(row)]);
    rawBatch.push({
      id: `${runId}:${String(recordNumber).padStart(8, '0')}`,
      run_id: runId,
      source,
      source_url: clean(row.urlavis) || clean(row.url),
      document_type: 'csv_row',
      external_id: clean(row.idweb) || clean(row.id),
      raw_text: recordText,
      raw_json: {
        row,
        line_number: lineNumber,
        record_number: recordNumber,
        content_hash: contentHash
      },
      fetched_at: new Date().toISOString(),
      checksum: makeFingerprint([runId, String(recordNumber), contentHash]),
      tenant_id: tenantId
    });

    if (!rawOnly) {
      const mapped = mapRow(row);
      if (!mapped) skipped += 1;
      else {
        const canonicalId = String(mapped.canonicalTender.canonical_id);
        if (seenCanonicalIds.has(canonicalId)) dedupedInFile += 1;
        else {
          seenCanonicalIds.add(canonicalId);
          canonicalBatch.push(mapped.canonicalTender);
          currentBatch.push(mapped.currentTender);
          processed += 1;
        }
      }
    } else {
      processed += 1;
    }

    if (rawBatch.length >= batchSize || canonicalBatch.length >= batchSize) {
      await flush();
    }
    if (processed % 2000 === 0 && processed > 0) {
      console.log(`Processed ${processed} rows (record ${recordNumber}, line ${lineNumber})`);
    }
    if (limit && processed >= limit) break;
  }

  if (pendingRecord) malformedRecords += 1;
  await flush();

  const runErrors = [...rawMetrics.errors, ...canonicalMetrics.errors, ...currentMetrics.errors].slice(0, 50);
    const metrics = {
    total_rows: Math.max(recordNumber - 1, 0),
    start_record: startRecord,
    skipped_before_start: skippedBeforeStart,
    processed_rows: processed,
    skipped_missing_required: skipped,
    deduped_in_file: dedupedInFile,
    raw_rows: rawMetrics.inserted,
    canonical_inserted: canonicalMetrics.inserted,
    current_inserted: currentMetrics.inserted,
    malformed_records: malformedRecords,
    mode: rawOnly ? 'raw_only' : 'canonical_and_current'
  };

  if (!dryRun) {
    await putEntityById({
      baseUrl,
      appId,
      tenantId,
      table: 'ingestion_runs',
      id: runId,
      payload: {
        status: runErrors.length ? 'completed_with_errors' : 'completed',
        metrics,
        errors: runErrors,
        cursor: `record:${recordNumber}:line:${lineNumber}`,
        finished_at: new Date().toISOString()
      },
      includeTenantHeader
    });
  }

  console.log(JSON.stringify({
    run_id: runId,
    file,
    start_record: startRecord,
    skipped_before_start: skippedBeforeStart,
    processed,
    skipped,
    deduped_in_file: dedupedInFile,
    raw_documents: rawMetrics,
    canonical_tenders: canonicalMetrics,
    TendersCurrent: currentMetrics,
    errors: runErrors
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
