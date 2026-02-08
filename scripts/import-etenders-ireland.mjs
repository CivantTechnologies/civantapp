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

function makeRunId(prefix = 'etenders_csv') {
  const stamp = new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14);
  const suffix = crypto.randomUUID().replace(/-/g, '').slice(0, 8);
  return `${prefix}_${stamp}_${suffix}`;
}

function parseCsvLine(line) {
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

    if (char === ',' && !inQuotes) {
      out.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  out.push(current);
  return { fields: out, inQuotes };
}

function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.toLowerCase() === 'null') return null;
  return text;
}

function parseDate(value) {
  const text = clean(value);
  if (!text) return { date: null, iso: null };

  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    return { date: text, iso: `${text}T00:00:00.000Z` };
  }

  const match = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (match) {
    const day = match[1].padStart(2, '0');
    const month = match[2].padStart(2, '0');
    const year = match[3];
    const date = `${year}-${month}-${day}`;
    return { date, iso: `${date}T00:00:00.000Z` };
  }

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
  const normalized = text.replace(/[^0-9.-]/g, '');
  if (!normalized) return null;
  const parsed = Number(normalized);
  if (Number.isNaN(parsed)) return null;
  return parsed;
}

function parseInteger(value) {
  const n = parseNumber(value);
  if (n === null) return null;
  return Math.round(n);
}

function splitCodes(mainCode, additionalCodes) {
  const set = new Set();
  const main = clean(mainCode);
  if (main) {
    set.add(main);
  }

  const extra = clean(additionalCodes);
  if (extra) {
    for (const token of extra.split(/[;,]/)) {
      const code = clean(token);
      if (!code) continue;
      set.add(code);
    }
  }

  return Array.from(set);
}

function pickFirst(...values) {
  for (const value of values) {
    const normalized = clean(value);
    if (normalized) return normalized;
  }
  return null;
}

function detectNoticeType(rawAwardPublished) {
  const value = clean(rawAwardPublished);
  if (!value) return 'tender';
  const lowered = value.toLowerCase();
  if (lowered === 'yes' || lowered === 'true' || lowered.includes('award')) {
    return 'award';
  }
  return 'tender';
}

function makeFingerprint(parts) {
  return crypto.createHash('sha256').update(parts.filter(Boolean).join('|')).digest('hex');
}

function inferTenderId(record, title) {
  const seed = [
    clean(record['Contracting Authority']),
    clean(record['Name of Client Contracting Authority']),
    title,
    clean(record['Notice Published Date/Contract Created Date']),
    clean(record['Threshold Level']),
    clean(record['Platform']),
    clean(record['Procedure']),
    clean(record['Notice Estimated Value (€)'])
  ]
    .filter(Boolean)
    .join('|');

  const digest = makeFingerprint([seed]).slice(0, 24);
  return `INF_${digest}`;
}

function mapRow(record, options = {}) {
  const allowInferredId = options.allowInferredId === true;
  const rawTenderId = clean(record['Tender ID']);
  const title = clean(record['Tender/Contract Name']);

  if (!title) return null;

  let tenderId = rawTenderId;
  let inferredId = false;
  if (!tenderId) {
    if (!allowInferredId) return null;
    tenderId = inferTenderId(record, title);
    inferredId = true;
  }

  const source = 'ETENDERS_IE';
  const canonicalId = `${source}:${tenderId}`;
  const buyerName = pickFirst(record['Contracting Authority'], record['Name of Client Contracting Authority'], record['Agreement Owner']);
  const publication = parseDate(record['Notice Published Date/Contract Created Date']);
  const deadline = parseDate(record['Tender Submission Deadline']);
  const cpvCodes = splitCodes(record['Main Cpv Code'], record['Additional CPV Codes on CFT']);
  const cpvCsv = cpvCodes.join(',');
  const estimatedValue = parseNumber(record['Notice Estimated Value (€)']);
  const awardedValue = parseNumber(record['Awarded Value (€)']);
  const sourceUrl = pickFirst(record['TED Notice Link'], record['TED CAN Link']);
  const noticeType = detectNoticeType(record['Award Published']);
  const nowIso = new Date().toISOString();

  const baseFields = {
    tender_id: tenderId,
    original_tender_id: rawTenderId,
    id_inferred: inferredId,
    parent_agreement_id: clean(record['Parent Agreement ID']),
    contracting_authority: clean(record['Contracting Authority']),
    client_contracting_authority: clean(record['Name of Client Contracting Authority']),
    agreement_owner: clean(record['Agreement Owner']),
    tender_name: title,
    directive: clean(record['Directive']),
    competition_type: clean(record['Competition Type']),
    main_cpv_code: clean(record['Main Cpv Code']),
    main_cpv_description: clean(record['Main Cpv Code Description']),
    additional_cpv_codes: clean(record['Additional CPV Codes on CFT']),
    spend_category: clean(record['Spend Category']),
    contract_type: clean(record['Contract Type']),
    threshold_level: clean(record['Threshold Level']),
    procedure: clean(record['Procedure']),
    evaluation_type: clean(record['Evaluation Type']),
    contract_duration_months: parseInteger(record['Contract Duration (Months)']),
    cancelled_date: parseDate(record['Cancelled Date']).date,
    award_published: clean(record['Award Published']),
    no_of_bids_received: parseInteger(record['No of Bids Received']),
    no_of_smes_bids_received: parseInteger(record['No of SMEs Bids Received']),
    awarded_suppliers: clean(record['Awarded Suppliers']),
    no_of_awarded_smes: parseInteger(record['No of Awarded SMEs']),
    ted_notice_link: clean(record['TED Notice Link']),
    ted_can_link: clean(record['TED CAN Link']),
    platform: clean(record['Platform'])
  };

  const fingerprint = makeFingerprint([
    canonicalId,
    title,
    buyerName,
    publication.date,
    deadline.date,
    cpvCsv,
    String(estimatedValue || ''),
    String(awardedValue || '')
  ]);

  const normalizedJson = {
    ...baseFields,
    source,
    country: 'IE',
    canonical_id: canonicalId,
    source_notice_id: tenderId,
    source_notice_id_inferred: inferredId,
    title,
    buyer_name: buyerName,
    notice_type: noticeType,
    publication_date: publication.date,
    deadline_date: deadline.date,
    cpv_codes: cpvCodes,
    estimated_value: estimatedValue,
    awarded_value: awardedValue,
    currency: 'EUR',
    source_url: sourceUrl,
    fingerprint,
    first_seen_at: publication.iso || nowIso,
    last_seen_at: nowIso,
    version_count: 1
  };

  const canonicalTender = {
    canonical_id: canonicalId,
    source,
    external_id: tenderId,
    title,
    category: baseFields.spend_category,
    subcategory: baseFields.contract_type,
    cpv_codes: cpvCodes,
    publication_date: publication.date,
    deadline_date: deadline.date,
    estimated_value: estimatedValue,
    currency: 'EUR',
    source_url: sourceUrl,
    dedupe_quality: 0.9,
    normalized_json: normalizedJson
  };

  const currentTender = {
    tender_id: canonicalId,
    source,
    published_at: publication.iso || nowIso,
    data: normalizedJson
  };

  return { canonicalTender, currentTender };
}

function isDuplicateError(message) {
  const text = String(message || '').toLowerCase();
  return text.includes('duplicate key') || text.includes('already exists') || text.includes('conflict');
}

async function postRows({ baseUrl, appId, tenantId, table, rows, includeTenantHeader }) {
  const headers = {
    'content-type': 'application/json'
  };

  if (includeTenantHeader && tenantId) {
    headers['x-tenant-id'] = tenantId;
  }

  const response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(rows)
  });

  const text = await response.text();
  const payload = text ? (() => {
    try { return JSON.parse(text); } catch { return { raw: text }; }
  })() : null;

  if (!response.ok) {
    const errorMessage = payload?.error || payload?.message || `HTTP ${response.status}`;
    const error = new Error(String(errorMessage));
    error.status = response.status;
    error.payload = payload;
    throw error;
  }

  return payload;
}

async function putEntityById({ baseUrl, appId, tenantId, table, id, payload, includeTenantHeader }) {
  const headers = {
    'content-type': 'application/json'
  };

  if (includeTenantHeader && tenantId) {
    headers['x-tenant-id'] = tenantId;
  }

  const response = await fetch(
    `${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}/${encodeURIComponent(id)}`,
    {
      method: 'PUT',
      headers,
      body: JSON.stringify(payload)
    }
  );

  const text = await response.text();
  const parsed = text ? (() => {
    try {
      return JSON.parse(text);
    } catch {
      return { raw: text };
    }
  })() : null;

  if (!response.ok) {
    const errorMessage = parsed?.error || parsed?.message || `HTTP ${response.status}`;
    throw new Error(String(errorMessage));
  }

  return parsed;
}

async function insertBatchWithFallback({ baseUrl, appId, tenantId, table, rows, metrics, includeTenantHeader }) {
  if (!rows.length) return;

  try {
    await postRows({ baseUrl, appId, tenantId, table, rows, includeTenantHeader });
    metrics.inserted += rows.length;
    return;
  } catch (error) {
    if (rows.length === 1) {
      if (isDuplicateError(error.message)) {
        metrics.duplicates += 1;
      } else {
        metrics.failed += 1;
        metrics.errors.push(`${table} ${rows[0].canonical_id || rows[0].tender_id || 'unknown'}: ${error.message}`);
      }
      return;
    }
  }

  for (const row of rows) {
    try {
      await postRows({ baseUrl, appId, tenantId, table, rows: [row], includeTenantHeader });
      metrics.inserted += 1;
    } catch (error) {
      if (isDuplicateError(error.message)) {
        metrics.duplicates += 1;
      } else {
        metrics.failed += 1;
        metrics.errors.push(`${table} ${row.canonical_id || row.tender_id || 'unknown'}: ${error.message}`);
      }
    }
  }
}

async function main() {
  const args = parseArgs(process.argv);
  const file = args.file;
  const baseUrl = String(args['api-base'] || 'https://civantapp.vercel.app').replace(/\/$/, '');
  const appId = String(args['app-id'] || 'civantapp');
  const tenantId = String(args['tenant-id'] || 'civant_default');
  const includeTenantHeader = args['with-tenant-header'] === 'true';
  const allowInferredId = args['allow-inferred-id'] === 'true';
  const onlyMissingId = args['only-missing-id'] === 'true';
  const rawOnly = args['raw-only'] === 'true';
  const logRaw = args['log-raw'] !== 'false';
  const runId = String(args['run-id'] || makeRunId());
  const batchSize = Math.max(10, Number(args['batch-size'] || 200));
  const limit = args.limit ? Math.max(1, Number(args.limit)) : null;
  const dryRun = args['dry-run'] === 'true';

  if (!file) {
    console.error('Missing --file argument');
    process.exit(1);
  }

  if (!fs.existsSync(file)) {
    console.error(`File not found: ${file}`);
    process.exit(1);
  }

  const source = 'ETENDERS_IE';
  const startedAt = new Date().toISOString();
  const sourceCursor = `file:${file}`;

  const rawMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };
  const canonicalMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };
  const currentMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };

  let headers = [];
  let lineNumber = 0;
  let recordNumber = 0;
  let processed = 0;
  let skipped = 0;
  let skippedNonMissing = 0;
  let dedupedInFile = 0;
  let inferredIdRows = 0;
  let malformedRecords = 0;
  let pendingRecord = '';
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

  // Start reading the file only after the ingestion run row is created.
  // Otherwise the stream can finish before iteration begins in non-dry runs.
  const stream = fs.createReadStream(file, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  const flush = async () => {
    if (!rawBatch.length && (rawOnly || !canonicalBatch.length)) return;

    if (!dryRun) {
      if (rawBatch.length) {
        await insertBatchWithFallback({
          baseUrl,
          appId,
          tenantId,
          table: 'raw_documents',
          rows: rawBatch.splice(0, rawBatch.length),
          metrics: rawMetrics,
          includeTenantHeader
        });
      }

      if (!rawOnly && canonicalBatch.length) {
        await insertBatchWithFallback({
          baseUrl,
          appId,
          tenantId,
          table: 'canonical_tenders',
          rows: canonicalBatch.splice(0, canonicalBatch.length),
          metrics: canonicalMetrics,
          includeTenantHeader
        });
        await insertBatchWithFallback({
          baseUrl,
          appId,
          tenantId,
          table: 'TendersCurrent',
          rows: currentBatch.splice(0, currentBatch.length),
          metrics: currentMetrics,
          includeTenantHeader
        });
      } else {
        canonicalBatch.length = 0;
        currentBatch.length = 0;
      }
    } else {
      rawMetrics.inserted += rawBatch.length;
      rawBatch.length = 0;

      if (!rawOnly) {
        canonicalMetrics.inserted += canonicalBatch.length;
        currentMetrics.inserted += currentBatch.length;
      }
      canonicalBatch.length = 0;
      currentBatch.length = 0;
    }
  };

  for await (const rawLine of rl) {
    lineNumber += 1;
    if (!pendingRecord && !rawLine.trim()) continue;

    pendingRecord = pendingRecord ? `${pendingRecord}\n${rawLine}` : rawLine;
    const currentRecordText = pendingRecord;
    const parsed = parseCsvLine(currentRecordText);
    if (parsed.inQuotes) {
      continue;
    }

    pendingRecord = '';
    recordNumber += 1;
    const values = parsed.fields;

    if (recordNumber === 1) {
      headers = values.map((value) => String(value || '').replace(/^\uFEFF/, '').trim());
      continue;
    }

    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });

    const hasTenderId = Boolean(clean(row['Tender ID']));
    if (onlyMissingId && hasTenderId) {
      skippedNonMissing += 1;
      continue;
    }

    if (logRaw) {
      const contentHash = makeFingerprint([JSON.stringify(row)]);
      const checksum = makeFingerprint([runId, String(recordNumber), contentHash]);

      rawBatch.push({
        id: `${runId}:${String(recordNumber).padStart(8, '0')}`,
        run_id: runId,
        source,
        source_url: pickFirst(row['TED Notice Link'], row['TED CAN Link']),
        document_type: 'csv_row',
        external_id: clean(row['Tender ID']),
        raw_text: currentRecordText,
        raw_json: {
          row,
          line_number: lineNumber,
          record_number: recordNumber,
          content_hash: contentHash,
          only_missing_id_mode: onlyMissingId
        },
        fetched_at: new Date().toISOString(),
        checksum,
        tenant_id: tenantId
      });
    }

    if (rawOnly) {
      processed += 1;
      if (rawBatch.length >= batchSize) {
        await flush();
      }
      if (limit && processed >= limit) {
        break;
      }
      continue;
    }

    const mapped = mapRow(row, { allowInferredId });
    if (!mapped) {
      skipped += 1;
      if (rawBatch.length >= batchSize) {
        await flush();
      }
      continue;
    }

    const canonicalId = String(mapped.canonicalTender?.canonical_id || '').trim();
    if (canonicalId) {
      if (seenCanonicalIds.has(canonicalId)) {
        dedupedInFile += 1;
        if (rawBatch.length >= batchSize) {
          await flush();
        }
        continue;
      }
      seenCanonicalIds.add(canonicalId);
    }

    if (mapped.canonicalTender?.normalized_json?.id_inferred) {
      inferredIdRows += 1;
    }

    canonicalBatch.push(mapped.canonicalTender);
    currentBatch.push(mapped.currentTender);
    processed += 1;

    if (rawBatch.length >= batchSize || canonicalBatch.length >= batchSize) {
      await flush();
    }

    if (processed % 2000 === 0) {
      console.log(`Processed ${processed} rows (record ${recordNumber}, line ${lineNumber})`);
    }

    if (limit && processed >= limit) {
      break;
    }
  }

  if (pendingRecord) {
    malformedRecords += 1;
  }

  await flush();

  const duplicatesTotal = dedupedInFile + Math.max(canonicalMetrics.duplicates, currentMetrics.duplicates);
  const metrics = {
    total_rows: Math.max(recordNumber - 1, 0),
    processed_rows: processed,
    skipped_missing_required: skipped,
    skipped_non_missing: skippedNonMissing,
    deduped_in_file: dedupedInFile,
    inferred_id_rows: inferredIdRows,
    duplicates_total: duplicatesTotal,
    raw_rows: rawMetrics.inserted,
    raw_duplicates: rawMetrics.duplicates,
    raw_failed: rawMetrics.failed,
    canonical_inserted: canonicalMetrics.inserted,
    canonical_duplicates: canonicalMetrics.duplicates,
    canonical_failed: canonicalMetrics.failed,
    current_inserted: currentMetrics.inserted,
    current_duplicates: currentMetrics.duplicates,
    current_failed: currentMetrics.failed,
    malformed_records: malformedRecords,
    mode: rawOnly ? 'raw_only' : 'canonical_and_current'
  };

  const runErrors = [...rawMetrics.errors, ...canonicalMetrics.errors, ...currentMetrics.errors].slice(0, 50);
  const runStatus = runErrors.length > 0 || rawMetrics.failed || canonicalMetrics.failed || currentMetrics.failed
    ? 'completed_with_errors'
    : 'completed';

  if (!dryRun) {
    try {
      await putEntityById({
        baseUrl,
        appId,
        tenantId,
        table: 'ingestion_runs',
        id: runId,
        payload: {
          status: runStatus,
          metrics,
          errors: runErrors,
          cursor: `record:${recordNumber}:line:${lineNumber}`,
          finished_at: new Date().toISOString()
        },
        includeTenantHeader
      });
    } catch (error) {
      console.error(`Failed to update ingestion run ${runId}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  const summary = {
    run_id: runId,
    file,
    processed,
    skipped,
    skipped_non_missing: skippedNonMissing,
    deduped_in_file: dedupedInFile,
    inferred_id_rows: inferredIdRows,
    malformed_records: malformedRecords,
    raw_documents: {
      inserted: rawMetrics.inserted,
      duplicates: rawMetrics.duplicates,
      failed: rawMetrics.failed
    },
    canonical_tenders: {
      inserted: canonicalMetrics.inserted,
      duplicates: canonicalMetrics.duplicates,
      failed: canonicalMetrics.failed
    },
    TendersCurrent: {
      inserted: currentMetrics.inserted,
      duplicates: currentMetrics.duplicates,
      failed: currentMetrics.failed
    },
    errors: runErrors
  };

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
