#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';

const DEFAULT_FEED_URL = 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom';
const DEFAULT_ZIP_PREFIX = 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_';
const DEFAULT_DOWNLOAD_DIR = '/tmp/placsp-es-zips';
const DEFAULT_STATUS_FILE = '/tmp/placsp-es-import-status.json';
const DEFAULT_CHECKPOINT_FILE = '/tmp/placsp-es-checkpoint.json';
const DEFAULT_CONTROL_FILE = '/tmp/placsp-es-control.json';
const DEFAULT_PID_FILE = '/tmp/placsp-es-import.pid';
const REQUEST_TIMEOUT_MS = 45000;
const MAX_HTTP_RETRIES = 5;

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

function printHelp() {
  console.log(`PLACSP/CODICE ES ingestion utility

Modes:
  --mode backfill       Process historical ZIP archives into raw/canonical/current tables
  --mode incremental    Pull current Atom feed chain and ingest new entries only
  --mode check-current  Validate recent feed entries against canonical/current and optionally repair

Core options:
  --api-base <url>                Default: https://civantapp.vercel.app
  --app-id <id>                   Default: civantapp
  --tenant-id <id>                Default: civant_default
  --with-tenant-header <bool>     Default: true
  --run-id <id>                   Optional custom run id
  --batch-size <n>                Default: 120
  --status-file <path>            Default: ${DEFAULT_STATUS_FILE}
  --checkpoint-file <path>        Default: ${DEFAULT_CHECKPOINT_FILE}
  --control-file <path>           Default: ${DEFAULT_CONTROL_FILE}
  --pid-file <path>               Default: ${DEFAULT_PID_FILE}
  --insecure-tls <bool>           Default: false

Backfill options:
  --zip <path_or_url[,path_or_url...]>   Explicit ZIP(s) to process
  --historical-from-year <yyyy>          Build historical ZIP URLs from year (default 2012)
  --historical-to-year <yyyy>            End year (default current year)
  --include-monthly-current-year <bool>  Default: true
  --download-dir <path>                  Default: ${DEFAULT_DOWNLOAD_DIR}
  --start-record <n>                     Resume from parsed entry number
  --max-archives <n>                     Limit ZIP archives processed (debug)
  --max-files-per-archive <n>            Limit atom files per ZIP (debug)

Incremental/check options:
  --feed-url <url>                 Default: ${DEFAULT_FEED_URL}
  --since-iso <iso_datetime>       Optional lower bound for entry updated timestamp
  --lookback-minutes <n>           Default: 90 (used with checkpoint)
  --max-pages <n>                  Default: 120

Check-current options:
  --repair <bool>                  Default: false
  --lookback-days <n>              Default: 14

Examples:
  node scripts/import-placsp-es.mjs --mode backfill --historical-from-year 2012
  node scripts/import-placsp-es.mjs --mode incremental
  node scripts/import-placsp-es.mjs --mode check-current --repair true --lookback-days 30

Safe controls while running:
  echo '{"action":"pause","reason":"maintenance"}' > ${DEFAULT_CONTROL_FILE}
  echo '{"action":"stop","reason":"manual stop"}' > ${DEFAULT_CONTROL_FILE}
`);
}

function writeJsonFile(filePath, value) {
  if (!filePath) return;
  try {
    fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
  } catch {
    // best effort
  }
}

function readJsonFile(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function unlinkIfExists(filePath) {
  if (!filePath) return;
  try {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  } catch {
    // best effort
  }
}

function readControlInstruction(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  try {
    const raw = fs.readFileSync(filePath, 'utf8').trim();
    if (!raw) return null;

    if (raw.startsWith('{')) {
      const parsed = JSON.parse(raw);
      const action = clean(parsed.action)?.toLowerCase();
      if (!action) return null;
      if (!['pause', 'stop'].includes(action)) return null;
      return {
        action,
        reason: clean(parsed.reason) || null
      };
    }

    const action = clean(raw)?.toLowerCase();
    if (!action || !['pause', 'stop'].includes(action)) return null;
    return { action, reason: null };
  } catch {
    return null;
  }
}

function makeRunId(prefix = 'placsp_es') {
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

function decodeXmlEntities(value) {
  const text = clean(value);
  if (!text) return null;
  return text
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, '&');
}

function parseDate(value) {
  const text = clean(value);
  if (!text) return { date: null, iso: null, ms: null };
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
      return { date: text, iso: `${text}T00:00:00.000Z`, ms: Date.parse(`${text}T00:00:00.000Z`) };
    }
    return { date: null, iso: null, ms: null };
  }
  return {
    date: parsed.toISOString().slice(0, 10),
    iso: parsed.toISOString(),
    ms: parsed.getTime()
  };
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

function extractFirst(xml, regex) {
  const match = regex.exec(xml);
  if (!match || !match[1]) return null;
  return decodeXmlEntities(match[1]);
}

function extractAll(xml, regex) {
  const out = [];
  let match;
  const local = new RegExp(regex.source, regex.flags.includes('g') ? regex.flags : `${regex.flags}g`);
  while ((match = local.exec(xml)) !== null) {
    if (!match[1]) continue;
    const value = decodeXmlEntities(match[1]);
    if (value) out.push(value);
  }
  return out;
}

function splitEntryBlocks(feedXml) {
  return feedXml.match(/<entry>[\s\S]*?<\/entry>/gi) || [];
}

function parseSummaryField(summary, key) {
  const text = clean(summary);
  if (!text) return null;
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = new RegExp(`${escaped}:\\s*([^;]+)`, 'i').exec(text);
  return decodeXmlEntities(match?.[1] || null);
}

function inferNoticeType(statusCode) {
  const code = clean(statusCode)?.toUpperCase() || '';
  if (code === 'ADJ' || code === 'RES') return 'award';
  if (code === 'ANUL') return 'cancellation';
  return 'tender';
}

function mapContractType(code) {
  const normalized = clean(code);
  switch (normalized) {
    case '1': return 'supplies';
    case '2': return 'services';
    case '3': return 'works';
    default: return normalized;
  }
}

function parsePlacspEntry(entryXml, context = {}) {
  const entryId = extractFirst(entryXml, /<id>([\s\S]*?)<\/id>/i);
  const sourceUrl = extractFirst(entryXml, /<link[^>]*href="([^"]+)"[^>]*\/?>(?:<\/link>)?/i);
  const title = extractFirst(entryXml, /<title[^>]*>([\s\S]*?)<\/title>/i);
  const summary = extractFirst(entryXml, /<summary[^>]*>([\s\S]*?)<\/summary>/i);
  const updatedText = extractFirst(entryXml, /<updated>([^<]+)<\/updated>/i);
  const updated = parseDate(updatedText);

  const contractFolderId =
    extractFirst(entryXml, /<cbc:ContractFolderID>([\s\S]*?)<\/cbc:ContractFolderID>/i) ||
    parseSummaryField(summary, 'Id licitación') ||
    (entryId ? entryId.split('/').pop() : null);

  if (!contractFolderId || !title) return null;

  const statusCode = extractFirst(entryXml, /<cbc-place-ext:ContractFolderStatusCode[^>]*>([^<]+)<\/cbc-place-ext:ContractFolderStatusCode>/i);
  const buyerName =
    extractFirst(entryXml, /<cac-place-ext:LocatedContractingParty[\s\S]*?<cac:PartyName>\s*<cbc:Name>([\s\S]*?)<\/cbc:Name>/i) ||
    extractFirst(entryXml, /<cac:PartyName>\s*<cbc:Name>([\s\S]*?)<\/cbc:Name>\s*<\/cac:PartyName>/i) ||
    parseSummaryField(summary, 'Órgano de Contratación');

  const cpvCodes = Array.from(
    new Set(
      extractAll(entryXml, /<cbc:ItemClassificationCode[^>]*>([0-9]{6,9})<\/cbc:ItemClassificationCode>/ig)
        .map((cpv) => clean(cpv))
        .filter(Boolean)
    )
  );

  const estimatedOverall = parseNumber(extractFirst(entryXml, /<cbc:EstimatedOverallContractAmount[^>]*>([^<]+)<\/cbc:EstimatedOverallContractAmount>/i));
  const totalAmount = parseNumber(extractFirst(entryXml, /<cbc:TotalAmount[^>]*>([^<]+)<\/cbc:TotalAmount>/i));
  const taxExclusiveAmount = parseNumber(extractFirst(entryXml, /<cbc:TaxExclusiveAmount[^>]*>([^<]+)<\/cbc:TaxExclusiveAmount>/i));
  const summaryAmount = parseNumber(parseSummaryField(summary, 'Importe'));
  const estimatedValue = estimatedOverall ?? taxExclusiveAmount ?? totalAmount ?? summaryAmount;

  const awardDate = parseDate(extractFirst(entryXml, /<cbc:AwardDate>([^<]+)<\/cbc:AwardDate>/i));
  const deadlineDate = parseDate(
    extractFirst(entryXml, /<cac:TenderingProcess[\s\S]*?<cbc:EndDate>([^<]+)<\/cbc:EndDate>/i) ||
    extractFirst(entryXml, /<cbc:EndDate>([^<]+)<\/cbc:EndDate>/i)
  );

  const contractTypeCode = extractFirst(entryXml, /<cbc:TypeCode[^>]*>([^<]+)<\/cbc:TypeCode>/i);
  const contractSubTypeCode = extractFirst(entryXml, /<cbc:SubTypeCode[^>]*>([^<]+)<\/cbc:SubTypeCode>/i);
  const procedureCode = extractFirst(entryXml, /<cbc:ProcedureCode[^>]*>([^<]+)<\/cbc:ProcedureCode>/i);
  const countryCode = extractFirst(entryXml, /<cac:Country>\s*<cbc:IdentificationCode[^>]*>([^<]+)<\/cbc:IdentificationCode>/i) || 'ES';
  const nutsCode = extractFirst(entryXml, /<cbc:CountrySubentityCode[^>]*>([^<]+)<\/cbc:CountrySubentityCode>/i);
  const receivedTenderQuantity = parseNumber(extractFirst(entryXml, /<cbc:ReceivedTenderQuantity>([^<]+)<\/cbc:ReceivedTenderQuantity>/i));

  const noticeType = inferNoticeType(statusCode);
  const source = 'PLACSP_ES';
  const canonicalId = `${source}:${contractFolderId}`;

  const versionFingerprint = makeFingerprint([
    canonicalId,
    title,
    buyerName,
    statusCode,
    updated.iso,
    deadlineDate.date,
    awardDate.date,
    cpvCodes.join(','),
    String(estimatedValue ?? ''),
    procedureCode,
    contractTypeCode,
    contractSubTypeCode
  ]);

  const normalizedJson = {
    source,
    country: clean(countryCode) || 'ES',
    canonical_id: canonicalId,
    source_notice_id: contractFolderId,
    title,
    summary,
    buyer_name: buyerName,
    notice_type: noticeType,
    status_code: statusCode,
    publication_date: updated.date,
    deadline_date: deadlineDate.date,
    award_date: awardDate.date,
    cpv_codes: cpvCodes,
    estimated_value: estimatedValue,
    currency: 'EUR',
    source_url: sourceUrl,
    contract_type_code: contractTypeCode,
    contract_subtype_code: contractSubTypeCode,
    procedure_code: procedureCode,
    nuts_code: nutsCode,
    received_tender_quantity: receivedTenderQuantity,
    feed_entry_id: entryId,
    feed_url: context.feedUrl || null,
    feed_updated_at: context.feedUpdated || null,
    version_fingerprint: versionFingerprint,
    first_seen_at: updated.iso || new Date().toISOString(),
    last_seen_at: updated.iso || new Date().toISOString()
  };

  return {
    source,
    canonicalId,
    externalId: contractFolderId,
    entryId,
    entryUpdated: updated.iso,
    entryUpdatedMs: updated.ms,
    versionFingerprint,
    rawDocument: {
      id: null,
      run_id: null,
      source,
      source_url: sourceUrl,
      document_type: 'atom_entry',
      external_id: contractFolderId,
      raw_text: entryXml,
      raw_json: {
        feed_url: context.feedUrl || null,
        feed_updated_at: context.feedUpdated || null,
        entry_id: entryId,
        entry_updated_at: updated.iso,
        status_code: statusCode,
        title,
        summary,
        source_notice_id: contractFolderId,
        version_fingerprint: versionFingerprint
      },
      fetched_at: new Date().toISOString(),
      checksum: makeFingerprint([source, contractFolderId, updated.iso, versionFingerprint]),
      tenant_id: context.tenantId
    },
    canonicalTender: {
      canonical_id: canonicalId,
      source,
      external_id: contractFolderId,
      title,
      category: mapContractType(contractTypeCode),
      subcategory: contractSubTypeCode,
      cpv_codes: cpvCodes,
      publication_date: updated.date,
      deadline_date: deadlineDate.date,
      estimated_value: estimatedValue,
      currency: 'EUR',
      source_url: sourceUrl,
      dedupe_quality: 0.95,
      normalized_json: normalizedJson,
      tenant_id: context.tenantId
    },
    currentTender: {
      tender_id: canonicalId,
      source,
      published_at: updated.iso || new Date().toISOString(),
      data: normalizedJson,
      tenant_id: context.tenantId
    }
  };
}

function parseFeedDocument(feedXml, feedUrl) {
  const feedUpdated = extractFirst(feedXml, /<feed[\s\S]*?<updated>([^<]+)<\/updated>/i);
  const nextLink = extractFirst(feedXml, /<link[^>]*href="([^"]+)"[^>]*rel="next"[^>]*\/?>(?:<\/link>)?/i);
  const entryBlocks = splitEntryBlocks(feedXml);
  return {
    feedUpdated,
    nextLink,
    entryBlocks
  };
}

async function fetchText(url) {
  let lastError = null;
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.8',
          'User-Agent': 'Civant Ingestor/1.0'
        },
        signal: controller.signal
      });
      clearTimeout(timer);
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 300)}`);
      }
      return await response.text();
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
      }
    }
  }
  throw lastError || new Error(`Failed to fetch ${url}`);
}

async function downloadToFile(url, destination) {
  if (fs.existsSync(destination)) return destination;
  fs.mkdirSync(path.dirname(destination), { recursive: true });

  let lastError = null;
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS * 2);
    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          Accept: 'application/zip,application/octet-stream,*/*;q=0.8',
          'User-Agent': 'Civant Ingestor/1.0'
        },
        signal: controller.signal
      });
      clearTimeout(timer);
      if (!response.ok || !response.body) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 300)}`);
      }
      const file = fs.createWriteStream(destination);
      await pipeline(Readable.fromWeb(response.body), file);
      return destination;
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (fs.existsSync(destination)) {
        try { fs.unlinkSync(destination); } catch {
          // noop
        }
      }
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
      }
    }
  }
  throw lastError || new Error(`Download failed: ${url}`);
}

function listZipAtomEntries(zipPath) {
  const text = execFileSync('unzip', ['-Z1', zipPath], { encoding: 'utf8', maxBuffer: 1024 * 1024 * 64 });
  return text
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.toLowerCase().endsWith('.atom'))
    .sort((a, b) => a.localeCompare(b));
}

function readZipEntry(zipPath, entryName) {
  return execFileSync('unzip', ['-p', zipPath, entryName], {
    encoding: 'utf8',
    maxBuffer: 1024 * 1024 * 256
  });
}

function parseBoolean(value, fallback = false) {
  const text = String(value ?? '').trim().toLowerCase();
  if (!text) return fallback;
  if (['1', 'true', 'yes', 'y'].includes(text)) return true;
  if (['0', 'false', 'no', 'n'].includes(text)) return false;
  return fallback;
}

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  if (Number.isNaN(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function isDuplicateError(message) {
  const text = String(message || '').toLowerCase();
  return text.includes('duplicate key') || text.includes('already exists') || text.includes('conflict');
}

function isIngestionRunDuplicateError(message) {
  const text = String(message || '').toLowerCase();
  return isDuplicateError(text) && text.includes('ingestion_runs');
}

async function postRows({ baseUrl, appId, tenantId, table, rows, includeTenantHeader }) {
  const headers = { 'content-type': 'application/json' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;

  let lastError = null;
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(rows),
        signal: controller.signal
      });
      clearTimeout(timer);

      const text = await response.text();
      const payload = text
        ? (() => {
            try { return JSON.parse(text); } catch { return { raw: text }; }
          })()
        : null;

      if (!response.ok) {
        throw new Error(payload?.error || payload?.message || `HTTP ${response.status}`);
      }
      return payload;
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
      }
    }
  }
  throw lastError || new Error(`Failed POST ${table}`);
}

async function putEntityById({ baseUrl, appId, tenantId, table, id, payload, includeTenantHeader }) {
  const headers = { 'content-type': 'application/json' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;

  let lastError = null;
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}/${encodeURIComponent(id)}`, {
        method: 'PUT',
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal
      });
      clearTimeout(timer);

      const text = await response.text();
      const parsed = text
        ? (() => {
            try { return JSON.parse(text); } catch { return { raw: text }; }
          })()
        : null;
      if (!response.ok) throw new Error(parsed?.error || parsed?.message || `HTTP ${response.status}`);
      return parsed;
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
      }
    }
  }
  throw lastError || new Error(`Failed PUT ${table}/${id}`);
}

async function fetchEntitiesPage({ baseUrl, appId, tenantId, table, query = {}, sort, fields, limit = 1000, skip = 0, includeTenantHeader }) {
  const url = new URL(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}`);
  url.searchParams.set('limit', String(limit));
  url.searchParams.set('skip', String(skip));
  if (sort) url.searchParams.set('sort', sort);
  if (fields) url.searchParams.set('fields', fields);
  if (query && Object.keys(query).length) {
    url.searchParams.set('q', JSON.stringify(query));
  }
  const headers = {};
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;

  const response = await fetch(url, { method: 'GET', headers });
  const text = await response.text();
  const parsed = text
    ? (() => {
        try { return JSON.parse(text); } catch { return { raw: text }; }
      })()
    : null;
  if (!response.ok) throw new Error(parsed?.error || parsed?.message || `HTTP ${response.status}`);
  if (!Array.isArray(parsed)) return [];
  return parsed;
}

async function loadSourceIdSet({ baseUrl, appId, tenantId, table, fieldName, includeTenantHeader, source }) {
  const ids = new Set();
  const limit = 1000;
  let skip = 0;
  for (;;) {
    const rows = await fetchEntitiesPage({
      baseUrl,
      appId,
      tenantId,
      table,
      query: { source },
      sort: fieldName,
      fields: fieldName,
      limit,
      skip,
      includeTenantHeader
    });
    for (const row of rows) {
      const value = clean(row[fieldName] || row.id || row.tender_id || row.canonical_id);
      if (value) ids.add(value);
    }
    if (rows.length < limit) break;
    skip += limit;
  }
  return ids;
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
      if (isDuplicateError(error.message)) {
        metrics.duplicates += 1;
      } else {
        metrics.failed += 1;
        metrics.errors.push(`${table} ${row.canonical_id || row.tender_id || row.id || 'unknown'}: ${error.message}`);
      }
    }
  }
}

async function insertBatchWithUpsertById({
  baseUrl,
  appId,
  tenantId,
  table,
  idField,
  rows,
  metrics,
  includeTenantHeader
}) {
  if (!rows.length) return;

  try {
    await postRows({ baseUrl, appId, tenantId, table, rows, includeTenantHeader });
    metrics.inserted += rows.length;
    return;
  } catch (error) {
    if (rows.length === 1) {
      const row = rows[0];
      if (isDuplicateError(error.message)) {
        metrics.duplicates += 1;
        const id = clean(row[idField]);
        if (id) {
          try {
            await putEntityById({
              baseUrl,
              appId,
              tenantId,
              table,
              id,
              payload: row,
              includeTenantHeader
            });
            metrics.updated += 1;
            return;
          } catch (updateError) {
            metrics.failed += 1;
            metrics.errors.push(`${table} upsert ${id}: ${updateError.message}`);
            return;
          }
        }
      }
      metrics.failed += 1;
      metrics.errors.push(`${table}: ${error.message}`);
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
        const id = clean(row[idField]);
        if (!id) continue;
        try {
          await putEntityById({
            baseUrl,
            appId,
            tenantId,
            table,
            id,
            payload: row,
            includeTenantHeader
          });
          metrics.updated += 1;
        } catch (updateError) {
          metrics.failed += 1;
          metrics.errors.push(`${table} upsert ${id}: ${updateError.message}`);
        }
      } else {
        metrics.failed += 1;
        metrics.errors.push(`${table} ${row[idField] || 'unknown'}: ${error.message}`);
      }
    }
  }
}

function buildHistoricalZipUrls({ fromYear, toYear, includeMonthlyCurrentYear }) {
  const urls = [];
  const now = new Date();
  const currentYear = now.getUTCFullYear();
  const currentMonth = now.getUTCMonth() + 1;

  for (let year = fromYear; year <= toYear; year += 1) {
    if (year < currentYear || !includeMonthlyCurrentYear) {
      urls.push(`${DEFAULT_ZIP_PREFIX}${year}.zip`);
      continue;
    }

    for (let month = 1; month <= currentMonth; month += 1) {
      urls.push(`${DEFAULT_ZIP_PREFIX}${year}${String(month).padStart(2, '0')}.zip`);
    }
  }

  return urls;
}

function parseZipList(value) {
  const text = clean(value);
  if (!text) return [];
  return text
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);
}

function normalizeUrl(value) {
  const text = clean(value);
  if (!text) return null;
  if (text.startsWith('http://') || text.startsWith('https://')) return text;
  return null;
}

function safeFileNameFromUrl(url) {
  const pathname = new URL(url).pathname;
  const base = path.basename(pathname) || 'download.zip';
  return base.replace(/[^a-zA-Z0-9._-]/g, '_');
}

function applyLookbackIso(iso, lookbackMinutes) {
  const parsed = parseDate(iso);
  if (!parsed.ms) return null;
  const adjusted = new Date(parsed.ms - lookbackMinutes * 60 * 1000);
  return adjusted.toISOString();
}

async function* iterateFeedChain({ feedUrl, sinceIso, maxPages, tenantId, modeLabel, status, shouldStopOnOld }) {
  const sinceMs = sinceIso ? parseDate(sinceIso).ms : null;
  let url = feedUrl;
  let pages = 0;

  while (url && pages < maxPages) {
    pages += 1;
    status.pages = pages;
    status.current_feed_url = url;

    const xml = await fetchText(url);
    const parsedFeed = parseFeedDocument(xml, url);
    const feedUpdated = parseDate(parsedFeed.feedUpdated).iso;

    const entries = [];
    for (const entryXml of parsedFeed.entryBlocks) {
      const mapped = parsePlacspEntry(entryXml, {
        feedUrl: url,
        feedUpdated,
        tenantId
      });
      if (!mapped) continue;
      if (sinceMs && mapped.entryUpdatedMs && mapped.entryUpdatedMs < sinceMs) {
        continue;
      }
      entries.push(mapped);
    }

    yield {
      modeLabel,
      page: pages,
      feedUrl: url,
      feedUpdated,
      nextLink: parsedFeed.nextLink,
      entries
    };

    const oldestEntry = parsedFeed.entryBlocks
      .map((entryXml) => parseDate(extractFirst(entryXml, /<updated>([^<]+)<\/updated>/i)).ms)
      .filter((n) => Number.isFinite(n))
      .reduce((acc, n) => (acc === null ? n : Math.min(acc, n)), null);

    if (sinceMs && shouldStopOnOld && oldestEntry !== null && oldestEntry < sinceMs && entries.length === 0) {
      break;
    }

    url = parsedFeed.nextLink;
  }
}

async function* iterateZipSources({ zipItems, downloadDir, maxArchives, maxFilesPerArchive, tenantId, startArchiveIndex = 0 }) {
  let archiveIndex = 0;

  for (const item of zipItems) {
    archiveIndex += 1;
    if (archiveIndex < startArchiveIndex) continue;
    if (archiveIndex > maxArchives) break;

    const remoteUrl = normalizeUrl(item);
    const zipPath = remoteUrl
      ? await downloadToFile(remoteUrl, path.join(downloadDir, safeFileNameFromUrl(remoteUrl)))
      : item;

    if (!fs.existsSync(zipPath)) {
      throw new Error(`ZIP file not found: ${zipPath}`);
    }

    const atomEntries = listZipAtomEntries(zipPath);
    let atomIndex = 0;
    for (const atomEntryName of atomEntries) {
      atomIndex += 1;
      if (atomIndex > maxFilesPerArchive) break;

      const xml = readZipEntry(zipPath, atomEntryName);
      const parsedFeed = parseFeedDocument(xml, `${zipPath}:${atomEntryName}`);
      const feedUpdated = parseDate(parsedFeed.feedUpdated).iso;

      const parsedEntries = [];
      for (const entryXml of parsedFeed.entryBlocks) {
        const mapped = parsePlacspEntry(entryXml, {
          feedUrl: `${zipPath}:${atomEntryName}`,
          feedUpdated,
          tenantId
        });
        if (mapped) parsedEntries.push(mapped);
      }

      yield {
        archiveIndex,
        atomIndex,
        feedUrl: `${zipPath}:${atomEntryName}`,
        feedUpdated,
        nextLink: parsedFeed.nextLink,
        entries: parsedEntries
      };
    }
  }
}

async function runIngestion({
  mode,
  baseUrl,
  appId,
  tenantId,
  includeTenantHeader,
  batchSize,
  runId,
  statusFile,
  startRecord,
  sourceIterator,
  checkpointFile,
  checkpointPayload,
  controlFile
}) {
  const source = 'PLACSP_ES';
  const startedAt = new Date().toISOString();
  const rawMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };
  const canonicalMetrics = { inserted: 0, duplicates: 0, updated: 0, failed: 0, errors: [] };
  const currentMetrics = { inserted: 0, duplicates: 0, updated: 0, failed: 0, errors: [] };

  let parsedRecords = 0;
  let processed = 0;
  let skippedBeforeStart = 0;
  let dedupedInRun = 0;
  let pages = 0;
  let feeds = 0;
  let lastCursor = null;
  let maxEntryUpdatedIso = null;
  const rawBatch = [];
  const canonicalBatch = [];
  const currentBatch = [];
  const seenVersionKeys = new Set();
  let flushCount = 0;
  let lastHeartbeatAt = Date.now();
  let requestedAction = null;

  const onSignalStop = () => {
    requestedAction = 'stop';
  };

  process.on('SIGTERM', onSignalStop);
  process.on('SIGINT', onSignalStop);

  const buildMetrics = () => ({
    mode,
    parsed_records: parsedRecords,
    processed_rows: processed,
    skipped_before_start: skippedBeforeStart,
    deduped_in_run: dedupedInRun,
    pages,
    feeds,
    raw_rows: rawMetrics.inserted,
    canonical_inserted: canonicalMetrics.inserted,
    canonical_updated: canonicalMetrics.updated,
    current_inserted: currentMetrics.inserted,
    current_updated: currentMetrics.updated,
    raw_failed: rawMetrics.failed,
    canonical_failed: canonicalMetrics.failed,
    current_failed: currentMetrics.failed,
    max_entry_updated: maxEntryUpdatedIso
  });

  const buildRunErrors = () =>
    [...rawMetrics.errors, ...canonicalMetrics.errors, ...currentMetrics.errors].slice(0, 100);

  const writeCheckpointSnapshot = () => {
    if (!checkpointFile) return;
    writeJsonFile(checkpointFile, {
      ...(checkpointPayload || {}),
      run_id: runId,
      mode,
      max_entry_updated: maxEntryUpdatedIso,
      processed,
      parsed_records: parsedRecords,
      skipped_before_start: skippedBeforeStart,
      deduped_in_run: dedupedInRun,
      pages,
      feeds,
      cursor: lastCursor,
      updated_at: new Date().toISOString()
    });
  };

  const emitStatus = (phase = 'running') => {
    writeJsonFile(statusFile, {
      phase,
      mode,
      run_id: runId,
      parsed_records: parsedRecords,
      processed,
      skipped_before_start: skippedBeforeStart,
      deduped_in_run: dedupedInRun,
      pages,
      feeds,
      cursor: lastCursor,
      raw_inserted: rawMetrics.inserted,
      canonical_inserted: canonicalMetrics.inserted,
      canonical_updated: canonicalMetrics.updated,
      current_inserted: currentMetrics.inserted,
      current_updated: currentMetrics.updated,
      raw_failed: rawMetrics.failed,
      canonical_failed: canonicalMetrics.failed,
      current_failed: currentMetrics.failed,
      max_entry_updated: maxEntryUpdatedIso,
      updated_at: new Date().toISOString()
    });
  };

  const updateRunProgress = async ({ status = 'running', finishedAt = null } = {}) => {
    try {
      await putEntityById({
        baseUrl,
        appId,
        tenantId,
        table: 'ingestion_runs',
        id: runId,
        payload: {
          status,
          metrics: buildMetrics(),
          errors: buildRunErrors().slice(0, 50),
          cursor: lastCursor,
          finished_at: finishedAt
        },
        includeTenantHeader
      });
    } catch {
      // best effort
    }
  };

  try {
    await postRows({
      baseUrl,
      appId,
      tenantId,
      table: 'ingestion_runs',
      rows: [{
        run_id: runId,
        source,
        cursor: `${mode}:start`,
        status: 'running',
        metrics: {},
        errors: [],
        started_at: startedAt,
        finished_at: null,
        tenant_id: tenantId
      }],
      includeTenantHeader
    });
  } catch (error) {
    if (!isIngestionRunDuplicateError(error?.message)) {
      throw error;
    }
    await putEntityById({
      baseUrl,
      appId,
      tenantId,
      table: 'ingestion_runs',
      id: runId,
      payload: {
        status: 'running',
        cursor: `${mode}:start`,
        finished_at: null
      },
      includeTenantHeader
    });
  }

  emitStatus('started');
  writeCheckpointSnapshot();

  const flush = async () => {
    if (!rawBatch.length && !canonicalBatch.length && !currentBatch.length) return;

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

    if (canonicalBatch.length) {
      await insertBatchWithUpsertById({
        baseUrl,
        appId,
        tenantId,
        table: 'canonical_tenders',
        idField: 'canonical_id',
        rows: canonicalBatch.splice(0, canonicalBatch.length),
        metrics: canonicalMetrics,
        includeTenantHeader
      });
    }

    if (currentBatch.length) {
      await insertBatchWithUpsertById({
        baseUrl,
        appId,
        tenantId,
        table: 'TendersCurrent',
        idField: 'tender_id',
        rows: currentBatch.splice(0, currentBatch.length),
        metrics: currentMetrics,
        includeTenantHeader
      });
    }

    flushCount += 1;
    if (flushCount % 8 === 0) {
      await updateRunProgress({ status: 'running', finishedAt: null });
      writeCheckpointSnapshot();
    }
  };

  const finalizeResult = (status) => ({
    run_id: runId,
    mode,
    status,
    parsed_records: parsedRecords,
    processed,
    skipped_before_start: skippedBeforeStart,
    deduped_in_run: dedupedInRun,
    pages,
    feeds,
    max_entry_updated: maxEntryUpdatedIso,
    raw_documents: rawMetrics,
    canonical_tenders: canonicalMetrics,
    TendersCurrent: currentMetrics,
    errors: buildRunErrors()
  });

  const maybeHandleControl = async () => {
    const fileInstruction = readControlInstruction(controlFile);
    if (!requestedAction && fileInstruction?.action) {
      requestedAction = fileInstruction.action;
    }
    if (!requestedAction) return null;

    await flush();
    const terminalStatus = requestedAction === 'pause' ? 'paused' : 'stopped';
    const finishedAt = new Date().toISOString();
    const statusCursor = lastCursor || `${mode}:${terminalStatus}`;
    lastCursor = statusCursor;

    await updateRunProgress({ status: terminalStatus, finishedAt });
    emitStatus(terminalStatus);
    writeCheckpointSnapshot();
    if (fileInstruction) unlinkIfExists(controlFile);
    return finalizeResult(terminalStatus);
  };

  try {
    for await (const feedChunk of sourceIterator) {
      feeds += 1;
      pages = Math.max(pages, Number(feedChunk.page || 0));

      const requestedBeforeChunk = await maybeHandleControl();
      if (requestedBeforeChunk) return requestedBeforeChunk;

      for (const mapped of feedChunk.entries) {
        parsedRecords += 1;

        if (parsedRecords < startRecord) {
          skippedBeforeStart += 1;
          if (parsedRecords % 1000 === 0) {
            writeCheckpointSnapshot();
          }
          continue;
        }

        const versionKey = `${mapped.canonicalId}:${mapped.versionFingerprint}`;
        if (seenVersionKeys.has(versionKey)) {
          dedupedInRun += 1;
          continue;
        }
        seenVersionKeys.add(versionKey);

        const rawRow = {
          ...mapped.rawDocument,
          id: `${runId}:${String(parsedRecords).padStart(10, '0')}`,
          run_id: runId
        };

        rawBatch.push(rawRow);
        canonicalBatch.push(mapped.canonicalTender);
        currentBatch.push(mapped.currentTender);
        processed += 1;

        if (mapped.entryUpdated) {
          if (!maxEntryUpdatedIso) maxEntryUpdatedIso = mapped.entryUpdated;
          else {
            const currentMs = parseDate(maxEntryUpdatedIso).ms;
            const candidateMs = parseDate(mapped.entryUpdated).ms;
            if (candidateMs && currentMs && candidateMs > currentMs) {
              maxEntryUpdatedIso = mapped.entryUpdated;
            }
          }
        }

        lastCursor = JSON.stringify({
          mode,
          feed: feedChunk.feedUrl,
          page: feedChunk.page || null,
          parsed_records: parsedRecords,
          processed,
          max_entry_updated: maxEntryUpdatedIso
        });

        if (rawBatch.length >= batchSize || canonicalBatch.length >= batchSize || currentBatch.length >= batchSize) {
          await flush();
          emitStatus('running');
          const requestedAfterFlush = await maybeHandleControl();
          if (requestedAfterFlush) return requestedAfterFlush;
        }

        if (processed % 500 === 0) {
          console.log(`[${mode}] processed ${processed} rows (parsed ${parsedRecords})`);
          emitStatus('running');
          writeCheckpointSnapshot();
          lastHeartbeatAt = Date.now();
          const requestedAfterHeartbeat = await maybeHandleControl();
          if (requestedAfterHeartbeat) return requestedAfterHeartbeat;
        } else if (Date.now() - lastHeartbeatAt > 60000) {
          console.log(`[${mode}] heartbeat processed ${processed} rows (parsed ${parsedRecords})`);
          emitStatus('running');
          writeCheckpointSnapshot();
          lastHeartbeatAt = Date.now();
          const requestedAfterHeartbeat = await maybeHandleControl();
          if (requestedAfterHeartbeat) return requestedAfterHeartbeat;
        }
      }
    }

    await flush();
    emitStatus('finalizing');

    const runErrors = buildRunErrors();
    const completedStatus = runErrors.length ? 'completed_with_errors' : 'completed';
    await putEntityById({
      baseUrl,
      appId,
      tenantId,
      table: 'ingestion_runs',
      id: runId,
      payload: {
        status: completedStatus,
        metrics: buildMetrics(),
        errors: runErrors,
        cursor: lastCursor || `${mode}:completed`,
        finished_at: new Date().toISOString()
      },
      includeTenantHeader
    });

    writeCheckpointSnapshot();
    emitStatus('completed');
    return finalizeResult(completedStatus);
  } finally {
    process.off('SIGTERM', onSignalStop);
    process.off('SIGINT', onSignalStop);
  }
}

async function runCheckCurrent({
  baseUrl,
  appId,
  tenantId,
  includeTenantHeader,
  feedUrl,
  maxPages,
  sinceIso,
  repair,
  batchSize,
  runId,
  statusFile
}) {
  const status = { pages: 0, current_feed_url: feedUrl };
  const feedIterator = iterateFeedChain({
    feedUrl,
    sinceIso,
    maxPages,
    tenantId,
    modeLabel: 'check-current',
    status,
    shouldStopOnOld: true
  });

  const expectedById = new Map();
  let parsed = 0;

  for await (const chunk of feedIterator) {
    for (const mapped of chunk.entries) {
      parsed += 1;
      expectedById.set(mapped.canonicalId, mapped);
    }
    writeJsonFile(statusFile, {
      phase: 'checking',
      run_id: runId,
      parsed,
      expected_unique: expectedById.size,
      pages: status.pages,
      feed_url: status.current_feed_url,
      since_iso: sinceIso,
      updated_at: new Date().toISOString()
    });
  }

  const canonicalSet = await loadSourceIdSet({
    baseUrl,
    appId,
    tenantId,
    table: 'canonical_tenders',
    fieldName: 'canonical_id',
    includeTenantHeader,
    source: 'PLACSP_ES'
  });

  const currentSet = await loadSourceIdSet({
    baseUrl,
    appId,
    tenantId,
    table: 'TendersCurrent',
    fieldName: 'tender_id',
    includeTenantHeader,
    source: 'PLACSP_ES'
  });

  const missingCanonical = [];
  const missingCurrent = [];
  for (const [id, mapped] of expectedById.entries()) {
    if (!canonicalSet.has(id)) missingCanonical.push(mapped.canonicalTender);
    if (!currentSet.has(id)) missingCurrent.push(mapped.currentTender);
  }

  const canonicalMetrics = { inserted: 0, duplicates: 0, updated: 0, failed: 0, errors: [] };
  const currentMetrics = { inserted: 0, duplicates: 0, updated: 0, failed: 0, errors: [] };

  if (repair) {
    for (let i = 0; i < missingCanonical.length; i += batchSize) {
      await insertBatchWithUpsertById({
        baseUrl,
        appId,
        tenantId,
        table: 'canonical_tenders',
        idField: 'canonical_id',
        rows: missingCanonical.slice(i, i + batchSize),
        metrics: canonicalMetrics,
        includeTenantHeader
      });
    }

    for (let i = 0; i < missingCurrent.length; i += batchSize) {
      await insertBatchWithUpsertById({
        baseUrl,
        appId,
        tenantId,
        table: 'TendersCurrent',
        idField: 'tender_id',
        rows: missingCurrent.slice(i, i + batchSize),
        metrics: currentMetrics,
        includeTenantHeader
      });
    }
  }

  const result = {
    run_id: runId,
    mode: 'check-current',
    since_iso: sinceIso,
    parsed,
    expected_unique: expectedById.size,
    live_canonical: canonicalSet.size,
    live_current: currentSet.size,
    missing_canonical: missingCanonical.length,
    missing_current: missingCurrent.length,
    repaired: repair,
    canonical_repair_metrics: canonicalMetrics,
    current_repair_metrics: currentMetrics
  };

  writeJsonFile(statusFile, {
    phase: 'completed',
    ...result,
    updated_at: new Date().toISOString()
  });

  return result;
}

async function main() {
  const args = parseArgs(process.argv);
  if (parseBoolean(args.help, false) || parseBoolean(args.h, false)) {
    printHelp();
    return;
  }

  const mode = clean(args.mode) || 'incremental';
  const baseUrl = String(args['api-base'] || 'https://civantapp.vercel.app').replace(/\/$/, '');
  const appId = String(args['app-id'] || 'civantapp');
  const tenantId = String(args['tenant-id'] || 'civant_default');
  const includeTenantHeader = args['with-tenant-header'] !== 'false';
  const batchSize = Math.max(20, parsePositiveInt(args['batch-size'], 120));
  const runId = String(args['run-id'] || makeRunId(mode === 'check-current' ? 'placsp_es_check' : 'placsp_es'));
  const statusFile = String(args['status-file'] || DEFAULT_STATUS_FILE);
  const checkpointFile = String(args['checkpoint-file'] || DEFAULT_CHECKPOINT_FILE);
  const controlFile = String(args['control-file'] || DEFAULT_CONTROL_FILE);
  const pidFile = String(args['pid-file'] || DEFAULT_PID_FILE);
  const insecureTls = parseBoolean(args['insecure-tls'], false);

  writeJsonFile(pidFile, {
    pid: process.pid,
    run_id: runId,
    mode,
    started_at: new Date().toISOString(),
    status_file: statusFile,
    checkpoint_file: checkpointFile,
    control_file: controlFile
  });

  if (insecureTls) {
    process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
  }

  try {
    if (mode === 'incremental') {
      const feedUrl = String(args['feed-url'] || DEFAULT_FEED_URL);
      const checkpoint = readJsonFile(checkpointFile) || {};
      const lookbackMinutes = parsePositiveInt(args['lookback-minutes'], 90);
      const maxPages = parsePositiveInt(args['max-pages'], 120);
      const explicitSince = clean(args['since-iso']);
      const sinceIso = explicitSince || applyLookbackIso(checkpoint.max_entry_updated, lookbackMinutes);

      const status = { pages: 0, current_feed_url: feedUrl };
      const sourceIterator = iterateFeedChain({
        feedUrl,
        sinceIso,
        maxPages,
        tenantId,
        modeLabel: 'incremental',
        status,
        shouldStopOnOld: true
      });

      const result = await runIngestion({
        mode: 'incremental',
        baseUrl,
        appId,
        tenantId,
        includeTenantHeader,
        batchSize,
        runId,
        statusFile,
        startRecord: Math.max(1, parsePositiveInt(args['start-record'], 1)),
        sourceIterator,
        checkpointFile,
        checkpointPayload: {
          source: 'PLACSP_ES',
          feed_url: feedUrl,
          since_iso: sinceIso,
          lookback_minutes: lookbackMinutes,
          max_pages: maxPages
        },
        controlFile
      });

      console.log(JSON.stringify(result, null, 2));
      return;
    }

    if (mode === 'backfill') {
      const fromYear = parsePositiveInt(args['historical-from-year'], 2012);
      const toYear = parsePositiveInt(args['historical-to-year'], new Date().getUTCFullYear());
      const includeMonthlyCurrentYear = parseBoolean(args['include-monthly-current-year'], true);
      const downloadDir = String(args['download-dir'] || DEFAULT_DOWNLOAD_DIR);
      const maxArchives = parsePositiveInt(args['max-archives'], Number.MAX_SAFE_INTEGER);
      const maxFilesPerArchive = parsePositiveInt(args['max-files-per-archive'], Number.MAX_SAFE_INTEGER);
      const startRecord = Math.max(1, parsePositiveInt(args['start-record'], 1));

      const explicitZips = parseZipList(args.zip);
      const zipItems = explicitZips.length
        ? explicitZips
        : buildHistoricalZipUrls({ fromYear, toYear, includeMonthlyCurrentYear });

      if (!zipItems.length) {
        throw new Error('No ZIP sources resolved for backfill mode');
      }

      const sourceIterator = iterateZipSources({
        zipItems,
        downloadDir,
        maxArchives,
        maxFilesPerArchive,
        tenantId
      });

      const result = await runIngestion({
        mode: 'backfill',
        baseUrl,
        appId,
        tenantId,
        includeTenantHeader,
        batchSize,
        runId,
        statusFile,
        startRecord,
        sourceIterator,
        checkpointFile,
        checkpointPayload: {
          source: 'PLACSP_ES',
          historical_from_year: fromYear,
          historical_to_year: toYear,
          include_monthly_current_year: includeMonthlyCurrentYear
        },
        controlFile
      });

      console.log(JSON.stringify({
        ...result,
        zip_sources_count: zipItems.length,
        zip_sources: zipItems.slice(0, 20)
      }, null, 2));
      return;
    }

    if (mode === 'check-current') {
      const feedUrl = String(args['feed-url'] || DEFAULT_FEED_URL);
      const lookbackDays = parsePositiveInt(args['lookback-days'], 14);
      const sinceIso = clean(args['since-iso']) || new Date(Date.now() - lookbackDays * 24 * 60 * 60 * 1000).toISOString();
      const maxPages = parsePositiveInt(args['max-pages'], 120);
      const repair = parseBoolean(args.repair, false);

      const result = await runCheckCurrent({
        baseUrl,
        appId,
        tenantId,
        includeTenantHeader,
        feedUrl,
        maxPages,
        sinceIso,
        repair,
        batchSize,
        runId,
        statusFile
      });

      console.log(JSON.stringify(result, null, 2));
      return;
    }

    throw new Error(`Unsupported mode: ${mode}`);
  } finally {
    unlinkIfExists(pidFile);
  }
}

main().catch((error) => {
  const args = parseArgs(process.argv);
  const statusFile = String(args['status-file'] || DEFAULT_STATUS_FILE);
  const pidFile = String(args['pid-file'] || DEFAULT_PID_FILE);
  writeJsonFile(statusFile, {
    phase: 'failed',
    error: error instanceof Error ? error.message : String(error),
    updated_at: new Date().toISOString()
  });
  unlinkIfExists(pidFile);
  console.error(error);
  process.exit(1);
});
