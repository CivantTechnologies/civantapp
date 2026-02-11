#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFile } from 'node:child_process';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import { promisify } from 'node:util';
import readline from 'node:readline';

const DEFAULT_FEED_URL = 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom';
const DEFAULT_ZIP_PREFIX = 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_';
const DEFAULT_DOWNLOAD_DIR = '/tmp/placsp-es-zips';
const DEFAULT_LOCAL_OUTPUT_DIR = '/tmp/placsp-es-local';
const DEFAULT_STATUS_FILE = '/tmp/placsp-es-import-status.json';
const DEFAULT_CHECKPOINT_FILE = '/tmp/placsp-es-checkpoint.json';
const DEFAULT_CONTROL_FILE = '/tmp/placsp-es-control.json';
const DEFAULT_PID_FILE = '/tmp/placsp-es-import.pid';
const REQUEST_TIMEOUT_MS = 45000;
const MAX_HTTP_RETRIES = 5;
const SOURCE_WAIT_HEARTBEAT_MS = 15000;
const ZIP_EXEC_TIMEOUT_MS = 120000;
const DEBUG_INGEST = String(process.env.PLACSP_DEBUG || '').toLowerCase() === 'true';

const execFileAsync = promisify(execFile);

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
  --mode backfill-local Process historical ZIP archives into local NDJSON files
  --mode incremental    Pull current Atom feed chain and ingest new entries only
  --mode check-current  Validate recent feed entries against canonical/current and optionally repair
  --mode upload-local   Upload previously staged local NDJSON files into API tables

Core options:
  --api-base <url>                Default: https://civantapp.vercel.app
  --app-id <id>                   Default: civantapp
  --tenant-id <id>                Default: civant_default
  --with-tenant-header <bool>     Default: true
  --run-id <id>                   Optional custom run id
  --batch-size <n>                Default: 120
  --sink <api|local>              Default: api
  --local-output-dir <path>       Default: ${DEFAULT_LOCAL_OUTPUT_DIR}
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

Upload-local options:
  --upload-tables <csv>            Default: raw_documents,canonical_tenders,TendersCurrent
                                  Accepted: raw_documents,canonical_tenders,TendersCurrent
  --upload-start-line <n>          Optional line offset for first upload run

Examples:
  node scripts/import-placsp-es.mjs --mode backfill --historical-from-year 2012
  node scripts/import-placsp-es.mjs --mode backfill-local --zip /path/to/2023.zip
  node scripts/import-placsp-es.mjs --mode incremental
  node scripts/import-placsp-es.mjs --mode check-current --repair true --lookback-days 30
  node scripts/import-placsp-es.mjs --mode upload-local --local-output-dir /tmp/placsp-es-local

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

function ensureDir(dirPath) {
  if (!dirPath) return;
  fs.mkdirSync(dirPath, { recursive: true });
}

function appendNdjsonRows(filePath, rows) {
  if (!rows?.length) return;
  const payload = rows.map((row) => JSON.stringify(row)).join('\n') + '\n';
  fs.appendFileSync(filePath, payload, 'utf8');
}

function getLocalSinkPaths(baseDir) {
  ensureDir(baseDir);
  return {
    baseDir,
    rawDocuments: path.join(baseDir, 'raw_documents.ndjson'),
    canonicalTenders: path.join(baseDir, 'canonical_tenders.ndjson'),
    currentTenders: path.join(baseDir, 'TendersCurrent.ndjson'),
    manifest: path.join(baseDir, 'manifest.json')
  };
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function debugLog(event, payload = null) {
  if (!DEBUG_INGEST) return;
  const line = payload ? `${event} ${JSON.stringify(payload)}` : event;
  console.log(`[placsp-debug] ${new Date().toISOString()} ${line}`);
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
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 300)}`);
      }
      const text = await response.text();
      return text;
    } catch (error) {
      lastError = error;
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
      }
    } finally {
      clearTimeout(timer);
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
      if (!response.ok || !response.body) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 300)}`);
      }
      const file = fs.createWriteStream(destination);
      await pipeline(Readable.fromWeb(response.body), file);
      return destination;
    } catch (error) {
      lastError = error;
      if (fs.existsSync(destination)) {
        try { fs.unlinkSync(destination); } catch {
          // noop
        }
      }
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
      }
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error(`Download failed: ${url}`);
}

async function listZipAtomEntries(zipPath) {
  const { stdout } = await execFileAsync('unzip', ['-Z1', zipPath], {
    encoding: 'utf8',
    maxBuffer: 1024 * 1024 * 64,
    timeout: ZIP_EXEC_TIMEOUT_MS,
    killSignal: 'SIGKILL'
  });
  return stdout
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.toLowerCase().endsWith('.atom'))
    .sort((a, b) => a.localeCompare(b));
}

async function readZipEntry(zipPath, entryName) {
  const { stdout } = await execFileAsync('unzip', ['-p', zipPath, entryName], {
    encoding: 'utf8',
    maxBuffer: 1024 * 1024 * 256,
    timeout: ZIP_EXEC_TIMEOUT_MS,
    killSignal: 'SIGKILL'
  });
  return stdout;
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
  const headers = { 'content-type': 'application/json', prefer: 'return=minimal' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;
  const requestBody = JSON.stringify(rows);

  let lastError = null;
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    const startedAt = Date.now();
    try {
      debugLog('postRows-attempt-start', { table, attempt, rows: rows.length, body_bytes: Buffer.byteLength(requestBody) });
      const response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}`, {
        method: 'POST',
        headers,
        body: requestBody,
        signal: controller.signal
      });
      debugLog('postRows-attempt-response', { table, attempt, status: response.status, ms: Date.now() - startedAt });
      if (response.ok) {
        if (response.body) {
          try { await response.body.cancel(); } catch {
            // best effort
          }
        }
        return { ok: true };
      }

      const text = await response.text();
      const payload = text
        ? (() => {
            try { return JSON.parse(text); } catch { return { raw: text }; }
          })()
        : null;
      debugLog('postRows-attempt-failed-status', {
        table,
        attempt,
        status: response.status,
        ms: Date.now() - startedAt,
        message: payload?.error || payload?.message || `HTTP ${response.status}`
      });
      throw new Error(payload?.error || payload?.message || `HTTP ${response.status}`);
    } catch (error) {
      const message = String(error?.message || error);
      debugLog('postRows-attempt-error', { table, attempt, ms: Date.now() - startedAt, message });
      lastError = error;
      if (isDuplicateError(message)) {
        break;
      }
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
      }
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError || new Error(`Failed POST ${table}`);
}

async function putEntityById({ baseUrl, appId, tenantId, table, id, payload, includeTenantHeader }) {
  const headers = { 'content-type': 'application/json', prefer: 'return=minimal' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;
  const requestBody = JSON.stringify(payload);

  let lastError = null;
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    const startedAt = Date.now();
    try {
      debugLog('putEntity-attempt-start', { table, id, attempt, body_bytes: Buffer.byteLength(requestBody) });
      const response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}/${encodeURIComponent(id)}`, {
        method: 'PUT',
        headers,
        body: requestBody,
        signal: controller.signal
      });
      debugLog('putEntity-attempt-response', { table, id, attempt, status: response.status, ms: Date.now() - startedAt });
      if (response.ok) {
        if (response.body) {
          try { await response.body.cancel(); } catch {
            // best effort
          }
        }
        return { ok: true };
      }

      const text = await response.text();
      const parsed = text
        ? (() => {
            try { return JSON.parse(text); } catch { return { raw: text }; }
          })()
        : null;
      debugLog('putEntity-attempt-failed-status', {
        table,
        id,
        attempt,
        status: response.status,
        ms: Date.now() - startedAt,
        message: parsed?.error || parsed?.message || `HTTP ${response.status}`
      });
      throw new Error(parsed?.error || parsed?.message || `HTTP ${response.status}`);
    } catch (error) {
      const message = String(error?.message || error);
      debugLog('putEntity-attempt-error', { table, id, attempt, ms: Date.now() - startedAt, message });
      lastError = error;
      if (isDuplicateError(message)) {
        break;
      }
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, 500 * attempt));
      }
    } finally {
      clearTimeout(timer);
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

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(url, { method: 'GET', headers, signal: controller.signal });
    const text = await response.text();
    const parsed = text
      ? (() => {
          try { return JSON.parse(text); } catch { return { raw: text }; }
        })()
      : null;
    if (!response.ok) throw new Error(parsed?.error || parsed?.message || `HTTP ${response.status}`);
    if (!Array.isArray(parsed)) return [];
    return parsed;
  } finally {
    clearTimeout(timer);
  }
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

function makeFeedKey(feedUrl) {
  const text = clean(feedUrl);
  if (!text) return null;
  const marker = '.zip:';
  const splitAt = text.toLowerCase().lastIndexOf(marker);
  if (splitAt === -1) return path.basename(text);
  const zipPart = text.slice(0, splitAt + 4);
  const atomPart = text.slice(splitAt + 5);
  return `${path.basename(zipPart)}:${atomPart}`;
}

function zipItemBaseName(item) {
  const remote = normalizeUrl(item);
  if (remote) return safeFileNameFromUrl(remote);
  return path.basename(String(item));
}

function parseBackfillCursor(cursorValue) {
  if (!cursorValue) return null;
  let parsed = cursorValue;
  if (typeof cursorValue === 'string') {
    try {
      parsed = JSON.parse(cursorValue);
    } catch {
      return null;
    }
  }
  if (!parsed || (parsed.mode !== 'backfill' && parsed.mode !== 'backfill-local')) return null;
  const feed = clean(parsed.feed);
  if (!feed) return null;
  const feedKey = makeFeedKey(feed);
  if (!feedKey) return null;
  const archiveIndex = parsePositiveInt(parsed.archive_index, 0);
  const atomIndex = parsePositiveInt(parsed.atom_index, 0);
  const entryIndex = parsePositiveInt(parsed.entry_index_in_feed, 0);
  return {
    feed,
    feedKey,
    archiveIndex: archiveIndex > 0 ? archiveIndex : null,
    atomIndex: atomIndex > 0 ? atomIndex : null,
    entryIndex: entryIndex > 0 ? entryIndex : null
  };
}

function resolveBackfillResumePlan({ zipItems, startRecordArg, checkpoint }) {
  const plan = {
    effectiveStartRecord: Math.max(1, startRecordArg),
    startArchiveIndex: 1,
    startFeedKey: null,
    startEntryIndex: 1,
    usedCursorResume: false
  };

  if (plan.effectiveStartRecord <= 1) return plan;

  const cursor = parseBackfillCursor(checkpoint?.cursor);
  if (!cursor) return plan;

  let archiveIndex = cursor.archiveIndex;
  if (!archiveIndex) {
    const [archiveBase] = cursor.feedKey.split(':');
    const found = zipItems.findIndex((item) => zipItemBaseName(item) === archiveBase);
    archiveIndex = found >= 0 ? found + 1 : null;
  }

  if (!archiveIndex) return plan;

  plan.effectiveStartRecord = 1;
  plan.startArchiveIndex = archiveIndex;
  plan.startFeedKey = cursor.feedKey;
  plan.startEntryIndex = (cursor.entryIndex || 0) + 1;
  plan.usedCursorResume = true;
  return plan;
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

async function* iterateZipSources({
  zipItems,
  downloadDir,
  maxArchives,
  maxFilesPerArchive,
  tenantId,
  startArchiveIndex = 1,
  startFeedKey = null,
  startEntryIndex = 1
}) {
  let archiveIndex = 0;
  let resumeFeedPending = Boolean(startFeedKey);
  let resumeEntryOffset = Math.max(1, Number(startEntryIndex || 1));

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

    const atomEntries = await listZipAtomEntries(zipPath);
    let atomIndex = 0;
    for (const atomEntryName of atomEntries) {
      atomIndex += 1;
      if (atomIndex > maxFilesPerArchive) break;

      const feedKey = `${path.basename(zipPath)}:${atomEntryName}`;
      if (resumeFeedPending && feedKey !== startFeedKey) continue;

      const xml = await readZipEntry(zipPath, atomEntryName);
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

      let entries = parsedEntries;
      if (resumeFeedPending && feedKey === startFeedKey) {
        entries = parsedEntries.slice(Math.max(0, resumeEntryOffset - 1));
        resumeFeedPending = false;
        resumeEntryOffset = 1;
      }

      yield {
        archiveIndex,
        atomIndex,
        feedKey,
        feedUrl: `${zipPath}:${atomEntryName}`,
        feedUpdated,
        nextLink: parsedFeed.nextLink,
        entries
      };
    }
  }
}

async function runIngestion({
  mode,
  sink = 'api',
  baseUrl,
  appId,
  tenantId,
  includeTenantHeader,
  batchSize,
  runId,
  statusFile,
  localOutputDir = DEFAULT_LOCAL_OUTPUT_DIR,
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
  let activity = 'initializing';
  let lastProgressAt = new Date().toISOString();
  const rawBatch = [];
  const canonicalBatch = [];
  const currentBatch = [];
  const seenVersionKeys = new Set();
  let flushCount = 0;
  let lastHeartbeatAt = Date.now();
  let requestedAction = null;
  let heartbeatTimer = null;
  const localSinkPaths = sink === 'local' ? getLocalSinkPaths(localOutputDir) : null;

  const onSignalStop = () => {
    requestedAction = 'stop';
  };

  process.on('SIGTERM', onSignalStop);
  process.on('SIGINT', onSignalStop);

  const markProgress = () => {
    lastProgressAt = new Date().toISOString();
  };

  const buildMetrics = () => ({
    sink,
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
      activity,
      progress_at: lastProgressAt,
      updated_at: new Date().toISOString()
    });
  };

  const emitStatus = (phase = 'running') => {
    writeJsonFile(statusFile, {
      phase,
      sink,
      mode,
      run_id: runId,
      parsed_records: parsedRecords,
      processed,
      skipped_before_start: skippedBeforeStart,
      deduped_in_run: dedupedInRun,
      pages,
      feeds,
      cursor: lastCursor,
      activity,
      progress_at: lastProgressAt,
      raw_inserted: rawMetrics.inserted,
      canonical_inserted: canonicalMetrics.inserted,
      canonical_updated: canonicalMetrics.updated,
      current_inserted: currentMetrics.inserted,
      current_updated: currentMetrics.updated,
      raw_failed: rawMetrics.failed,
      canonical_failed: canonicalMetrics.failed,
      current_failed: currentMetrics.failed,
      max_entry_updated: maxEntryUpdatedIso,
      local_output_dir: localSinkPaths?.baseDir || null,
      updated_at: new Date().toISOString()
    });
  };

  const updateRunProgress = async ({ status = 'running', finishedAt = null } = {}) => {
    if (sink !== 'api') return;
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

  if (sink === 'api') {
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
  }

  emitStatus('started');
  writeCheckpointSnapshot();

  const flush = async () => {
    if (!rawBatch.length && !canonicalBatch.length && !currentBatch.length) return;

    if (sink === 'local') {
      const rawRows = rawBatch.splice(0, rawBatch.length);
      const canonicalRows = canonicalBatch.splice(0, canonicalBatch.length);
      const currentRows = currentBatch.splice(0, currentBatch.length);

      if (rawRows.length) {
        activity = `flush-local:raw_documents:${rawRows.length}`;
        emitStatus('running');
        writeCheckpointSnapshot();
        appendNdjsonRows(localSinkPaths.rawDocuments, rawRows);
        rawMetrics.inserted += rawRows.length;
        markProgress();
      }

      if (canonicalRows.length) {
        activity = `flush-local:canonical_tenders:${canonicalRows.length}`;
        emitStatus('running');
        writeCheckpointSnapshot();
        appendNdjsonRows(localSinkPaths.canonicalTenders, canonicalRows);
        canonicalMetrics.inserted += canonicalRows.length;
        markProgress();
      }

      if (currentRows.length) {
        activity = `flush-local:TendersCurrent:${currentRows.length}`;
        emitStatus('running');
        writeCheckpointSnapshot();
        appendNdjsonRows(localSinkPaths.currentTenders, currentRows);
        currentMetrics.inserted += currentRows.length;
        markProgress();
      }
    } else {
      if (rawBatch.length) {
        activity = `flush:raw_documents:${rawBatch.length}`;
        emitStatus('running');
        writeCheckpointSnapshot();
        await insertBatchWithFallback({
          baseUrl,
          appId,
          tenantId,
          table: 'raw_documents',
          rows: rawBatch.splice(0, rawBatch.length),
          metrics: rawMetrics,
          includeTenantHeader
        });
        markProgress();
      }

      if (canonicalBatch.length) {
        activity = `flush:canonical_tenders:${canonicalBatch.length}`;
        emitStatus('running');
        writeCheckpointSnapshot();
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
        markProgress();
      }

      if (currentBatch.length) {
        activity = `flush:TendersCurrent:${currentBatch.length}`;
        emitStatus('running');
        writeCheckpointSnapshot();
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
        markProgress();
      }
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
    sink,
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
    local_output_dir: localSinkPaths?.baseDir || null,
    errors: buildRunErrors()
  });

  const maybeHandleControl = async () => {
    const fileInstruction = readControlInstruction(controlFile);
    if (!requestedAction && fileInstruction?.action) {
      requestedAction = fileInstruction.action;
    }
    if (!requestedAction) return null;

    await flush();
    activity = requestedAction === 'pause' ? 'paused' : 'stopped';
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
      const iterator = sourceIterator[Symbol.asyncIterator]();
      heartbeatTimer = setInterval(() => {
        if (requestedAction) return;
        emitStatus('running');
        writeCheckpointSnapshot();
      }, SOURCE_WAIT_HEARTBEAT_MS);
      if (typeof heartbeatTimer.unref === 'function') heartbeatTimer.unref();

      while (true) {
        activity = 'waiting:source-next';
        const nextPromise = iterator.next();
        let nextChunkResult = null;

        while (!nextChunkResult) {
          const raceResult = await Promise.race([
            nextPromise.then((value) => ({ type: 'next', value })).catch((error) => ({ type: 'error', error })),
            sleep(SOURCE_WAIT_HEARTBEAT_MS).then(() => ({ type: 'tick' }))
          ]);

          if (raceResult.type === 'tick') {
            activity = 'waiting:source-next';
            const requestedDuringWait = await maybeHandleControl();
            if (requestedDuringWait) return requestedDuringWait;
            continue;
        }

        if (raceResult.type === 'error') {
          throw raceResult.error;
        }

        nextChunkResult = raceResult.value;
      }

      if (nextChunkResult.done) break;

        const feedChunk = nextChunkResult.value;
        activity = `processing:${feedChunk.feedKey || makeFeedKey(feedChunk.feedUrl) || 'feed'}`;
        feeds += 1;
        pages = Math.max(pages, Number(feedChunk.page || 0));
        markProgress();

        const requestedBeforeChunk = await maybeHandleControl();
        if (requestedBeforeChunk) return requestedBeforeChunk;

        for (let entryIndex = 0; entryIndex < feedChunk.entries.length; entryIndex += 1) {
          const mapped = feedChunk.entries[entryIndex];
          parsedRecords += 1;
          if (parsedRecords % 50 === 0) markProgress();

          if (parsedRecords < startRecord) {
            skippedBeforeStart += 1;
          if (Date.now() - lastHeartbeatAt > 30000) {
            emitStatus('running');
            writeCheckpointSnapshot();
            lastHeartbeatAt = Date.now();
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
        markProgress();

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
          feed_key: feedChunk.feedKey || makeFeedKey(feedChunk.feedUrl),
          archive_index: feedChunk.archiveIndex || null,
          atom_index: feedChunk.atomIndex || null,
          entry_index_in_feed: entryIndex + 1,
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
    activity = 'finalizing';
    emitStatus('finalizing');

    const runErrors = buildRunErrors();
    const completedStatus = runErrors.length ? 'completed_with_errors' : 'completed';
    if (sink === 'api') {
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
    } else {
      writeJsonFile(localSinkPaths.manifest, {
        run_id: runId,
        mode,
        sink,
        tenant_id: tenantId,
        source: 'PLACSP_ES',
        started_at: startedAt,
        finished_at: new Date().toISOString(),
        metrics: buildMetrics(),
        errors: runErrors,
        files: {
          raw_documents: localSinkPaths.rawDocuments,
          canonical_tenders: localSinkPaths.canonicalTenders,
          TendersCurrent: localSinkPaths.currentTenders
        },
        checkpoint_file: checkpointFile
      });
    }

    writeCheckpointSnapshot();
    activity = 'completed';
    emitStatus('completed');
    return finalizeResult(completedStatus);
  } finally {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
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

function parseUploadTables(value) {
  const allowed = new Set(['raw_documents', 'canonical_tenders', 'TendersCurrent']);
  const requested = (clean(value) || 'raw_documents,canonical_tenders,TendersCurrent')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
  const unique = [];
  for (const item of requested) {
    if (!allowed.has(item)) {
      throw new Error(`Unsupported upload table: ${item}`);
    }
    if (!unique.includes(item)) unique.push(item);
  }
  return unique;
}

async function runUploadLocal({
  baseUrl,
  appId,
  tenantId,
  includeTenantHeader,
  batchSize,
  runId,
  statusFile,
  checkpointFile,
  localOutputDir,
  uploadTables,
  uploadStartLine
}) {
  const paths = getLocalSinkPaths(localOutputDir);
  const tableToFile = {
    raw_documents: paths.rawDocuments,
    canonical_tenders: paths.canonicalTenders,
    TendersCurrent: paths.currentTenders
  };

  const checkpoint = readJsonFile(checkpointFile) || {};
  const tablePositions = { ...(checkpoint.upload_positions || {}) };
  const metrics = {
    raw_documents: { inserted: 0, duplicates: 0, updated: 0, failed: 0, errors: [] },
    canonical_tenders: { inserted: 0, duplicates: 0, updated: 0, failed: 0, errors: [] },
    TendersCurrent: { inserted: 0, duplicates: 0, updated: 0, failed: 0, errors: [] }
  };

  const startedAt = new Date().toISOString();
  let requestedAction = null;
  let activity = 'initializing upload-local';

  const emitStatus = (phase = 'running', extra = {}) => {
    writeJsonFile(statusFile, {
      phase,
      mode: 'upload-local',
      sink: 'api',
      run_id: runId,
      tenant_id: tenantId,
      local_output_dir: localOutputDir,
      activity,
      upload_positions: tablePositions,
      metrics,
      ...extra,
      updated_at: new Date().toISOString()
    });
  };

  const persistCheckpoint = () => {
    writeJsonFile(checkpointFile, {
      ...(checkpoint || {}),
      mode: 'upload-local',
      run_id: runId,
      tenant_id: tenantId,
      upload_positions: tablePositions,
      updated_at: new Date().toISOString()
    });
  };

  const onSignalStop = () => {
    requestedAction = 'stop';
  };
  process.on('SIGTERM', onSignalStop);
  process.on('SIGINT', onSignalStop);

  try {
    for (const table of uploadTables) {
      const filePath = tableToFile[table];
      if (!fs.existsSync(filePath)) {
        emitStatus('failed', { error: `Missing local file for ${table}: ${filePath}` });
        throw new Error(`Missing local file for ${table}: ${filePath}`);
      }

      const startLine = Math.max(
        1,
        Number(tablePositions[table] || 0) > 0
          ? Number(tablePositions[table]) + 1
          : Number(uploadStartLine || 1)
      );

      activity = `uploading ${table} from line ${startLine}`;
      emitStatus('running');
      persistCheckpoint();

      const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
      const batch = [];
      let lineNo = 0;

      for await (const line of rl) {
        lineNo += 1;
        if (lineNo < startLine) continue;
        if (requestedAction === 'stop') break;
        if (!line.trim()) {
          tablePositions[table] = lineNo;
          continue;
        }

        let row;
        try {
          row = JSON.parse(line);
        } catch (error) {
          metrics[table].failed += 1;
          metrics[table].errors.push(`line ${lineNo}: invalid JSON (${error.message})`);
          tablePositions[table] = lineNo;
          if (metrics[table].errors.length > 100) metrics[table].errors = metrics[table].errors.slice(-100);
          continue;
        }

        batch.push(row);
        if (batch.length >= batchSize) {
          if (table === 'raw_documents') {
            await insertBatchWithFallback({
              baseUrl,
              appId,
              tenantId,
              table,
              rows: batch.splice(0, batch.length),
              metrics: metrics[table],
              includeTenantHeader
            });
          } else if (table === 'canonical_tenders') {
            await insertBatchWithUpsertById({
              baseUrl,
              appId,
              tenantId,
              table,
              idField: 'canonical_id',
              rows: batch.splice(0, batch.length),
              metrics: metrics[table],
              includeTenantHeader
            });
          } else {
            await insertBatchWithUpsertById({
              baseUrl,
              appId,
              tenantId,
              table,
              idField: 'tender_id',
              rows: batch.splice(0, batch.length),
              metrics: metrics[table],
              includeTenantHeader
            });
          }
          tablePositions[table] = lineNo;
          emitStatus('running', { table, line: lineNo });
          persistCheckpoint();
        }
      }

      if (batch.length) {
        if (table === 'raw_documents') {
          await insertBatchWithFallback({
            baseUrl,
            appId,
            tenantId,
            table,
            rows: batch.splice(0, batch.length),
            metrics: metrics[table],
            includeTenantHeader
          });
        } else if (table === 'canonical_tenders') {
          await insertBatchWithUpsertById({
            baseUrl,
            appId,
            tenantId,
            table,
            idField: 'canonical_id',
            rows: batch.splice(0, batch.length),
            metrics: metrics[table],
            includeTenantHeader
          });
        } else {
          await insertBatchWithUpsertById({
            baseUrl,
            appId,
            tenantId,
            table,
            idField: 'tender_id',
            rows: batch.splice(0, batch.length),
            metrics: metrics[table],
            includeTenantHeader
          });
        }
      }

      tablePositions[table] = lineNo;
      emitStatus('running', { table, line: lineNo });
      persistCheckpoint();

      if (requestedAction === 'stop') {
        activity = `stopped during ${table}`;
        emitStatus('stopped');
        return {
          run_id: runId,
          mode: 'upload-local',
          status: 'stopped',
          metrics,
          upload_positions: tablePositions,
          started_at: startedAt,
          finished_at: new Date().toISOString()
        };
      }
    }

    activity = 'completed upload-local';
    emitStatus('completed');
    persistCheckpoint();
    return {
      run_id: runId,
      mode: 'upload-local',
      status: 'completed',
      metrics,
      upload_positions: tablePositions,
      started_at: startedAt,
      finished_at: new Date().toISOString()
    };
  } finally {
    process.off('SIGTERM', onSignalStop);
    process.off('SIGINT', onSignalStop);
  }
}

async function main() {
  const args = parseArgs(process.argv);
  if (parseBoolean(args.help, false) || parseBoolean(args.h, false)) {
    printHelp();
    return;
  }

  const mode = clean(args.mode) || 'incremental';
  const sink = clean(args.sink)?.toLowerCase() || (mode === 'backfill-local' ? 'local' : 'api');
  if (!['api', 'local'].includes(sink)) {
    throw new Error(`Unsupported sink: ${sink}. Use --sink api or --sink local`);
  }
  const baseUrl = String(args['api-base'] || 'https://civantapp.vercel.app').replace(/\/$/, '');
  const appId = String(args['app-id'] || 'civantapp');
  const tenantId = String(args['tenant-id'] || 'civant_default');
  const includeTenantHeader = args['with-tenant-header'] !== 'false';
  const batchSize = Math.max(20, parsePositiveInt(args['batch-size'], 120));
  const runPrefix = (() => {
    if (mode === 'check-current') return 'placsp_es_check';
    if (mode === 'upload-local') return 'placsp_es_upload';
    if (mode === 'backfill-local' || sink === 'local') return 'placsp_es_local';
    return 'placsp_es';
  })();
  const runId = String(args['run-id'] || makeRunId(runPrefix));
  const statusFile = String(args['status-file'] || DEFAULT_STATUS_FILE);
  const checkpointFile = String(args['checkpoint-file'] || DEFAULT_CHECKPOINT_FILE);
  const controlFile = String(args['control-file'] || DEFAULT_CONTROL_FILE);
  const pidFile = String(args['pid-file'] || DEFAULT_PID_FILE);
  const localOutputDir = String(args['local-output-dir'] || DEFAULT_LOCAL_OUTPUT_DIR);
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
        sink,
        baseUrl,
        appId,
        tenantId,
        includeTenantHeader,
        batchSize,
        runId,
        statusFile,
        localOutputDir,
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

    if (mode === 'backfill' || mode === 'backfill-local') {
      const fromYear = parsePositiveInt(args['historical-from-year'], 2012);
      const toYear = parsePositiveInt(args['historical-to-year'], new Date().getUTCFullYear());
      const includeMonthlyCurrentYear = parseBoolean(args['include-monthly-current-year'], true);
      const downloadDir = String(args['download-dir'] || DEFAULT_DOWNLOAD_DIR);
      const maxArchives = parsePositiveInt(args['max-archives'], Number.MAX_SAFE_INTEGER);
      const maxFilesPerArchive = parsePositiveInt(args['max-files-per-archive'], Number.MAX_SAFE_INTEGER);
      const startRecordArg = Math.max(1, parsePositiveInt(args['start-record'], 1));

      const explicitZips = parseZipList(args.zip);
      const zipItems = explicitZips.length
        ? explicitZips
        : buildHistoricalZipUrls({ fromYear, toYear, includeMonthlyCurrentYear });

      if (!zipItems.length) {
        throw new Error('No ZIP sources resolved for backfill mode');
      }

      const checkpoint = readJsonFile(checkpointFile) || {};
      const resumePlan = resolveBackfillResumePlan({
        zipItems,
        startRecordArg,
        checkpoint
      });

      const sourceIterator = iterateZipSources({
        zipItems,
        downloadDir,
        maxArchives,
        maxFilesPerArchive,
        tenantId,
        startArchiveIndex: resumePlan.startArchiveIndex,
        startFeedKey: resumePlan.startFeedKey,
        startEntryIndex: resumePlan.startEntryIndex
      });

      const result = await runIngestion({
        mode: mode === 'backfill-local' ? 'backfill-local' : 'backfill',
        sink: mode === 'backfill-local' ? 'local' : sink,
        baseUrl,
        appId,
        tenantId,
        includeTenantHeader,
        batchSize,
        runId,
        statusFile,
        localOutputDir,
        startRecord: resumePlan.effectiveStartRecord,
        sourceIterator,
        checkpointFile,
        checkpointPayload: {
          source: 'PLACSP_ES',
          historical_from_year: fromYear,
          historical_to_year: toYear,
          include_monthly_current_year: includeMonthlyCurrentYear,
          resume_by_cursor: resumePlan.usedCursorResume,
          start_archive_index: resumePlan.startArchiveIndex,
          start_feed_key: resumePlan.startFeedKey,
          start_entry_index: resumePlan.startEntryIndex
        },
        controlFile
      });

      console.log(JSON.stringify({
        ...result,
        sink: mode === 'backfill-local' ? 'local' : sink,
        local_output_dir: mode === 'backfill-local' || sink === 'local' ? localOutputDir : null,
        zip_sources_count: zipItems.length,
        zip_sources: zipItems.slice(0, 20)
      }, null, 2));
      return;
    }

    if (mode === 'upload-local') {
      const uploadTables = parseUploadTables(args['upload-tables']);
      const uploadStartLine = Math.max(1, parsePositiveInt(args['upload-start-line'], 1));

      const result = await runUploadLocal({
        baseUrl,
        appId,
        tenantId,
        includeTenantHeader,
        batchSize,
        runId,
        statusFile,
        checkpointFile,
        localOutputDir,
        uploadTables,
        uploadStartLine
      });

      console.log(JSON.stringify(result, null, 2));
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
