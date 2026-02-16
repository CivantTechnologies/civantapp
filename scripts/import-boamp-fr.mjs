#!/usr/bin/env node

import fs from 'node:fs';
import readline from 'node:readline';
import crypto from 'node:crypto';

const REQUEST_TIMEOUT_MS = 30000;
const MAX_HTTP_RETRIES = 4;
const MIN_PLAUSIBLE_YEAR = 1990;
const MAX_PLAUSIBLE_YEAR = new Date().getUTCFullYear() + 5;
const DAY_MS = 24 * 60 * 60 * 1000;
const DEADLINE_MAX_SPAN_DAYS = 730;

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

function writeJsonFile(path, value) {
  if (!path) return;
  try {
    fs.writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
  } catch {
    // best effort status reporting
  }
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

function isPlausibleYear(year) {
  if (!Number.isFinite(year)) return false;
  return year >= MIN_PLAUSIBLE_YEAR && year <= MAX_PLAUSIBLE_YEAR;
}

function parseDate(value) {
  const text = clean(value);
  if (!text) return { date: null, iso: null };

  // Prefer strict patterns over Date.parse() so we don't accept weird years like 0209.
  let match = text.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (match) {
    const year = Number(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    if (!isPlausibleYear(year) || month < 1 || month > 12 || day < 1 || day > 31) {
      return { date: null, iso: null };
    }
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime()) && isPlausibleYear(parsed.getUTCFullYear())) {
      return { date: parsed.toISOString().slice(0, 10), iso: parsed.toISOString() };
    }
    // Fallback for date-only strings without a timezone.
    return { date: `${match[1]}-${match[2]}-${match[3]}`, iso: null };
  }

  // Common French format: DD/MM/YYYY (optionally with time).
  match = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (match) {
    const day = Number(match[1]);
    const month = Number(match[2]);
    const year = Number(match[3]);
    const hour = match[4] ? Number(match[4]) : 0;
    const minute = match[5] ? Number(match[5]) : 0;
    const second = match[6] ? Number(match[6]) : 0;
    if (!isPlausibleYear(year) || month < 1 || month > 12 || day < 1 || day > 31) {
      return { date: null, iso: null };
    }
    const ms = Date.UTC(year, month - 1, day, hour, minute, second);
    const parsed = new Date(ms);
    if (!Number.isNaN(parsed.getTime()) && isPlausibleYear(parsed.getUTCFullYear())) {
      return { date: parsed.toISOString().slice(0, 10), iso: parsed.toISOString() };
    }
    return { date: null, iso: null };
  }

  // Last resort: only accept Date.parse() when the resulting year is plausible.
  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime()) && isPlausibleYear(parsed.getUTCFullYear())) {
    return { date: parsed.toISOString().slice(0, 10), iso: parsed.toISOString() };
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

function parseMaybeJson(value) {
  const text = clean(value);
  if (!text) return null;
  if (!(text.startsWith('{') || text.startsWith('['))) return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function extractDeadlineFromDonnees(donneesJson, publicationDate) {
  if (!donneesJson || typeof donneesJson !== 'object') return { date: null, source: null };

  const publication = publicationDate ? new Date(`${publicationDate}T00:00:00.000Z`) : null;
  const candidates = [];

  const addCandidate = (value, source, weight) => {
    const parsed = parseDate(value);
    if (!parsed.date) return;
    if (publication) {
      const deadline = new Date(`${parsed.date}T00:00:00.000Z`);
      const diffDays = Math.floor((deadline.getTime() - publication.getTime()) / DAY_MS);
      if (diffDays < 0 || diffDays > DEADLINE_MAX_SPAN_DAYS) return;
    }
    candidates.push({ date: parsed.date, source, weight });
  };

  const inspectRectifModification = (mod) => {
    if (!mod || typeof mod !== 'object') return;
    const rub = clean(mod.RUB_INIT ?? mod.rub_init ?? mod.rubInit);
    if (!rub) return;
    const rubLower = rub.toLowerCase();
    if (!rubLower.includes('date limite')) return;
    if (!/(reception|réception|offre|offres|demande|demandes|participation)/i.test(rub)) return;

    const lire = clean(mod.LIRE ?? mod.lire);
    const txtInit = clean(mod.TXT_INIT ?? mod.txt_init);

    // Prefer the corrected value if present.
    if (lire) addCandidate(lire, 'donnees.rectif.modification.lire', 3);
    if (txtInit) addCandidate(txtInit, 'donnees.rectif.modification.txt_init', 2);
  };

  const rectif = donneesJson.RECTIF ?? donneesJson.rectif;
  if (rectif && typeof rectif === 'object') {
    const mod = rectif.MODIFICATION ?? rectif.modification;
    if (Array.isArray(mod)) mod.forEach(inspectRectifModification);
    else inspectRectifModification(mod);
  }

  // Generic fallback: scan for obvious "date limite" / deadline-ish keys.
  const keyHint = /(date.*limit|datelimit|deadline|date_limite|date\s*limite)/i;
  const visit = (node) => {
    if (!node) return;
    if (Array.isArray(node)) {
      for (const item of node) visit(item);
      return;
    }
    if (typeof node !== 'object') return;

    for (const [key, value] of Object.entries(node)) {
      if (typeof value === 'string' && keyHint.test(String(key))) {
        addCandidate(value, `donnees.${String(key)}`, 1);
      } else if (value && typeof value === 'object') {
        // Handle nested "modification-like" objects without relying on exact casing.
        if (typeof value === 'object' && (value.RUB_INIT || value.rub_init) && (value.LIRE || value.lire || value.TXT_INIT || value.txt_init)) {
          inspectRectifModification(value);
        }
        visit(value);
      }
    }
  };
  visit(donneesJson);

  if (!candidates.length) return { date: null, source: null };

  // Choose highest-confidence bucket, then the latest date.
  candidates.sort((a, b) => (b.weight - a.weight) || (b.date.localeCompare(a.date)));
  return { date: candidates[0].date, source: candidates[0].source };
}

function addCpvCodesFromText(text, set) {
  if (text === undefined || text === null) return;
  const source = String(text);
  const regex = /\b(\d{8})(?:-\d)?\b/g;
  let match;
  while ((match = regex.exec(source)) !== null) {
    set.add(match[1]);
  }
}

function collectCpvCodesFromNode(node, set, inCpvContext = false) {
  if (node === undefined || node === null) return;

  if (Array.isArray(node)) {
    for (const item of node) {
      collectCpvCodesFromNode(item, set, inCpvContext);
    }
    return;
  }

  if (typeof node === 'object') {
    const listHint = clean(
      node['@listName']
      || node['@schemeName']
      || node.listName
      || node.schemeName
    );
    const objectHasCpvHint = /cpv/i.test(listHint || '');
    if (objectHasCpvHint) {
      collectCpvCodesFromNode(node['#text'] ?? node.text ?? node.value, set, true);
    }

    for (const [key, value] of Object.entries(node)) {
      const keyHasCpvHint = /(cpv|itemclassificationcode)/i.test(String(key));
      collectCpvCodesFromNode(value, set, inCpvContext || objectHasCpvHint || keyHasCpvHint);
    }
    return;
  }

  if (inCpvContext) {
    addCpvCodesFromText(node, set);
  }
}

function collectCpvCodes(record, donneesJson = null) {
  const set = new Set();

  // Legacy BOAMP rows expose CPV in top-level keys (e.g. CPV, codecpv).
  for (const [key, value] of Object.entries(record)) {
    if (/cpv/i.test(String(key))) {
      collectCpvCodesFromNode(value, set, true);
    }
  }

  // eForms rows store CPV inside the DONNEES JSON string.
  if (donneesJson) {
    collectCpvCodesFromNode(donneesJson, set, false);
  }

  return Array.from(set).sort();
}

function mapRow(record) {
  const sourceNoticeId = clean(record.idweb) || clean(record.id);
  const title = clean(record.objet) || clean(record.intitule) || clean(record.designation);
  if (!sourceNoticeId || !title) return null;

  const source = 'BOAMP_FR';
  const canonicalId = `${source}:${sourceNoticeId}`;
  const publication = parseDate(record.dateparution);
  const donneesJson = parseMaybeJson(record.DONNEES);

  let deadline = parseDate(record.datelimitereponse);
  let deadlineSource = deadline.date ? 'datelimitereponse' : null;
  if (!deadline.date && donneesJson) {
    const fromDonnees = extractDeadlineFromDonnees(donneesJson, publication.date);
    if (fromDonnees.date) {
      deadline = { date: fromDonnees.date, iso: null };
      deadlineSource = fromDonnees.source || 'donnees';
    }
  }

  const cpvCodes = collectCpvCodes(record, donneesJson);
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
    deadline_date_source: deadlineSource,
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
  let response = null;
  let lastError = null;

  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(rows),
        signal: controller.signal
      });
      clearTimeout(timer);
      break;
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 500));
        continue;
      }
      throw error;
    }
  }
  if (!response) throw lastError || new Error('Request failed');

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

async function getRows({ baseUrl, appId, tenantId, table, query = {}, limit, includeTenantHeader }) {
  const headers = { accept: 'application/json' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;

  const params = new URLSearchParams();
  if (query && typeof query === 'object' && Object.keys(query).length) {
    params.set('q', JSON.stringify(query));
  }
  if (Number.isFinite(limit) && limit > 0) {
    params.set('limit', String(limit));
  }

  const url = `${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}${params.size ? `?${params.toString()}` : ''}`;
  let response = null;
  let lastError = null;

  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      response = await fetch(url, { method: 'GET', headers, signal: controller.signal });
      clearTimeout(timer);
      break;
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 500));
        continue;
      }
      throw error;
    }
  }
  if (!response) throw lastError || new Error('Request failed');

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

  return Array.isArray(payload) ? payload : [];
}

async function putEntityById({ baseUrl, appId, tenantId, table, id, payload, includeTenantHeader }) {
  const headers = { 'content-type': 'application/json' };
  if (includeTenantHeader && tenantId) headers['x-tenant-id'] = tenantId;
  let response = null;
  let lastError = null;
  for (let attempt = 1; attempt <= MAX_HTTP_RETRIES; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      response = await fetch(`${baseUrl}/api/apps/${encodeURIComponent(appId)}/entities/${encodeURIComponent(table)}/${encodeURIComponent(id)}`, {
        method: 'PUT',
        headers,
        body: JSON.stringify(payload),
        signal: controller.signal
      });
      clearTimeout(timer);
      break;
    } catch (error) {
      clearTimeout(timer);
      lastError = error;
      if (attempt < MAX_HTTP_RETRIES) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 500));
        continue;
      }
      throw error;
    }
  }
  if (!response) throw lastError || new Error('Request failed');

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

function isIngestionRunDuplicateError(message) {
  const text = String(message || '').toLowerCase();
  return isDuplicateError(text) && text.includes('ingestion_runs');
}

function chunkArray(items, size) {
  const output = [];
  for (let i = 0; i < items.length; i += size) {
    output.push(items.slice(i, i + size));
  }
  return output;
}

function getRowKey(table, row) {
  if (!row || typeof row !== 'object') return '';
  if (table === 'canonical_tenders') {
    return String(row.canonical_id || '').trim();
  }
  if (table === 'TendersCurrent') {
    return String(row.tender_id || row.id || row.tender_uid || '').trim();
  }
  return '';
}

function getFingerprintForRow(table, row) {
  if (!row || typeof row !== 'object') return '';
  if (table === 'canonical_tenders') {
    return String(row?.normalized_json?.fingerprint || row?.fingerprint || '').trim();
  }
  if (table === 'TendersCurrent') {
    return String(row?.data?.fingerprint || row?.fingerprint || '').trim();
  }
  return '';
}

function normalizeComparableRow(table, row) {
  if (!row || typeof row !== 'object') return {};
  if (table === 'canonical_tenders') {
    const normalized = row.normalized_json && typeof row.normalized_json === 'object' ? row.normalized_json : {};
    const cpvCodes = Array.isArray(row.cpv_codes)
      ? row.cpv_codes.join(',')
      : Array.isArray(normalized.cpv_codes)
        ? normalized.cpv_codes.join(',')
        : String(row.cpv_codes || normalized.cpv_codes || '');
    return {
      title: row.title || normalized.title || null,
      buyer_name: normalized.buyer_name || null,
      publication_date: row.publication_date || normalized.publication_date || null,
      deadline_date: row.deadline_date || normalized.deadline_date || null,
      cpv_codes: cpvCodes || null,
      estimated_value: row.estimated_value ?? normalized.estimated_value ?? null,
      source_url: row.source_url || normalized.source_url || null
    };
  }

  if (table === 'TendersCurrent') {
    const data = row.data && typeof row.data === 'object' ? row.data : {};
    const cpvCodes = Array.isArray(data.cpv_codes)
      ? data.cpv_codes.join(',')
      : String(row.cpv_codes || data.cpv_codes || '');
    return {
      title: row.title || data.title || null,
      buyer_name: row.buyer_name || data.buyer_name || null,
      publication_date: row.publication_date || data.publication_date || row.published_at || null,
      deadline_date: row.deadline_date || data.deadline_date || null,
      cpv_codes: cpvCodes || null,
      estimated_value: row.estimated_value ?? data.estimated_value ?? null,
      source_url: row.source_url || data.source_url || row.url || null
    };
  }

  return {};
}

function classifyRow(table, incoming, existing) {
  if (!existing) return 'inserted';

  const incomingFingerprint = getFingerprintForRow(table, incoming);
  const existingFingerprint = getFingerprintForRow(table, existing);
  if (incomingFingerprint && existingFingerprint) {
    return incomingFingerprint === existingFingerprint ? 'noop' : 'updated';
  }

  const incomingComparable = JSON.stringify(normalizeComparableRow(table, incoming));
  const existingComparable = JSON.stringify(normalizeComparableRow(table, existing));
  return incomingComparable === existingComparable ? 'noop' : 'updated';
}

function addPlannedStatus(metrics, status) {
  if (status === 'inserted') metrics.inserted += 1;
  else if (status === 'updated') metrics.updated += 1;
  else metrics.noop += 1;
}

async function buildUpsertPlan({ baseUrl, appId, tenantId, table, rows, includeTenantHeader }) {
  if (!rows.length || (table !== 'canonical_tenders' && table !== 'TendersCurrent')) {
    return new Map();
  }

  const keyField = table === 'canonical_tenders' ? 'canonical_id' : 'tender_id';
  const keys = Array.from(new Set(rows.map((row) => getRowKey(table, row)).filter(Boolean)));
  if (!keys.length) return new Map();

  const existingByKey = new Map();
  const keyChunks = chunkArray(keys, 60);
  for (const chunk of keyChunks) {
    const query = { tenant_id: tenantId, [keyField]: chunk };
    const existingRows = await getRows({
      baseUrl,
      appId,
      tenantId,
      table,
      query,
      limit: chunk.length + 5,
      includeTenantHeader
    });
    for (const existing of existingRows) {
      const key = getRowKey(table, existing);
      if (key) existingByKey.set(key, existing);
    }
  }

  const plan = new Map();
  for (const row of rows) {
    const key = getRowKey(table, row);
    if (!key) continue;
    plan.set(key, classifyRow(table, row, existingByKey.get(key)));
  }
  return plan;
}

async function insertBatchWithFallback({ baseUrl, appId, tenantId, table, rows, metrics, includeTenantHeader }) {
  if (!rows.length) return;
  let upsertPlan = new Map();

  try {
    upsertPlan = await buildUpsertPlan({ baseUrl, appId, tenantId, table, rows, includeTenantHeader });
  } catch (planError) {
    metrics.errors.push(`${table}: failed to precompute upsert plan (${planError?.message || planError})`);
  }

  try {
    await postRows({ baseUrl, appId, tenantId, table, rows, includeTenantHeader });
    if (upsertPlan.size > 0) {
      for (const row of rows) {
        const key = getRowKey(table, row);
        addPlannedStatus(metrics, upsertPlan.get(key) || 'updated');
      }
    } else {
      metrics.inserted += rows.length;
    }
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
      if (upsertPlan.size > 0) {
        const key = getRowKey(table, row);
        addPlannedStatus(metrics, upsertPlan.get(key) || 'updated');
      } else {
        metrics.inserted += 1;
      }
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
  const statusFile = String(args['status-file'] || '/tmp/boamp-import-status.json');

  if (!fs.existsSync(file)) {
    console.error(`File not found: ${file}`);
    process.exit(1);
  }

  const source = 'BOAMP_FR';
  const connectorKey = String(args['connector-key'] || source);
  const sourceCursor = `file:${file}`;
  const startedAt = new Date().toISOString();
  const rawMetrics = { inserted: 0, duplicates: 0, failed: 0, errors: [] };
  const canonicalMetrics = { inserted: 0, updated: 0, noop: 0, duplicates: 0, failed: 0, errors: [] };
  const currentMetrics = { inserted: 0, updated: 0, noop: 0, duplicates: 0, failed: 0, errors: [] };
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
  let lastProgressAt = Date.now();
  let flushCount = 0;

  const emitStatus = (phase = 'running') => {
    writeJsonFile(statusFile, {
      phase,
      run_id: runId,
      file,
      start_record: startRecord,
      record_number: recordNumber,
      line_number: lineNumber,
      processed,
      skipped,
      skipped_before_start: skippedBeforeStart,
      deduped_in_file: dedupedInFile,
      raw_inserted: rawMetrics.inserted,
      canonical_inserted: canonicalMetrics.inserted,
      canonical_updated: canonicalMetrics.updated,
      canonical_noop: canonicalMetrics.noop,
      current_inserted: currentMetrics.inserted,
      current_updated: currentMetrics.updated,
      current_noop: currentMetrics.noop,
      raw_failed: rawMetrics.failed,
      canonical_failed: canonicalMetrics.failed,
      current_failed: currentMetrics.failed,
      updated_at: new Date().toISOString()
    });
  };

  const updateRunProgress = async (status = 'running') => {
    if (dryRun) return;
    try {
      await putEntityById({
        baseUrl,
        appId,
        tenantId,
        table: 'ingestion_runs',
        id: runId,
        payload: {
          status,
          metrics: {
            processed_rows: processed,
            skipped_missing_required: skipped,
            skipped_before_start: skippedBeforeStart,
            deduped_in_file: dedupedInFile,
            raw_rows: rawMetrics.inserted,
            canonical_inserted: canonicalMetrics.inserted,
            canonical_updated: canonicalMetrics.updated,
            canonical_noop: canonicalMetrics.noop,
            current_inserted: currentMetrics.inserted,
            current_updated: currentMetrics.updated,
            current_noop: currentMetrics.noop,
            raw_failed: rawMetrics.failed,
            canonical_failed: canonicalMetrics.failed,
            current_failed: currentMetrics.failed
          },
          errors: [...rawMetrics.errors, ...canonicalMetrics.errors, ...currentMetrics.errors].slice(0, 25),
          cursor: `record:${recordNumber}:line:${lineNumber}`,
          finished_at: null
        },
        includeTenantHeader
      });
    } catch {
      // best effort progress update
    }
  };

  if (!dryRun) {
    try {
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
          cursor: sourceCursor,
          finished_at: null
        },
        includeTenantHeader
      });
    }
  }
  emitStatus('started');

  const stream = fs.createReadStream(file, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  const flush = async () => {
    if (!rawBatch.length && (rawOnly || !canonicalBatch.length)) return;
    if (dryRun) {
      rawMetrics.inserted += rawBatch.length;
      rawBatch.length = 0;
      if (!rawOnly && canonicalBatch.length) {
        const canonicalPlan = await buildUpsertPlan({ baseUrl, appId, tenantId, table: 'canonical_tenders', rows: canonicalBatch, includeTenantHeader });
        const currentPlan = await buildUpsertPlan({ baseUrl, appId, tenantId, table: 'TendersCurrent', rows: currentBatch, includeTenantHeader });
        for (const row of canonicalBatch) {
          const key = getRowKey('canonical_tenders', row);
          addPlannedStatus(canonicalMetrics, canonicalPlan.get(key) || 'inserted');
        }
        for (const row of currentBatch) {
          const key = getRowKey('TendersCurrent', row);
          addPlannedStatus(currentMetrics, currentPlan.get(key) || 'inserted');
        }
      }
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
    flushCount += 1;
    if (flushCount % 10 === 0) {
      await updateRunProgress('running');
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
      emitStatus('running');
    }
    if (processed % 500 === 0 && processed > 0) {
      console.log(`Processed ${processed} rows (record ${recordNumber}, line ${lineNumber})`);
      lastProgressAt = Date.now();
      emitStatus('running');
    } else if (Date.now() - lastProgressAt > 60000) {
      console.log(`Heartbeat: processed ${processed} rows (record ${recordNumber}, line ${lineNumber})`);
      lastProgressAt = Date.now();
      emitStatus('running');
    }
    if (limit && processed >= limit) break;
  }

  if (pendingRecord) malformedRecords += 1;
  await flush();
  emitStatus('finalizing');

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
    canonical_updated: canonicalMetrics.updated,
    canonical_noop: canonicalMetrics.noop,
    current_inserted: currentMetrics.inserted,
    current_updated: currentMetrics.updated,
    current_noop: currentMetrics.noop,
    malformed_records: malformedRecords,
    mode: rawOnly ? 'raw_only' : 'canonical_and_current'
  };

  if (!dryRun) {
    const finishedAt = new Date().toISOString();
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
        finished_at: finishedAt
      },
      includeTenantHeader
    });

    // Mirror BOAMP runs into ConnectorRuns so Connector Health reflects real execution status.
    // Keep this best-effort to avoid breaking ingestion when connector logging has schema drift.
    try {
      const failedCount = rawMetrics.failed + canonicalMetrics.failed + currentMetrics.failed;
      const insertedCount = currentMetrics.inserted;
      const updatedCount = currentMetrics.updated;
      const noopCount = currentMetrics.noop;
      await postRows({
        baseUrl,
        appId,
        tenantId,
        table: 'ConnectorRuns',
        rows: [{
          tenant_id: tenantId,
          connector_key: connectorKey,
          status: runErrors.length ? 'partial' : 'success',
          started_at: startedAt,
          finished_at: finishedAt,
          metadata: {
            source,
            run_id: runId,
            fetched_count: processed,
            inserted_count: insertedCount,
            updated_count: updatedCount,
            noop_count: noopCount,
            versioned_count: updatedCount,
            deduped_in_file: dedupedInFile,
            failed_count: failedCount,
            cursor: `record:${recordNumber}:line:${lineNumber}`
          }
        }],
        includeTenantHeader
      });
    } catch (connectorRunError) {
      console.warn(`WARN: failed to write ConnectorRuns mirror: ${connectorRunError?.message || connectorRunError}`);
    }
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
  emitStatus('completed');
}

main().catch((error) => {
  const args = parseArgs(process.argv);
  const statusFile = String(args['status-file'] || '/tmp/boamp-import-status.json');
  writeJsonFile(statusFile, {
    phase: 'failed',
    error: error instanceof Error ? error.message : String(error),
    updated_at: new Date().toISOString()
  });
  console.error(error);
  process.exit(1);
});
