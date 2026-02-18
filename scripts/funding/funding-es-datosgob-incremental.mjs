#!/usr/bin/env node

import crypto from 'node:crypto';

process.stdout.on('error', (err) => {
  if (err && err.code === 'EPIPE') process.exit(0);
});

const BASE_URL = 'https://datos.gob.es/apidata/catalog/dataset.json';
const SIGNAL_SOURCE = 'datosgob_es_funding';

const FUNDING_KEYWORDS = [
  'funding', 'grant', 'capital', 'allocation', 'programme', 'program', 'budget',
  'subvencion', 'subvenciones', 'financiacion', 'financiación', 'ayuda', 'ayudas',
  'presupuesto', 'dotacion', 'dotación', 'convocatoria'
];

const CORE_FUNDING_KEYWORDS = [
  'funding', 'grant', 'capital', 'allocation', 'budget',
  'subvencion', 'subvenciones', 'financiacion', 'financiación',
  'ayuda', 'ayudas', 'presupuesto', 'dotacion', 'dotación'
];

const DIGITAL_KEYWORDS = [
  'ict', 'digital', 'digit', 'tecnologia', 'tecnología', 'plataforma', 'sistema', 'informatic', 'datos'
];

const MONTH_MAP = {
  ene: '01', feb: '02', mar: '03', abr: '04', may: '05', jun: '06',
  jul: '07', ago: '08', sep: '09', oct: '10', nov: '11', dic: '12'
};

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) {
      args._.push(token);
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith('--')) {
      args[key] = next;
      i += 1;
    } else {
      args[key] = 'true';
    }
  }
  return args;
}

function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.toLowerCase() === 'null') return null;
  return text;
}

function toBool(raw, fallback = false) {
  if (raw === undefined || raw === null || raw === '') return fallback;
  const value = String(raw).toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(value)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(value)) return false;
  return fallback;
}

function safeTsvField(value) {
  return String(value ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, ' ')
    .replace(/\r/g, ' ')
    .replace(/\n/g, ' ');
}

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function localizedText(value) {
  if (!value) return '';
  if (Array.isArray(value)) {
    const es = value.find((v) => normalizeText(v?._lang) === 'es');
    const first = value.find((v) => clean(v?._value));
    return clean(es?._value) || clean(first?._value) || '';
  }
  if (typeof value === 'object') {
    return clean(value?._value) || '';
  }
  return clean(value) || '';
}

function parseSpanishDate(raw) {
  const text = clean(raw);
  if (!text) return null;

  const direct = new Date(text);
  if (!Number.isNaN(direct.getTime())) return direct;

  const normalized = text
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(',', '')
    .trim();

  const m = normalized.match(/^[a-z]{3}\s+(\d{1,2})\s+([a-z]{3})\s+(\d{4})\s+(\d{2}:\d{2}:\d{2})\s+gmt([+-]\d{4})$/i);
  if (!m) return null;

  const day = m[1].padStart(2, '0');
  const month = MONTH_MAP[m[2]];
  const year = m[3];
  const time = m[4];
  const offsetRaw = m[5];
  if (!month) return null;

  const offset = `${offsetRaw.slice(0, 3)}:${offsetRaw.slice(3)}`;
  const iso = `${year}-${month}-${day}T${time}${offset}`;
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function asStartDate(raw) {
  const text = clean(raw);
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function parseIsoDate(raw, fallbackDate) {
  const parsed = raw instanceof Date ? raw : parseSpanishDate(raw);
  if (!parsed) return fallbackDate;
  return parsed.toISOString().slice(0, 10);
}

function parseIsoTs(raw, fallbackTs) {
  const parsed = raw instanceof Date ? raw : parseSpanishDate(raw);
  if (!parsed) return fallbackTs;
  return parsed.toISOString();
}

function hashShort(value) {
  return crypto.createHash('sha1').update(String(value || '')).digest('hex').slice(0, 16);
}

function extractPublisherName(publisherRaw) {
  const publisher = clean(publisherRaw);
  if (!publisher) return null;

  const byLabel = publisher.match(/(?:[?&]label=)([^&]+)/i);
  if (byLabel) {
    return decodeURIComponent(byLabel[1]).replace(/[+_]/g, ' ').trim() || null;
  }

  const parts = publisher.split('/').filter(Boolean);
  const last = parts[parts.length - 1];
  if (!last) return null;
  return decodeURIComponent(last).replace(/[-_]/g, ' ').trim() || null;
}

function normalizeDistribution(distribution) {
  if (!distribution) return [];
  if (Array.isArray(distribution)) return distribution;
  return [distribution];
}

function extractFormat(formatRaw) {
  if (formatRaw && typeof formatRaw === 'object') {
    const nested = clean(formatRaw.value || formatRaw._value || formatRaw._about);
    if (nested) return extractFormat(nested);
  }
  const text = clean(formatRaw);
  if (!text) return null;
  const parts = text.split('/').filter(Boolean);
  return parts[parts.length - 1]?.toUpperCase() || text.toUpperCase();
}

function computeSignalStrength(matchedKeywords, textNormalized) {
  let strength = 0.65;

  if (matchedKeywords.some((kw) => ['capital', 'allocation', 'dotacion', 'presupuesto'].includes(normalizeText(kw)))) {
    strength += 0.10;
  }

  if (DIGITAL_KEYWORDS.some((kw) => textNormalized.includes(normalizeText(kw)))) {
    strength += 0.05;
  }

  return Math.min(0.9, Number(strength.toFixed(3)));
}

async function fetchJson(url, maxRetries = 3) {
  let lastError = null;
  for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          accept: 'application/json',
          'user-agent': 'CivantBot/1.0 (+https://civant.ai)'
        }
      });
      if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(`HTTP ${response.status}: ${body.slice(0, 180)}`);
      }
      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, attempt * 500));
      }
    }
  }
  throw lastError || new Error('Request failed');
}

async function main() {
  const args = parseArgs(process.argv);
  const tenantId = clean(args['tenant-id'] || args.tenant_id);
  if (!tenantId) {
    console.error('ERROR: --tenant-id is required');
    process.exit(1);
  }

  const startDate = asStartDate(args['start-date'] || args.start_date);
  const dryRun = toBool(args['dry-run'], false);
  const pageSize = Math.max(10, Number(args['page-size'] || 100));
  const maxPages = Math.max(1, Number(args['max-pages'] || 20));
  const maxRows = Math.max(10, Number(args['max-rows'] || 2000));

  const rowsByEvent = new Map();
  let fetchedDatasets = 0;

  for (let page = 0; page < maxPages; page += 1) {
    const params = new URLSearchParams();
    params.set('_pageSize', String(pageSize));
    params.set('_page', String(page));

    const payload = await fetchJson(`${BASE_URL}?${params.toString()}`);
    const datasets = Array.isArray(payload?.result?.items) ? payload.result.items : [];
    if (!datasets.length) break;

    fetchedDatasets += datasets.length;

    for (const dataset of datasets) {
      const datasetId = clean(dataset?.identifier) || clean(dataset?._about);
      const datasetTitle = localizedText(dataset?.title);
      if (!datasetId || !datasetTitle) continue;

      const datasetUrl = clean(dataset?._about) || 'https://datos.gob.es/';
      const datasetDescription = localizedText(dataset?.description);
      const keywordValues = Array.isArray(dataset?.keyword)
        ? dataset.keyword.map((kw) => localizedText(kw)).filter(Boolean)
        : [];

      const publisherName = extractPublisherName(dataset?.publisher);

      const datasetTs = parseIsoTs(dataset?.modified || dataset?.issued, new Date().toISOString());
      const datasetDate = datasetTs.slice(0, 10);

      const distributions = normalizeDistribution(dataset?.distribution);
      const resources = distributions.length
        ? distributions
        : [{
            _about: `${datasetUrl}#dataset`,
            accessURL: datasetUrl,
            title: datasetTitle,
            format: null
          }];

      for (const resource of resources) {
        const resourceUrl = clean(resource?.accessURL) || clean(resource?._about) || datasetUrl;
        const resourceName = localizedText(resource?.title) || datasetTitle;
        const resourceId = clean(resource?._about)?.split('/').filter(Boolean).pop()
          || `resource-${hashShort(`${datasetId}|${resourceUrl}|${resourceName}`)}`;

        const signalTs = parseIsoTs(resource?.modified || dataset?.modified || dataset?.issued, datasetTs);
        const signalDate = parseIsoDate(signalTs, datasetDate);
        if (startDate && signalDate < startDate) continue;

        const searchable = normalizeText([
          datasetTitle,
          datasetDescription,
          resourceName,
          resourceUrl,
          publisherName,
          ...keywordValues
        ].filter(Boolean).join(' '));

        const matchedKeywords = FUNDING_KEYWORDS.filter((kw) => searchable.includes(normalizeText(kw)));
        if (!matchedKeywords.length) continue;
        const coreMatched = CORE_FUNDING_KEYWORDS.filter((kw) => searchable.includes(normalizeText(kw)));
        if (!coreMatched.length) continue;

        const signalStrength = computeSignalStrength(matchedKeywords, searchable);
        const sourceEventId = `dataset:${datasetId}:resource:${resourceId}:modified:${signalTs}`;

        const extractedEntities = {
          dataset_id: datasetId,
          dataset_title: datasetTitle,
          org_name: publisherName,
          resource_id: resourceId,
          resource_name: resourceName,
          formats: [extractFormat(resource?.format)].filter(Boolean),
          keywords_matched: matchedKeywords
        };

        const evidenceText = `${datasetTitle} | ${publisherName || 'Unknown organization'} | ${resourceName}`.slice(0, 800);

        const rawPayload = {
          dataset: {
            identifier: datasetId,
            about: dataset?._about || null,
            title: dataset?.title || null,
            description: dataset?.description || null,
            modified: dataset?.modified || null,
            issued: dataset?.issued || null,
            publisher: dataset?.publisher || null,
            keyword: dataset?.keyword || null
          },
          resource: {
            about: resource?._about || null,
            accessURL: resource?.accessURL || null,
            title: resource?.title || null,
            format: resource?.format || null,
            modified: resource?.modified || null
          }
        };

        rowsByEvent.set(sourceEventId, {
          tenantId,
          signalType: 'funding',
          signalSource: SIGNAL_SOURCE,
          sourceEventId,
          signalDate,
          evidenceUrl: resourceUrl,
          evidenceText,
          buyerNameRaw: publisherName,
          signalStrength,
          extractedEntities,
          rawPayload
        });

        if (rowsByEvent.size >= maxRows) break;
      }

      if (rowsByEvent.size >= maxRows) break;
    }

    if (rowsByEvent.size >= maxRows || datasets.length < pageSize) break;
  }

  const rows = Array.from(rowsByEvent.values());
  console.error(
    `funding_es_datosgob fetched_datasets=${fetchedDatasets} staged_rows=${rows.length} ` +
    `start_date=${startDate || '<none>'} dry_run=${dryRun}`
  );

  if (dryRun) {
    rows.slice(0, 5).forEach((row) => {
      console.error(`sample: ${row.sourceEventId} | ${row.signalDate} | ${row.evidenceUrl}`);
    });
  }

  for (const row of rows) {
    const line = [
      safeTsvField(row.tenantId),
      safeTsvField(row.signalType),
      safeTsvField(row.signalSource),
      safeTsvField(row.sourceEventId),
      safeTsvField(row.signalDate),
      safeTsvField(row.evidenceUrl),
      safeTsvField(row.evidenceText),
      safeTsvField(row.buyerNameRaw || ''),
      safeTsvField(row.signalStrength),
      safeTsvField(JSON.stringify(row.extractedEntities)),
      safeTsvField(JSON.stringify(row.rawPayload))
    ].join('\t');

    process.stdout.write(line);
    process.stdout.write('\n');
  }
}

main().catch((error) => {
  console.error(`ERROR: ${error?.message || String(error)}`);
  process.exit(1);
});
