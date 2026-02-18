#!/usr/bin/env node

import crypto from 'node:crypto';

process.stdout.on('error', (err) => {
  if (err && err.code === 'EPIPE') process.exit(0);
});

const BASE_URL = 'https://www.data.gouv.fr/api/1/datasets/';
const SIGNAL_SOURCE = 'datagouv_fr_funding';

const FUNDING_KEYWORDS = [
  'funding', 'grant', 'capital', 'allocation', 'programme', 'program', 'budget',
  'subvention', 'subventions', 'financement', 'aide', 'aides', 'dotation'
];

const CORE_FUNDING_KEYWORDS = [
  'funding', 'grant', 'capital', 'allocation', 'budget',
  'subvention', 'subventions', 'financement', 'aide', 'aides', 'dotation'
];

const DIGITAL_KEYWORDS = [
  'ict', 'digital', 'numerique', 'numérique', 'informatique', 'plateforme', 'systeme', 'système'
];

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

function asStartDate(raw) {
  const text = clean(raw);
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function parseIsoDate(raw, fallbackDate) {
  const text = clean(raw);
  if (!text) return fallbackDate;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return fallbackDate;
  return parsed.toISOString().slice(0, 10);
}

function parseIsoTs(raw, fallbackTs) {
  const text = clean(raw);
  if (!text) return fallbackTs;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return fallbackTs;
  return parsed.toISOString();
}

function extractFormats(resource) {
  const set = new Set();
  const candidates = [resource?.format, resource?.type, resource?.mime];
  candidates.forEach((value) => {
    const text = clean(value);
    if (!text) return;
    set.add(text.toUpperCase());
  });
  return Array.from(set);
}

function urlForDataset(dataset) {
  const page = clean(dataset?.page);
  if (page) {
    if (page.startsWith('http://') || page.startsWith('https://')) return page;
    return `https://www.data.gouv.fr${page}`;
  }
  const slug = clean(dataset?.slug);
  if (slug) return `https://www.data.gouv.fr/fr/datasets/${encodeURIComponent(slug)}/`;
  return 'https://www.data.gouv.fr/';
}

function hashShort(value) {
  return crypto.createHash('sha1').update(String(value || '')).digest('hex').slice(0, 16);
}

function computeSignalStrength(matchedKeywords, textNormalized) {
  let strength = 0.65;

  if (matchedKeywords.some((kw) => ['capital', 'allocation', 'dotation'].includes(kw))) {
    strength += 0.10;
  }

  if (DIGITAL_KEYWORDS.some((kw) => textNormalized.includes(kw))) {
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
        await new Promise((resolve) => setTimeout(resolve, attempt * 400));
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
  const pageSize = Math.max(10, Number(args['page-size'] || 50));
  const maxPages = Math.max(1, Number(args['max-pages'] || 10));
  const maxRows = Math.max(10, Number(args['max-rows'] || 2000));

  const rowsByEvent = new Map();
  let fetchedDatasets = 0;

  for (const keyword of FUNDING_KEYWORDS) {
    for (let page = 1; page <= maxPages; page += 1) {
      const params = new URLSearchParams();
      params.set('q', keyword);
      params.set('page', String(page));
      params.set('page_size', String(pageSize));

      const payload = await fetchJson(`${BASE_URL}?${params.toString()}`);
      const datasets = Array.isArray(payload?.data) ? payload.data : [];
      if (!datasets.length) break;

      fetchedDatasets += datasets.length;

      for (const dataset of datasets) {
        const datasetId = clean(dataset?.id);
        const datasetTitle = clean(dataset?.title);
        if (!datasetId || !datasetTitle) continue;

        const datasetUrl = urlForDataset(dataset);
        const datasetModifiedTs = parseIsoTs(dataset?.last_modified || dataset?.last_update || dataset?.created_at, new Date().toISOString());
        const datasetModifiedDate = datasetModifiedTs.slice(0, 10);

        const orgName = clean(dataset?.organization?.name);
        const tags = Array.isArray(dataset?.tags) ? dataset.tags.map((tag) => clean(tag)).filter(Boolean) : [];
        const resources = Array.isArray(dataset?.resources) && dataset.resources.length
          ? dataset.resources
          : [{
              id: `dataset-${datasetId}`,
              title: datasetTitle,
              url: datasetUrl,
              format: null,
              last_modified: datasetModifiedTs,
              created_at: dataset?.created_at
            }];

        for (const resource of resources) {
          const resourceId = clean(resource?.id) || `resource-${hashShort(`${datasetId}|${resource?.url || resource?.title || ''}`)}`;
          const resourceName = clean(resource?.title) || datasetTitle;
          const evidenceUrl = clean(resource?.url) || datasetUrl;

          const resourceTs = parseIsoTs(resource?.last_modified || resource?.created_at || datasetModifiedTs, datasetModifiedTs);
          const signalDate = parseIsoDate(resourceTs, datasetModifiedDate);
          if (startDate && signalDate < startDate) continue;

          const searchable = normalizeText([
            datasetTitle,
            dataset?.description,
            orgName,
            resourceName,
            evidenceUrl,
            ...tags
          ].filter(Boolean).join(' '));

          const matchedKeywords = FUNDING_KEYWORDS.filter((kw) => searchable.includes(normalizeText(kw)));
          if (!matchedKeywords.length) continue;
          const coreMatched = CORE_FUNDING_KEYWORDS.filter((kw) => searchable.includes(normalizeText(kw)));
          if (!coreMatched.length) continue;

          const signalStrength = computeSignalStrength(matchedKeywords, searchable);
          const sourceEventId = `dataset:${datasetId}:resource:${resourceId}:modified:${resourceTs}`;

          const extractedEntities = {
            dataset_id: datasetId,
            dataset_title: datasetTitle,
            org_name: orgName,
            resource_id: resourceId,
            resource_name: resourceName,
            formats: extractFormats(resource),
            keywords_matched: matchedKeywords
          };

          const evidenceText = `${datasetTitle} | ${orgName || 'Unknown organization'} | ${resourceName}`.slice(0, 800);

          const rawPayload = {
            dataset: {
              id: datasetId,
              title: datasetTitle,
              last_modified: dataset?.last_modified || dataset?.last_update || null,
              page: dataset?.page || null,
              organization: dataset?.organization || null,
              tags,
              license: dataset?.license || null
            },
            resource: {
              id: resourceId,
              title: resourceName,
              url: evidenceUrl,
              last_modified: resource?.last_modified || null,
              created_at: resource?.created_at || null,
              format: resource?.format || null,
              mime: resource?.mime || null
            }
          };

          rowsByEvent.set(sourceEventId, {
            tenantId,
            signalType: 'funding',
            signalSource: SIGNAL_SOURCE,
            sourceEventId,
            signalDate,
            evidenceUrl,
            evidenceText,
            buyerNameRaw: orgName,
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

    if (rowsByEvent.size >= maxRows) break;
  }

  const rows = Array.from(rowsByEvent.values());
  console.error(
    `funding_fr_datagouv fetched_datasets=${fetchedDatasets} staged_rows=${rows.length} ` +
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
