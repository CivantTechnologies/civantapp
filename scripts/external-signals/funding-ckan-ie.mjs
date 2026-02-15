#!/usr/bin/env node

import {
  clamp01,
  clean,
  jsonSafe,
  mustGetArg,
  parseArgs,
  parseBool,
  parseISOToDate,
  stableHash,
  todayISODate,
  tsvSafe
} from './_lib.mjs';

// Ignore EPIPE when piped (e.g., `... | head`).
process.stdout.on('error', (err) => {
  if (err && err.code === 'EPIPE') process.exit(0);
});

const DEFAULT_BASE_URL = 'https://data.gov.ie';
const DEFAULT_ROWS = 50;
const DEFAULT_MAX_PAGES = 10;

const CORE_KEYWORDS = ['funding', 'grant', 'grants', 'capital', 'allocation', 'allocations', 'budget'];
const EXTRA_KEYWORDS = ['programme', 'programmes'];
const KEYWORDS = [...CORE_KEYWORDS, ...EXTRA_KEYWORDS];
const STRONG_KEYWORDS = ['capital', 'allocation', 'allocations'];
const ICT_KEYWORDS = ['ict', 'digital', 'platform', 'systems', 'it ', 'cyber', 'software', 'data'];

function buildQuery() {
  // CKAN q syntax is Solr-ish; keep the query tightly scoped to reduce noise.
  return CORE_KEYWORDS.join(' OR ');
}

function containsAny(haystack, needles) {
  const text = clean(haystack);
  if (!text) return false;
  const lowered = ` ${text.toLowerCase()} `;
  return needles.some((n) => lowered.includes(n));
}

function hasCoreMatch(matched) {
  return matched.some((k) => CORE_KEYWORDS.includes(k));
}

function matchedKeywords(payloadText) {
  const text = clean(payloadText);
  if (!text) return [];
  const lowered = ` ${text.toLowerCase()} `;
  const matched = [];
  for (const k of KEYWORDS) {
    const needle = ` ${k.toLowerCase()} `;
    // Also allow substring match for non-word-boundary cases like "funding," etc.
    if (lowered.includes(needle) || lowered.includes(k.toLowerCase())) matched.push(k);
  }
  return Array.from(new Set(matched));
}

function computeStrength(matched, payloadText) {
  let strength = 0.65;
  if (matched.some((k) => STRONG_KEYWORDS.includes(k))) strength += 0.10;
  if (containsAny(payloadText, ICT_KEYWORDS)) strength += 0.05;
  return clamp01(Math.min(strength, 0.9));
}

function datasetResourcePageUrl(baseUrl, datasetName, resourceId) {
  const base = baseUrl.replace(/\/$/, '');
  if (datasetName && resourceId) return `${base}/dataset/${encodeURIComponent(datasetName)}/resource/${encodeURIComponent(resourceId)}`;
  if (datasetName) return `${base}/dataset/${encodeURIComponent(datasetName)}`;
  return base;
}

async function ckanAction(baseUrl, action, params) {
  const base = baseUrl.replace(/\/$/, '');
  const url = new URL(`${base}/api/3/action/${action}`);
  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null || v === '') continue;
    url.searchParams.set(k, String(v));
  }

  const res = await fetch(url, {
    headers: {
      accept: 'application/json'
    }
  });

  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`CKAN ${action} failed: ${res.status} ${res.statusText}${text ? ` :: ${text.slice(0, 500)}` : ''}`);
  }

  const body = await res.json();
  if (!body || body.success !== true) {
    throw new Error(`CKAN ${action} returned success=false`);
  }
  return body.result;
}

function toRow({
  tenantId,
  baseUrl,
  dataset,
  resource,
  signalDate,
  keywordsMatched
}) {
  const datasetId = clean(dataset?.id);
  const datasetName = clean(dataset?.name);
  const datasetTitle = clean(dataset?.title) || clean(dataset?.name) || 'Unknown dataset';

  const orgTitle = clean(dataset?.organization?.title) || clean(dataset?.organization?.name) || null;
  const resourceId = clean(resource?.id) || null;
  const resourceName = clean(resource?.name) || clean(resource?.description) || null;
  const format = clean(resource?.format) || null;

  const modifiedRaw = clean(resource?.last_modified) || clean(resource?.metadata_modified) || clean(dataset?.metadata_modified) || null;
  const modifiedDate = parseISOToDate(modifiedRaw) || signalDate;

  const evidenceUrl = datasetResourcePageUrl(baseUrl, datasetName, resourceId);

  const sourceEventId = [
    'ckan',
    `dataset:${datasetId || stableHash([datasetTitle]).slice(0, 16)}`,
    `resource:${resourceId || stableHash([datasetTitle, resourceName, format]).slice(0, 16)}`,
    `modified:${modifiedRaw || modifiedDate || todayISODate()}`
  ].join(':');

  const evidenceTextParts = [
    datasetTitle,
    orgTitle ? `(${orgTitle})` : null,
    resourceName ? `Resource: ${resourceName}` : null,
    format ? `Format: ${format}` : null
  ].filter(Boolean);

  const payloadText = [
    datasetTitle,
    clean(dataset?.notes),
    orgTitle,
    resourceName,
    format,
    JSON.stringify(dataset?.tags || [])
  ]
    .filter(Boolean)
    .join(' ');

  const signalStrength = computeStrength(keywordsMatched, payloadText);

  const extractedEntities = {
    dataset_id: datasetId,
    dataset_name: datasetName,
    dataset_title: datasetTitle,
    org_name: orgTitle,
    resource_id: resourceId,
    resource_name: resourceName,
    formats: format ? [format] : [],
    keywords_matched: keywordsMatched
  };

  const rawPayload = {
    dataset: {
      id: clean(dataset?.id),
      name: clean(dataset?.name),
      title: clean(dataset?.title),
      metadata_modified: clean(dataset?.metadata_modified),
      organization: dataset?.organization
        ? { id: clean(dataset.organization.id), name: clean(dataset.organization.name), title: clean(dataset.organization.title) }
        : null
    },
    resource: resource
      ? {
          id: clean(resource?.id),
          name: clean(resource?.name),
          format: clean(resource?.format),
          url: clean(resource?.url),
          access_url: clean(resource?.access_url),
          last_modified: clean(resource?.last_modified),
          metadata_modified: clean(resource?.metadata_modified)
        }
      : null
  };

  return {
    tenant_id: tenantId,
    signal_type: 'funding',
    signal_source: 'ckan_ie_dataportal',
    source_event_id: sourceEventId,
    signal_date: modifiedDate || todayISODate(),
    evidence_url: evidenceUrl,
    evidence_text: evidenceTextParts.join(' | ').slice(0, 800),
    buyer_id: '',
    buyer_name_raw: orgTitle || '',
    signal_strength: String(signalStrength.toFixed(3)),
    extracted_entities: extractedEntities,
    raw_payload: rawPayload
  };
}

function rowToTSV(row) {
  // Column order must match COPY in rollout script.
  return [
    row.tenant_id,
    row.signal_type,
    row.signal_source,
    row.source_event_id,
    row.signal_date,
    row.evidence_url,
    row.evidence_text,
    row.buyer_id,
    row.buyer_name_raw,
    row.signal_strength,
    jsonSafe(row.extracted_entities),
    jsonSafe(row.raw_payload)
  ]
    .map(tsvSafe)
    .join('\t');
}

async function main() {
  const args = parseArgs(process.argv);
  const tenantId = mustGetArg(args, 'tenant-id');
  const startDate = clean(args['start-date']);
  const dryRun = parseBool(args['dry-run'], false);

  const baseUrl = clean(process.env.CKAN_IE_BASE_URL) || DEFAULT_BASE_URL;
  const rows = Number(clean(args.rows) || process.env.CKAN_ROWS || DEFAULT_ROWS);
  const maxPages = Number(clean(args['max-pages']) || process.env.CKAN_MAX_PAGES || DEFAULT_MAX_PAGES);
  const maxEvents = Number(clean(args['max-events']) || process.env.CKAN_MAX_EVENTS || 2000);

  const q = buildQuery();

  const out = [];
  const seen = new Set();

  for (let page = 0; page < maxPages; page += 1) {
    const result = await ckanAction(baseUrl, 'package_search', {
      q,
      rows,
      start: page * rows,
      sort: 'metadata_modified desc'
    });

    const results = Array.isArray(result?.results) ? result.results : [];
    if (results.length === 0) break;

    let pageAllOld = true;

    for (const dataset of results) {
      const datasetText = [clean(dataset?.title), clean(dataset?.notes), clean(dataset?.organization?.title)].filter(Boolean).join(' ');
      const kw = matchedKeywords(datasetText);

      const resources = Array.isArray(dataset?.resources) ? dataset.resources : [];
      if (resources.length === 0) {
        if (!hasCoreMatch(kw)) continue;
        // Emit a dataset-level signal if it matches keywords strongly enough.
        const date = parseISOToDate(dataset?.metadata_modified) || todayISODate();
        if (startDate && date < startDate) continue;
        pageAllOld = false;

        const syntheticResource = {
          id: null,
          name: null,
          format: null,
          url: null,
          last_modified: null,
          metadata_modified: dataset?.metadata_modified
        };

        const row = toRow({ tenantId, baseUrl, dataset, resource: syntheticResource, signalDate: date, keywordsMatched: kw });
        if (seen.has(row.source_event_id)) continue;
        seen.add(row.source_event_id);
        out.push(row);

        if (out.length >= maxEvents) break;
        continue;
      }

      for (const resource of resources) {
        const resourceText = [datasetText, clean(resource?.name), clean(resource?.description), clean(resource?.format)].filter(Boolean).join(' ');
        const kw2 = Array.from(new Set([...kw, ...matchedKeywords(resourceText)]));
        if (!hasCoreMatch(kw2)) continue;

        const date =
          parseISOToDate(resource?.last_modified) ||
          parseISOToDate(resource?.metadata_modified) ||
          parseISOToDate(dataset?.metadata_modified) ||
          todayISODate();

        if (startDate && date < startDate) continue;
        pageAllOld = false;

        const row = toRow({ tenantId, baseUrl, dataset, resource, signalDate: date, keywordsMatched: kw2 });
        if (seen.has(row.source_event_id)) continue;
        seen.add(row.source_event_id);
        out.push(row);
      }
    }

    if (startDate && pageAllOld) break;
  }

  // Deterministic output for replay/debug.
  out.sort((a, b) => {
    if (a.signal_date !== b.signal_date) return a.signal_date < b.signal_date ? 1 : -1;
    if (a.signal_source !== b.signal_source) return a.signal_source < b.signal_source ? -1 : 1;
    return a.source_event_id < b.source_event_id ? -1 : 1;
  });

  if (dryRun) {
    console.error(`funding-ckan-ie: dry-run rows=${out.length} tenant_id=${tenantId} start_date=${startDate || 'null'}`);
  }

  for (const row of out) {
    process.stdout.write(`${rowToTSV(row)}\n`);
  }
}

main().catch((err) => {
  console.error(String(err?.stack || err));
  process.exit(1);
});
