#!/usr/bin/env node

import {
  clamp01,
  clean,
  jsonSafe,
  mustGetArg,
  parseArgs,
  parseBool,
  parseISOToDate,
  todayISODate,
  tsvSafe
} from './_lib.mjs';

// Ignore EPIPE when piped (e.g., `... | head`).
process.stdout.on('error', (err) => {
  if (err && err.code === 'EPIPE') process.exit(0);
});

const DEFAULT_JOBBOARD_URL = 'https://publicjobs.tal.net/candidate/jobboard/vacancy/3/adv/';
const DEFAULT_MAX_EVENTS = 500;

const TITLE_SENIORITY_RE = /(\bmanager\b|\bdirector\b|\blead\b|\bhead\b)/i;
const ICT_RE = /(\bict\b|\bdigital\b|\bplatform\b|\bsystems\b|\bsoftware\b|\bdeveloper\b)/i;

function decodeHtmlEntities(input) {
  const text = clean(input);
  if (!text) return '';
  return text
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#160;|&nbsp;/g, ' ')
    .replace(/&#(\d+);/g, (_m, code) => {
      const n = Number(code);
      if (!Number.isFinite(n)) return '';
      try {
        return String.fromCodePoint(n);
      } catch {
        return '';
      }
    });
}

function stripTags(html) {
  return decodeHtmlEntities(String(html || '').replace(/<[^>]*>/g, ' ')).replace(/[\s\t\r\n]+/g, ' ').trim();
}

function parseTalDateToISO(value) {
  const text = clean(value);
  if (!text) return null;

  // Examples seen:
  // - "6 Feb 2026"
  // - "30/01/2026, 10:37"
  // - "30/01/2026"

  const ddmmyyyy = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (ddmmyyyy) {
    const day = ddmmyyyy[1].padStart(2, '0');
    const month = ddmmyyyy[2].padStart(2, '0');
    const year = ddmmyyyy[3];
    return `${year}-${month}-${day}`;
  }

  const m = text.match(/^(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})$/);
  if (m) {
    const day = m[1].padStart(2, '0');
    const mon = m[2].toLowerCase();
    const year = m[3];
    const months = {
      jan: '01',
      feb: '02',
      mar: '03',
      apr: '04',
      may: '05',
      jun: '06',
      jul: '07',
      aug: '08',
      sep: '09',
      oct: '10',
      nov: '11',
      dec: '12'
    };
    const month = months[mon];
    if (!month) return null;
    return `${year}-${month}-${day}`;
  }

  return parseISOToDate(text);
}

function normalizeTalCandidateUrl(url) {
  const text = clean(url);
  if (!text) return null;

  // Prefer a stable "bare" URL without the rotating /vx/.../xf-... segment.
  const idx = text.indexOf('/candidate/');
  if (idx !== -1) return `https://publicjobs.tal.net${text.slice(idx)}`;
  if (text.startsWith('https://publicjobs.tal.net/')) return text;
  if (text.startsWith('/candidate/')) return `https://publicjobs.tal.net${text}`;
  return text;
}

function computeStrength({ title, detailText }) {
  let strength = 0.60;
  if (TITLE_SENIORITY_RE.test(title || '')) strength += 0.10;
  if (ICT_RE.test(`${title || ''} ${detailText || ''}`)) strength += 0.05;
  return clamp01(Math.min(strength, 0.9));
}

function extractAfterSpan(block, fieldClass) {
  const re = new RegExp(`<div\\s+class=\"${fieldClass.replace(/[.*+?^${}()|[\\]\\]/g, '\\$&')}\"[^>]*>[\\s\\S]*?<\\/span>\\s*([\\s\\S]*?)<\\/div>`, 'i');
  const m = block.match(re);
  if (!m) return null;
  return stripTags(m[1]);
}

function extractHref(block) {
  const m = block.match(/<a\s+class=\"subject\"\s+href=\"([^\"]+)\"/i);
  if (!m) return null;
  return decodeHtmlEntities(m[1]);
}

function extractOppidAndTitle(openTag) {
  const idm = openTag.match(/data-oppid=\"(\d+)\"/i);
  const tm = openTag.match(/data-title=\"([^\"]*)\"/i);
  return {
    oppid: idm ? idm[1] : null,
    title: tm ? decodeHtmlEntities(tm[1]).trim() : null
  };
}

function extractTiles(html) {
  const tiles = [];
  const re = /<div\s+class=\"opp_[^\"]*candidate-opp-tile\"[^>]*>/gi;
  const starts = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    starts.push({ index: m.index, openTag: m[0] });
  }

  for (let i = 0; i < starts.length; i += 1) {
    const start = starts[i].index;
    const end = i + 1 < starts.length ? starts[i + 1].index : html.length;
    const block = html.slice(start, end);
    const { oppid, title } = extractOppidAndTitle(starts[i].openTag);
    if (!oppid) continue;

    const href = extractHref(block);
    const vacancyType = extractAfterSpan(block, 'candidate-opp-field-3');
    const org = extractAfterSpan(block, 'candidate-opp-field-5');
    const location = extractAfterSpan(block, 'candidate-opp-field-6');
    const advertisingDateRaw = extractAfterSpan(block, 'candidate-opp-field-7');
    const closingDateRaw = extractAfterSpan(block, 'candidate-opp-field-8');

    tiles.push({
      oppid,
      title: clean(title) || clean(stripTags(title)) || null,
      href: normalizeTalCandidateUrl(href),
      vacancyType,
      org,
      location,
      advertisingDateRaw,
      closingDateRaw
    });
  }

  return tiles;
}

function parseDetailLabels(detailHtml) {
  // Labels are rendered as <h4>Label</h4><p>Value</p>
  const labels = {};
  const re = /<h4>([^<]+)<\/h4>\s*<p>([\s\S]*?)<\/p>/gi;
  let m;
  while ((m = re.exec(detailHtml)) !== null) {
    const key = stripTags(m[1]);
    const value = stripTags(m[2]);
    if (!key) continue;
    if (!value) continue;
    // Keep first occurrence.
    if (!(key in labels)) labels[key] = value;
  }
  return labels;
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      accept: 'text/html'
    }
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}${body ? ` :: ${body.slice(0, 200)}` : ''}`);
  }
  return res.text();
}

async function mapWithConcurrency(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;

  async function worker() {
    while (true) {
      const i = next;
      next += 1;
      if (i >= items.length) return;
      results[i] = await fn(items[i], i);
    }
  }

  const workers = [];
  const n = Math.max(1, Math.min(limit, items.length));
  for (let i = 0; i < n; i += 1) workers.push(worker());
  await Promise.all(workers);
  return results;
}

function rowToTSV(row) {
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

  const jobboardUrl = clean(process.env.PUBLICJOBS_IE_JOBBOARD_URL) || DEFAULT_JOBBOARD_URL;
  const maxEvents = Number(clean(args['max-events']) || process.env.PUBLICJOBS_MAX_EVENTS || DEFAULT_MAX_EVENTS);

  const shouldFetchDetails = (() => {
    if (args['fetch-details'] !== undefined) return parseBool(args['fetch-details'], false);
    if (process.env.PUBLICJOBS_FETCH_DETAILS !== undefined) return parseBool(process.env.PUBLICJOBS_FETCH_DETAILS, false);
    // Default: only fetch details when scoped by date.
    return Boolean(startDate);
  })();

  const detailConcurrency = Number(clean(args['detail-concurrency']) || process.env.PUBLICJOBS_DETAIL_CONCURRENCY || 4);

  const html = await fetchText(jobboardUrl);
  const tiles = extractTiles(html);

  // Newest first (approx): advertising date desc.
  const enriched = tiles
    .map((t) => {
      const adDate = parseTalDateToISO(t.advertisingDateRaw);
      return { ...t, advertisingDate: adDate };
    })
    .filter((t) => !startDate || (t.advertisingDate && t.advertisingDate >= startDate))
    .slice(0, maxEvents);

  let detailsByOpp = {};
  if (shouldFetchDetails) {
    const detailUrls = enriched
      .map((t) => ({ oppid: t.oppid, url: t.href }))
      .filter((x) => clean(x.url));

    const fetched = await mapWithConcurrency(detailUrls, detailConcurrency, async (item) => {
      const detailHtml = await fetchText(item.url);
      return { oppid: item.oppid, labels: parseDetailLabels(detailHtml), detailHtml: null };
    });

    detailsByOpp = Object.fromEntries(fetched.map((x) => [x.oppid, x.labels]));
  }

  const rows = [];

  for (const t of enriched) {
    const evidenceUrl = normalizeTalCandidateUrl(t.href) || normalizeTalCandidateUrl(jobboardUrl);
    const labels = detailsByOpp[t.oppid] || {};

    const advertisingDate =
      t.advertisingDate ||
      parseTalDateToISO(labels['Advertising Date']) ||
      todayISODate();

    const org = clean(t.org) || clean(labels['Department/Organisation']) || clean(labels['Department/Authority']) || '';
    const location = clean(t.location) || clean(labels['Location']) || '';

    const jobTitle = clean(t.title) || clean(labels['Grade']) || `Vacancy ${t.oppid}`;

    const detailText = Object.entries(labels)
      .filter(([k]) => ['Salary', 'Pay', 'Reference', 'Competition Reference', 'Grade', 'Contract', 'Working pattern'].some((x) => k.toLowerCase().includes(x.toLowerCase())))
      .map(([k, v]) => `${k}: ${v}`)
      .join(' | ');

    const signalStrength = computeStrength({ title: jobTitle, detailText });

    const extractedEntities = {
      vacancy_id: t.oppid,
      job_title: jobTitle,
      org,
      location,
      vacancy_type: clean(t.vacancyType) || null,
      advertising_date: advertisingDate,
      closing_date: parseTalDateToISO(t.closingDateRaw) || parseTalDateToISO(labels['Closing Date for Application']) || null,
      grade: clean(labels['Grade']) || null,
      contract: clean(labels['Contract']) || null,
      working_pattern: clean(labels['Working pattern']) || null,
      salary: null,
      ref_no: null
    };

    // Try to discover a reference number / competition reference if present.
    for (const [k, v] of Object.entries(labels)) {
      const lk = k.toLowerCase();
      if (!extractedEntities.salary && (lk.includes('salary') || lk.includes('pay') || lk.includes('remuneration'))) {
        extractedEntities.salary = clean(v);
        continue;
      }
      if (lk.includes('reference') || lk.includes('competition')) {
        extractedEntities.ref_no = clean(v);
        break;
      }
    }

    const evidenceText = [
      jobTitle,
      org ? `Org: ${org}` : null,
      location ? `Location: ${location}` : null
    ]
      .filter(Boolean)
      .join(' | ')
      .slice(0, 800);

    const rawPayload = {
      source: 'publicjobs.tal.net',
      jobboard_url: normalizeTalCandidateUrl(jobboardUrl),
      tile: {
        oppid: t.oppid,
        title: t.title,
        href: normalizeTalCandidateUrl(t.href),
        vacancy_type: clean(t.vacancyType),
        org: clean(t.org),
        location: clean(t.location),
        advertising_date_raw: clean(t.advertisingDateRaw),
        closing_date_raw: clean(t.closingDateRaw)
      },
      detail_labels: labels
    };

    rows.push({
      tenant_id: tenantId,
      signal_type: 'hiring',
      signal_source: 'publicjobs_ie',
      source_event_id: `talnet_opp:${t.oppid}`,
      signal_date: advertisingDate,
      evidence_url: evidenceUrl,
      evidence_text: evidenceText,
      buyer_id: '',
      buyer_name_raw: org,
      signal_strength: String(signalStrength.toFixed(3)),
      extracted_entities: extractedEntities,
      raw_payload: rawPayload
    });
  }

  rows.sort((a, b) => {
    if (a.signal_date !== b.signal_date) return a.signal_date < b.signal_date ? 1 : -1;
    return a.source_event_id < b.source_event_id ? -1 : 1;
  });

  if (dryRun) {
    console.error(
      `publicjobs-ie: dry-run rows=${rows.length} tenant_id=${tenantId} start_date=${startDate || 'null'} fetch_details=${shouldFetchDetails}`
    );
  }

  for (const row of rows) {
    process.stdout.write(`${rowToTSV(row)}\n`);
  }
}

main().catch((err) => {
  console.error(String(err?.stack || err));
  process.exit(1);
});
