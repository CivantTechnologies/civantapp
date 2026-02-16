#!/usr/bin/env node

import crypto from 'node:crypto';

process.stdout.on('error', (err) => {
  if (err && err.code === 'EPIPE') process.exit(0);
});

const TED_API_URL = 'https://api.ted.europa.eu/v3/notices/search';
const DEFAULT_FIELDS = [
  'ND',
  'PD',
  'DS',
  'CY',
  'buyer-name',
  'title-proc',
  'notice-type',
  'BT-137-Lot',
  'links'
];
const PREFERRED_LANG_ORDER = ['ENG', 'FRA', 'SPA', 'MUL'];

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

function toBool(raw, fallback = false) {
  if (raw === undefined || raw === null || raw === '') return fallback;
  const value = String(raw).toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(value)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(value)) return false;
  return fallback;
}

function toInt(raw, fallback) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.toLowerCase() === 'null') return null;
  return text;
}

function parseCountryList(raw) {
  const values = String(raw || 'IRL,FRA,ESP')
    .split(',')
    .map((v) => String(v || '').trim().toUpperCase())
    .filter(Boolean);
  const unique = [...new Set(values)];
  if (!unique.length) throw new Error('At least one country is required.');
  return unique;
}

function parseStartDate(raw) {
  const text = clean(raw);
  if (!text) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  if (/^\d{8}$/.test(text)) {
    return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
  }

  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function defaultStartDate(daysBack = 30) {
  const ms = Date.now() - daysBack * 24 * 60 * 60 * 1000;
  return new Date(ms).toISOString().slice(0, 10);
}

function parseTedDate(raw) {
  const text = clean(raw);
  if (!text) return { date: null, ts: null };

  let isoCandidate = text;
  const noTimeWithOffset = text.match(/^(\d{4}-\d{2}-\d{2})([+-]\d{2}:\d{2})$/);
  if (noTimeWithOffset) isoCandidate = `${noTimeWithOffset[1]}T00:00:00${noTimeWithOffset[2]}`;
  const yyyymmdd = text.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (yyyymmdd) isoCandidate = `${yyyymmdd[1]}-${yyyymmdd[2]}-${yyyymmdd[3]}T00:00:00Z`;
  const dateOnly = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (dateOnly) isoCandidate = `${dateOnly[1]}-${dateOnly[2]}-${dateOnly[3]}T00:00:00Z`;

  const parsed = new Date(isoCandidate);
  if (Number.isNaN(parsed.getTime())) return { date: null, ts: null };
  const ts = parsed.toISOString();
  return { date: ts.slice(0, 10), ts };
}

function pickLocalizedText(value) {
  if (!value) return null;
  if (typeof value === 'string') return clean(value);
  if (Array.isArray(value)) return clean(value[0]);
  if (typeof value !== 'object') return null;

  const entries = Object.entries(value);
  if (!entries.length) return null;

  const sorted = entries.sort((a, b) => {
    const ia = PREFERRED_LANG_ORDER.indexOf(String(a[0] || '').toUpperCase());
    const ib = PREFERRED_LANG_ORDER.indexOf(String(b[0] || '').toUpperCase());
    const pa = ia === -1 ? 999 : ia;
    const pb = ib === -1 ? 999 : ib;
    if (pa !== pb) return pa - pb;
    return String(a[0]).localeCompare(String(b[0]));
  });

  for (const [, candidate] of sorted) {
    if (typeof candidate === 'string') {
      const text = clean(candidate);
      if (text) return text;
      continue;
    }
    if (Array.isArray(candidate)) {
      const text = clean(candidate[0]);
      if (text) return text;
      continue;
    }
    if (candidate && typeof candidate === 'object') {
      const text = clean(candidate['#text'] ?? candidate.text ?? candidate.value);
      if (text) return text;
    }
  }

  return null;
}

function pickSourceUrl(links) {
  if (!links || typeof links !== 'object') return null;
  const candidates = [
    links?.html?.ENG,
    links?.html?.FRA,
    links?.html?.SPA,
    links?.html?.MUL,
    links?.htmlDirect?.ENG,
    links?.htmlDirect?.FRA,
    links?.htmlDirect?.SPA,
    links?.xml?.MUL
  ];
  for (const candidate of candidates) {
    const url = clean(candidate);
    if (url) return url;
  }
  return null;
}

function md5Hex(value) {
  return crypto.createHash('md5').update(value).digest('hex');
}

function safeTsvField(value) {
  return String(value ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, ' ')
    .replace(/\r/g, ' ')
    .replace(/\n/g, ' ');
}

async function sleep(ms) {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function postTed(payload, retries = 6) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const response = await fetch(TED_API_URL, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          accept: 'application/json',
          'user-agent': 'CivantBot/1.0 (+https://civant.ai)'
        },
        body: JSON.stringify(payload)
      });

      const text = await response.text();
      let data = null;
      try {
        data = JSON.parse(text);
      } catch {
        data = null;
      }

      if (!response.ok || !data) {
        const message = data?.message || text.slice(0, 180);
        throw new Error(`TED HTTP ${response.status}: ${message}`);
      }

      if (data.error || (typeof data.message === 'string' && data.message.toLowerCase().includes('invalid'))) {
        throw new Error(`TED API error: ${data.message || JSON.stringify(data.error)}`);
      }

      return data;
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        const waitMs = Math.min(30000, 1200 * attempt * attempt);
        await sleep(waitMs);
      }
    }
  }

  throw lastError || new Error('TED request failed');
}

function buildQuery(country, sinceYmd) {
  return `CY = ${country} AND (PD >= ${sinceYmd} OR DS >= ${sinceYmd})`;
}

function buildVersionBasis(data) {
  return {
    title: data.title,
    buyer_name: data.buyer_name,
    publication_date: data.publication_date,
    dispatch_date: data.dispatch_date,
    notice_type: data.notice_type,
    lot_count: data.lot_count,
    source_url: data.source_url
  };
}

async function main() {
  const args = parseArgs(process.argv);
  const tenantId = clean(args['tenant-id'] || args.tenant_id);
  if (!tenantId) {
    console.error('ERROR: --tenant-id is required');
    process.exit(1);
  }

  const dryRun = toBool(args['dry-run'], false);
  const countries = parseCountryList(args.countries || 'IRL,FRA,ESP');
  const startDate = parseStartDate(args['start-date'] || args.start_date) || defaultStartDate(30);
  const sinceYmd = startDate.replace(/-/g, '');
  const pageSize = Math.max(10, Math.min(250, toInt(args['page-size'], 100)));
  const maxPages = Math.max(1, toInt(args['max-pages'], 40));
  const sleepMs = Math.max(0, toInt(args['sleep-ms'], 120));

  const byTenderId = new Map();
  const fetchStats = [];
  let fetchedRows = 0;
  let maxPublishedAt = null;

  for (const country of countries) {
    let countryFetched = 0;
    let countryAccepted = 0;
    let token = null;

    for (let page = 1; page <= maxPages; page += 1) {
      const payload = {
        query: buildQuery(country, sinceYmd),
        fields: DEFAULT_FIELDS,
        limit: pageSize,
        paginationMode: 'ITERATION',
        scope: 'ALL'
      };
      if (token) payload.iterationNextToken = token;

      const data = await postTed(payload);
      const notices = Array.isArray(data?.notices) ? data.notices : [];
      const nextToken = clean(data?.iterationNextToken);

      if (!notices.length) break;
      countryFetched += notices.length;
      fetchedRows += notices.length;

      for (const notice of notices) {
        const sourceNoticeId = clean(notice?.ND) || clean(notice?.['publication-number']);
        if (!sourceNoticeId) continue;

        const publication = parseTedDate(notice?.PD);
        const dispatch = parseTedDate(notice?.DS);
        const publishedTs = publication.ts || dispatch.ts;
        const publishedDate = publication.date || dispatch.date;
        if (!publishedTs) continue;

        const title = pickLocalizedText(notice?.['title-proc']);
        const buyerName = pickLocalizedText(notice?.['buyer-name']);
        const noticeCountries = Array.isArray(notice?.CY)
          ? notice.CY.map((v) => clean(v)).filter(Boolean)
          : [];

        const sourceUrl = pickSourceUrl(notice?.links) || `https://ted.europa.eu/en/notice/${encodeURIComponent(sourceNoticeId)}/html`;
        const lotCount = Array.isArray(notice?.['BT-137-Lot']) ? notice['BT-137-Lot'].length : null;
        const tenderId = `TED:${sourceNoticeId}`;

        const dataPayload = {
          source: 'TED',
          country: clean(noticeCountries[0]) || country,
          countries: noticeCountries,
          canonical_id: tenderId,
          source_notice_id: sourceNoticeId,
          title,
          buyer_name: buyerName,
          notice_type: clean(notice?.['notice-type']),
          publication_date: publishedDate,
          dispatch_date: dispatch.date,
          lot_count: Number.isFinite(lotCount) ? lotCount : null,
          source_url: sourceUrl,
          first_seen_at: publishedTs,
          version_count: 1,
          raw_notice: notice
        };

        const versionHash = md5Hex(JSON.stringify(buildVersionBasis(dataPayload)));
        const existing = byTenderId.get(tenderId);
        if (!existing || existing.published_at < publishedTs) {
          byTenderId.set(tenderId, {
            tenant_id: tenantId,
            tender_id: tenderId,
            source: 'TED',
            published_at: publishedTs,
            data: dataPayload,
            version_hash: versionHash
          });
        }

        countryAccepted += 1;
        if (!maxPublishedAt || publishedTs > maxPublishedAt) maxPublishedAt = publishedTs;
      }

      token = nextToken;
      if (!token) break;
      await sleep(sleepMs);
    }

    fetchStats.push({ country, fetched: countryFetched, accepted: countryAccepted });
  }

  const outLines = Array.from(byTenderId.values()).map((row) => [
    safeTsvField(row.tenant_id),
    safeTsvField(row.tender_id),
    safeTsvField(row.source),
    safeTsvField(row.published_at),
    safeTsvField(JSON.stringify(row.data)),
    safeTsvField(row.version_hash)
  ].join('\t'));

  console.error(`ted_incremental fetched_rows=${fetchedRows} staged_rows=${outLines.length} start_date=${startDate} max_published_at=${maxPublishedAt || '<none>'} dry_run=${dryRun} countries=${countries.join(',')} max_pages=${maxPages}`);
  for (const stat of fetchStats) {
    console.error(`ted_incremental_country country=${stat.country} fetched=${stat.fetched} accepted=${stat.accepted}`);
  }
  if (dryRun) {
    for (const line of outLines.slice(0, 5)) {
      console.error(`sample_tsv: ${line.slice(0, 220)}...`);
    }
  }

  process.stdout.write(outLines.join('\n'));
  if (outLines.length) process.stdout.write('\n');
}

main().catch((error) => {
  console.error(`ERROR: ${error?.message || String(error)}`);
  process.exit(1);
});
