#!/usr/bin/env node
/*
  eTenders Ireland incremental fetch (no DB writes).

  Fetches the official "Latest CfTs" listing and emits TSV rows suitable for psql \copy.

  Output TSV columns:
    tenant_id, tender_id, source, published_at, data_json, version_hash

  Notes:
  - Source is the official eTenders portal HTML listing (no auth).
  - Replay-safe when paired with DB-side upserts on tender_id + (tender_id, version_hash).
*/

import crypto from 'node:crypto';

process.stdout.on('error', (err) => {
  if (err && err.code === 'EPIPE') process.exit(0);
});

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 2; i < argv.length; i++) {
    const token = argv[i];
    if (!token.startsWith('--')) {
      args._.push(token);
      continue;
    }
    const key = token.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith('--')) {
      args[key] = next;
      i++;
    } else {
      args[key] = 'true';
    }
  }
  return args;
}

function toBool(raw, fallback = false) {
  if (raw === undefined || raw === null || raw === '') return fallback;
  const v = String(raw).toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(v)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(v)) return false;
  return fallback;
}

function decodeHtmlEntities(input) {
  const s = String(input || '');
  return s
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/\r/g, '')
    .replace(/\n/g, ' ');
}

function stripTags(html) {
  return decodeHtmlEntities(String(html || '').replace(/<[^>]*>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim();
}

function extractFirst(re, text) {
  const m = re.exec(text);
  return m ? m[1] : '';
}

function md5Hex(text) {
  return crypto.createHash('md5').update(text).digest('hex');
}

function safeTsvField(value) {
  // \copy ... format text delimiter \t: do not allow tabs/newlines.
  return String(value ?? '')
    .replace(/\t/g, ' ')
    .replace(/\r/g, ' ')
    .replace(/\n/g, ' ');
}

function parseEtendersDate(raw) {
  const s = stripTags(raw);
  if (!s) return null;
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms);
}

function toIsoDate(d) {
  if (!(d instanceof Date) || !Number.isFinite(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function toIsoTs(d) {
  if (!(d instanceof Date) || !Number.isFinite(d.getTime())) return null;
  return d.toISOString();
}

function parseLatestCftsTable(html) {
  // Find the table pagination id (d-<id>-p=) dynamically.
  const tableId = extractFirst(/\bd-(\d+)-p=/, html);

  const tbodyMatch = /<tbody[^>]*>([\s\S]*?)<\/tbody>/i.exec(html);
  const tbody = tbodyMatch ? tbodyMatch[1] : '';
  if (!tbody) return { tableId, rows: [] };

  const trMatches = tbody.split(/<tr[^>]*>/i).slice(1);
  const rows = [];
  for (const trChunk of trMatches) {
    const tr = trChunk.split(/<\/tr>/i)[0] || '';
    const tds = tr.split(/<td[^>]*>/i).slice(1).map((c) => c.split(/<\/td>/i)[0] || '');
    if (tds.length < 8) continue;

    // Column layout observed on Latest CfTs:
    // 0 idx, 1 title(a href prepareView...), 2 resource id, 3 CA, 4 info (img title='...'),
    // 5 date published, 6 submission deadline, 7 procedure, 8 status, 9 pdf, 10 award date, 11 est value, 12 cycle

    const titleTd = tds[1] || '';
    const title = stripTags(titleTd);
    const resourceId = stripTags(tds[2] || '').replace(/\D/g, '');
    if (!resourceId) continue;

    const ca = stripTags(tds[3] || '');

    const publishedAt = parseEtendersDate(tds[5] || '');
    const deadlineAt = parseEtendersDate(tds[6] || '');

    const procedure = stripTags(tds[7] || '');
    const status = stripTags(tds[8] || '');

    const estValueRaw = stripTags(tds[11] || '');
    const estimatedValue = estValueRaw ? Number(estValueRaw) : null;

    const cycleRaw = stripTags(tds[12] || '');
    const cycle = cycleRaw ? Number(cycleRaw) : null;

    const url = `https://www.etenders.gov.ie/epps/cft/prepareViewCfTWS.do?resourceId=${encodeURIComponent(resourceId)}`;

    rows.push({
      resourceId,
      title,
      contractingAuthority: ca,
      publishedAt,
      deadlineAt,
      procedure,
      status,
      estimatedValue: Number.isFinite(estimatedValue) ? estimatedValue : null,
      cycle: Number.isFinite(cycle) ? cycle : null,
      url
    });
  }

  return { tableId, rows };
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      'Accept': 'text/html,application/xhtml+xml',
      'User-Agent': 'CivantBot/1.0 (+https://civant.ai)'
    }
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}: ${text.slice(0, 200)}`);
  }
  return text;
}

function asStartDate(raw) {
  const s = String(raw || '').trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return new Date(`${s}T00:00:00.000Z`);
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return null;
  return new Date(ms);
}

async function main() {
  const args = parseArgs(process.argv);

  const tenantId = String(args['tenant-id'] || args.tenant_id || '').trim();
  if (!tenantId) {
    console.error('ERROR: --tenant-id is required');
    process.exit(1);
  }

  const dryRun = toBool(args['dry-run'], false);
  const startDate = asStartDate(args['start-date'] || args.start_date) || null;
  const maxPages = Math.max(1, Number(args['max-pages'] || 30));

  const base = 'https://www.etenders.gov.ie';
  const firstUrl = `${base}/epps/quickSearchAction.do?searchType=cftFTS&latest=true`;

  let tableId = '';
  let allRows = [];

  for (let page = 1; page <= maxPages; page++) {
    const pageUrl = page === 1
      ? firstUrl
      : `${base}/epps/quickSearchAction.do?d-${encodeURIComponent(tableId)}-p=${page}&searchType=cftFTS&latest=true`;

    const html = await fetchText(pageUrl);
    const parsed = parseLatestCftsTable(html);
    if (!tableId) tableId = parsed.tableId;

    if (page > 1 && !tableId) {
      throw new Error('Could not resolve pagination table id (d-<id>-p=).');
    }

    if (!parsed.rows.length) {
      break;
    }

    allRows = allRows.concat(parsed.rows);

    // Stop early if everything on this page is older than our start date.
    if (startDate) {
      const newest = parsed.rows.reduce((acc, r) => {
        const t = r.publishedAt?.getTime() ?? 0;
        return Math.max(acc, t);
      }, 0);
      const oldest = parsed.rows.reduce((acc, r) => {
        const t = r.publishedAt?.getTime() ?? Number.POSITIVE_INFINITY;
        return Math.min(acc, t);
      }, Number.POSITIVE_INFINITY);

      if (newest && oldest && oldest < startDate.getTime()) {
        // Pages are typically sorted by published date desc.
        break;
      }
    }
  }

  const filtered = startDate
    ? allRows.filter((r) => (r.publishedAt ? r.publishedAt.getTime() >= startDate.getTime() : false))
    : allRows;

  // Build TSV rows for DB ingest.
  const out = [];
  let maxPublishedAt = null;

  for (const r of filtered) {
    const tenderId = `ETENDERS_IE:${r.resourceId}`;
    const publishedIso = toIsoTs(r.publishedAt);
    if (!publishedIso) continue;


    const publicationDate = toIsoDate(r.publishedAt);
    
    const deadlineDate = toIsoDate(r.deadlineAt);

    const futureCutoffMs = Date.now() + 24 * 60 * 60 * 1000;
    if (r.publishedAt && r.publishedAt.getTime() > futureCutoffMs) continue;

    if (!maxPublishedAt || publishedIso > maxPublishedAt) maxPublishedAt = publishedIso;

    const data = {
      source: 'ETENDERS_IE',
      country: 'IE',
      canonical_id: tenderId,
      source_notice_id: r.resourceId,
      title: r.title,
      buyer_name: r.contractingAuthority,
      contracting_authority: r.contractingAuthority,
      notice_type: 'tender',
      publication_date: publicationDate,
      deadline_date: deadlineDate,
      procedure: r.procedure || null,
      status: r.status || null,
      estimated_value: r.estimatedValue,
      currency: 'EUR',
      cft_active_cycle: r.cycle,
      source_url: r.url,
      first_seen_at: publishedIso,
      version_count: 1
    };

    const versionBasis = {
      title: data.title,
      buyer_name: data.buyer_name,
      publication_date: data.publication_date,
      deadline_date: data.deadline_date,
      procedure: data.procedure,
      status: data.status,
      estimated_value: data.estimated_value,
      cft_active_cycle: data.cft_active_cycle,
      source_url: data.source_url
    };

    const versionHash = md5Hex(JSON.stringify(versionBasis));

    out.push([
      safeTsvField(tenantId),
      safeTsvField(tenderId),
      safeTsvField('ETENDERS_IE'),
      safeTsvField(publishedIso),
      safeTsvField(JSON.stringify(data)),
      safeTsvField(versionHash)
    ].join('\t'));
  }

  console.error(`etenders_ie_incremental fetched_rows=${allRows.length} filtered_rows=${filtered.length} start_date=${startDate ? startDate.toISOString().slice(0,10) : '<none>'} max_published_at=${maxPublishedAt || '<none>'} dry_run=${dryRun}`);

  if (dryRun) {
    const sample = out.slice(0, 5);
    for (const line of sample) {
      console.error(`sample_tsv: ${line.slice(0, 200)}...`);
    }
  }

  process.stdout.write(out.join('\n'));
  if (out.length) process.stdout.write('\n');
}

main().catch((err) => {
  console.error(`ERROR: ${err?.message || String(err)}`);
  process.exit(1);
});
