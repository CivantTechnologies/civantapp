#!/usr/bin/env node

import crypto from 'node:crypto';

process.stdout.on('error', (err) => {
  if (err && err.code === 'EPIPE') process.exit(0);
});

const BOAMP_API_URL = 'https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records';

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

function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.toLowerCase() === 'null') return null;
  return text;
}

function parseBoampDate(raw) {
  const text = clean(raw);
  if (!text) return { date: null, ts: null };

  const direct = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (direct) {
    const isoDate = `${direct[1]}-${direct[2]}-${direct[3]}`;
    return { date: isoDate, ts: `${isoDate}T00:00:00.000Z` };
  }

  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return { date: null, ts: null };
  const ts = parsed.toISOString();
  return { date: ts.slice(0, 10), ts };
}

function parseNumber(value) {
  const text = clean(value);
  if (!text) return null;
  const normalized = text.replace(',', '.').replace(/[^0-9.-]/g, '');
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isNaN(parsed) ? null : parsed;
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
    for (const item of node) collectCpvCodesFromNode(item, set, inCpvContext);
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

  if (inCpvContext) addCpvCodesFromText(node, set);
}

function collectCpvCodes(record) {
  const set = new Set();

  for (const [key, value] of Object.entries(record || {})) {
    if (/cpv/i.test(String(key))) {
      collectCpvCodesFromNode(value, set, true);
    }
  }

  const payloads = [record?.gestion, record?.donnees]
    .map(parseMaybeJson)
    .filter(Boolean);

  for (const payload of payloads) {
    collectCpvCodesFromNode(payload, set, false);
  }

  return Array.from(set).sort();
}

function inferNoticeType(record) {
  const nature = clean(record?.nature)?.toLowerCase() || '';
  if (nature.includes('attrib') || nature.includes('award')) return 'award';
  if (nature.includes('rectif') || nature.includes('corrig')) return 'corrigendum';
  return 'tender';
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

async function fetchJsonWithRetry(url, maxRetries = 4) {
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
        const message = await response.text().catch(() => '');
        throw new Error(`HTTP ${response.status}: ${message.slice(0, 180)}`);
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

function asStartDate(raw) {
  const text = clean(raw);
  if (!text) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

async function main() {
  const args = parseArgs(process.argv);
  const tenantId = clean(args['tenant-id'] || args.tenant_id);
  if (!tenantId) {
    console.error('ERROR: --tenant-id is required');
    process.exit(1);
  }

  const dryRun = toBool(args['dry-run'], false);
  const startDate = asStartDate(args['start-date'] || args.start_date);
  const pageSize = Math.max(10, Number(args['page-size'] || 100));
  const maxPages = Math.max(1, Number(args['max-pages'] || 30));

  const out = [];
  let fetchedRows = 0;
  let filteredRows = 0;
  let maxPublishedAt = null;

  for (let page = 0; page < maxPages; page += 1) {
    const offset = page * pageSize;
    const params = new URLSearchParams();
    params.set('limit', String(pageSize));
    params.set('offset', String(offset));
    params.set('order_by', 'dateparution desc,idweb desc');
    if (startDate) {
      params.set('where', `dateparution >= '${startDate}'`);
    }

    const url = `${BOAMP_API_URL}?${params.toString()}`;
    const payload = await fetchJsonWithRetry(url);
    const rows = Array.isArray(payload?.results) ? payload.results : [];

    if (!rows.length) break;
    fetchedRows += rows.length;

    for (const row of rows) {
      const sourceNoticeId = clean(row?.idweb) || clean(row?.id);
      const title = clean(row?.objet) || clean(row?.intitule) || clean(row?.designation);
      if (!sourceNoticeId || !title) continue;

      const publication = parseBoampDate(row?.dateparution);
      if (!publication.ts) continue;

      const futureCutoffMs = Date.now() + 24 * 60 * 60 * 1000;
      if (new Date(publication.ts).getTime() > futureCutoffMs) continue;

      const deadline = parseBoampDate(row?.datelimitereponse);
      const cpvCodes = collectCpvCodes(row);
      const sourceUrl = clean(row?.url_avis) || clean(row?.urlavis) || `https://www.boamp.fr/avis/detail/${sourceNoticeId}`;
      const buyerName = clean(row?.nomacheteur) || clean(row?.organisme) || clean(row?.nomorganisme);
      const estimatedValue = parseNumber(row?.montant) ?? parseNumber(row?.montantmarche) ?? parseNumber(row?.valeurestimee);

      const tenderId = `BOAMP_FR:${sourceNoticeId}`;
      const data = {
        source: 'BOAMP_FR',
        country: 'FR',
        canonical_id: tenderId,
        source_notice_id: sourceNoticeId,
        title,
        buyer_name: buyerName,
        notice_type: inferNoticeType(row),
        publication_date: publication.date,
        deadline_date: deadline.date,
        cpv_codes: cpvCodes,
        estimated_value: estimatedValue,
        currency: 'EUR',
        procedure: clean(row?.procedure_libelle) || clean(row?.type_procedure),
        source_url: sourceUrl,
        first_seen_at: publication.ts,
        version_count: 1,
        raw: row
      };

      const versionBasis = {
        title: data.title,
        buyer_name: data.buyer_name,
        publication_date: data.publication_date,
        deadline_date: data.deadline_date,
        cpv_codes: Array.isArray(data.cpv_codes) ? data.cpv_codes.join(',') : '',
        estimated_value: data.estimated_value,
        procedure: data.procedure,
        source_url: data.source_url,
        notice_type: data.notice_type
      };

      const versionHash = md5Hex(JSON.stringify(versionBasis));

      out.push([
        safeTsvField(tenantId),
        safeTsvField(tenderId),
        safeTsvField('BOAMP_FR'),
        safeTsvField(publication.ts),
        safeTsvField(JSON.stringify(data)),
        safeTsvField(versionHash)
      ].join('\t'));

      filteredRows += 1;
      if (!maxPublishedAt || publication.ts > maxPublishedAt) maxPublishedAt = publication.ts;
    }

    if (rows.length < pageSize) break;
  }

  console.error(`boamp_fr_incremental fetched_rows=${fetchedRows} staged_rows=${filteredRows} start_date=${startDate || '<none>'} max_published_at=${maxPublishedAt || '<none>'} dry_run=${dryRun}`);

  if (dryRun) {
    for (const line of out.slice(0, 5)) {
      console.error(`sample_tsv: ${line.slice(0, 200)}...`);
    }
  }

  process.stdout.write(out.join('\n'));
  if (out.length) process.stdout.write('\n');
}

main().catch((error) => {
  console.error(`ERROR: ${error?.message || String(error)}`);
  process.exit(1);
});
