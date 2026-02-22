#!/usr/bin/env node

import fs from 'node:fs/promises';

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

function toInt(raw, fallback) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

function clean(value) {
  if (value === undefined || value === null) return '';
  return String(value).trim();
}

function parseYmd(value) {
  const text = clean(value);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const d = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

function formatYmd(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return '';
  return date.toISOString().slice(0, 10);
}

function addMonths(anchor, months) {
  if (!(anchor instanceof Date) || Number.isNaN(anchor.getTime())) return null;
  if (!Number.isFinite(months) || months <= 0) return null;
  const out = new Date(anchor.getTime());
  const day = out.getUTCDate();
  out.setUTCDate(1);
  out.setUTCMonth(out.getUTCMonth() + Math.trunc(months));
  const monthEnd = new Date(Date.UTC(out.getUTCFullYear(), out.getUTCMonth() + 1, 0)).getUTCDate();
  out.setUTCDate(Math.min(day, monthEnd));
  return out;
}

function diffDays(a, b) {
  if (!(a instanceof Date) || !(b instanceof Date)) return null;
  const ms = a.getTime() - b.getTime();
  return Math.floor(ms / (24 * 60 * 60 * 1000));
}

function normalizeTedXmlUrl(sourceNoticeId, xmlUrl, sourceUrl) {
  const explicit = clean(xmlUrl);
  if (explicit) return explicit;
  const notice = clean(sourceNoticeId);
  if (notice) return `https://ted.europa.eu/en/notice/${encodeURIComponent(notice)}/xml`;
  const detailUrl = clean(sourceUrl);
  if (!detailUrl) return '';
  const m = detailUrl.match(/\/detail\/([^/?#]+)/i);
  if (m && m[1]) {
    return `https://ted.europa.eu/en/notice/${encodeURIComponent(m[1])}/xml`;
  }
  return '';
}

function parseDurationMonths(valueText, attrText) {
  const raw = clean(valueText).replace(',', '.');
  if (!/^\d+(\.\d+)?$/.test(raw)) return null;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) return null;

  const unitMatch = clean(attrText).match(/unitCode\s*=\s*["']([^"']+)["']/i);
  const unit = (unitMatch?.[1] || '').toUpperCase();

  let months = null;
  if (!unit || unit.startsWith('MON') || unit === 'MTH') {
    months = Math.ceil(value);
  } else if (unit.startsWith('ANN') || unit.startsWith('YEA')) {
    months = Math.ceil(value * 12);
  } else if (unit.startsWith('DAY')) {
    months = Math.ceil(value / 30);
  } else if (unit.startsWith('WEE')) {
    months = Math.ceil((value * 7) / 30);
  }

  if (!Number.isFinite(months) || months < 1 || months > 240) return null;
  return months;
}

function extractFromBlock(block) {
  const startMatch = block.match(/<cbc:StartDate[^>]*>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*<\/cbc:StartDate>/i);
  const endMatch = block.match(/<cbc:EndDate[^>]*>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*<\/cbc:EndDate>/i);

  let durationMonths = null;
  for (const m of block.matchAll(/<cbc:DurationMeasure([^>]*)>\s*([^<]+)\s*<\/cbc:DurationMeasure>/gi)) {
    const parsed = parseDurationMonths(m[2], m[1]);
    if (parsed && (!durationMonths || parsed > durationMonths)) durationMonths = parsed;
  }

  return {
    startDate: startMatch?.[1] || '',
    endDate: endMatch?.[1] || '',
    durationMonths: durationMonths || null
  };
}

function extractCandidates(xmlText) {
  const xml = clean(xmlText);
  if (!xml) return [];

  const candidates = [];
  for (const blockMatch of xml.matchAll(/<cac:ContractPeriod\b[\s\S]*?<\/cac:ContractPeriod>/gi)) {
    const c = extractFromBlock(blockMatch[0]);
    if (c.startDate || c.endDate || c.durationMonths) candidates.push(c);
  }

  if (candidates.length) return candidates;

  let fallbackDuration = null;
  for (const m of xml.matchAll(/<cbc:DurationMeasure([^>]*)>\s*([^<]+)\s*<\/cbc:DurationMeasure>/gi)) {
    const parsed = parseDurationMonths(m[2], m[1]);
    if (parsed && (!fallbackDuration || parsed > fallbackDuration)) fallbackDuration = parsed;
  }
  if (fallbackDuration) {
    candidates.push({ startDate: '', endDate: '', durationMonths: fallbackDuration });
  }
  return candidates;
}

function chooseBest(row, candidates) {
  if (!candidates.length) return null;

  const awardDate = parseYmd(row.awardDate);
  let startDate = null;
  let endDate = null;
  let durationMonths = null;

  for (const c of candidates) {
    const s = parseYmd(c.startDate);
    const e = parseYmd(c.endDate);
    if (s && (!startDate || s < startDate)) startDate = s;
    if (e && (!endDate || e > endDate)) endDate = e;
    if (Number.isFinite(c.durationMonths) && (!durationMonths || c.durationMonths > durationMonths)) {
      durationMonths = c.durationMonths;
    }
  }

  let method = '';
  if (!endDate && durationMonths) {
    const anchor = startDate || awardDate;
    endDate = addMonths(anchor, durationMonths);
    if (endDate) {
      method = startDate ? 'ted_xml_start_plus_duration' : 'ted_xml_award_plus_duration';
    }
  } else if (endDate) {
    method = 'ted_xml_contractperiod_enddate';
  }

  if (!endDate || !awardDate) return null;

  const daysFromAward = diffDays(endDate, awardDate);
  if (daysFromAward === null) return null;

  // Conservative plausibility window to avoid polluting facts with unrelated dates.
  if (daysFromAward < -365 || daysFromAward > 365 * 20) return null;

  return {
    startDate: formatYmd(startDate),
    durationMonths: durationMonths || '',
    endDate: formatYmd(endDate),
    method
  };
}

async function fetchWithRetry(url, retries = 4) {
  let lastError = null;
  for (let i = 1; i <= retries; i += 1) {
    try {
      const res = await fetch(url, {
        headers: {
          accept: 'application/xml,text/xml,*/*',
          'user-agent': 'CivantBot/1.0 (+https://civant.ai)'
        }
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (error) {
      lastError = error;
      if (i < retries) {
        await new Promise((resolve) => setTimeout(resolve, 400 * i * i));
      }
    }
  }
  throw lastError || new Error('TED XML fetch failed');
}

function safeTsv(value) {
  return String(value ?? '')
    .replace(/\t/g, ' ')
    .replace(/\r/g, ' ')
    .replace(/\n/g, ' ');
}

async function main() {
  const args = parseArgs(process.argv);
  const inputPath = clean(args.input || args.i);
  const outputPath = clean(args.output || args.o);
  if (!inputPath || !outputPath) {
    console.error('Usage: node extract-fr-award-hints-from-ted-xml.mjs --input <candidates.tsv> --output <hints.tsv> [--concurrency 6]');
    process.exit(1);
  }

  const concurrency = Math.max(1, Math.min(16, toInt(args.concurrency, 6)));
  const raw = await fs.readFile(inputPath, 'utf8');
  const rows = raw
    .split('\n')
    .map((line) => line.trimEnd())
    .filter(Boolean)
    .map((line) => {
      const [tenantId, awardCanonicalId, awardDate, sourceNoticeId, xmlUrl, sourceUrl] = line.split('\t');
      return {
        tenantId: clean(tenantId),
        awardCanonicalId: clean(awardCanonicalId),
        awardDate: clean(awardDate),
        sourceNoticeId: clean(sourceNoticeId),
        xmlUrl: clean(xmlUrl),
        sourceUrl: clean(sourceUrl)
      };
    })
    .filter((r) => r.tenantId && r.awardCanonicalId);

  const out = [];
  let cursor = 0;
  let fetched = 0;
  let enriched = 0;
  let failed = 0;

  async function worker() {
    while (true) {
      const idx = cursor;
      cursor += 1;
      if (idx >= rows.length) return;

      const row = rows[idx];
      const url = normalizeTedXmlUrl(row.sourceNoticeId, row.xmlUrl, row.sourceUrl);
      if (!url) continue;

      try {
        const xml = await fetchWithRetry(url);
        fetched += 1;
        const candidates = extractCandidates(xml);
        const best = chooseBest(row, candidates);
        if (!best) continue;

        enriched += 1;
        out.push([
          safeTsv(row.tenantId),
          safeTsv(row.awardCanonicalId),
          safeTsv(best.startDate),
          safeTsv(best.durationMonths),
          safeTsv(best.endDate),
          safeTsv(row.sourceNoticeId),
          safeTsv(url),
          safeTsv(best.method)
        ].join('\t'));
      } catch (error) {
        failed += 1;
        const msg = String(error?.message || error).slice(0, 180);
        console.error(`ted_xml_fetch_failed canonical_id=${row.awardCanonicalId} notice_id=${row.sourceNoticeId || '<none>'} url=${url} err=${msg}`);
      }
    }
  }

  const workers = Array.from({ length: concurrency }, () => worker());
  await Promise.all(workers);

  await fs.writeFile(outputPath, out.join('\n') + (out.length ? '\n' : ''), 'utf8');
  console.error(
    `ted_fr_xml_extract candidates=${rows.length} fetched=${fetched} enriched=${enriched} failed=${failed} output=${outputPath}`
  );
}

main().catch((error) => {
  console.error(`ERROR: ${error?.message || String(error)}`);
  process.exit(1);
});
