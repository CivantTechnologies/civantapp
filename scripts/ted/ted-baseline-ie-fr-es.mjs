#!/usr/bin/env node

import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';

const API_URL = 'https://api.ted.europa.eu/v3/notices/search';
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

function monthStart(raw) {
  const s = String(raw || '').trim();
  if (!/^\d{4}-\d{2}$/.test(s)) throw new Error(`Invalid month format: ${raw}. Expected YYYY-MM.`);
  const [yy, mm] = s.split('-').map(Number);
  if (mm < 1 || mm > 12) throw new Error(`Invalid month: ${raw}`);
  return new Date(Date.UTC(yy, mm - 1, 1));
}

function monthToKey(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  return `${y}-${m}`;
}

function nextMonth(d) {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1));
}

function endOfMonth(d) {
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0));
}

function monthRange(startMonth, endMonth) {
  const out = [];
  let cursor = monthStart(startMonth);
  const end = monthStart(endMonth);
  while (cursor.getTime() <= end.getTime()) {
    out.push(new Date(cursor));
    cursor = nextMonth(cursor);
  }
  return out;
}

function countryList(raw) {
  const values = String(raw || 'IRL,FRA,ESP')
    .split(',')
    .map((v) => v.trim().toUpperCase())
    .filter(Boolean);
  const unique = [...new Set(values)];
  if (!unique.length) throw new Error('At least one country is required.');
  return unique;
}

async function ensureDir(p) {
  await fsp.mkdir(p, { recursive: true });
}

async function readJson(file, fallback) {
  try {
    const raw = await fsp.readFile(file, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function writeJson(file, value) {
  await ensureDir(path.dirname(file));
  await fsp.writeFile(file, JSON.stringify(value, null, 2));
}

function tedQuery(country, monthDate) {
  const y = monthDate.getUTCFullYear();
  const m = String(monthDate.getUTCMonth() + 1).padStart(2, '0');
  const end = String(endOfMonth(monthDate).getUTCDate()).padStart(2, '0');
  return `CY = ${country} AND PD = (${y}${m}01 <> ${y}${m}${end})`;
}

async function sleep(ms) {
  if (ms <= 0) return;
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function postTed(payload, retries = 7) {
  let lastErr = null;
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const res = await fetch(API_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify(payload)
      });
      const text = await res.text();
      let data = null;
      try {
        data = JSON.parse(text);
      } catch {
        data = null;
      }
      if (!res.ok || !data) {
        const maybeMessage = data?.message || text.slice(0, 200);
        throw new Error(`TED HTTP ${res.status}: ${maybeMessage}`);
      }
      if (data.error || data.message?.startsWith('Value') || data.message?.startsWith('Invalid')) {
        throw new Error(`TED API error: ${data.message || JSON.stringify(data.error)}`);
      }
      return data;
    } catch (err) {
      lastErr = err;
      const waitMs = Math.min(30000, 1200 * (attempt + 1) * (attempt + 1));
      await sleep(waitMs);
    }
  }
  throw lastErr || new Error('TED API failed');
}

function newState(config) {
  return {
    version: 1,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    config,
    chunks: {}
  };
}

function chunkKey(country, month) {
  return `${country}:${month}`;
}

function chunkFile(outRoot, country, month) {
  const [y, m] = month.split('-');
  return path.join(outRoot, country, y, `${month}.ndjson`);
}

async function buildChunks(countries, months) {
  const list = [];
  for (const country of countries) {
    for (const monthDate of months) {
      const month = monthToKey(monthDate);
      list.push({
        country,
        month,
        query: tedQuery(country, monthDate)
      });
    }
  }
  return list;
}

async function runSizing({ chunks, limit, reportFile, rateSleepMs }) {
  const rows = [];
  for (const c of chunks) {
    const payload = {
      query: c.query,
      fields: ['ND'],
      limit,
      paginationMode: 'PAGE_NUMBER',
      scope: 'ALL'
    };
    const data = await postTed(payload);
    const total = Number(data.totalNoticeCount || 0);
    rows.push({ ...c, totalNoticeCount: total, approxPages: Math.ceil(total / limit) });
    console.log(`SIZE ${c.country} ${c.month}: total=${total} approx_pages=${Math.ceil(total / limit)}`);
    await sleep(rateSleepMs);
  }

  const totalNoticeCount = rows.reduce((acc, r) => acc + r.totalNoticeCount, 0);
  const approxPages = rows.reduce((acc, r) => acc + r.approxPages, 0);

  const report = {
    generated_at: new Date().toISOString(),
    totalNoticeCount,
    approxPages,
    rows
  };
  await writeJson(reportFile, report);
  console.log(`\nSIZE SUMMARY notices=${totalNoticeCount} approx_pages=${approxPages} report=${reportFile}`);
}

async function runFetch({
  chunks,
  outRoot,
  checkpointFile,
  reportFile,
  limit,
  fields,
  rateSleepMs,
  maxSeconds,
  forceRedo
}) {
  const startedAt = Date.now();
  const state = (await readJson(checkpointFile, null)) || newState({ outRoot, limit, fields });

  for (const c of chunks) {
    const key = chunkKey(c.country, c.month);
    const existing = state.chunks[key] || {};
    if (!forceRedo && existing.status === 'done') {
      console.log(`SKIP ${key} (already done)`);
      continue;
    }

    if ((Date.now() - startedAt) / 1000 > maxSeconds) {
      console.log(`STOP maxSeconds reached (${maxSeconds}s). Safe to resume later.`);
      state.updated_at = new Date().toISOString();
      await writeJson(checkpointFile, state);
      break;
    }

    const finalFile = chunkFile(outRoot, c.country, c.month);
    const partFile = `${finalFile}.part`;
    await ensureDir(path.dirname(finalFile));
    if (forceRedo) {
      await fsp.rm(finalFile, { force: true });
      await fsp.rm(partFile, { force: true });
    }

    const stream = fs.createWriteStream(partFile, { flags: 'w' });

    let fetched = 0;
    let requests = 0;
    let token = null;
    let totalNoticeCount = 0;
    let done = false;

    state.chunks[key] = {
      ...(state.chunks[key] || {}),
      country: c.country,
      month: c.month,
      query: c.query,
      file: finalFile,
      status: 'running',
      started_at: new Date().toISOString(),
      fetched: 0,
      requests: 0
    };
    state.updated_at = new Date().toISOString();
    await writeJson(checkpointFile, state);

    try {
      while (!done) {
        const payload = {
          query: c.query,
          fields,
          limit,
          paginationMode: 'ITERATION',
          scope: 'ALL'
        };
        if (token) payload.iterationNextToken = token;

        const data = await postTed(payload);
        requests += 1;

        if (requests === 1) totalNoticeCount = Number(data.totalNoticeCount || 0);

        const notices = Array.isArray(data.notices) ? data.notices : [];
        const fetchedAt = new Date().toISOString();
        for (const notice of notices) {
          const line = JSON.stringify({
            source: 'TED',
            chunk_country: c.country,
            chunk_month: c.month,
            fetched_at: fetchedAt,
            notice
          });
          stream.write(`${line}\n`);
          fetched += 1;
        }

        token = data.iterationNextToken || null;
        done = !token || notices.length === 0;

        state.chunks[key] = {
          ...state.chunks[key],
          status: 'running',
          fetched,
          requests,
          totalNoticeCount,
          iterationNextToken: token,
          updated_at: new Date().toISOString()
        };
        state.updated_at = new Date().toISOString();
        await writeJson(checkpointFile, state);

        await sleep(rateSleepMs);
      }

      await new Promise((resolve, reject) => {
        stream.end(() => resolve());
        stream.on('error', reject);
      });

      await fsp.rename(partFile, finalFile);

      state.chunks[key] = {
        ...state.chunks[key],
        status: 'done',
        fetched,
        requests,
        totalNoticeCount,
        finished_at: new Date().toISOString(),
        iterationNextToken: null
      };
      state.updated_at = new Date().toISOString();
      await writeJson(checkpointFile, state);

      console.log(`DONE ${key}: fetched=${fetched} total=${totalNoticeCount} requests=${requests}`);
    } catch (err) {
      await new Promise((resolve) => stream.end(resolve));
      state.chunks[key] = {
        ...state.chunks[key],
        status: 'failed',
        error: String(err?.message || err),
        failed_at: new Date().toISOString()
      };
      state.updated_at = new Date().toISOString();
      await writeJson(checkpointFile, state);
      throw err;
    }
  }

  const rows = Object.values(state.chunks);
  const doneChunks = rows.filter((r) => r.status === 'done').length;
  const failedChunks = rows.filter((r) => r.status === 'failed').length;
  const totalFetched = rows.reduce((acc, r) => acc + Number(r.fetched || 0), 0);

  const report = {
    generated_at: new Date().toISOString(),
    doneChunks,
    failedChunks,
    totalChunks: chunks.length,
    totalFetched,
    checkpointFile,
    outRoot
  };
  await writeJson(reportFile, report);

  console.log(`\nFETCH SUMMARY done=${doneChunks}/${chunks.length} failed=${failedChunks} fetched=${totalFetched}`);
  console.log(`checkpoint=${checkpointFile}`);
  console.log(`report=${reportFile}`);
}

async function main() {
  const args = parseArgs(process.argv);
  const now = new Date();
  const currentMonth = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}`;

  const mode = String(args.mode || 'size').toLowerCase();
  const countries = countryList(args.countries || 'IRL,FRA,ESP');
  const startMonth = String(args['start-month'] || '2024-08');
  const endMonth = String(args['end-month'] || currentMonth);
  const limit = Math.max(1, Math.min(250, toInt(args.limit, 250)));
  const rateSleepMs = Math.max(0, toInt(args['sleep-ms'], 150));
  const maxSeconds = Math.max(60, toInt(args['max-seconds'], 9 * 3600));
  const outRoot = String(args['out-root'] || '/Volumes/Civant/_system/raw/ted_baseline');
  const checkpointRoot = String(args['checkpoint-root'] || '/Volumes/Civant/_system/checkpoints/ted_baseline');
  const reportRoot = String(args['report-root'] || '/Volumes/Civant/_system/reports/ted_baseline');
  const forceRedo = toBool(args['force-redo'], false);
  const fields = DEFAULT_FIELDS;

  const months = monthRange(startMonth, endMonth);
  const chunks = await buildChunks(countries, months);

  await ensureDir(outRoot);
  await ensureDir(checkpointRoot);
  await ensureDir(reportRoot);

  const runStamp = new Date().toISOString().replace(/[:.]/g, '-');
  const checkpointFile = path.join(checkpointRoot, `ted_baseline_${startMonth}_${endMonth}.checkpoint.json`);
  const reportFile = path.join(reportRoot, `ted_${mode}_${startMonth}_${endMonth}_${runStamp}.json`);

  console.log(`TED baseline mode=${mode} countries=${countries.join(',')} months=${startMonth}..${endMonth} chunks=${chunks.length}`);
  console.log(`outRoot=${outRoot}`);

  if (mode === 'size') {
    await runSizing({ chunks, limit, reportFile, rateSleepMs });
    return;
  }

  if (mode === 'fetch') {
    await runFetch({
      chunks,
      outRoot,
      checkpointFile,
      reportFile,
      limit,
      fields,
      rateSleepMs,
      maxSeconds,
      forceRedo
    });
    return;
  }

  throw new Error(`Unsupported mode: ${mode}. Use --mode size|fetch`);
}

main().catch((err) => {
  console.error(`ERROR: ${err?.message || err}`);
  process.exit(1);
});
