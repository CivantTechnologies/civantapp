#!/usr/bin/env node

import fs from 'node:fs';
import readline from 'node:readline';

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

function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.toLowerCase() === 'null') return null;
  return text;
}

function isPlausibleYear(year) {
  if (!Number.isFinite(year)) return false;
  return year >= MIN_PLAUSIBLE_YEAR && year <= MAX_PLAUSIBLE_YEAR;
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

  const inspectModification = (mod) => {
    if (!mod || typeof mod !== 'object') return;
    const rub = clean(mod.RUB_INIT ?? mod.rub_init ?? mod.rubInit);
    if (!rub) return;
    const rubLower = rub.toLowerCase();
    if (!rubLower.includes('date limite')) return;
    if (!/(reception|réception|offre|offres|demande|demandes|participation)/i.test(rub)) return;

    const lire = clean(mod.LIRE ?? mod.lire);
    const txtInit = clean(mod.TXT_INIT ?? mod.txt_init);
    if (lire) addCandidate(lire, 'donnees.rectif.modification.lire', 3);
    if (txtInit) addCandidate(txtInit, 'donnees.rectif.modification.txt_init', 2);
  };

  const rectif = donneesJson.RECTIF ?? donneesJson.rectif;
  if (rectif && typeof rectif === 'object') {
    const mod = rectif.MODIFICATION ?? rectif.modification;
    if (Array.isArray(mod)) mod.forEach(inspectModification);
    else inspectModification(mod);
  }

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
        if ((value.RUB_INIT || value.rub_init) && (value.LIRE || value.lire || value.TXT_INIT || value.txt_init)) {
          inspectModification(value);
        }
        visit(value);
      }
    }
  };
  visit(donneesJson);

  if (!candidates.length) return { date: null, source: null };
  candidates.sort((a, b) => (b.weight - a.weight) || (b.date.localeCompare(a.date)));
  return { date: candidates[0].date, source: candidates[0].source };
}

function loadTargets(path) {
  if (!path) return null;
  if (!fs.existsSync(path)) {
    console.error(`Targets file not found: ${path}`);
    process.exit(1);
  }
  const set = new Set();
  const lines = fs.readFileSync(path, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    set.add(trimmed);
  }
  return set;
}

async function main() {
  const args = parseArgs(process.argv);
  const input = String(args.input || '/Users/davidmanrique/projects/boamp.csv');
  const output = String(args.output || '/tmp/boamp-deadline-map.tsv');
  const targetsPath = args.targets ? String(args.targets) : '';
  const maxRows = args['max-rows'] ? Math.max(1, Number(args['max-rows'])) : null;
  const progressEvery = Math.max(1000, Number(args['progress-every'] || 50000));

  if (!fs.existsSync(input)) {
    console.error(`Input file not found: ${input}`);
    process.exit(1);
  }

  const targetSet = loadTargets(targetsPath);
  const emitted = new Set();

  const out = fs.createWriteStream(output, { encoding: 'utf8' });
  const writeRow = (canonicalId, deadlineDate, deadlineSource) => {
    out.write(`${canonicalId}\t${deadlineDate}\t${deadlineSource || ''}\n`);
  };

  const stream = fs.createReadStream(input, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = [];
  let pendingRecord = '';
  let recordNumber = 0;
  let dataRows = 0;
  let rowsWithId = 0;
  let rowsMatchedTargets = 0;
  let deadlinesExtracted = 0;
  let malformedRecords = 0;

  for await (const rawLine of rl) {
    if (!pendingRecord && !rawLine.trim()) continue;
    pendingRecord = pendingRecord ? `${pendingRecord}\n${rawLine}` : rawLine;
    const parsed = parseCsvLine(pendingRecord, ';');
    if (parsed.inQuotes) continue;

    recordNumber += 1;
    const values = parsed.fields;
    pendingRecord = '';

    if (recordNumber === 1) {
      headers = values.map((value) => String(value || '').replace(/^\uFEFF/, '').trim());
      continue;
    }

    dataRows += 1;
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });

    const sourceNoticeId = clean(row.idweb) || clean(row.id);
    if (!sourceNoticeId) {
      if (maxRows && dataRows >= maxRows) break;
      continue;
    }
    rowsWithId += 1;

    const canonicalId = `BOAMP_FR:${sourceNoticeId}`;
    if (targetSet && !targetSet.has(canonicalId)) {
      if (maxRows && dataRows >= maxRows) break;
      continue;
    }
    rowsMatchedTargets += 1;
    if (emitted.has(canonicalId)) {
      if (maxRows && dataRows >= maxRows) break;
      continue;
    }

    const publication = parseDate(row.dateparution);
    let deadline = parseDate(row.datelimitereponse);
    let deadlineSource = deadline.date ? 'datelimitereponse' : null;

    if (!deadline.date) {
      const donneesText = clean(row.DONNEES) || '';
      // Cheap pre-check to avoid JSON.parse() on huge blobs that don't mention a deadline.
      if (donneesText.toLowerCase().includes('limite')) {
        const donneesJson = parseMaybeJson(donneesText);
        if (donneesJson) {
          const fromDonnees = extractDeadlineFromDonnees(donneesJson, publication.date);
          if (fromDonnees.date) {
            deadline = { date: fromDonnees.date, iso: null };
            deadlineSource = fromDonnees.source || 'donnees';
          }
        }
      }
    }

    if (deadline.date) {
      writeRow(canonicalId, deadline.date, deadlineSource || '');
      emitted.add(canonicalId);
      if (targetSet) targetSet.delete(canonicalId);
      deadlinesExtracted += 1;
    }

    if (dataRows % progressEvery === 0) {
      console.log(`parsed=${dataRows} matched=${rowsMatchedTargets} extracted=${deadlinesExtracted} remaining_targets=${targetSet ? targetSet.size : 'n/a'}`);
    }
    if (maxRows && dataRows >= maxRows) break;
  }

  if (pendingRecord) malformedRecords += 1;

  await new Promise((resolve, reject) => {
    out.on('error', reject);
    out.end(resolve);
  });

  console.log(JSON.stringify({
    input,
    output,
    data_rows: dataRows,
    rows_with_id: rowsWithId,
    targets: targetsPath ? targetsPath : null,
    matched_targets: rowsMatchedTargets,
    deadlines_extracted: deadlinesExtracted,
    unique_canonical_ids_with_deadline: emitted.size,
    remaining_targets: targetSet ? targetSet.size : null,
    malformed_records: malformedRecords
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

