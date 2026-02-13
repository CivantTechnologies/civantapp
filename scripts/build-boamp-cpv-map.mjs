#!/usr/bin/env node

import fs from 'node:fs';
import readline from 'node:readline';

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
    for (const item of node) {
      collectCpvCodesFromNode(item, set, inCpvContext);
    }
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

  if (inCpvContext) {
    addCpvCodesFromText(node, set);
  }
}

function collectCpvCodes(record) {
  const set = new Set();

  // Legacy BOAMP rows expose CPV in top-level keys.
  for (const [key, value] of Object.entries(record)) {
    if (/cpv/i.test(String(key))) {
      collectCpvCodesFromNode(value, set, true);
    }
  }

  // eForms rows often keep CPV inside DONNEES JSON.
  const donneesJson = parseMaybeJson(record.DONNEES);
  if (donneesJson) {
    collectCpvCodesFromNode(donneesJson, set, false);
  }

  return Array.from(set).sort();
}

async function main() {
  const args = parseArgs(process.argv);
  const input = String(args.input || '/Users/davidmanrique/projects/boamp.csv');
  const output = String(args.output || '/tmp/boamp-cpv-map.tsv');
  const maxRows = args['max-rows'] ? Math.max(1, Number(args['max-rows'])) : null;
  const progressEvery = Math.max(1000, Number(args['progress-every'] || 50000));

  if (!fs.existsSync(input)) {
    console.error(`Input file not found: ${input}`);
    process.exit(1);
  }

  const stream = fs.createReadStream(input, { encoding: 'utf8' });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = [];
  let pendingRecord = '';
  let recordNumber = 0;
  let dataRows = 0;
  let rowsWithId = 0;
  let rowsWithCpv = 0;
  let malformedRecords = 0;
  const cpvByCanonicalId = new Map();

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

    const cpvCodes = collectCpvCodes(row);
    if (cpvCodes.length) {
      const canonicalId = `BOAMP_FR:${sourceNoticeId}`;
      cpvByCanonicalId.set(canonicalId, cpvCodes.join(','));
      rowsWithCpv += 1;
    }

    if (dataRows % progressEvery === 0) {
      console.log(`parsed=${dataRows} mapped=${cpvByCanonicalId.size}`);
    }
    if (maxRows && dataRows >= maxRows) break;
  }

  if (pendingRecord) malformedRecords += 1;

  const out = fs.createWriteStream(output, { encoding: 'utf8' });
  for (const [canonicalId, cpvCsv] of cpvByCanonicalId.entries()) {
    out.write(`${canonicalId}\t${cpvCsv}\n`);
  }
  await new Promise((resolve, reject) => {
    out.on('error', reject);
    out.end(resolve);
  });

  console.log(JSON.stringify({
    input,
    output,
    data_rows: dataRows,
    rows_with_id: rowsWithId,
    rows_with_cpv: rowsWithCpv,
    canonical_ids_with_cpv: cpvByCanonicalId.size,
    malformed_records: malformedRecords
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
