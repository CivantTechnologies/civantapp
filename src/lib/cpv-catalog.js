import { getAllRaw } from 'cpv-eu';

let cpvEntriesCache = null;
let cpvByCodeCache = null;

function normalizeCode(value, maxLength = 8) {
  return String(value || '')
    .replace(/\D/g, '')
    .slice(0, maxLength);
}

function chooseLabel(labels, language = 'en') {
  if (!labels || typeof labels !== 'object') return '';
  return (
    labels[language] ||
    labels.en ||
    labels.es ||
    labels.fr ||
    labels.de ||
    Object.values(labels)[0] ||
    ''
  );
}

function ensureCatalog() {
  if (cpvEntriesCache && cpvByCodeCache) return;

  const raw = getAllRaw();
  const entries = [];
  const byCode = new Map();

  for (const item of raw) {
    const code = normalizeCode(item?.code, 8);
    if (code.length !== 8 || byCode.has(code)) continue;

    const labels = item?.labels && typeof item.labels === 'object' ? item.labels : {};
    const entry = {
      code,
      labels,
      level: Number(item?.level || 4)
    };
    entries.push(entry);
    byCode.set(code, entry);
  }

  entries.sort((a, b) => a.code.localeCompare(b.code));
  cpvEntriesCache = entries;
  cpvByCodeCache = byCode;
}

function scoreEntry(entry, rawQuery, normalizedDigits, language) {
  const label = String(chooseLabel(entry.labels, language)).toLowerCase();
  const query = rawQuery.toLowerCase();
  let score = 0;

  if (normalizedDigits) {
    if (entry.code === normalizedDigits) score += 10000;
    else if (entry.code.startsWith(normalizedDigits)) score += 8000 - (entry.code.length - normalizedDigits.length);
  }

  if (query) {
    if (label.startsWith(query)) score += 5000;
    else if (label.includes(query)) score += 3500;
  }

  if (!query) score += entry.level <= 2 ? 200 : 50;
  return score;
}

export function getCpvEntryByCode(code, language = 'en') {
  ensureCatalog();
  const normalized = normalizeCode(code, 8);
  if (normalized.length !== 8) return null;
  const entry = cpvByCodeCache.get(normalized);
  if (!entry) return null;
  return {
    code: entry.code,
    level: entry.level,
    label: chooseLabel(entry.labels, language)
  };
}

export function normalizeCpvCodeList(value) {
  const source = Array.isArray(value) ? value : String(value || '').split(',');
  const seen = new Set();
  const output = [];

  for (const item of source) {
    const code = normalizeCode(item, 8);
    if (code.length !== 8 || seen.has(code)) continue;
    seen.add(code);
    output.push(code);
  }

  return output;
}

export function searchCpvCatalog(query, options = {}) {
  ensureCatalog();
  const language = options.language || 'en';
  const limit = Number(options.limit || 20);
  const excluded = new Set(normalizeCpvCodeList(options.excludeCodes || []));
  const rawQuery = String(query || '').trim();
  const normalizedDigits = normalizeCode(rawQuery, 8);

  const ranked = [];
  for (const entry of cpvEntriesCache) {
    if (excluded.has(entry.code)) continue;
    const score = scoreEntry(entry, rawQuery, normalizedDigits, language);
    if (score <= 0) continue;

    ranked.push({
      code: entry.code,
      level: entry.level,
      label: chooseLabel(entry.labels, language),
      score
    });
  }

  ranked.sort((a, b) => b.score - a.score || a.code.localeCompare(b.code));
  return ranked.slice(0, limit).map(({ code, level, label }) => ({ code, level, label }));
}
