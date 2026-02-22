import crypto from 'node:crypto';

export function parseArgs(argv) {
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

export function clean(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  if (!text) return null;
  if (text.toLowerCase() === 'null') return null;
  return text;
}

export function parseISOToDate(value) {
  const text = clean(value);
  if (!text) return null;

  // Already a date.
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text;

  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

export function clamp01(n) {
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

export function stableHash(parts) {
  const seed = parts.filter(Boolean).join('|');
  return crypto.createHash('sha256').update(seed).digest('hex');
}

export function tsvSafe(value) {
  if (value === undefined || value === null) return '';
  // Keep single-line and avoid COPY delimiter collisions.
  return String(value).replace(/[\t\r\n]+/g, ' ').trim();
}

export function jsonSafe(obj) {
  try {
    return tsvSafe(JSON.stringify(obj ?? {}));
  } catch {
    return '{}';
  }
}

export function parseBool(value, fallback = false) {
  const text = clean(value);
  if (!text) return fallback;
  const lowered = text.toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(lowered)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(lowered)) return false;
  return fallback;
}

export function mustGetArg(args, key) {
  const value = clean(args[key]);
  if (!value) {
    throw new Error(`Missing required --${key}`);
  }
  return value;
}

export function todayISODate() {
  return new Date().toISOString().slice(0, 10);
}
