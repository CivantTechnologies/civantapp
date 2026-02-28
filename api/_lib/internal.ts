import { getHeader, type RequestLike } from './http.js';

const INTERNAL_TOKEN_HEADERS = ['x-civant-internal-token', 'x-internal-token'];
const INTERNAL_TOKEN_ENV_KEYS = [
  'CIVANT_INTERNAL_FUNCTION_TOKEN',
  'CIVANT_INTERNAL_CRON_TOKEN',
  'INTERNAL_FUNCTION_TOKEN'
];

function forbidden(message = 'Forbidden') {
  return Object.assign(new Error(message), { status: 403 });
}

function normalizeToken(value: unknown) {
  return String(value || '').trim();
}

function getConfiguredInternalToken() {
  for (const key of INTERNAL_TOKEN_ENV_KEYS) {
    const value = normalizeToken(process.env[key]);
    if (value) return value;
  }
  return '';
}

function getProvidedToken(req: RequestLike) {
  for (const name of INTERNAL_TOKEN_HEADERS) {
    const value = normalizeToken(getHeader(req, name));
    if (value) return value;
  }
  return '';
}

export function assertInternalRequest(req: RequestLike, endpointLabel: string) {
  const configuredToken = getConfiguredInternalToken();
  if (!configuredToken) {
    throw forbidden(`${endpointLabel} is disabled: internal token is not configured`);
  }

  const providedToken = getProvidedToken(req);
  if (!providedToken || providedToken !== configuredToken) {
    throw forbidden(`${endpointLabel} requires a valid internal token`);
  }
}
