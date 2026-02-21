const INTERNAL_TOKEN_HEADERS = ['x-civant-internal-token', 'x-internal-token'];
const INTERNAL_TOKEN_ENV_KEYS = [
  'CIVANT_INTERNAL_FUNCTION_TOKEN',
  'CIVANT_INTERNAL_CRON_TOKEN',
  'INTERNAL_FUNCTION_TOKEN'
];

function forbidden(message = 'Forbidden') {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 403;
  return err;
}

function normalizeToken(value: unknown): string {
  return String(value || '').trim();
}

function getHeaderToken(req: Request): string {
  for (const name of INTERNAL_TOKEN_HEADERS) {
    const value = normalizeToken(req.headers.get(name));
    if (value) return value;
  }
  return '';
}

function getConfiguredInternalToken(): string {
  for (const key of INTERNAL_TOKEN_ENV_KEYS) {
    const value = normalizeToken(Deno.env.get(key));
    if (value) return value;
  }
  return '';
}

export function getInternalAuthHeader(req: Request): Record<string, string> {
  const token = getHeaderToken(req) || getConfiguredInternalToken();
  if (!token) return {};
  return { 'x-civant-internal-token': token };
}

export function assertInternalRequest(req: Request, endpointLabel: string) {
  const configuredToken = getConfiguredInternalToken();
  if (!configuredToken) {
    throw forbidden(`${endpointLabel} is disabled: internal token is not configured`);
  }

  const providedToken = getHeaderToken(req);
  if (!providedToken || providedToken !== configuredToken) {
    throw forbidden(`${endpointLabel} requires a valid internal token`);
  }
}
