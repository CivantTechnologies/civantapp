export type RequestLike = {
  method?: string;
  headers?: Record<string, unknown>;
  body?: unknown;
};

export type ResponseLike = {
  status: (code: number) => ResponseLike;
  json: (payload: unknown) => void;
  setHeader?: (name: string, value: string) => void;
};

export function getHeader(req: RequestLike, name: string) {
  const key = String(name || '').toLowerCase();
  const headers = req.headers || {};

  for (const [headerName, headerValue] of Object.entries(headers)) {
    if (headerName.toLowerCase() === key) {
      return Array.isArray(headerValue) ? String(headerValue[0] || '') : String(headerValue || '');
    }
  }

  return '';
}

export function readJsonBody<T = Record<string, unknown>>(req: RequestLike): T {
  const body = req.body;
  if (!body) return {} as T;
  if (typeof body === 'string') {
    try {
      return JSON.parse(body) as T;
    } catch {
      return {} as T;
    }
  }
  if (typeof body === 'object') {
    return body as T;
  }
  return {} as T;
}

export function sendJson(res: ResponseLike, status: number, payload: unknown) {
  if (typeof res.setHeader === 'function') {
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    // These API responses are tenant/auth scoped (via headers), so browser caching is unsafe.
    // Prevent 304 responses (Axios treats 304 as an error) and avoid cross-tenant cache pollution.
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Vary', 'x-tenant-id, authorization');
  }
  res.status(status).json(payload);
}

export function normalizeError(error: unknown, fallbackMessage: string) {
  const status = Number((error as { status?: number })?.status || 500);
  const message = error instanceof Error ? error.message : fallbackMessage;
  return { status, message };
}

export function methodNotAllowed(expected = 'POST') {
  return Object.assign(new Error(`Method not allowed. Use ${expected}.`), { status: 405 });
}
