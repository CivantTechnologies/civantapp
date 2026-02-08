const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

function badRequest(message: string) {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 400;
  return err;
}

export function getTenantFromHeader(req: Request) {
  const raw = String(req.headers.get('x-tenant-id') || req.headers.get('X-Tenant-Id') || '').trim().toLowerCase();
  if (!raw) {
    throw badRequest('Missing x-tenant-id');
  }
  if (!TENANT_ID_PATTERN.test(raw)) {
    throw badRequest('Invalid x-tenant-id');
  }
  return raw;
}
