import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';

function normalizeLimit(value: unknown) {
  const n = Number(value || 50);
  if (Number.isNaN(n) || n <= 0) return 50;
  return Math.min(Math.round(n), 200);
}

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);

    const body = await req.json().catch(() => ({}));
    const limit = normalizeLimit(body.limit);

    await requireAdminForTenant({ civant, req, tenantId });

    const rows = await civant.asServiceRole.entities.support_access_audit.filter({
      tenant_id: tenantId
    }, '-created_at', limit);

    return Response.json(Array.isArray(rows) ? rows : []);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to list support access audit' }, { status });
  }
});
