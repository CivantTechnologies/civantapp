import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant, resolveTenantId } from './requireAdmin.ts';
import { computeSupportStatus } from './supportAccessAllowed.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);

    const body = await req.json().catch(() => ({}));
    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    const user = await requireAdminForTenant({ civant, req, tenantId });

    const status = await computeSupportStatus({ civant, tenantId, actor: { id: user.userId, email: user.email } });
    return Response.json(status);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to load support access status' }, { status });
  }
});
