import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant, requireAuthenticatedUser, resolveTenantId } from './requireAdmin.ts';
import { computeSupportStatus } from './supportAccessAllowed.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await requireAuthenticatedUser(civant);

    const body = await req.json().catch(() => ({}));
    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    await requireAdminForTenant({ civant, user, tenantId });

    const status = await computeSupportStatus({ civant, tenantId, actor: user });
    return Response.json(status);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to load support access status' }, { status });
  }
});
