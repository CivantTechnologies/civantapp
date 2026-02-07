import { createClientFromRequest } from './civantSdk.ts';
import { checkIsAdminForTenant, resolveTenantId } from './requireAdmin.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);

    const body = await req.json().catch(() => ({}));
    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    const isAdmin = await checkIsAdminForTenant({ civant, req, tenantId });

    return Response.json({
      isAdmin,
      tenantId
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to load capabilities' }, { status });
  }
});
