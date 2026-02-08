import { createClientFromRequest } from './civantSdk.ts';
import { checkIsAdminForTenant } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);

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
