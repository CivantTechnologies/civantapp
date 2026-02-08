import { createClientFromRequest } from './civantSdk.ts';
import { requireTenantAccess } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);

    await requireTenantAccess({ civant, req, tenantId });

    const rows = await civant.asServiceRole.entities.tenants.filter({ id: tenantId }, '-created_at', 1);
    const tenant = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;

    return Response.json({
      tenantId,
      name: tenant?.name || null,
      regions: Array.isArray(tenant?.regions) ? tenant.regions : [],
      planTier: tenant?.plan || null,
      planLimits: tenant?.plan_limits || tenant?.limits || null,
      createdAt: tenant?.created_at || null
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to get tenant' }, { status });
  }
});
