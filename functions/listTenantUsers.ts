import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant, resolveTenantId } from './requireAdmin.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);

    const body = await req.json().catch(() => ({}));
    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    await requireAdminForTenant({ civant, req, tenantId });

    const tenantUsers = await civant.asServiceRole.entities.users.filter({ tenant_id: tenantId }, '-created_at', 200);

    const withRoles = (Array.isArray(tenantUsers) ? tenantUsers : []).map((tenantUser: Record<string, unknown>) => {
      const role = String(tenantUser.role || 'user');
      return {
        userId: String(tenantUser.id || ''),
        email: String(tenantUser.email || ''),
        roles: [role]
      };
    });

    return Response.json(withRoles);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to list tenant users' }, { status });
  }
});
