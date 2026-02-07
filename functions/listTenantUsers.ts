import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant, requireAuthenticatedUser, resolveTenantId } from './requireAdmin.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await requireAuthenticatedUser(civant);

    const body = await req.json().catch(() => ({}));
    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    await requireAdminForTenant({ civant, user, tenantId });

    const tenantUsers = await civant.asServiceRole.entities.users.filter({ tenant_id: tenantId }, '-created_at', 200);

    const withRoles = await Promise.all(
      (Array.isArray(tenantUsers) ? tenantUsers : []).map(async (tenantUser: Record<string, unknown>) => {
        const roles = await civant.asServiceRole.entities.user_roles.filter({ user_id: tenantUser.id }, '-role', 20);
        return {
          userId: String(tenantUser.id || ''),
          email: String(tenantUser.email || ''),
          roles: (Array.isArray(roles) ? roles : []).map((r: Record<string, unknown>) => String(r.role || '')).filter(Boolean)
        };
      })
    );

    return Response.json(withRoles);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to list tenant users' }, { status });
  }
});
