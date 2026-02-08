import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);

    await requireAdminForTenant({ civant, req, tenantId });

    const tenantUsers = await civant.asServiceRole.entities.users.filter({ tenant_id: tenantId }, '-created_at', 200);

    const withRoles = await Promise.all((Array.isArray(tenantUsers) ? tenantUsers : []).map(async (tenantUser: Record<string, unknown>) => {
      const roleRows = await civant.asServiceRole.entities.user_roles.filter({ user_id: String(tenantUser.id || '') }, '-role', 50)
        .catch(() => []);
      const roles = Array.isArray(roleRows)
        ? roleRows.map((row: Record<string, unknown>) => String(row.role || '').trim().toLowerCase()).filter(Boolean)
        : [];
      const fallbackRole = String(tenantUser.role || '').trim().toLowerCase();
      if (!roles.length && fallbackRole) roles.push(fallbackRole);

      return {
        userId: String(tenantUser.id || ''),
        email: String(tenantUser.email || ''),
        roles: roles.length ? roles : ['user']
      };
    }));

    return Response.json(withRoles);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to list tenant users' }, { status });
  }
});
