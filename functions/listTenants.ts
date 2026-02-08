import { createClientFromRequest } from './civantSdk.ts';
import { getCurrentUserFromRequest, hasRole } from './requireAdmin.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await getCurrentUserFromRequest(civant, req);

    let rows: Array<Record<string, unknown>> = [];
    if (hasRole(user, 'creator')) {
      const result = await civant.asServiceRole.entities.tenants.list('created_at', 500);
      rows = Array.isArray(result) ? result as Array<Record<string, unknown>> : [];
    } else {
      const result = await civant.asServiceRole.entities.tenants.filter({ id: user.tenantId }, 'created_at', 1);
      rows = Array.isArray(result) ? result as Array<Record<string, unknown>> : [];
    }

    return Response.json({
      tenants: rows.map((row) => ({
        id: String(row.id || ''),
        name: String(row.name || row.id || '')
      })).filter((row) => row.id)
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to list tenants' }, { status });
  }
});
