import { createClientFromRequest } from './civantSdk.ts';
import { getCurrentUserFromRequest } from './requireAdmin.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await getCurrentUserFromRequest(civant, req);

    return Response.json({
      email: user.email,
      tenant_id: user.tenantId,
      roles: user.roles
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 401;
    return Response.json({ error: error instanceof Error ? error.message : 'Unauthorized' }, { status });
  }
});
