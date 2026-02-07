import { createClientFromRequest } from './civantSdk.ts';
import { getCurrentUserFromRequest } from './requireAdmin.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await getCurrentUserFromRequest(civant, req);

    return Response.json({
      userId: user.userId,
      email: user.email,
      role: user.role,
      tenantId: user.tenantId
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 401;
    return Response.json({ error: error instanceof Error ? error.message : 'Unauthorized' }, { status });
  }
});
