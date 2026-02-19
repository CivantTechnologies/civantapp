import { createClientFromRequest } from './civantSdk.ts';
import { runPipeline } from './pipeline/steps.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);
    await requireAdminForTenant({ civant, req, tenantId });
    const body = await req.json().catch(() => ({}));
    const source = String(body.source || 'MANUAL');
    const documents = Array.isArray(body.documents) ? body.documents : [];
    const run_id = String(body.run_id || `run_${Date.now()}`);

    if (!documents.length) {
      return Response.json({ error: 'documents[] is required' }, { status: 400 });
    }

    const result = await runPipeline(civant, {
      run_id,
      tenant_id: tenantId,
      source,
      cursor: body.cursor,
      documents
    });

    return Response.json({ success: true, ...result });
  } catch (error) {
    return Response.json({ error: error instanceof Error ? error.message : 'Pipeline failed' }, { status: 500 });
  }
});
