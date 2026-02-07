import { createClientFromRequest } from './civantSdk.ts';
import { runPipeline } from './pipeline/steps.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await civant.auth.me();

    if (!user || user.role !== 'admin') {
      return Response.json({ error: 'Unauthorized - Admin access required' }, { status: 403 });
    }

    const body = await req.json().catch(() => ({}));
    const source = String(body.source || 'MANUAL');
    const documents = Array.isArray(body.documents) ? body.documents : [];
    const run_id = String(body.run_id || `run_${Date.now()}`);

    if (!documents.length) {
      return Response.json({ error: 'documents[] is required' }, { status: 400 });
    }

    const result = await runPipeline(civant, {
      run_id,
      source,
      cursor: body.cursor,
      documents
    });

    return Response.json({ success: true, ...result });
  } catch (error) {
    return Response.json({ error: error instanceof Error ? error.message : 'Pipeline failed' }, { status: 500 });
  }
});
