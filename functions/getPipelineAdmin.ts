import { createClientFromRequest } from './civantSdk.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { PIPELINE_NAMES } from './pipeline/models.ts';

function forbidden(message = 'Forbidden') {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 403;
  return err;
}

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);
    const user = await requireAdminForTenant({ civant, req, tenantId });

    const body = await req.json().catch(() => ({}));
    const action = String(body.action || 'overview');

    if (action === 'review_decision') {
      const queueId = String(body.queue_id || '');
      const decision = String(body.decision || '');
      if (!queueId || !['approve', 'reject'].includes(decision)) {
        return Response.json({ error: 'queue_id and decision=approve|reject are required' }, { status: 400 });
      }

      const queueRows = await civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].filter({
        id: queueId,
        tenant_id: tenantId
      });
      if (!queueRows.length) {
        return Response.json({ error: 'Queue item not found' }, { status: 404 });
      }
      const queueRow = queueRows[0] as Record<string, unknown>;
      if (String(queueRow.tenant_id || '').trim().toLowerCase() !== tenantId) {
        console.warn('[pipeline-admin] cross-tenant queue access attempt denied', { queueId, tenantId });
        throw forbidden('Cross-tenant queue access denied');
      }
      const scopedQueueRowId = String(queueRow.id || '');
      if (!scopedQueueRowId) {
        throw new Error('Queue item missing id');
      }

      const updated = await civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].update(scopedQueueRowId, {
        status: decision === 'approve' ? 'approved' : 'rejected',
        reviewed_by: user.email || user.userId || 'admin',
        reviewed_at: new Date().toISOString(),
        review_notes: String(body.review_notes || '')
      });

      return Response.json({ success: true, item: updated });
    }

    if (action === 'prediction_detail') {
      const predictionId = String(body.prediction_id || '');
      if (!predictionId) {
        return Response.json({ error: 'prediction_id is required' }, { status: 400 });
      }

      const predictionRows = await civant.asServiceRole.entities[PIPELINE_NAMES.predictions].filter({
        id: predictionId,
        tenant_id: tenantId
      });
      if (!predictionRows.length) {
        return Response.json({ error: 'Prediction not found' }, { status: 404 });
      }
      const predictionRow = predictionRows[0] as Record<string, unknown>;
      if (String(predictionRow.tenant_id || '').trim().toLowerCase() !== tenantId) {
        console.warn('[pipeline-admin] cross-tenant prediction access attempt denied', { predictionId, tenantId });
        throw forbidden('Cross-tenant prediction access denied');
      }

      return Response.json({ success: true, prediction: predictionRow });
    }

    const [runs, queue, predictions] = await Promise.all([
      civant.asServiceRole.entities[PIPELINE_NAMES.ingestionRuns].filter({ tenant_id: tenantId }, '-started_at', 100),
      civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].filter({ tenant_id: tenantId, status: 'pending' }, '-created_at', 200),
      civant.asServiceRole.entities[PIPELINE_NAMES.predictions].filter({ tenant_id: tenantId }, '-generated_at', 200)
    ]);

    return Response.json({
      success: true,
      runs,
      queue,
      predictions
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Admin endpoint failed' }, { status });
  }
});
