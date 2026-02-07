import { createClientFromRequest } from './civantSdk.ts';
import { PIPELINE_NAMES } from './pipeline/models.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await civant.auth.me();

    if (!user || user.role !== 'admin') {
      return Response.json({ error: 'Unauthorized - Admin access required' }, { status: 403 });
    }

    const body = await req.json().catch(() => ({}));
    const action = String(body.action || 'overview');

    if (action === 'review_decision') {
      const queueId = String(body.queue_id || '');
      const decision = String(body.decision || '');
      if (!queueId || !['approve', 'reject'].includes(decision)) {
        return Response.json({ error: 'queue_id and decision=approve|reject are required' }, { status: 400 });
      }

      const queueRows = await civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].filter({ id: queueId });
      if (!queueRows.length) {
        return Response.json({ error: 'Queue item not found' }, { status: 404 });
      }

      const updated = await civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].update(queueRows[0].id, {
        status: decision === 'approve' ? 'approved' : 'rejected',
        reviewed_by: user.email || user.id || 'admin',
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

      const predictionRows = await civant.asServiceRole.entities[PIPELINE_NAMES.predictions].filter({ id: predictionId });
      if (!predictionRows.length) {
        return Response.json({ error: 'Prediction not found' }, { status: 404 });
      }

      return Response.json({ success: true, prediction: predictionRows[0] });
    }

    const [runs, queue, predictions] = await Promise.all([
      civant.asServiceRole.entities[PIPELINE_NAMES.ingestionRuns].list('-started_at', 100),
      civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].filter({ status: 'pending' }, '-created_at', 200),
      civant.asServiceRole.entities[PIPELINE_NAMES.predictions].list('-generated_at', 200)
    ]);

    return Response.json({
      success: true,
      runs,
      queue,
      predictions
    });
  } catch (error) {
    return Response.json({ error: error instanceof Error ? error.message : 'Admin endpoint failed' }, { status: 500 });
  }
});
