import { createClientFromRequest } from './civantSdk.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { PIPELINE_NAMES } from './pipeline/models.ts';

function forbidden(message = 'Forbidden') {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 403;
  return err;
}

async function callRpc<T>(name: string, payload: Record<string, unknown>): Promise<T> {
  const supabaseUrl = Deno.env.get('SUPABASE_URL') || Deno.env.get('VITE_SUPABASE_URL');
  const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if (!supabaseUrl || !serviceRoleKey) {
    throw new Error('SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY not configured');
  }

  const response = await fetch(`${supabaseUrl}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${serviceRoleKey}`,
      'apikey': serviceRoleKey,
      'Prefer': 'return=representation'
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`RPC ${name} failed: ${response.status} ${errorText}`);
  }

  return await response.json() as T;
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

    if (action === 'lifecycle_review_decision') {
      const candidateId = String(body.candidate_id || '');
      const decision = String(body.decision || '');
      const reviewNotes = String(body.review_notes || '');
      if (!candidateId || !['approve', 'reject'].includes(decision)) {
        return Response.json({ error: 'candidate_id and decision=approve|reject are required' }, { status: 400 });
      }

      const result = await callRpc<Record<string, unknown>>(
        'resolve_prediction_reconciliation_candidate',
        {
          p_tenant_id: tenantId,
          p_candidate_id: candidateId,
          p_decision: decision,
          p_reviewed_by: user.email || user.userId || 'admin',
          p_review_notes: reviewNotes
        }
      );
      return Response.json({ success: true, result });
    }

    const [runs, queue, predictions, lifecyclePredictions, lifecycleQueue] = await Promise.all([
      civant.asServiceRole.entities[PIPELINE_NAMES.ingestionRuns].filter({ tenant_id: tenantId }, '-started_at', 100),
      civant.asServiceRole.entities[PIPELINE_NAMES.reconciliationQueue].filter({ tenant_id: tenantId, status: 'pending' }, '-created_at', 200),
      civant.asServiceRole.entities[PIPELINE_NAMES.predictions].filter({ tenant_id: tenantId }, '-generated_at', 200),
      civant.asServiceRole.entities[PIPELINE_NAMES.predictionLifecycle].filter({ tenant_id: tenantId }, '-updated_at', 250),
      civant.asServiceRole.entities[PIPELINE_NAMES.predictionReconciliationCandidates].filter({ tenant_id: tenantId, status: 'pending' }, '-created_at', 250)
    ]);

    const lifecycleSummary = (Array.isArray(lifecyclePredictions) ? lifecyclePredictions : []).reduce<Record<string, number>>((acc, row) => {
      const key = String((row as Record<string, unknown>)?.status || 'Unknown');
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});

    return Response.json({
      success: true,
      runs,
      queue,
      predictions,
      lifecycle_predictions: lifecyclePredictions,
      lifecycle_queue: lifecycleQueue,
      lifecycle_summary: lifecycleSummary
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Admin endpoint failed' }, { status });
  }
});
