import { getCurrentUser } from '../_lib/auth.js';
import { methodNotAllowed, normalizeError, readJsonBody, sendJson, type RequestLike, type ResponseLike } from '../_lib/http.js';
import { getServerSupabase } from '../_lib/supabase.js';

type TriageBody = {
  predictionId?: string;
  opportunityId?: string;
  action?: string;
};

const ALLOWED_ACTIONS = new Set(['save', 'ignore', 'assign']);

function badRequest(message: string) {
  return Object.assign(new Error(message), { status: 400 });
}

export default async function handler(req: RequestLike, res: ResponseLike) {
  try {
    if ((req.method || 'POST').toUpperCase() !== 'POST') {
      throw methodNotAllowed('POST');
    }

    const user = await getCurrentUser(req);
    const tenantId = String(user.tenantId || '').trim().toLowerCase();
    if (!tenantId) {
      sendJson(res, 403, { error: 'Tenant not available for user' });
      return;
    }

    const body = readJsonBody<TriageBody>(req);
    const predictionId = String(body.predictionId || body.opportunityId || '').trim();
    const action = String(body.action || '').trim().toLowerCase();

    if (!predictionId) throw badRequest('predictionId is required');
    if (!ALLOWED_ACTIONS.has(action)) throw badRequest('action must be one of: save, ignore, assign');

    const supabase = getServerSupabase() as any;

    // Guard against cross-tenant action writes by validating target prediction ownership first.
    const ownershipResult = await supabase
      .from('predictions_current')
      .select('prediction_id,tenant_id')
      .eq('prediction_id', predictionId)
      .eq('tenant_id', tenantId)
      .limit(1);

    if (ownershipResult.error) {
      throw Object.assign(new Error(ownershipResult.error.message), { status: 500 });
    }
    if (!Array.isArray(ownershipResult.data) || ownershipResult.data.length === 0) {
      sendJson(res, 404, { error: 'Prediction not found for tenant' });
      return;
    }

    const recordedAt = new Date().toISOString();
    const insertResult = await supabase
      .from('prediction_extensions')
      .insert({
        tenant_id: tenantId,
        prediction_id: predictionId,
        extension_type: 'mobile_triage_action_v1',
        model_version: 'mobile-v1',
        payload_json: {
          action,
          source: 'mobile_app',
          actor_user_id: user.userId,
          actor_email: user.email,
          recorded_at: recordedAt,
        },
      })
      .select('id')
      .limit(1);

    if (insertResult.error) {
      throw Object.assign(new Error(insertResult.error.message), { status: 500 });
    }

    sendJson(res, 200, {
      success: true,
      predictionId,
      action,
      recordedAt,
    });
  } catch (error) {
    const normalized = normalizeError(error, 'Failed to record triage action');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
