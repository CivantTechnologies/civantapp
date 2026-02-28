import { assertInternalRequest } from '../../_lib/internal.js';
import { methodNotAllowed, normalizeError, readJsonBody, sendJson, type RequestLike, type ResponseLike } from '../../_lib/http.js';
import { getServerSupabase } from '../../_lib/supabase.js';

type DispatchBody = {
  tenantId?: string;
  limit?: number;
  dryRun?: boolean;
};

type SubscriptionRow = {
  id: string;
  tenant_id: string;
  expo_push_token: string | null;
  enabled: boolean;
  last_notified_on: string | null;
};

type PredictionRow = {
  prediction_id: string;
  buyer_display_name: string | null;
  cpv_cluster_label: string | null;
  confidence: number | null;
  expected_window_start: string | null;
};

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;
const EXPO_PUSH_URL = 'https://exp.host/--/api/v2/push/send';
const EXPO_CHUNK_SIZE = 100;
const MAX_SUBSCRIPTIONS = 2000;

function badRequest(message: string) {
  return Object.assign(new Error(message), { status: 400 });
}

function normalizeTenantId(value: unknown) {
  const tenantId = String(value || '').trim().toLowerCase();
  if (!tenantId) return '';
  if (!TENANT_ID_PATTERN.test(tenantId)) throw badRequest('Invalid tenantId');
  return tenantId;
}

function normalizeLimit(value: unknown, fallback: number, max: number) {
  const n = Number(value || fallback);
  if (Number.isNaN(n) || n <= 0) return fallback;
  return Math.min(Math.round(n), max);
}

function chunk<T>(arr: T[], size: number) {
  const output: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    output.push(arr.slice(i, i + size));
  }
  return output;
}

function isoDate(offsetDays = 0) {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() + offsetDays);
  return date.toISOString().slice(0, 10);
}

async function fetchTenantPredictions(tenantId: string) {
  const supabase = getServerSupabase() as any;
  const today = isoDate(0);
  const dueSoon = isoDate(14);

  const result = await supabase
    .from('predictions_current')
    .select('prediction_id,buyer_display_name,cpv_cluster_label,confidence,expected_window_start,forecast_score')
    .eq('tenant_id', tenantId)
    .gte('expected_window_start', today)
    .or(`confidence.gte.65,expected_window_start.lte.${dueSoon}`)
    .order('confidence', { ascending: false })
    .order('forecast_score', { ascending: false })
    .limit(3);

  if (result.error) {
    throw Object.assign(new Error(result.error.message), { status: 500 });
  }

  return (Array.isArray(result.data) ? result.data : []) as PredictionRow[];
}

function buildMessageForPrediction(row: PredictionRow, tenantId: string, toToken: string) {
  const buyer = String(row.buyer_display_name || 'Unknown buyer');
  const cluster = String(row.cpv_cluster_label || 'Predicted opportunity');
  const confidence = Number(row.confidence || 0);
  const startDate = String(row.expected_window_start || '').trim();

  return {
    to: toToken,
    sound: 'default',
    title: 'Civant high-signal opportunity',
    body: `${buyer}: ${cluster} (${confidence}% confidence)`,
    data: {
      predictionId: row.prediction_id,
      tenantId,
      expectedWindowStart: startDate || null,
      confidence
    }
  };
}

export default async function handler(req: RequestLike, res: ResponseLike) {
  try {
    if ((req.method || 'POST').toUpperCase() !== 'POST') {
      throw methodNotAllowed('POST');
    }

    assertInternalRequest(req, 'mobilePushDispatch');

    const body = readJsonBody<DispatchBody>(req);
    const tenantFilter = normalizeTenantId(body.tenantId);
    const dryRun = body.dryRun === true;
    const limit = normalizeLimit(body.limit, 500, MAX_SUBSCRIPTIONS);
    const today = isoDate(0);
    const staleCutoffIso = new Date(Date.now() - 45 * 24 * 60 * 60 * 1000).toISOString();

    const supabase = getServerSupabase() as any;
    let subscriptionsQuery = supabase
      .from('mobile_push_subscriptions')
      .select('id,tenant_id,expo_push_token,enabled,last_notified_on,last_seen_at')
      .eq('enabled', true)
      .not('expo_push_token', 'is', null)
      .gte('last_seen_at', staleCutoffIso)
      .order('updated_at', { ascending: false })
      .limit(limit);

    if (tenantFilter) {
      subscriptionsQuery = subscriptionsQuery.eq('tenant_id', tenantFilter);
    }

    const subscriptionsResult = await subscriptionsQuery;
    if (subscriptionsResult.error) {
      throw Object.assign(new Error(subscriptionsResult.error.message), { status: 500 });
    }

    const allRows = (Array.isArray(subscriptionsResult.data) ? subscriptionsResult.data : []) as SubscriptionRow[];
    const candidateRows = allRows.filter((row) => String(row.last_notified_on || '') !== today);

    const subsByTenant = new Map<string, SubscriptionRow[]>();
    for (const row of candidateRows) {
      const tenantId = String(row.tenant_id || '').trim().toLowerCase();
      const token = String(row.expo_push_token || '').trim();
      if (!tenantId || !token) continue;
      const list = subsByTenant.get(tenantId) || [];
      list.push(row);
      subsByTenant.set(tenantId, list);
    }

    const sendQueue: Array<{
      subscriptionId: string;
      tenantId: string;
      token: string;
      message: Record<string, unknown>;
    }> = [];

    for (const [tenantId, tenantSubscriptions] of subsByTenant.entries()) {
      const predictions = await fetchTenantPredictions(tenantId);
      if (!predictions.length) continue;
      const topPrediction = predictions[0];
      for (const subscription of tenantSubscriptions) {
        const token = String(subscription.expo_push_token || '').trim();
        if (!token) continue;
        sendQueue.push({
          subscriptionId: String(subscription.id || ''),
          tenantId,
          token,
          message: buildMessageForPrediction(topPrediction, tenantId, token)
        });
      }
    }

    if (dryRun) {
      sendJson(res, 200, {
        success: true,
        dryRun: true,
        candidates: sendQueue.length,
        tenants: Array.from(subsByTenant.keys()).length
      });
      return;
    }

    let sent = 0;
    let disabled = 0;
    let failed = 0;

    for (const messageChunk of chunk(sendQueue, EXPO_CHUNK_SIZE)) {
      const response = await fetch(EXPO_PUSH_URL, {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Accept-Encoding': 'gzip, deflate',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(messageChunk.map((entry) => entry.message))
      });

      if (!response.ok) {
        const errorText = await response.text();
        for (const entry of messageChunk) {
          failed += 1;
          await supabase
            .from('mobile_push_subscriptions')
            .update({
              last_error: `expo_http_${response.status}:${errorText.slice(0, 200)}`,
              updated_at: new Date().toISOString()
            })
            .eq('id', entry.subscriptionId)
            .eq('tenant_id', entry.tenantId);
        }
        continue;
      }

      const payload = (await response.json()) as { data?: Array<Record<string, unknown>> };
      const resultList = Array.isArray(payload?.data) ? payload.data : [];

      for (let i = 0; i < messageChunk.length; i += 1) {
        const entry = messageChunk[i];
        const ticket = resultList[i] || {};
        const status = String(ticket.status || '').trim().toLowerCase();
        const details = ticket.details && typeof ticket.details === 'object'
          ? (ticket.details as Record<string, unknown>)
          : {};
        const expoError = String(details.error || ticket.message || '').trim();

        if (status === 'ok') {
          sent += 1;
          await supabase
            .from('mobile_push_subscriptions')
            .update({
              last_notified_on: today,
              last_error: null,
              updated_at: new Date().toISOString()
            })
            .eq('id', entry.subscriptionId)
            .eq('tenant_id', entry.tenantId);
          continue;
        }

        if (expoError === 'DeviceNotRegistered') {
          disabled += 1;
          await supabase
            .from('mobile_push_subscriptions')
            .update({
              enabled: false,
              last_error: expoError,
              updated_at: new Date().toISOString()
            })
            .eq('id', entry.subscriptionId)
            .eq('tenant_id', entry.tenantId);
          continue;
        }

        failed += 1;
        await supabase
          .from('mobile_push_subscriptions')
          .update({
            last_error: expoError || 'unknown_push_error',
            updated_at: new Date().toISOString()
          })
          .eq('id', entry.subscriptionId)
          .eq('tenant_id', entry.tenantId);
      }
    }

    sendJson(res, 200, {
      success: true,
      sent,
      failed,
      disabled,
      candidates: sendQueue.length
    });
  } catch (error) {
    const normalized = normalizeError(error, 'Failed to dispatch mobile push notifications');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
