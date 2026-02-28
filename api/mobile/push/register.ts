import { getCurrentUser } from '../../_lib/auth.js';
import { methodNotAllowed, normalizeError, readJsonBody, sendJson, type RequestLike, type ResponseLike } from '../../_lib/http.js';
import { getServerSupabase } from '../../_lib/supabase.js';

type RegisterBody = {
  deviceId?: string;
  platform?: string;
  expoPushToken?: string | null;
  appVersion?: string | null;
  enabled?: boolean;
};

const DEVICE_ID_PATTERN = /^[a-zA-Z0-9._:-]{6,160}$/;
const PUSH_TOKEN_PATTERN = /^(ExponentPushToken|ExpoPushToken)\[[A-Za-z0-9_-]+\]$/;

function badRequest(message: string) {
  return Object.assign(new Error(message), { status: 400 });
}

function normalizePlatform(value: unknown) {
  const platform = String(value || '').trim().toLowerCase();
  if (platform === 'ios' || platform === 'android') return platform;
  return '';
}

export default async function handler(req: RequestLike, res: ResponseLike) {
  try {
    if ((req.method || 'POST').toUpperCase() !== 'POST') {
      throw methodNotAllowed('POST');
    }

    const user = await getCurrentUser(req);
    const tenantId = String(user.tenantId || '').trim().toLowerCase();
    if (!tenantId) throw badRequest('Tenant not available for user');

    const body = readJsonBody<RegisterBody>(req);
    const deviceId = String(body.deviceId || '').trim();
    const platform = normalizePlatform(body.platform);
    const enabled = body.enabled !== false;
    const expoPushToken = String(body.expoPushToken || '').trim();
    const appVersion = String(body.appVersion || '').trim();

    if (!DEVICE_ID_PATTERN.test(deviceId)) {
      throw badRequest('Invalid deviceId');
    }
    if (!platform) {
      throw badRequest('platform must be ios or android');
    }
    if (enabled && !PUSH_TOKEN_PATTERN.test(expoPushToken)) {
      throw badRequest('Valid expoPushToken is required when enabled=true');
    }

    const nowIso = new Date().toISOString();
    const supabase = getServerSupabase() as any;
    const upsertResult = await supabase
      .from('mobile_push_subscriptions')
      .upsert(
        {
          tenant_id: tenantId,
          user_id: user.userId,
          user_email: user.email || null,
          device_id: deviceId,
          platform,
          expo_push_token: enabled ? expoPushToken : null,
          app_version: appVersion || null,
          enabled,
          last_seen_at: nowIso,
          updated_at: nowIso,
          last_error: null
        },
        {
          onConflict: 'tenant_id,user_id,device_id'
        }
      )
      .select('id,enabled,last_seen_at')
      .limit(1);

    if (upsertResult.error) {
      throw Object.assign(new Error(upsertResult.error.message), { status: 500 });
    }

    const row = Array.isArray(upsertResult.data) ? upsertResult.data[0] : null;
    sendJson(res, 200, {
      success: true,
      subscriptionId: String(row?.id || ''),
      enabled: Boolean(row?.enabled),
      lastSeenAt: row?.last_seen_at || nowIso
    });
  } catch (error) {
    const normalized = normalizeError(error, 'Failed to register mobile push token');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
