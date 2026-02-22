import { methodNotAllowed, normalizeError, readJsonBody, sendJson, type RequestLike, type ResponseLike } from '../_lib/http.js';

type ClientEventPayload = {
  event_type?: string;
  severity?: string;
  path?: string;
  message?: string;
  stack?: string;
  status_code?: number | null;
  duration_ms?: number | null;
  context?: Record<string, unknown> | null;
  occurred_at?: string;
  user_agent?: string;
};

const MAX_MESSAGE_LENGTH = 600;
const MAX_STACK_LENGTH = 3500;
const MAX_PATH_LENGTH = 300;
const MAX_EVENTS_PER_MINUTE_PER_SIGNATURE = 20;
const RATE_LIMIT_WINDOW_MS = 60_000;

const ALLOWED_EVENT_TYPES = new Set([
  'ui_error',
  'unhandled_promise',
  'api_error',
  'slow_api',
  'query_error',
  'mutation_error'
]);

const signatureWindowState = new Map<string, { windowStartMs: number; count: number }>();

function clampText(value: unknown, maxLength: number) {
  const normalized = String(value || '').trim();
  return normalized.length > maxLength ? normalized.slice(0, maxLength) : normalized;
}

function allowEvent(signature: string) {
  const nowMs = Date.now();
  const state = signatureWindowState.get(signature);

  if (!state) {
    signatureWindowState.set(signature, { windowStartMs: nowMs, count: 1 });
    return true;
  }

  if (nowMs - state.windowStartMs > RATE_LIMIT_WINDOW_MS) {
    state.windowStartMs = nowMs;
    state.count = 1;
    return true;
  }

  state.count += 1;
  return state.count <= MAX_EVENTS_PER_MINUTE_PER_SIGNATURE;
}

function normalizePayload(body: ClientEventPayload) {
  const eventType = clampText(body?.event_type || 'ui_error', 48);
  const severity = clampText(body?.severity || 'error', 16) || 'error';
  const path = clampText(body?.path || '', MAX_PATH_LENGTH);
  const message = clampText(body?.message || '', MAX_MESSAGE_LENGTH);
  const stack = clampText(body?.stack || '', MAX_STACK_LENGTH);
  const statusCode = Number(body?.status_code);
  const durationMs = Number(body?.duration_ms);
  const occurredAt = clampText(body?.occurred_at || '', 60);
  const userAgent = clampText(body?.user_agent || '', 300);

  return {
    eventType,
    severity,
    path,
    message,
    stack,
    statusCode: Number.isFinite(statusCode) ? statusCode : null,
    durationMs: Number.isFinite(durationMs) ? durationMs : null,
    occurredAt,
    userAgent,
    context: body?.context && typeof body.context === 'object' ? body.context : null
  };
}

export default async function handler(req: RequestLike, res: ResponseLike) {
  try {
    if ((req.method || 'POST').toUpperCase() !== 'POST') {
      throw methodNotAllowed('POST');
    }

    const payload = normalizePayload(readJsonBody<ClientEventPayload>(req));
    if (!ALLOWED_EVENT_TYPES.has(payload.eventType)) {
      sendJson(res, 400, { error: 'Unsupported telemetry event_type' });
      return;
    }

    const signature = `${payload.eventType}:${payload.path}:${payload.message}`;
    if (!allowEvent(signature)) {
      sendJson(res, 202, { ok: true, dropped: true, reason: 'rate_limited' });
      return;
    }

    // Intentionally structured for Vercel log parsers and alerting tools.
    console.error('[client-telemetry]', JSON.stringify({
      event_type: payload.eventType,
      severity: payload.severity,
      path: payload.path,
      status_code: payload.statusCode,
      duration_ms: payload.durationMs,
      occurred_at: payload.occurredAt || new Date().toISOString(),
      message: payload.message,
      stack: payload.stack,
      user_agent: payload.userAgent,
      context: payload.context
    }));

    sendJson(res, 200, { ok: true });
  } catch (error) {
    const normalized = normalizeError(error, 'Telemetry ingestion failed');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
