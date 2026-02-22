const TELEMETRY_ENDPOINT = '/api/monitoring/client-event';
const MAX_EVENTS_PER_MINUTE_PER_KEY = 6;
const EVENT_WINDOW_MS = 60_000;
const MAX_MESSAGE_LENGTH = 500;
const MAX_STACK_LENGTH = 3000;
const MAX_PATH_LENGTH = 240;

const eventRateState = new Map();

function text(value, maxLen) {
  const normalized = String(value || '').trim();
  return normalized.length > maxLen ? normalized.slice(0, maxLen) : normalized;
}

function now() {
  return Date.now();
}

function shouldDropEvent(key) {
  const ts = now();
  const state = eventRateState.get(key);
  if (!state) {
    eventRateState.set(key, { windowStart: ts, count: 1 });
    return false;
  }

  if (ts - state.windowStart > EVENT_WINDOW_MS) {
    state.windowStart = ts;
    state.count = 1;
    return false;
  }

  state.count += 1;
  return state.count > MAX_EVENTS_PER_MINUTE_PER_KEY;
}

function normalizeEvent(event) {
  const path = text(event.path || (typeof window !== 'undefined' ? window.location.pathname : ''), MAX_PATH_LENGTH);
  const message = text(event.message, MAX_MESSAGE_LENGTH);
  const stack = text(event.stack, MAX_STACK_LENGTH);
  return {
    event_type: text(event.event_type || 'client_event', 48),
    severity: text(event.severity || 'error', 16),
    path,
    message,
    stack,
    status_code: Number(event.status_code) || null,
    duration_ms: Number(event.duration_ms) || null,
    context: event.context && typeof event.context === 'object' ? event.context : null,
    occurred_at: new Date().toISOString(),
    user_agent: typeof navigator !== 'undefined' ? text(navigator.userAgent, 220) : ''
  };
}

function postTelemetry(payload) {
  const body = JSON.stringify(payload);

  if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
    try {
      const blob = new Blob([body], { type: 'application/json' });
      if (navigator.sendBeacon(TELEMETRY_ENDPOINT, blob)) return;
    } catch {
      // Fall back to fetch below.
    }
  }

  if (typeof fetch !== 'function') return;
  fetch(TELEMETRY_ENDPOINT, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body,
    keepalive: true
  }).catch(() => {
    // Never throw from telemetry path.
  });
}

export function sendClientTelemetry(event) {
  if (typeof window === 'undefined') return;
  if (!event || typeof event !== 'object') return;

  const payload = normalizeEvent(event);
  const dedupeKey = `${payload.event_type}:${payload.path}:${payload.message}`;
  if (shouldDropEvent(dedupeKey)) return;

  postTelemetry(payload);
}
