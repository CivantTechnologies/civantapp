import { normalizeError, sendJson, type RequestLike, type ResponseLike } from '../../../../../_lib/http.js';

type LogRequest = RequestLike & {
  query?: Record<string, unknown>;
};

export default async function handler(req: LogRequest, res: ResponseLike) {
  try {
    if ((req.method || 'POST').toUpperCase() !== 'POST') {
      sendJson(res, 405, { error: 'Method not allowed. Use POST.' });
      return;
    }

    const pageName = String(req.query?.pageName || '').trim();
    if (!pageName) {
      sendJson(res, 400, { error: 'Missing page name' });
      return;
    }

    // Compatibility endpoint for legacy page tracking.
    // No-op by design in the Vercel API layer.
    sendJson(res, 200, { ok: true });
  } catch (error) {
    const normalized = normalizeError(error, 'Request failed');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
