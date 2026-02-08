import { dispatchFunction } from '../../../_lib/handlers';
import { methodNotAllowed, normalizeError, sendJson, type RequestLike, type ResponseLike } from '../../../_lib/http';

export default async function handler(req: RequestLike & { query?: Record<string, unknown> }, res: ResponseLike) {
  try {
    if ((req.method || 'GET').toUpperCase() !== 'POST') {
      throw methodNotAllowed('POST');
    }

    const functionName = String(req.query?.functionName || '').trim();
    if (!functionName) {
      sendJson(res, 400, { error: 'Missing function name' });
      return;
    }

    const payload = await dispatchFunction(functionName, req);
    sendJson(res, 200, payload);
  } catch (error) {
    const normalized = normalizeError(error, 'Request failed');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
