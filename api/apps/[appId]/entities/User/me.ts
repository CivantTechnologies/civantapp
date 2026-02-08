import { getCurrentUserPayload } from '../../../../_lib/handlers.js';
import { methodNotAllowed, normalizeError, sendJson, type RequestLike, type ResponseLike } from '../../../../_lib/http.js';

export default async function handler(req: RequestLike, res: ResponseLike) {
  try {
    if ((req.method || 'GET').toUpperCase() !== 'GET') {
      throw methodNotAllowed('GET');
    }

    const payload = await getCurrentUserPayload(req);
    sendJson(res, 200, payload);
  } catch (error) {
    const normalized = normalizeError(error, 'Request failed');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
