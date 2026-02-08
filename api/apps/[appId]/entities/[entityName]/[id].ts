import {
  deleteEntityById,
  getEntityById,
  updateEntityById
} from '../../../../../_lib/entities.js';
import { methodNotAllowed, normalizeError, sendJson, type RequestLike, type ResponseLike } from '../../../../../_lib/http.js';

type EntityRequest = RequestLike & {
  query?: Record<string, unknown>;
};

export default async function handler(req: EntityRequest, res: ResponseLike) {
  try {
    const method = (req.method || 'GET').toUpperCase();

    if (method === 'GET') {
      const payload = await getEntityById(req);
      sendJson(res, 200, payload);
      return;
    }

    if (method === 'PUT') {
      const payload = await updateEntityById(req);
      sendJson(res, 200, payload);
      return;
    }

    if (method === 'DELETE') {
      const payload = await deleteEntityById(req);
      sendJson(res, 200, payload);
      return;
    }

    throw methodNotAllowed('GET, PUT or DELETE');
  } catch (error) {
    const normalized = normalizeError(error, 'Request failed');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
