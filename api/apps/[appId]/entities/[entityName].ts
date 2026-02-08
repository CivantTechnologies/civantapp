import {
  createEntity,
  deleteManyEntity,
  listOrFilterEntity
} from '../../../../_lib/entities.js';
import { methodNotAllowed, normalizeError, sendJson, type RequestLike, type ResponseLike } from '../../../../_lib/http.js';

type EntityRequest = RequestLike & {
  query?: Record<string, unknown>;
};

export default async function handler(req: EntityRequest, res: ResponseLike) {
  try {
    const method = (req.method || 'GET').toUpperCase();

    if (method === 'GET') {
      const payload = await listOrFilterEntity(req);
      sendJson(res, 200, payload);
      return;
    }

    if (method === 'POST') {
      const payload = await createEntity(req);
      sendJson(res, 200, payload);
      return;
    }

    if (method === 'DELETE') {
      const payload = await deleteManyEntity(req);
      sendJson(res, 200, payload);
      return;
    }

    throw methodNotAllowed('GET, POST or DELETE');
  } catch (error) {
    const normalized = normalizeError(error, 'Request failed');
    sendJson(res, normalized.status, { error: normalized.message });
  }
}
