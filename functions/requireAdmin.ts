import { createClientFromRequest } from './civantSdk.ts';

type CivantClient = ReturnType<typeof createClientFromRequest>;

export type CurrentUser = {
  userId: string;
  email: string;
  role: string;
  tenantId: string;
  sessionId: string;
};

export function resolveTenantId(requestedTenantId?: unknown) {
  const normalized = String(requestedTenantId || '').trim();
  return normalized || Deno.env.get('DEFAULT_TENANT_ID') || 'civant_default';
}

function unauthorized(message = 'Unauthorized') {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 401;
  return err;
}

function forbidden(message = 'Forbidden') {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 403;
  return err;
}

function parseBearerToken(req: Request) {
  const header = String(req.headers.get('Authorization') || '');
  if (!header) return '';
  if (header.toLowerCase().startsWith('bearer ')) {
    return header.slice(7).trim();
  }
  return header.trim();
}

export async function getCurrentUserFromRequest(civant: CivantClient, req: Request): Promise<CurrentUser> {
  const token = parseBearerToken(req);
  if (!token) throw unauthorized('Missing bearer token');

  const sessions = await civant.asServiceRole.entities.sessions.filter({ token }, '-created_at', 1);
  const session = Array.isArray(sessions) && sessions.length > 0
    ? sessions[0] as Record<string, unknown>
    : null;
  if (!session) throw unauthorized('Invalid session token');

  const expiresAt = new Date(String(session.expires_at || ''));
  if (Number.isNaN(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    throw unauthorized('Session expired');
  }

  const userId = String(session.user_id || '');
  if (!userId) {
    throw unauthorized('Invalid session user');
  }
  const user = await civant.asServiceRole.entities.users.get(userId) as Record<string, unknown> | null;
  if (!user) throw unauthorized('User not found');

  return {
    userId: String(user.id || ''),
    email: String(user.email || ''),
    role: String(user.role || 'user'),
    tenantId: resolveTenantId(user.tenant_id),
    sessionId: String(session.id || '')
  };
}

export async function requireAuthenticatedUser(params: {
  civant: CivantClient;
  req: Request;
}) {
  const { civant, req } = params;
  return getCurrentUserFromRequest(civant, req);
}

export async function checkIsAdminForTenant(params: {
  civant: CivantClient;
  req: Request;
  tenantId: string;
}) {
  const user = await getCurrentUserFromRequest(params.civant, params.req);
  if (user.tenantId !== params.tenantId) return false;
  return user.role === 'admin';
}

export async function requireAdminForTenant(params: {
  civant: CivantClient;
  req: Request;
  tenantId: string;
}) {
  const user = await getCurrentUserFromRequest(params.civant, params.req);
  if (user.tenantId !== params.tenantId) {
    throw forbidden('Forbidden for tenant');
  }
  if (user.role !== 'admin') {
    throw forbidden('Admin role required');
  }
  return user;
}
