import { createClientFromRequest } from './civantSdk.ts';

type CivantClient = ReturnType<typeof createClientFromRequest>;

type SupabaseUser = {
  id?: string;
  email?: string;
};

export type CurrentUser = {
  userId: string;
  email: string;
  role: string;
  roles: string[];
  tenantId: string;
  sessionId: string;
};

const ALLOWED_SYSTEM_ROLES = new Set(['admin', 'creator']);

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

async function getSupabaseUserFromToken(token: string): Promise<SupabaseUser> {
  const supabaseUrl = String(Deno.env.get('SUPABASE_URL') || Deno.env.get('VITE_SUPABASE_URL') || '').trim();
  const supabaseAnonKey = String(Deno.env.get('SUPABASE_ANON_KEY') || Deno.env.get('VITE_SUPABASE_ANON_KEY') || '').trim();

  if (!supabaseUrl || !supabaseAnonKey) {
    throw unauthorized('Supabase is not configured');
  }

  const response = await fetch(`${supabaseUrl.replace(/\/$/, '')}/auth/v1/user`, {
    method: 'GET',
    headers: {
      Authorization: `Bearer ${token}`,
      apikey: supabaseAnonKey
    }
  });

  if (!response.ok) {
    throw unauthorized('Invalid auth token');
  }

  const payload = await response.json().catch(() => ({}));
  return payload as SupabaseUser;
}

async function resolveCurrentUser(civant: CivantClient, email: string): Promise<CurrentUser> {
  const normalizedEmail = String(email || '').trim().toLowerCase();
  if (!normalizedEmail) throw unauthorized('Email claim missing in token');

  const rows = await civant.asServiceRole.entities.users.filter({ email: normalizedEmail }, '-created_at', 1);
  const userRow = Array.isArray(rows) && rows.length > 0 ? rows[0] as Record<string, unknown> : null;
  if (!userRow) throw forbidden('User not registered in Civant');

  const userId = String(userRow.id || '');
  const tenantId = resolveTenantId(userRow.tenant_id);

  const roleRows = await civant.asServiceRole.entities.user_roles.filter({ user_id: userId }, '-role', 50)
    .catch(() => []);

  const roles = new Set<string>();
  const inlineRole = String(userRow.role || '').trim().toLowerCase();
  if (inlineRole) roles.add(inlineRole);

  if (Array.isArray(roleRows)) {
    for (const row of roleRows as Array<Record<string, unknown>>) {
      const role = String(row.role || '').trim().toLowerCase();
      if (role) roles.add(role);
    }
  }

  if (!roles.size) {
    roles.add('user');
  }

  const roleList = Array.from(roles);
  const primaryRole = roleList.includes('admin')
    ? 'admin'
    : roleList.includes('creator')
      ? 'creator'
      : roleList[0];

  return {
    userId,
    email: normalizedEmail,
    role: primaryRole,
    roles: roleList,
    tenantId,
    sessionId: ''
  };
}

export async function getCurrentUserFromRequest(civant: CivantClient, req: Request): Promise<CurrentUser> {
  const token = parseBearerToken(req);
  if (!token) throw unauthorized('Missing bearer token');

  const authUser = await getSupabaseUserFromToken(token);
  return resolveCurrentUser(civant, String(authUser.email || ''));
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
  return user.roles.some((role) => ALLOWED_SYSTEM_ROLES.has(role));
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
  if (!user.roles.some((role) => ALLOWED_SYSTEM_ROLES.has(role))) {
    throw forbidden('Admin or creator role required');
  }
  return user;
}
