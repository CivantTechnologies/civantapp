import { createClientFromRequest } from './civantSdk.ts';
import {
  isSupportGrantActive,
  normalizeSupportUserId,
  requiresSupportGrant
} from '../shared/supportAccessPolicy.js';

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

const PRIVILEGED_ROLES = new Set(['admin', 'creator']);

export function resolveTenantId(requestedTenantId?: unknown) {
  const normalized = String(requestedTenantId || '').trim().toLowerCase();
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

function internalServerError(message = 'Internal server error') {
  const err = new Error(message);
  (err as Error & { status?: number }).status = 500;
  return err;
}

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
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

export function hasRole(user: CurrentUser, role: string) {
  return user.roles.includes(String(role || '').toLowerCase());
}

export function isPrivileged(user: CurrentUser) {
  return user.roles.some((role) => PRIVILEGED_ROLES.has(role));
}

export function canAccessTenant(user: CurrentUser, tenantId: string) {
  const normalizedTenant = resolveTenantId(tenantId);
  return user.tenantId === normalizedTenant;
}

async function getActiveSupportGrantForUser(params: {
  civant: CivantClient;
  tenantId: string;
  supportUserId: string;
}) {
  const { civant, tenantId } = params;
  const supportUserId = normalizeSupportUserId(params.supportUserId);
  if (!supportUserId) return null;

  const rows = await civant.asServiceRole.entities.support_access_grants.filter({
    tenant_id: tenantId,
    support_user_id: supportUserId,
    enabled: true
  }, '-created_at', 50).catch(() => null);

  if (!Array.isArray(rows) || rows.length === 0) return null;
  return rows.find((row: Record<string, unknown>) => isSupportGrantActive(row)) || null;
}

async function writeSupportAccessAudit(params: {
  civant: CivantClient;
  user: CurrentUser;
  tenantId: string;
  action: string;
  metadata?: Record<string, unknown> | null;
}) {
  const { civant, user, tenantId, action, metadata } = params;
  await civant.asServiceRole.entities.support_access_audit.create({
    id: makeId('sa_audit'),
    tenant_id: tenantId,
    actor_user_id: user.userId || 'unknown',
    actor_email: user.email || null,
    action,
    reason: null,
    metadata_json: metadata || null,
    created_at: new Date().toISOString()
  });
}

export async function requireSupportAccessForTenant(params: {
  civant: CivantClient;
  req: Request;
  tenantId: string;
  user?: CurrentUser;
  action?: string;
  metadata?: Record<string, unknown> | null;
}) {
  const user = params.user || await getCurrentUserFromRequest(params.civant, params.req);
  const normalizedTenant = resolveTenantId(params.tenantId);

  if (!requiresSupportGrant(user.tenantId, normalizedTenant)) {
    return user;
  }
  if (!isPrivileged(user)) {
    throw forbidden('Forbidden for tenant');
  }

  const grant = await getActiveSupportGrantForUser({
    civant: params.civant,
    tenantId: normalizedTenant,
    supportUserId: user.userId
  });
  if (!grant) {
    throw forbidden('Support access grant required for tenant');
  }

  try {
    await writeSupportAccessAudit({
      civant: params.civant,
      user,
      tenantId: normalizedTenant,
      action: String(params.action || 'CROSS_TENANT_ACCESS'),
      metadata: {
        grantId: String((grant as Record<string, unknown>).id || ''),
        method: String(params.req.method || ''),
        ...((params.metadata && typeof params.metadata === 'object') ? params.metadata : {})
      }
    });
  } catch {
    throw internalServerError('Support access audit write failed');
  }

  return user;
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

export async function requireTenantAccess(params: {
  civant: CivantClient;
  req: Request;
  tenantId: string;
}) {
  const user = await getCurrentUserFromRequest(params.civant, params.req);
  const normalizedTenant = resolveTenantId(params.tenantId);
  if (canAccessTenant(user, normalizedTenant)) {
    return user;
  }
  return requireSupportAccessForTenant({
    civant: params.civant,
    req: params.req,
    tenantId: normalizedTenant,
    user
  });
}

export async function checkIsAdminForTenant(params: {
  civant: CivantClient;
  req: Request;
  tenantId: string;
}) {
  const user = await requireTenantAccess(params);
  return isPrivileged(user);
}

export async function requireAdminForTenant(params: {
  civant: CivantClient;
  req: Request;
  tenantId: string;
}) {
  const user = await requireTenantAccess(params);
  if (!isPrivileged(user)) {
    throw forbidden('Admin or creator role required');
  }
  return user;
}

export async function requireCreator(params: {
  civant: CivantClient;
  req: Request;
}) {
  const user = await getCurrentUserFromRequest(params.civant, params.req);
  if (!hasRole(user, 'creator')) {
    throw forbidden('Creator role required');
  }
  return user;
}
