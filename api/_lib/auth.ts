import { getHeader, type RequestLike } from './http.js';
import { getServerSupabase } from './supabase.js';

export type CurrentUser = {
  userId: string;
  email: string;
  role: string;
  roles: string[];
  tenantId: string;
};

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;
const PRIVILEGED_ROLES = new Set(['admin', 'creator']);

function unauthorized(message = 'Unauthorized') {
  return Object.assign(new Error(message), { status: 401 });
}

function forbidden(message = 'Forbidden') {
  return Object.assign(new Error(message), { status: 403 });
}

function badRequest(message: string) {
  return Object.assign(new Error(message), { status: 400 });
}

function normalizeTenantId(value: unknown) {
  return String(value || '').trim().toLowerCase();
}

function isMissingColumnOrTable(error: unknown) {
  const code = String((error as { code?: string })?.code || '').trim();
  const message = String((error as { message?: string })?.message || '').toLowerCase();
  return code === '42703' || code === '42P01' || message.includes('does not exist');
}

export function getBearerToken(req: RequestLike) {
  const header = getHeader(req, 'authorization').trim();
  if (!header) return '';
  if (header.toLowerCase().startsWith('bearer ')) return header.slice(7).trim();
  return header;
}

export function getTenantFromHeader(req: RequestLike) {
  const tenantId = normalizeTenantId(getHeader(req, 'x-tenant-id'));
  if (!tenantId) throw badRequest('Missing x-tenant-id');
  if (!TENANT_ID_PATTERN.test(tenantId)) throw badRequest('Invalid x-tenant-id');
  return tenantId;
}

export async function getCurrentUser(req: RequestLike): Promise<CurrentUser> {
  const token = getBearerToken(req);
  if (!token) throw unauthorized('Missing bearer token');

  const supabase = getServerSupabase();
  const { data: authData, error: authError } = await supabase.auth.getUser(token);
  if (authError || !authData?.user) throw unauthorized('Invalid auth token');

  const email = String(authData.user.email || '').trim().toLowerCase();
  if (!email) throw unauthorized('Email claim missing in token');

  let userRows: Array<Record<string, unknown>> = [];
  let inlineRole = '';
  const preferredUserSelect = await supabase
    .from('users')
    .select('id,email,tenant_id,role')
    .eq('email', email)
    .limit(1);

  if (preferredUserSelect.error) {
    if (!isMissingColumnOrTable(preferredUserSelect.error)) {
      throw Object.assign(new Error(preferredUserSelect.error.message), { status: 500 });
    }

    const fallbackUserSelect = await supabase
      .from('users')
      .select('id,email,tenant_id')
      .eq('email', email)
      .limit(1);

    if (fallbackUserSelect.error) {
      throw Object.assign(new Error(fallbackUserSelect.error.message), { status: 500 });
    }
    userRows = Array.isArray(fallbackUserSelect.data)
      ? (fallbackUserSelect.data as Array<Record<string, unknown>>)
      : [];
  } else {
    userRows = Array.isArray(preferredUserSelect.data)
      ? (preferredUserSelect.data as Array<Record<string, unknown>>)
      : [];
    const firstRow = userRows[0];
    inlineRole = String(firstRow?.role || '').trim().toLowerCase();
  }

  const userRow = userRows.length > 0 ? userRows[0] : null;
  if (!userRow) throw forbidden('User not registered in Civant');

  const userId = String(userRow.id || '');
  const tenantId = normalizeTenantId(userRow.tenant_id);

  const { data: roleRows, error: roleError } = await supabase
    .from('user_roles')
    .select('role')
    .eq('user_id', userId);

  const resolvedRoleRows = roleError && isMissingColumnOrTable(roleError)
    ? []
    : (Array.isArray(roleRows) ? roleRows : []);
  if (roleError && !isMissingColumnOrTable(roleError)) {
    throw Object.assign(new Error(roleError.message), { status: 500 });
  }

  const roleSet = new Set<string>();
  if (inlineRole) roleSet.add(inlineRole);

  for (const roleRow of resolvedRoleRows) {
    const role = String(roleRow.role || '').trim().toLowerCase();
    if (role) roleSet.add(role);
  }

  if (!roleSet.size) roleSet.add('user');

  const roles = Array.from(roleSet);
  const role = roles.includes('admin') ? 'admin' : roles.includes('creator') ? 'creator' : roles[0];

  return { userId, email, role, roles, tenantId };
}

export function hasRole(user: CurrentUser, role: string) {
  return user.roles.includes(String(role || '').trim().toLowerCase());
}

export function isPrivileged(user: CurrentUser) {
  return user.roles.some((role) => PRIVILEGED_ROLES.has(role));
}

export function userCanAccessTenant(user: CurrentUser, tenantId: string) {
  if (hasRole(user, 'creator')) return true;
  return normalizeTenantId(user.tenantId) === normalizeTenantId(tenantId);
}

export function requireTenantAccess(user: CurrentUser, tenantId: string) {
  if (!userCanAccessTenant(user, tenantId)) {
    throw forbidden('Forbidden for tenant');
  }
}

export function requirePrivileged(user: CurrentUser) {
  if (!isPrivileged(user)) {
    throw forbidden('Admin or creator role required');
  }
}

export function requireCreator(user: CurrentUser) {
  if (!hasRole(user, 'creator')) {
    throw forbidden('Creator role required');
  }
}
