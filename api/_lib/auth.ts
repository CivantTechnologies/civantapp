import { getHeader, type RequestLike } from './http.js';
import { getServerSupabase } from './supabase.js';
import type { Json } from './db.types.js';
import {
  isSupportGrantActive,
  normalizeSupportUserId,
  requiresSupportGrant
} from '../../shared/supportAccessPolicy.js';

export type CurrentUser = {
  userId: string;
  email: string;
  role: string;
  roles: string[];
  tenantId: string;
};

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;
const PRIVILEGED_ROLES = new Set(['admin', 'creator']);
const ROLE_PATTERN = /^(admin|creator|user|viewer)$/;

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

function normalizeRole(value: unknown) {
  const role = String(value || '').trim().toLowerCase();
  return ROLE_PATTERN.test(role) ? role : 'user';
}

function isMissingColumnOrTable(error: unknown) {
  const code = String((error as { code?: string })?.code || '').trim();
  const message = String((error as { message?: string })?.message || '').toLowerCase();
  return (
    code === '42703' ||
    code === '42P01' ||
    code === 'PGRST204' ||
    message.includes('does not exist') ||
    message.includes('could not find the') ||
    message.includes('schema cache')
  );
}

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
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
  const authUserId = String(authData.user.id || '').trim();
  if (!authUserId) throw unauthorized('User id missing in token');

  const bootstrapTenantId = 'civant_default';
  const bootstrapRole = 'user';

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

  let userRow = userRows.length > 0 ? userRows[0] : null;
  if (!userRow) {
    const ensureDefaultTenantResult = await supabase
      .from('tenants')
      .upsert(
        {
          id: 'civant_default',
          name: 'Civant Default Tenant',
          regions: ['IE', 'FR'],
          plan: 'starter'
        },
        { onConflict: 'id' }
      );
    if (ensureDefaultTenantResult.error && !isMissingColumnOrTable(ensureDefaultTenantResult.error)) {
      throw Object.assign(new Error(ensureDefaultTenantResult.error.message), { status: 500 });
    }

    const insertWithRoleResult = await supabase
      .from('users')
      .upsert(
        {
          id: authUserId,
          email,
          tenant_id: bootstrapTenantId,
          role: bootstrapRole
        },
        { onConflict: 'email' }
      )
      .select('id,email,tenant_id,role')
      .limit(1);

    if (insertWithRoleResult.error) {
      if (!isMissingColumnOrTable(insertWithRoleResult.error)) {
        throw Object.assign(new Error(insertWithRoleResult.error.message), { status: 500 });
      }

      const fallbackInsertResult = await supabase
        .from('users')
        .upsert(
          {
            id: authUserId,
            email,
            tenant_id: bootstrapTenantId
          },
          { onConflict: 'email' }
        )
        .select('id,email,tenant_id')
        .limit(1);

      if (fallbackInsertResult.error) {
        throw Object.assign(new Error(fallbackInsertResult.error.message), { status: 500 });
      }
      userRow = Array.isArray(fallbackInsertResult.data) ? fallbackInsertResult.data[0] : null;
      inlineRole = bootstrapRole;
    } else {
      userRow = Array.isArray(insertWithRoleResult.data) ? insertWithRoleResult.data[0] : null;
      inlineRole = normalizeRole(userRow?.role);
    }

    if (!userRow) {
      throw forbidden('User not registered in Civant');
    }
  }

  const userId = String(userRow.id || '');
  const tenantId = normalizeTenantId(userRow.tenant_id) || bootstrapTenantId;

  const roleWriteResult = await supabase
    .from('user_roles')
    .upsert(
      {
        user_id: userId,
        role: normalizeRole(inlineRole || bootstrapRole)
      },
      { onConflict: 'user_id,role' }
    );
  if (roleWriteResult.error && !isMissingColumnOrTable(roleWriteResult.error)) {
    throw Object.assign(new Error(roleWriteResult.error.message), { status: 500 });
  }

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
    const role = normalizeRole(roleRow.role);
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
  return normalizeTenantId(user.tenantId) === normalizeTenantId(tenantId);
}

export function requireTenantAccess(user: CurrentUser, tenantId: string) {
  if (!userCanAccessTenant(user, tenantId)) {
    throw forbidden('Forbidden for tenant');
  }
}

async function getActiveSupportGrantForUser(userId: string, tenantId: string) {
  const supportUserId = normalizeSupportUserId(userId);
  const normalizedTenantId = normalizeTenantId(tenantId);
  if (!supportUserId || !normalizedTenantId) return null;

  const supabase = getServerSupabase();
  const { data, error } = await supabase
    .from('support_access_grants')
    .select('id,tenant_id,support_user_id,enabled,expires_at,revoked_at,reason')
    .eq('tenant_id', normalizedTenantId)
    .eq('support_user_id', supportUserId)
    .eq('enabled', true)
    .order('created_at', { ascending: false })
    .limit(20);

  if (error) {
    if (isMissingColumnOrTable(error)) return null;
    throw Object.assign(new Error(error.message), { status: 500 });
  }

  const rows = Array.isArray(data) ? data : [];
  return rows.find((row) => isSupportGrantActive(row)) || null;
}

async function writeSupportAccessAudit(params: {
  user: CurrentUser;
  tenantId: string;
  action: string;
  metadata?: Json | null;
}) {
  const { user, tenantId, action, metadata } = params;
  const supabase = getServerSupabase();
  const { error } = await supabase
    .from('support_access_audit')
    .insert({
      id: makeId('sa_audit'),
      tenant_id: tenantId,
      actor_user_id: user.userId || 'unknown',
      actor_email: user.email || null,
      action,
      reason: null,
      metadata_json: metadata || null,
      created_at: new Date().toISOString()
    });

  if (error) {
    throw Object.assign(new Error(error.message), { status: 500 });
  }
}

export async function requireTenantAccessWithSupportGrant(params: {
  req: RequestLike;
  user: CurrentUser;
  tenantId: string;
  action?: string;
  metadata?: Json | null;
}) {
  const { req, user } = params;
  const tenantId = normalizeTenantId(params.tenantId);
  if (!tenantId || !TENANT_ID_PATTERN.test(tenantId)) {
    throw badRequest('Invalid tenant id');
  }

  if (!requiresSupportGrant(user.tenantId, tenantId)) {
    return;
  }

  if (!isPrivileged(user)) {
    throw forbidden('Forbidden for tenant');
  }

  const activeGrant = await getActiveSupportGrantForUser(user.userId, tenantId);
  if (!activeGrant) {
    throw forbidden('Support access grant required for tenant');
  }

  await writeSupportAccessAudit({
    user,
    tenantId,
    action: String(params.action || 'CROSS_TENANT_ACCESS'),
    metadata: {
      grantId: String(activeGrant.id || ''),
      method: String(req.method || ''),
      ...((params.metadata && typeof params.metadata === 'object') ? params.metadata : {})
    } as Json
  });
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
