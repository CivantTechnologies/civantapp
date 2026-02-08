import {
  getCurrentUser,
  getTenantFromHeader,
  hasRole,
  isPrivileged,
  requireCreator,
  requirePrivileged,
  requireTenantAccess,
  type CurrentUser
} from './auth.js';
import { readJsonBody, type RequestLike } from './http.js';
import { getServerSupabase } from './supabase.js';

const CONNECTOR_MAP: Record<string, { key: string; displayName: string }> = {
  BOAMP_FR: { key: 'BOAMP_FR', displayName: 'BOAMP France' },
  TED: { key: 'TED', displayName: 'TED (EU)' },
  TED_IE: { key: 'TED_IE', displayName: 'TED Ireland' },
  TED_FR: { key: 'TED_FR', displayName: 'TED France' },
  ETENDERS_IE: { key: 'ETENDERS_IE', displayName: 'eTenders Ireland' }
};

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

function badRequest(message: string) {
  return Object.assign(new Error(message), { status: 400 });
}

function conflict(message: string) {
  return Object.assign(new Error(message), { status: 409 });
}

function toDisplay(connectorKey: string) {
  if (CONNECTOR_MAP[connectorKey]) return CONNECTOR_MAP[connectorKey];
  return { key: connectorKey, displayName: connectorKey };
}

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

function isExpired(expiresAt: unknown) {
  if (!expiresAt) return false;
  const timestamp = new Date(String(expiresAt)).getTime();
  if (Number.isNaN(timestamp)) return false;
  return timestamp <= Date.now();
}

function sanitizeDuration(value: unknown) {
  const n = Number(value || 60);
  if (Number.isNaN(n) || n <= 0) return 60;
  return Math.min(Math.max(Math.round(n), 5), 24 * 60);
}

function normalizeLimit(value: unknown, fallback = 50, max = 200) {
  const n = Number(value || fallback);
  if (Number.isNaN(n) || n <= 0) return fallback;
  return Math.min(Math.round(n), max);
}

function slugifyTenantId(value: string) {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_')
    .slice(0, 40);
}

async function writeSupportAudit(params: {
  tenantId: string;
  actor: Pick<CurrentUser, 'userId' | 'email'>;
  action: string;
  reason?: string | null;
  metadata?: Record<string, unknown> | null;
}) {
  const { tenantId, actor, action, reason, metadata } = params;
  const supabase = getServerSupabase();

  const { error } = await supabase
    .from('support_access_audit')
    .insert({
      id: makeId('sa_audit'),
      tenant_id: tenantId,
      actor_user_id: actor.userId || 'unknown',
      actor_email: actor.email || null,
      action,
      reason: reason || null,
      metadata_json: metadata || null,
      created_at: new Date().toISOString()
    });

  if (error) {
    throw Object.assign(new Error(error.message), { status: 500 });
  }
}

async function getLatestSupportGrant(tenantId: string) {
  const supabase = getServerSupabase();
  const { data, error } = await supabase
    .from('support_access_grants')
    .select('*')
    .eq('tenant_id', tenantId)
    .order('created_at', { ascending: false })
    .limit(1);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  return Array.isArray(data) && data.length ? data[0] as Record<string, unknown> : null;
}

async function getActiveSupportGrant(tenantId: string) {
  const supabase = getServerSupabase();
  const { data, error } = await supabase
    .from('support_access_grants')
    .select('*')
    .eq('tenant_id', tenantId)
    .eq('enabled', true)
    .order('created_at', { ascending: false })
    .limit(50);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  const active = (Array.isArray(data) ? data : []).find((row) => !row.revoked_at && !isExpired(row.expires_at));
  return active || null;
}

async function computeSupportStatus(tenantId: string, actor?: Pick<CurrentUser, 'userId' | 'email'>) {
  const supabase = getServerSupabase();
  const latest = await getLatestSupportGrant(tenantId);

  if (!latest) {
    return {
      enabled: false,
      expiresAt: null,
      enabledBy: null,
      reason: null,
      revokedAt: null
    };
  }

  const expired = Boolean(latest.enabled) && !latest.revoked_at && isExpired(latest.expires_at);
  if (expired) {
    const { error: updateError } = await supabase
      .from('support_access_grants')
      .update({ enabled: false })
      .eq('id', String(latest.id || ''))
      .eq('tenant_id', tenantId);

    if (updateError) throw Object.assign(new Error(updateError.message), { status: 500 });

    if (actor) {
      await writeSupportAudit({
        tenantId,
        actor,
        action: 'EXPIRED',
        reason: 'Support access grant expired automatically',
        metadata: { grantId: latest.id || null, expiresAt: latest.expires_at || null }
      });
    }

    return {
      enabled: false,
      expiresAt: latest.expires_at || null,
      enabledBy: latest.enabled_by_user_id || null,
      reason: latest.reason || null,
      revokedAt: latest.revoked_at || null
    };
  }

  return {
    enabled: Boolean(latest.enabled) && !latest.revoked_at && !isExpired(latest.expires_at),
    expiresAt: latest.expires_at || null,
    enabledBy: latest.enabled_by_user_id || null,
    reason: latest.reason || null,
    revokedAt: latest.revoked_at || null
  };
}

async function requireTenantScopedPrivilegedUser(req: RequestLike) {
  const user = await getCurrentUser(req);
  const tenantId = getTenantFromHeader(req);
  requireTenantAccess(user, tenantId);
  requirePrivileged(user);
  return { user, tenantId };
}

export async function getMyProfile(req: RequestLike) {
  const user = await getCurrentUser(req);
  return {
    email: user.email,
    tenant_id: user.tenantId,
    roles: user.roles
  };
}

export async function getCurrentUserPayload(req: RequestLike) {
  const user = await getCurrentUser(req);
  return {
    userId: user.userId,
    email: user.email,
    role: user.role,
    roles: user.roles,
    tenantId: user.tenantId
  };
}

export async function listTenants(req: RequestLike) {
  const user = await getCurrentUser(req);
  const supabase = getServerSupabase();

  if (hasRole(user, 'creator')) {
    const { data, error } = await supabase
      .from('tenants')
      .select('id,name')
      .order('created_at', { ascending: true });

    if (error) throw Object.assign(new Error(error.message), { status: 500 });

    return {
      tenants: (Array.isArray(data) ? data : [])
        .map((row) => ({
          id: String(row.id || ''),
          name: String(row.name || row.id || '')
        }))
        .filter((row) => row.id)
    };
  }

  const { data, error } = await supabase
    .from('tenants')
    .select('id,name')
    .eq('id', user.tenantId)
    .order('created_at', { ascending: true });

  if (error) throw Object.assign(new Error(error.message), { status: 500 });

  return {
    tenants: (Array.isArray(data) ? data : [])
      .map((row) => ({
        id: String(row.id || ''),
        name: String(row.name || row.id || '')
      }))
      .filter((row) => row.id)
  };
}

export async function createTenant(req: RequestLike) {
  const user = await getCurrentUser(req);
  requireCreator(user);

  const body = readJsonBody<{ id?: string; name?: string }>(req);
  const name = String(body.name || '').trim();
  if (!name) throw badRequest('name is required');

  const requestedId = String(body.id || '').trim().toLowerCase();
  const tenantId = requestedId ? slugifyTenantId(requestedId) : slugifyTenantId(name);

  if (!TENANT_ID_PATTERN.test(tenantId)) {
    throw badRequest('invalid tenant id');
  }

  const supabase = getServerSupabase();
  const { error } = await supabase
    .from('tenants')
    .insert({ id: tenantId, name })
    .select('id,name')
    .single();

  if (error) {
    if (error.code === '23505') throw conflict('Tenant already exists');
    throw Object.assign(new Error(error.message), { status: 500 });
  }

  return {
    tenant: {
      id: tenantId,
      name
    }
  };
}

export async function getTenant(req: RequestLike) {
  const user = await getCurrentUser(req);
  const tenantId = getTenantFromHeader(req);
  requireTenantAccess(user, tenantId);

  const supabase = getServerSupabase();
  const { data, error } = await supabase
    .from('tenants')
    .select('id,name,regions,plan,plan_limits,limits,created_at')
    .eq('id', tenantId)
    .limit(1);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  const tenant = Array.isArray(data) && data.length ? data[0] as Record<string, unknown> : null;

  return {
    tenantId,
    name: tenant?.name || null,
    regions: Array.isArray(tenant?.regions) ? tenant.regions : [],
    planTier: tenant?.plan || null,
    planLimits: tenant?.plan_limits || tenant?.limits || null,
    createdAt: tenant?.created_at || null
  };
}

export async function listTenantUsers(req: RequestLike) {
  const { tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();

  const { data: users, error: usersError } = await supabase
    .from('users')
    .select('id,email,role,tenant_id,created_at')
    .eq('tenant_id', tenantId)
    .order('created_at', { ascending: false })
    .limit(200);

  if (usersError) throw Object.assign(new Error(usersError.message), { status: 500 });

  const userRows = Array.isArray(users) ? users : [];
  const userIds = userRows.map((row) => String(row.id || '')).filter(Boolean);

  let rolesByUserId = new Map<string, string[]>();
  if (userIds.length) {
    const { data: roleRows, error: rolesError } = await supabase
      .from('user_roles')
      .select('user_id,role')
      .in('user_id', userIds);

    if (rolesError) throw Object.assign(new Error(rolesError.message), { status: 500 });

    rolesByUserId = (Array.isArray(roleRows) ? roleRows : []).reduce((map, row) => {
      const userId = String(row.user_id || '');
      if (!userId) return map;
      const role = String(row.role || '').trim().toLowerCase();
      if (!role) return map;
      const next = map.get(userId) || [];
      if (!next.includes(role)) next.push(role);
      map.set(userId, next);
      return map;
    }, new Map<string, string[]>());
  }

  return userRows.map((tenantUser) => {
    const userId = String(tenantUser.id || '');
    const roles = rolesByUserId.get(userId) || [];
    const fallback = String(tenantUser.role || '').trim().toLowerCase();
    const effectiveRoles = roles.length ? roles : (fallback ? [fallback] : ['user']);

    return {
      userId,
      email: String(tenantUser.email || ''),
      roles: effectiveRoles
    };
  });
}

export async function getConnectorStatus(req: RequestLike) {
  const { tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();

  const [configResult, runsResult] = await Promise.all([
    supabase
      .from('ConnectorConfig')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('updated_at', { ascending: false })
      .limit(200),
    supabase
      .from('ConnectorRuns')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('started_at', { ascending: false })
      .limit(300)
  ]);

  if (configResult.error) throw Object.assign(new Error(configResult.error.message), { status: 500 });
  if (runsResult.error) throw Object.assign(new Error(runsResult.error.message), { status: 500 });

  const configs = Array.isArray(configResult.data) ? configResult.data : [];
  const runs = Array.isArray(runsResult.data) ? runsResult.data : [];

  const keys = new Set<string>();

  for (const cfg of configs) {
    const key = String(cfg.connector_key || cfg.connector_id || cfg.source || '').trim();
    if (key) keys.add(key);
  }

  for (const run of runs) {
    const key = String(run.connector_key || run.source || '').trim();
    if (key) keys.add(key);
  }

  return Array.from(keys)
    .map((key) => {
      const display = toDisplay(key);
      const cfg = configs.find((row) => String(row.connector_key || row.connector_id || row.source || '').trim() === key);
      const keyRuns = runs.filter((row) => String(row.connector_key || row.source || '').trim() === key);
      const latestRun = keyRuns[0];
      const latestSuccess = keyRuns.find((row) => String(row.status || '').toLowerCase() === 'success');

      const enabled = cfg?.enabled !== false;
      const status = !enabled
        ? 'disabled'
        : String(latestRun?.status || cfg?.status || 'no_data').toLowerCase();

      const metadata = (latestRun?.metadata && typeof latestRun.metadata === 'object') ? latestRun.metadata : null;

      return {
        key: display.key,
        displayName: display.displayName,
        status,
        lastRunAt: latestRun?.started_at || cfg?.updated_at || null,
        lastSuccessAt: latestSuccess?.finished_at || latestSuccess?.started_at || null,
        lastError: latestRun?.error_summary || metadata?.error || null,
        schedule: cfg?.schedule || cfg?.fetch_interval || null
      };
    })
    .sort((a, b) => a.key.localeCompare(b.key));
}

export async function getCapabilities(req: RequestLike) {
  const user = await getCurrentUser(req);
  const tenantId = getTenantFromHeader(req);
  requireTenantAccess(user, tenantId);

  return {
    isAdmin: isPrivileged(user),
    tenantId
  };
}

export async function enableSupportAccess(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();
  const body = readJsonBody<{ durationMinutes?: number; duration_minutes?: number; reason?: string }>(req);

  const reason = String(body.reason || '').trim();
  if (!reason) throw badRequest('reason is required');

  const durationMinutes = sanitizeDuration(body.durationMinutes || body.duration_minutes);
  const expiresAt = new Date(Date.now() + durationMinutes * 60 * 1000).toISOString();

  const active = await getActiveSupportGrant(tenantId);

  if (active) {
    const { error: updateError } = await supabase
      .from('support_access_grants')
      .update({
        enabled: true,
        expires_at: expiresAt,
        enabled_by_user_id: user.userId || null,
        reason,
        revoked_at: null,
        revoked_by_user_id: null,
        revoke_reason: null
      })
      .eq('id', String(active.id || ''))
      .eq('tenant_id', tenantId);

    if (updateError) throw Object.assign(new Error(updateError.message), { status: 500 });
  } else {
    const { error: insertError } = await supabase
      .from('support_access_grants')
      .insert({
        id: makeId('sa_grant'),
        tenant_id: tenantId,
        enabled: true,
        expires_at: expiresAt,
        enabled_by_user_id: user.userId || null,
        reason,
        created_at: new Date().toISOString(),
        revoked_at: null,
        revoked_by_user_id: null,
        revoke_reason: null
      });

    if (insertError) throw Object.assign(new Error(insertError.message), { status: 500 });
  }

  await writeSupportAudit({
    tenantId,
    actor: { userId: user.userId, email: user.email },
    action: 'ENABLED',
    reason,
    metadata: { durationMinutes, expiresAt }
  });

  return computeSupportStatus(tenantId, { userId: user.userId, email: user.email });
}

export async function revokeSupportAccess(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  const supabase = getServerSupabase();
  const body = readJsonBody<{ reason?: string }>(req);

  const reason = String(body.reason || '').trim();
  if (!reason) throw badRequest('reason is required');

  const active = await getActiveSupportGrant(tenantId);
  if (active) {
    const { error: updateError } = await supabase
      .from('support_access_grants')
      .update({
        enabled: false,
        revoked_at: new Date().toISOString(),
        revoked_by_user_id: user.userId || null,
        revoke_reason: reason
      })
      .eq('id', String(active.id || ''))
      .eq('tenant_id', tenantId);

    if (updateError) throw Object.assign(new Error(updateError.message), { status: 500 });
  }

  await writeSupportAudit({
    tenantId,
    actor: { userId: user.userId, email: user.email },
    action: 'REVOKED',
    reason,
    metadata: { hadActiveGrant: Boolean(active), grantId: active?.id || null }
  });

  return computeSupportStatus(tenantId, { userId: user.userId, email: user.email });
}

export async function getSupportAccessStatus(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  return computeSupportStatus(tenantId, { userId: user.userId, email: user.email });
}

export async function listSupportAccessAudit(req: RequestLike) {
  const { tenantId } = await requireTenantScopedPrivilegedUser(req);
  const body = readJsonBody<{ limit?: number }>(req);
  const limit = normalizeLimit(body.limit, 50, 200);
  const supabase = getServerSupabase();

  const { data, error } = await supabase
    .from('support_access_audit')
    .select('*')
    .eq('tenant_id', tenantId)
    .order('created_at', { ascending: false })
    .limit(limit);

  if (error) throw Object.assign(new Error(error.message), { status: 500 });
  return Array.isArray(data) ? data : [];
}

export async function dispatchFunction(functionName: string, req: RequestLike) {
  switch (functionName) {
    case 'getMyProfile':
      return getMyProfile(req);
    case 'getCurrentUser':
      return getCurrentUserPayload(req);
    case 'listTenants':
      return listTenants(req);
    case 'createTenant':
      return createTenant(req);
    case 'getTenant':
      return getTenant(req);
    case 'listTenantUsers':
      return listTenantUsers(req);
    case 'getConnectorStatus':
      return getConnectorStatus(req);
    case 'getCapabilities':
      return getCapabilities(req);
    case 'enableSupportAccess':
      return enableSupportAccess(req);
    case 'revokeSupportAccess':
      return revokeSupportAccess(req);
    case 'getSupportAccessStatus':
      return getSupportAccessStatus(req);
    case 'listSupportAccessAudit':
      return listSupportAccessAudit(req);
    default:
      throw Object.assign(new Error(`Function not found: ${functionName}`), { status: 404 });
  }
}
