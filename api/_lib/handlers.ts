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
import type { ApiDatabase, Json } from './db.types.js';

const CONNECTOR_MAP: Record<string, { key: string; displayName: string }> = {
  BOAMP_FR: { key: 'BOAMP_FR', displayName: 'BOAMP France' },
  TED: { key: 'TED', displayName: 'TED (EU)' },
  TED_IE: { key: 'TED_IE', displayName: 'TED Ireland' },
  TED_FR: { key: 'TED_FR', displayName: 'TED France' },
  ETENDERS_IE: { key: 'ETENDERS_IE', displayName: 'eTenders Ireland' }
};

const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

type SupportAccessGrantRow = ApiDatabase['public']['Tables']['support_access_grants']['Row'];
type SupportAccessAuditRow = ApiDatabase['public']['Tables']['support_access_audit']['Row'];
type TenantRow = ApiDatabase['public']['Tables']['tenants']['Row'];
type UserRow = ApiDatabase['public']['Tables']['users']['Row'];
type UserRoleRow = ApiDatabase['public']['Tables']['user_roles']['Row'];
type ConnectorConfigRow = ApiDatabase['public']['Tables']['ConnectorConfig']['Row'];
type ConnectorRunRow = ApiDatabase['public']['Tables']['ConnectorRuns']['Row'];

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
  metadata?: Json | null;
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
      metadata_json: metadata ?? null,
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
  return Array.isArray(data) && data.length ? (data[0] as SupportAccessGrantRow) : null;
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
  const grantRows = (Array.isArray(data) ? data : []) as SupportAccessGrantRow[];
  const active = grantRows.find((row: SupportAccessGrantRow) => !row.revoked_at && !isExpired(row.expires_at));
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

async function requireTenantScopedAdmin(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedPrivilegedUser(req);
  if (!hasRole(user, 'admin')) {
    throw Object.assign(new Error('Admin role required'), { status: 403 });
  }
  return { user, tenantId };
}

function toNumber(value: unknown) {
  const n = Number(value || 0);
  if (Number.isNaN(n) || !Number.isFinite(n)) return 0;
  return Math.max(0, Math.round(n));
}

function normalizeMetrics(value: unknown) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {} as Record<string, unknown>;
  }
  return value as Record<string, unknown>;
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
  const tenant = Array.isArray(data) && data.length ? (data[0] as TenantRow) : null;

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

  const userRows = (Array.isArray(users) ? users : []) as UserRow[];
  const userIds = userRows.map((row) => String(row.id || '')).filter(Boolean);

  let rolesByUserId = new Map<string, string[]>();
  if (userIds.length) {
    const { data: roleRows, error: rolesError } = await supabase
      .from('user_roles')
      .select('user_id,role')
      .in('user_id', userIds);

    if (rolesError) throw Object.assign(new Error(rolesError.message), { status: 500 });

    rolesByUserId = ((Array.isArray(roleRows) ? roleRows : []) as UserRoleRow[]).reduce((map, row: UserRoleRow) => {
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

  const configs = (Array.isArray(configResult.data) ? configResult.data : []) as ConnectorConfigRow[];
  const runs = (Array.isArray(runsResult.data) ? runsResult.data : []) as ConnectorRunRow[];

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
      const cfg = configs.find(
        (row: ConnectorConfigRow) => String(row.connector_key || row.connector_id || row.source || '').trim() === key
      );
      const keyRuns = runs.filter((row: ConnectorRunRow) => String(row.connector_key || row.source || '').trim() === key);
      const latestRun = keyRuns[0];
      const latestSuccess = keyRuns.find((row: ConnectorRunRow) => String(row.status || '').toLowerCase() === 'success');

      const enabled = cfg?.enabled !== false;
      const status = !enabled
        ? 'disabled'
        : String(latestRun?.status || cfg?.status || 'no_data').toLowerCase();

      const metadata = (latestRun?.metadata && typeof latestRun.metadata === 'object' && !Array.isArray(latestRun.metadata))
        ? (latestRun.metadata as Record<string, unknown>)
        : null;

      return {
        key: display.key,
        displayName: display.displayName,
        status,
        lastRunAt: latestRun?.started_at || cfg?.updated_at || null,
        lastSuccessAt: latestSuccess?.finished_at || latestSuccess?.started_at || null,
        lastError: latestRun?.error_summary || String(metadata?.error || '') || null,
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
  return (Array.isArray(data) ? data : []) as SupportAccessAuditRow[];
}

export async function getPipelineAdmin(req: RequestLike) {
  const { user, tenantId } = await requireTenantScopedAdmin(req);
  const body = readJsonBody<{
    action?: string;
    queue_id?: string;
    decision?: string;
    review_notes?: string;
    prediction_id?: string;
  }>(req);

  const action = String(body.action || 'overview').trim().toLowerCase();
  const supabase = getServerSupabase() as any;

  if (action === 'review_decision') {
    const queueId = String(body.queue_id || '').trim();
    const decision = String(body.decision || '').trim().toLowerCase();
    if (!queueId || !['approve', 'reject'].includes(decision)) {
      throw badRequest('queue_id and decision=approve|reject are required');
    }

    const { data, error } = await supabase
      .from('reconciliation_queue')
      .update({
        status: decision === 'approve' ? 'approved' : 'rejected',
        reviewed_by: user.email || user.userId || 'admin',
        reviewed_at: new Date().toISOString(),
        review_notes: String(body.review_notes || '')
      })
      .eq('id', queueId)
      .eq('tenant_id', tenantId)
      .select('*')
      .limit(1);

    if (error) throw Object.assign(new Error(error.message), { status: 500 });
    const row = Array.isArray(data) && data.length ? data[0] : null;
    if (!row) throw Object.assign(new Error('Queue item not found'), { status: 404 });
    return { success: true, item: row };
  }

  if (action === 'prediction_detail') {
    const predictionId = String(body.prediction_id || '').trim();
    if (!predictionId) throw badRequest('prediction_id is required');

    const { data, error } = await supabase
      .from('predictions')
      .select('*')
      .eq('id', predictionId)
      .eq('tenant_id', tenantId)
      .limit(1);

    if (error) throw Object.assign(new Error(error.message), { status: 500 });
    const row = Array.isArray(data) && data.length ? data[0] : null;
    if (!row) throw Object.assign(new Error('Prediction not found'), { status: 404 });
    return { success: true, prediction: row };
  }

  const [runsResult, queueResult, predictionsResult, rawCountResult] = await Promise.all([
    supabase
      .from('ingestion_runs')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('started_at', { ascending: false })
      .limit(100),
    supabase
      .from('reconciliation_queue')
      .select('*')
      .eq('tenant_id', tenantId)
      .eq('status', 'pending')
      .order('created_at', { ascending: false })
      .limit(200),
    supabase
      .from('predictions')
      .select('*')
      .eq('tenant_id', tenantId)
      .order('generated_at', { ascending: false })
      .limit(200),
    supabase
      .from('raw_documents')
      .select('id', { count: 'exact', head: true })
      .eq('tenant_id', tenantId)
      .eq('source', 'ETENDERS_IE')
  ]);

  if (runsResult.error) throw Object.assign(new Error(runsResult.error.message), { status: 500 });
  if (queueResult.error) throw Object.assign(new Error(queueResult.error.message), { status: 500 });
  if (predictionsResult.error) throw Object.assign(new Error(predictionsResult.error.message), { status: 500 });

  const runRows = Array.isArray(runsResult.data) ? runsResult.data : [];
  const queueRows = Array.isArray(queueResult.data) ? queueResult.data : [];
  const predictionRows = Array.isArray(predictionsResult.data) ? predictionsResult.data : [];

  const runs = runRows.map((run) => {
    const metrics = normalizeMetrics(run.metrics);
    const dedupedInFile = toNumber(metrics.deduped_in_file ?? metrics.duplicates_in_file);
    const canonicalDuplicates = toNumber(metrics.canonical_duplicates ?? metrics.duplicates);
    const currentDuplicates = toNumber(metrics.current_duplicates);
    const inferredIdRows = toNumber(metrics.inferred_id_rows);
    const rawRows = toNumber(metrics.raw_rows);
    const processedRows = toNumber(metrics.processed_rows ?? metrics.processed ?? metrics.total_rows);
    const totalDuplicates = Math.max(
      toNumber(metrics.duplicates_total),
      dedupedInFile + Math.max(canonicalDuplicates, currentDuplicates)
    );

    return {
      ...run,
      id: run.id || run.run_id,
      metrics,
      duplicate_stats: {
        deduped_in_file: dedupedInFile,
        canonical_duplicates: canonicalDuplicates,
        current_duplicates: currentDuplicates,
        inferred_id_rows: inferredIdRows,
        raw_rows: rawRows,
        processed_rows: processedRows,
        total_duplicates: totalDuplicates
      }
    };
  });

  const duplicateSummary = runs.reduce(
    (acc, run) => {
      const stats = normalizeMetrics(run.duplicate_stats);
      acc.total_duplicates += toNumber(stats.total_duplicates);
      acc.deduped_in_file += toNumber(stats.deduped_in_file);
      acc.canonical_duplicates += toNumber(stats.canonical_duplicates);
      acc.current_duplicates += toNumber(stats.current_duplicates);
      acc.inferred_id_rows += toNumber(stats.inferred_id_rows);
      acc.processed_rows += toNumber(stats.processed_rows);
      acc.raw_rows += toNumber(stats.raw_rows);
      return acc;
    },
    {
      total_duplicates: 0,
      deduped_in_file: 0,
      canonical_duplicates: 0,
      current_duplicates: 0,
      inferred_id_rows: 0,
      processed_rows: 0,
      raw_rows: 0
    }
  );

  const rawDocumentsLogged = Number(rawCountResult.count || 0);

  return {
    success: true,
    runs,
    queue: queueRows,
    predictions: predictionRows,
    duplicateSummary: {
      ...duplicateSummary,
      run_count: runs.length,
      raw_documents_logged: rawDocumentsLogged
    }
  };
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
    case 'getPipelineAdmin':
      return getPipelineAdmin(req);
    default:
      throw Object.assign(new Error(`Function not found: ${functionName}`), { status: 404 });
  }
}
