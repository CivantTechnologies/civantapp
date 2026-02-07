import { createClientFromRequest } from './civantSdk.ts';

type CivantClient = ReturnType<typeof createClientFromRequest>;

type RequestUser = {
  id?: string;
  email?: string;
  role?: string;
};

export function resolveTenantId(requestedTenantId?: unknown) {
  const normalized = String(requestedTenantId || '').trim();
  return normalized || Deno.env.get('DEFAULT_TENANT_ID') || 'civant_default';
}

export async function requireAuthenticatedUser(civant: CivantClient): Promise<RequestUser> {
  const user = await civant.auth.me();
  if (!user) {
    const err = new Error('Unauthorized');
    (err as Error & { status?: number }).status = 401;
    throw err;
  }
  return user as RequestUser;
}

function isAdminEmail(email?: string) {
  if (!email) return false;
  const configured = String(Deno.env.get('CIVANT_ADMIN_EMAIL') || '')
    .split(',')
    .map((x) => x.trim().toLowerCase())
    .filter(Boolean);
  if (!configured.length) return false;
  return configured.includes(email.toLowerCase());
}

export async function checkIsAdminForTenant(params: {
  civant: CivantClient;
  user: RequestUser;
  tenantId: string;
}) {
  const { civant, user, tenantId } = params;

  if (isAdminEmail(user.email)) {
    return true;
  }

  if (user.role === 'admin') {
    return true;
  }

  const tenantUsers = await civant.asServiceRole.entities.users.filter({
    tenant_id: tenantId,
    email: user.email || ''
  }, '-created_at', 20);

  if (!Array.isArray(tenantUsers) || !tenantUsers.length) {
    return false;
  }

  const roleChecks = await Promise.all(
    tenantUsers.map((tenantUser: Record<string, unknown>) =>
      civant.asServiceRole.entities.user_roles.filter({
        user_id: tenantUser.id,
        role: 'admin'
      }, '-role', 1)
    )
  );

  return roleChecks.some((rows: unknown) => Array.isArray(rows) && rows.length > 0);
}

export async function requireAdminForTenant(params: {
  civant: CivantClient;
  user: RequestUser;
  tenantId: string;
}) {
  const allowed = await checkIsAdminForTenant(params);
  if (!allowed) {
    const err = new Error('Forbidden');
    (err as Error & { status?: number }).status = 403;
    throw err;
  }
  return true;
}
