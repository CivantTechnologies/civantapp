import { createClientFromRequest } from './civantSdk.ts';
import { resolveTenantId } from './requireAdmin.ts';

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

function makeToken() {
  return crypto.randomUUID().replace(/-/g, '') + crypto.randomUUID().replace(/-/g, '');
}

function sanitizeEmail(value: unknown) {
  return String(value || '').trim().toLowerCase();
}

function getExpiryIso() {
  const ttlHours = Number(Deno.env.get('SESSION_TTL_HOURS') || 24);
  const safeTtlHours = Number.isFinite(ttlHours) && ttlHours > 0 ? ttlHours : 24;
  return new Date(Date.now() + safeTtlHours * 60 * 60 * 1000).toISOString();
}

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const body = await req.json().catch(() => ({}));

    const email = sanitizeEmail(body.email);
    if (!email) {
      return Response.json({ error: 'email is required' }, { status: 400 });
    }

    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    const existing = await civant.asServiceRole.entities.users.filter({ email }, '-created_at', 1);

    const user = Array.isArray(existing) && existing.length > 0
      ? existing[0] as Record<string, unknown>
      : await civant.asServiceRole.entities.users.create({
          id: makeId('usr'),
          email,
          tenant_id: tenantId,
          role: 'user',
          created_at: new Date().toISOString()
        }) as Record<string, unknown>;

    const token = makeToken();
    const expiresAt = getExpiryIso();

    await civant.asServiceRole.entities.sessions.create({
      id: makeId('sess'),
      user_id: user.id,
      token,
      expires_at: expiresAt,
      created_at: new Date().toISOString()
    });

    return Response.json({
      token,
      user: {
        userId: String(user.id || ''),
        email: String(user.email || ''),
        role: String(user.role || 'user'),
        tenantId: resolveTenantId(user.tenant_id),
        createdAt: user.created_at || null
      }
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to create session' }, { status });
  }
});
