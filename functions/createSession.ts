import { createClientFromRequest } from './civantSdk.ts';
import { getCurrentUserFromRequest, resolveTenantId } from './requireAdmin.ts';

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

const MIN_SESSION_MINT_INTERVAL_MS = 15_000;
const lastMintByUserId = new Map<string, number>();

function isCreateSessionEnabled() {
  const raw = String(Deno.env.get('ENABLE_CREATE_SESSION_DEV_ONLY') || '').trim().toLowerCase();
  return raw === 'true';
}

Deno.serve(async (req) => {
  try {
    if (!isCreateSessionEnabled()) {
      return Response.json({ error: 'Not found' }, { status: 404 });
    }

    const civant = createClientFromRequest(req);
    const user = await getCurrentUserFromRequest(civant, req);
    const body = await req.json().catch(() => ({}));

    const requestedEmail = sanitizeEmail(body.email);
    if (requestedEmail && requestedEmail !== user.email) {
      return Response.json({ error: 'Cannot create session for another user' }, { status: 403 });
    }

    const lastMintAt = lastMintByUserId.get(user.userId) || 0;
    if (Date.now() - lastMintAt < MIN_SESSION_MINT_INTERVAL_MS) {
      return Response.json({ error: 'Too many session requests' }, { status: 429 });
    }
    lastMintByUserId.set(user.userId, Date.now());

    const tenantId = resolveTenantId(user.tenantId);

    const token = makeToken();
    const expiresAt = getExpiryIso();

    await civant.asServiceRole.entities.sessions.create({
      id: makeId('sess'),
      user_id: user.userId,
      token,
      expires_at: expiresAt,
      created_at: new Date().toISOString()
    });

    return Response.json({
      token,
      user: {
        userId: user.userId,
        email: user.email,
        role: user.role,
        tenantId,
        createdAt: null
      }
    });
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to create session' }, { status });
  }
});
