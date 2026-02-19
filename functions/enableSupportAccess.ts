import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant } from './requireAdmin.ts';
import { getTenantFromHeader } from './getTenantFromHeader.ts';
import { computeSupportStatus, getActiveSupportGrant, writeSupportAudit } from './supportAccessAllowed.ts';

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

function sanitizeDuration(value: unknown) {
  const n = Number(value || 60);
  if (Number.isNaN(n) || n <= 0) return 60;
  return Math.min(Math.max(Math.round(n), 5), 24 * 60);
}

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const tenantId = getTenantFromHeader(req);

    const body = await req.json().catch(() => ({}));
    const user = await requireAdminForTenant({ civant, req, tenantId });

    const reason = String(body.reason || '').trim();
    if (!reason) {
      return Response.json({ error: 'reason is required' }, { status: 400 });
    }
    const supportUserId = String(body.support_user_id || body.supportUserId || '').trim();
    if (!supportUserId) {
      return Response.json({ error: 'support_user_id is required' }, { status: 400 });
    }

    const durationMinutes = sanitizeDuration(body.durationMinutes || body.duration_minutes);
    const expiresAt = new Date(Date.now() + durationMinutes * 60 * 1000).toISOString();

    const active = await getActiveSupportGrant(civant, tenantId, supportUserId);

    if (active) {
      await civant.asServiceRole.entities.support_access_grants.update(String(active.id), {
        enabled: true,
        support_user_id: supportUserId,
        expires_at: expiresAt,
        enabled_by_user_id: user.userId || null,
        reason,
        revoked_at: null,
        revoked_by_user_id: null,
        revoke_reason: null
      });
    } else {
      await civant.asServiceRole.entities.support_access_grants.create({
        id: makeId('sa_grant'),
        tenant_id: tenantId,
        support_user_id: supportUserId,
        enabled: true,
        expires_at: expiresAt,
        enabled_by_user_id: user.userId || null,
        reason,
        created_at: new Date().toISOString(),
        revoked_at: null,
        revoked_by_user_id: null,
        revoke_reason: null
      });
    }

    await writeSupportAudit({
      civant,
      tenantId,
      actor: { id: user.userId, email: user.email },
      action: 'ENABLED',
      reason,
      metadata: { durationMinutes, expiresAt, supportUserId }
    });

    const status = await computeSupportStatus({ civant, tenantId, actor: { id: user.userId, email: user.email } });
    return Response.json(status);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to enable support access' }, { status });
  }
});
