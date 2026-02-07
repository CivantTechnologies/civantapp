import { createClientFromRequest } from './civantSdk.ts';
import { requireAdminForTenant, requireAuthenticatedUser, resolveTenantId } from './requireAdmin.ts';
import { computeSupportStatus, getActiveSupportGrant, writeSupportAudit } from './supportAccessAllowed.ts';

Deno.serve(async (req) => {
  try {
    const civant = createClientFromRequest(req);
    const user = await requireAuthenticatedUser(civant);

    const body = await req.json().catch(() => ({}));
    const tenantId = resolveTenantId(body.tenantId || body.tenant_id || req.headers.get('X-Tenant-Id'));

    await requireAdminForTenant({ civant, user, tenantId });

    const reason = String(body.reason || '').trim();
    if (!reason) {
      return Response.json({ error: 'reason is required' }, { status: 400 });
    }

    const active = await getActiveSupportGrant(civant, tenantId);
    if (active) {
      await civant.asServiceRole.entities.support_access_grants.update(String(active.id), {
        enabled: false,
        revoked_at: new Date().toISOString(),
        revoked_by_user_id: user.id || null,
        revoke_reason: reason
      });
    }

    await writeSupportAudit({
      civant,
      tenantId,
      actor: user,
      action: 'REVOKED',
      reason,
      metadata: { hadActiveGrant: Boolean(active), grantId: active?.id || null }
    });

    const status = await computeSupportStatus({ civant, tenantId, actor: user });
    return Response.json(status);
  } catch (error) {
    const status = (error as Error & { status?: number }).status || 500;
    return Response.json({ error: error instanceof Error ? error.message : 'Failed to revoke support access' }, { status });
  }
});
