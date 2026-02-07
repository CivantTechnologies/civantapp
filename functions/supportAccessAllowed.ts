import { createClientFromRequest } from './civantSdk.ts';

type CivantClient = ReturnType<typeof createClientFromRequest>;

type SupportGrant = Record<string, unknown>;
type RequestUser = { id?: string; email?: string };

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replace(/-/g, '').slice(0, 20)}`;
}

function isExpired(expiresAt: unknown) {
  if (!expiresAt) return false;
  const timestamp = new Date(String(expiresAt)).getTime();
  if (Number.isNaN(timestamp)) return false;
  return timestamp <= Date.now();
}

export async function writeSupportAudit(params: {
  civant: CivantClient;
  tenantId: string;
  actor: RequestUser;
  action: string;
  reason?: string | null;
  metadata?: Record<string, unknown> | null;
}) {
  const { civant, tenantId, actor, action, reason, metadata } = params;
  await civant.asServiceRole.entities.support_access_audit.create({
    id: makeId('sa_audit'),
    tenant_id: tenantId,
    actor_user_id: actor.id || 'unknown',
    actor_email: actor.email || null,
    action,
    reason: reason || null,
    metadata_json: metadata || null,
    created_at: new Date().toISOString()
  });
}

export async function getLatestSupportGrant(civant: CivantClient, tenantId: string): Promise<SupportGrant | null> {
  const rows = await civant.asServiceRole.entities.support_access_grants.filter({
    tenant_id: tenantId
  }, '-created_at', 1);
  if (!Array.isArray(rows) || rows.length === 0) return null;
  return rows[0] as SupportGrant;
}

export async function getActiveSupportGrant(civant: CivantClient, tenantId: string): Promise<SupportGrant | null> {
  const rows = await civant.asServiceRole.entities.support_access_grants.filter({
    tenant_id: tenantId,
    enabled: true
  }, '-created_at', 50);

  if (!Array.isArray(rows) || rows.length === 0) return null;
  const active = rows.find((row: SupportGrant) => !row.revoked_at && !isExpired(row.expires_at));
  return (active || null) as SupportGrant | null;
}

export async function supportAccessAllowed(civant: CivantClient, tenantId: string) {
  const active = await getActiveSupportGrant(civant, tenantId);
  return Boolean(active);
}

export async function computeSupportStatus(params: {
  civant: CivantClient;
  tenantId: string;
  actor?: RequestUser;
}) {
  const { civant, tenantId, actor } = params;
  const latest = await getLatestSupportGrant(civant, tenantId);

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
    await civant.asServiceRole.entities.support_access_grants.update(String(latest.id), {
      enabled: false
    });

    if (actor) {
      await writeSupportAudit({
        civant,
        tenantId,
        actor,
        action: 'EXPIRED',
        reason: 'Support access grant expired automatically',
        metadata: { grantId: latest.id, expiresAt: latest.expires_at }
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
