export const TENANT_ID_PATTERN = /^[a-z0-9_]{3,40}$/;

export function normalizeTenantId(value) {
  const tenantId = String(value || '').trim().toLowerCase();
  if (!tenantId || !TENANT_ID_PATTERN.test(tenantId)) return '';
  return tenantId;
}

export function normalizeSupportUserId(value) {
  return String(value || '').trim();
}

export function requiresSupportGrant(userTenantId, requestedTenantId) {
  const currentTenant = normalizeTenantId(userTenantId);
  const requested = normalizeTenantId(requestedTenantId);
  if (!requested) return false;
  if (!currentTenant) return true;
  return currentTenant !== requested;
}

export function isSupportGrantActive(grant) {
  if (!grant || typeof grant !== 'object') return false;
  if (grant.enabled !== true) return false;
  if (grant.revoked_at) return false;

  const expiresAt = String(grant.expires_at || '').trim();
  if (!expiresAt) return true;

  const expiresMs = Date.parse(expiresAt);
  if (!Number.isFinite(expiresMs)) return true;
  return expiresMs > Date.now();
}
