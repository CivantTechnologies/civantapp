const temporaryScopeFilterDisableByTenant = new Map();

function normalizeTenantId(tenantId) {
  return String(tenantId || '').trim().toLowerCase();
}

export function isCompanyScopeFilterTemporarilyDisabled(tenantId) {
  const key = normalizeTenantId(tenantId);
  if (!key) return false;
  return temporaryScopeFilterDisableByTenant.get(key) === true;
}

export function setCompanyScopeFilterTemporarilyDisabled(tenantId, disabled) {
  const key = normalizeTenantId(tenantId);
  if (!key) return;
  if (disabled) {
    temporaryScopeFilterDisableByTenant.set(key, true);
    return;
  }
  temporaryScopeFilterDisableByTenant.delete(key);
}
